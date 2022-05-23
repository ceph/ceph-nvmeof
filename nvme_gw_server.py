#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import ctypes
import ctypes.util
import os
import shlex
import sys
import signal
import subprocess
import grpc
from concurrent import futures
import nvme_gw_pb2_grpc as pb2_grpc
import nvme_gw_pb2 as pb2
import nvme_gw_config
from nvme_gw_persistence import OmapPersistentConfig
import argparse
import json
from google.protobuf import json_format

libc = ctypes.CDLL(ctypes.util.find_library("c"))
PR_SET_PDEATHSIG = 1


def set_pdeathsig(sig=signal.SIGTERM):

    def callable():
        return libc.prctl(PR_SET_PDEATHSIG, sig)

    return callable


class GWService(pb2_grpc.NVMEGatewayServicer):
    """Implements gateway service interface.

    Handles configuration of the SPDK NVMEoF target according to client requests.

    Instance attributes:
        nvme_config: Basic gateway parameters
        logger: Logger instance to track server events
        server: gRPC server instance to receive gateway client requests
        persistent_config: Methods for target configuration persistence
        spdk_rpc: Module methods for SPDK
        spdk_rpc_client: Client of SPDK RPC server
        spdk_rpc_ping_client: Ping client of SPDK RPC server
        spdk_process: Subprocess running SPDK NVMEoF target application
    """

    def __init__(self, nvme_config):

        self.logger = nvme_config.logger
        self.nvme_config = nvme_config
        self.persistent_config = OmapPersistentConfig(nvme_config)
        self.spdk_process = None
        self.server = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleans up SPDK and server instances."""

        if self.spdk_process is not None:
            self.logger.info("Terminating SPDK...")
            self.spdk_process.terminate()
            try:
                timeout = self.nvme_config.getfloat("spdk", "timeout")
                self.spdk_process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.spdk_process.kill()

        if self.server is not None:
            self.logger.info("Stopping the server...")
            self.server.stop(None)

        self.logger.info("Exiting the gateway process.")
        return True

    def terminate(self, msg):
        """Prints error message and calls exit functionality."""

        self.logger.error(msg)
        self.logger.info("Exiting!")
        # Stop server manually - cannot raise exceptions due to gRPC thread
        # hanging during server deallocation
        self.server.stop(None)
        sys.exit(1)

    def serve(self):
        """Starts gateway server."""

        enable_auth = self.nvme_config.getboolean("config", "enable_auth")
        gateway_addr = self.nvme_config.get("config", "gateway_addr")
        gateway_port = self.nvme_config.get("config", "gateway_port")
        grpc_max_workers = self.nvme_config.getint("config",
                                                   "grpc_server_max_workers")

        # Create server and check for existing NVMeoF target configuration
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=grpc_max_workers))
        self.start_spdk()
        self.restore_config()
        pb2_grpc.add_NVMEGatewayServicer_to_server(self, self.server)

        if enable_auth:
            # Read in key and certificates for authentication
            server_key = self.nvme_config.get("mtls", "server_key")
            server_cert = self.nvme_config.get("mtls", "server_cert")
            client_cert = self.nvme_config.get("mtls", "client_cert")
            with open(server_key, "rb") as f:
                private_key = f.read()
            with open(server_cert, "rb") as f:
                server_crt = f.read()
            with open(client_cert, "rb") as f:
                client_crt = f.read()

            # Create appropriate server credentials
            server_credentials = grpc.ssl_server_credentials(
                private_key_certificate_chain_pairs=[(private_key, server_crt)],
                root_certificates=client_crt,
                require_client_auth=True,
            )

            # Add secure port using crendentials
            self.server.add_secure_port(
                "{}:{}".format(gateway_addr, gateway_port), server_credentials)
        else:
            # Authentication is not enabled
            self.server.add_insecure_port("{}:{}".format(
                gateway_addr, gateway_port))

        # Start server
        self.server.start()
        while True:
            timedout = self.server.wait_for_termination(timeout=1)
            if not timedout:
                break
            alive = gw_service.ping()
            if not alive:
                break

    def start_spdk(self):
        """Starts SPDK process."""

        spdk_path = self.nvme_config.get("spdk", "spdk_path")
        sys.path.append(spdk_path)
        self.logger.info(f"SPDK PATH: {spdk_path}")

        import spdk.scripts.rpc as spdk_rpc

        self.spdk_rpc = spdk_rpc
        tgt_path = self.nvme_config.get("spdk", "tgt_path")
        spdk_cmd = os.path.join(spdk_path, tgt_path)
        spdk_rpc_socket = self.nvme_config.get("spdk", "rpc_socket")
        spdk_tgt_cmd_extra_args = self.nvme_config.get("spdk",
                                                       "tgt_cmd_extra_args")

        cmd = [spdk_cmd, "-u", "-r", spdk_rpc_socket]
        if spdk_tgt_cmd_extra_args:
            cmd += shlex.split(spdk_tgt_cmd_extra_args)
        self.logger.info(f"Starting {' '.join(cmd)}")

        try:
            self.spdk_process = subprocess.Popen(cmd,
                                                 stderr=subprocess.PIPE,
                                                 stdout=subprocess.PIPE,
                                                 preexec_fn=set_pdeathsig(
                                                     signal.SIGTERM))

        except Exception as ex:
            self.logger.error(f"Unable to start SPDK: \n {ex}")
            raise

        timeout = self.nvme_config.getfloat("spdk", "timeout")
        log_level = self.nvme_config.get("spdk", "log_level")
        conn_retries = self.nvme_config.getint("spdk", "conn_retries")

        self.logger.info({
            f"Attempting to initialize SPDK: rpc_socket: {spdk_rpc_socket},",
            f" conn_retries: {conn_retries}, timeout: {timeout}",
        })

        try:
            self.spdk_rpc_client = self.spdk_rpc.client.JSONRPCClient(
                spdk_rpc_socket,
                None,
                timeout,
                log_level=log_level,
                conn_retries=conn_retries,
            )
            self.spdk_rpc_ping_client = self.spdk_rpc.client.JSONRPCClient(
                spdk_rpc_socket,
                None,
                timeout,
                log_level=log_level,
                conn_retries=conn_retries,
            )
        except Exception as ex:
            self.logger.error(f"Unable to initialize SPDK: \n {ex}")
            raise
        return

    def restore_config(self):
        callbacks = {
            self.persistent_config.BDEV_PREFIX: self.bdev_rbd_create,
            self.persistent_config.SUBSYSTEM_PREFIX: self.nvmf_create_subsystem,
            self.persistent_config.NAMESPACE_PREFIX: self.nvmf_subsystem_add_ns,
            self.persistent_config.HOST_PREFIX: self.nvmf_subsystem_add_host,
            self.persistent_config.TRANSPORT_PREFIX: self.nvmf_create_transport,
            self.persistent_config.LISTENER_PREFIX: self.nvmf_subsystem_add_listener
        }
        self.persistent_config.restore(callbacks)

    def bdev_rbd_create(self, request, context=None):
        """Creates bdev from a given RBD image."""
        self.logger.info({
            f"Received request to create bdev {request.bdev_name} from",
            f" {request.ceph_pool_name}/{request.rbd_name}",
            f" with block size {request.block_size}",
        })
        try:
            bdev_name = self.spdk_rpc.bdev.bdev_rbd_create(
                self.spdk_rpc_client,
                name=request.bdev_name,
                pool_name=request.ceph_pool_name,
                rbd_name=request.rbd_name,
                block_size=request.block_size,
            )
            self.logger.info(f"Created bdev {bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev create failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.bdev_info()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.add_bdev(bdev_name, json_req)
            except Exception as ex:
                self.terminate(f"Error persisting bdev {bdev_name}: {ex}")

        return pb2.bdev_info(bdev_name=bdev_name)

    def bdev_rbd_delete(self, request, context=None):
        """Deletes bdev."""
        self.logger.info({
            f"Received request to delete bdev: {request.bdev_name}",
        })
        try:
            return_string = self.spdk_rpc.bdev.bdev_rbd_delete(
                self.spdk_rpc_client,
                request.bdev_name,
            )
            self.logger.info(f"Deleted bdev {request.bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev delete failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                self.persistent_config.delete_bdev(request.bdev_name)
            except Exception as ex:
                self.terminate(
                    f"Error persisting {request.bdev_name} delete: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_create_subsystem(self, request, context=None):
        """Creates an NVMe subsystem."""
        self.logger.info({
            f"Received request to create: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_create_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                serial_number=request.serial_number,
                max_namespaces=request.max_namespaces,
            )
            self.logger.info(f"returned with status: {return_string}")
            return_status = return_string != "none"
        except Exception as ex:
            self.logger.error(f"create_subsystem failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.subsystem_info()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.add_subsystem(request.subsystem_nqn,
                                                     json_req)
            except Exception as ex:
                self.terminate(
                    f"Error persisting {request.subsystem_nqn}: {ex}")

        return pb2.subsystem_info(subsystem_nqn=request.subsystem_nqn,
                                  created=return_status)

    def nvmf_delete_subsystem(self, request, context=None):
        """Deletes an NVMe subsystem."""
        self.logger.info({
            f"Received request to delete: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_delete_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
            )
            self.logger.info(f"returned with status: {return_string}")
        except Exception as ex:
            self.logger.error(f"delete_subsystem failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                self.persistent_config.delete_subsystem(request.subsystem_nqn)
            except Exception as ex:
                self.terminate(
                    f"Error persisting {request.subsystem_nqn} delete: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_add_ns(self, request, context=None):
        """Adds a given namespace to a given subsystem."""
        self.logger.info({
            f"Received request to add: {request.bdev_name} to {request.subsystem_nqn}",
        })

        try:
            nsid = self.spdk_rpc.nvmf.nvmf_subsystem_add_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                bdev_name=request.bdev_name,
                nsid=request.nsid,
            )
            self.logger.info(f"returned with nsid: {nsid}")
        except Exception as ex:
            self.logger.error(f"Add NS returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.nsid()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.add_namespace(request.subsystem_nqn,
                                                     str(nsid), json_req)
            except Exception as ex:
                self.terminate(f"Error persisting namespace {nsid}: {ex}")

        return pb2.nsid(nsid=nsid)

    def nvmf_subsystem_remove_ns(self, request, context=None):
        """Removes a given namespace from a given subsystem."""
        self.logger.info({
            f"Received request to remove: {request.nsid} from {request.subsystem_nqn}",
        })

        try:
            status = self.spdk_rpc.nvmf.nvmf_subsystem_remove_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                nsid=request.nsid)
            self.logger.info(f"Returned with status: {status}")
        except Exception as ex:
            self.logger.error(f"Remove namespace returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                self.persistent_config.delete_namespace(request.subsystem_nqn,
                                                        str(request.nsid))
            except Exception as ex:
                self.terminate(
                    f"Error persisting namespace {request.nsid} delete: {ex}")

        return pb2.req_status(status=status)

    def nvmf_subsystem_add_host(self, request, context=None):
        """Grants host access to a given subsystem."""

        try:
            if request.host_nqn == "*":  # Allow any host access to subsystem
                self.logger.info({
                    f"Received request: allow any host to {request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=False,
                )
            else:  # Allow single host access to subsystem
                self.logger.info({
                    f"Received request: add host {request.host_nqn} to {request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
        except Exception as ex:
            self.logger.error(f"Add host access returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.add_host(request.subsystem_nqn,
                                                request.host_nqn,
                                                json_req)
            except Exception as ex:
                self.terminate(f"Error persisting add host: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_remove_host(self, request, context=None):
        """Removes host access from a given subsystem."""

        try:
            if request.host_nqn == "*":  # Disable allow any host access
                self.logger.info({
                    f"Received request: disable any host access to ",
                    f"{request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=True,
                )
            else:  # Remove single host access to subsystem
                self.logger.info({
                    f"Received request: remove host {request.host_nqn} from ",
                    f"{request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_remove_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
        except Exception as ex:
            self.logger.error(
                f"Remove host access returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                self.persistent_config.delete_host(request.subsystem_nqn,
                                                   request.host_nqn)
            except Exception as ex:
                self.terminate(f"Error persisting remove host: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_create_transport(self, request, context=None):
        """Sets a transport type for device access."""
        self.logger.info({f"Setting transport type to: {request.trtype}"})

        # Check if transport type has already been created
        if context:
            trtype = self.persistent_config.get_transport(request.trtype)
            if trtype is not None:
                self.logger.info(
                    f"Create Transport {trtype} already created.\n")
                return pb2.req_status(status=True)

        try:
            status = self.spdk_rpc.nvmf.nvmf_create_transport(
                self.spdk_rpc_client, trtype=request.trtype)
        except Exception as ex:
            self.logger.error(
                f"Create Transport {request.trtype} returned with error: \n {ex}"
            )
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.set_transport(request.trtype,
                                                     json_req)
            except Exception as ex:
                self.terminate(
                    f"Error persisting transport {request.trtype}: {ex}")

        return pb2.req_status(status=status)

    def nvmf_subsystem_add_listener(self, request, context=None):
        """Adds a listener at the given TCP/IP address for the given subsystem."""
        self.logger.info({
            f"Adding listener at {request.traddr} : {request.trsvcid} for {request.nqn}"
        })

        # Create transport if needed
        if context:
            self.nvmf_create_transport(pb2.create_transport_req(trtype='TCP'),
                                       context)

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_listener(
                self.spdk_rpc_client,
                nqn=request.nqn,
                trtype=request.trtype,
                traddr=request.traddr,
                trsvcid=request.trsvcid,
                adrfam=request.adrfam,
            )
            self.logger.info(f"Status of add listener: {return_string}")
        except Exception as ex:
            self.logger.error(f"Add Listener returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.persistent_config.add_listener(request.nqn, request.traddr,
                                                    request.trsvcid,
                                                    json_req)
            except Exception as ex:
                self.terminate(
                    f"Error persisting listener {request.traddr}: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_remove_listener(self, request, context=None):
        """Removes a listener at the given TCP/IP address for the given subsystem."""
        self.logger.info(
            {f"Removing listener at {request.traddr} for {request.nqn}."})

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_remove_listener(
                self.spdk_rpc_client,
                request.nqn,
                request.trtype,
                request.traddr,
                request.trsvcid,
                request.adrfam,
            )
            self.logger.info(f"Status of remove listener: {return_string}")
        except Exception as ex:
            self.logger.error(f"Remove listener returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update persistent configuration
            try:
                self.persistent_config.delete_listener(request.nqn,
                                                       request.traddr,
                                                       request.trsvcid)
            except Exception as ex:
                self.terminate(
                    f"Error persisting listener {request.traddr} delete: {ex}")

        return pb2.req_status(status=return_string)

    def nvmf_get_subsystems(self, request, context):
        """Gets NVMe subsystems."""
        self.logger.info({
            f"Received request to get subsystems",
        })

        try:
            ret = self.spdk_rpc.nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.info(f"returned with: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        return pb2.subsystems_info(subsystems=json.dumps(ret))

    def ping(self):
        """Confirms communication with SPDK process."""
        try:
            ret = self.spdk_rpc.spdk_get_version(self.spdk_rpc_ping_client)
            return True
        except Exception as ex:
            self.logger.error(f"spdk_get_version failed with: \n {ex}")
            return False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="python3 ./nvme_gw_server",
                                     description="Manage NVMe gateways")
    parser.add_argument(
        "-c",
        "--config",
        default="nvme_gw.config",
        type=str,
        help="Path to config file",
    )

    args = parser.parse_args()
    nvme_config = nvme_gw_config.NVMeGWConfig(args.config)
    with GWService(nvme_config) as gw_service:
        gw_service.serve()
