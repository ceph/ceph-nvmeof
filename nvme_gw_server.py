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
import argparse
import json

libc = ctypes.CDLL(ctypes.util.find_library("c"))
PR_SET_PDEATHSIG = 1
def set_pdeathsig(sig = signal.SIGTERM):
    def callable():
        return libc.prctl(PR_SET_PDEATHSIG, sig)
    return callable

class GWService(pb2_grpc.NVMEGatewayServicer):
    def __init__(self, nvme_config):

        self.logger = nvme_config.logger
        self.nvme_config = nvme_config

    def start_spdk(self):

        spdk_path = self.nvme_config.get("config", "spdk_path")
        sys.path.append(spdk_path)
        self.logger.info(f"SPDK PATH: {spdk_path}")

        import spdk.scripts.rpc as spdk_rpc

        self.spdk_rpc = spdk_rpc
        spdk_tgt = self.nvme_config.get("config", "spdk_tgt")
        spdk_cmd = os.path.join(spdk_path, spdk_tgt)
        spdk_rpc_socket = self.nvme_config.get("spdk", "rpc_socket")
        spdk_tgt_cmd_args = self.nvme_config.get("spdk", "tgt_cmd_args")

        cmd = [spdk_cmd, "-u", "-r", spdk_rpc_socket]
        if spdk_tgt_cmd_args:
            cmd += shlex.split(spdk_tgt_cmd_args)
        self.logger.info(f"Starting {' '.join(cmd)}")

        try:
            subprocess.Popen(cmd,
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             preexec_fn = set_pdeathsig(signal.SIGTERM))

        except Exception as ex:
            self.logger.error(f"Unable to start SPDK: \n {ex}")
            raise

        timeout = self.nvme_config.getfloat("spdk", "timeout")
        log_level = self.nvme_config.get("spdk", "log_level")
        conn_retries = self.nvme_config.getint("spdk", "conn_retries")

        self.logger.info(
            f"Attempting to initialize SPDK: rpc_socket: {spdk_rpc_socket}, conn_retries: {conn_retries}, timeout: {timeout}"
        )

        try:
            self.client = self.spdk_rpc.client.JSONRPCClient(
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

    def bdev_rbd_create(self, request, context):
        # Create bdev from a given RBD image
        self.logger.info({
            f"Received: {request.ceph_pool_name}, {request.rbd_name}, {request.block_size}",
        })
        try:
            bdev_name = self.spdk_rpc.bdev.bdev_rbd_create(
                self.client,
                request.ceph_pool_name,
                request.rbd_name,
                request.block_size,
            )
            self.logger.info(f"Created bdev {bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev create failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.bdev_info()

        return pb2.bdev_info(bdev_name=bdev_name)

    def bdev_rbd_delete(self, request, context):
        # Delete bdev
        self.logger.info({
            f"Received request to delete bdev: {request.bdev_name}",
        })
        try:
            return_string = self.spdk_rpc.bdev.bdev_rbd_delete(
                self.client,
                request.bdev_name,
            )
            self.logger.info(f"Deleted bdev {request.bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev delete failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_create_subsystem(self, request, context):
        # Create an NVMe Subsystem
        self.logger.info({
            f"Received request to create: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_create_subsystem(
                self.client,
                nqn=request.subsystem_nqn,
                serial_number=request.serial_number,
                max_namespaces=request.max_namespaces,
            )
            self.logger.info(f"returned with status: {return_string}")
            return_status = return_string != "none"
        except Exception as ex:
            self.logger.error(f"create_subsystem failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystem_info()

        return pb2.subsystem_info(subsystem_nqn=request.subsystem_nqn,
                                  created=return_status)

    def nvmf_delete_subsystem(self, request, context):
        # Delete an NVMe Subsystem
        self.logger.info({
            f"Received request to delete: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_delete_subsystem(
                self.client,
                nqn=request.subsystem_nqn,
            )
            self.logger.info(f"returned with status: {return_string}")
        except Exception as ex:
            self.logger.error(f"delete_subsystem failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_add_ns(self, request, context):
        # Add given NS to a given subsystem
        self.logger.info({
            f"Received request to add: {request.bdev_name} to {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_ns(
                self.client, request.subsystem_nqn, request.bdev_name)
            self.logger.info(f"returned with nsid: {return_string}")
        except Exception as ex:
            self.logger.error(f"Add NS returned with error: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.nsid()

        return pb2.nsid(nsid=return_string)

    def nvmf_subsystem_add_host(self, request, context):
        # grant host access to a given subsystem
        self.logger.info({
            f"Received request to add: {request.host_nqn} to {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_host(
                self.client, request.subsystem_nqn, request.host_nqn)
            self.logger.info(f"Status of add host: {return_string}")

        except Exception as ex:
            self.logger.error(f"Add Host returned with error: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_allow_any_host(self, request, context):
        # grant host access to a given subsystem
        self.logger.info({
            f"Set allow all hosts to {request.subsystem_nqn} to: {request.disable}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                self.client, request.subsystem_nqn, request.disable)
            self.logger.info(
                f"Status of allow all host request: {return_string}")
        except Exception as ex:
            self.logger.error(
                f"Allow any host set to {request.disable} returned error: \n {ex}"
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_create_transport(self, request, context):
        # set transport type for device access
        self.logger.info({f"Setting transport type to: {request.trtype}"})
        try:
            return_string = self.spdk_rpc.nvmf.nvmf_create_transport(
                self.client, request.trtype)
        except Exception as ex:
            self.logger.error(
                f"Create Transport {request.trtype} returned with error: \n {ex}"
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_add_listener(self, request, context):
        # Add a istener at the specified tcp-ip address for the subsystem specified
        self.logger.info({
            f"Adding listener at {request.traddr} : {request.trsvcid} for {request.nqn}"
        })
        try:
            return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_listener(
                self.client,
                request.nqn,
                request.trtype,
                request.traddr,
                request.trsvcid,
                request.adrfam,
            )
            self.logger.info(f"Status of add listener: {return_string}")
        except Exception as ex:
            self.logger.error(f"Add Listener returned with error: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=return_string)

    def nvmf_get_subsystems(self, request, context):
        # Get NVMe Subsystems
        self.logger.info({
            f"Received request to get subsystems",
        })

        try:
            ret = self.spdk_rpc.nvmf.nvmf_get_subsystems(
                self.client,
            )
            self.logger.info(f"returned with: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        return pb2.subsystems_info(subsystems=json.dumps(ret))

def serve(gw_config_filename):

    nvme_config = nvme_gw_config.NVMeGWConfig(gw_config_filename)

    enable_auth = nvme_config.getboolean("config", "enable_auth")
    gateway_addr = nvme_config.get("config", "gateway_addr")
    gateway_port = nvme_config.get("config", "gateway_port")

    server_key = nvme_config.get("mtls", "server_key")
    server_cert = nvme_config.get("mtls", "server_cert")
    client_cert = nvme_config.get("mtls", "client_cert")

    grpc_max_workers = nvme_config.getint("config", "grpc_server_max_workers")

    if enable_auth:

        # read in key and certificate
        with open(server_key, "rb") as f:
            private_key = f.read()
        with open(server_cert, "rb") as f:
            server_crt = f.read()
        with open(client_cert, "rb") as f:
            client_crt = f.read()

        # create server credentials & set client root certificate & set require_client_auth to True
        server_credentials = grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(private_key, server_crt)],
            root_certificates=client_crt,
            require_client_auth=True,
        )

        # create server
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=grpc_max_workers))
        gw_service = GWService(nvme_config)
        gw_service.start_spdk()
        pb2_grpc.add_NVMEGatewayServicer_to_server(gw_service, server)

        # add secure port using crendentials
        server.add_secure_port("{}:{}".format(gateway_addr, gateway_port),
                               server_credentials)
    else:

        # Authentication is not enabled
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=grpc_max_workers))
        gw_service = GWService(nvme_config)
        gw_service.start_spdk()
        pb2_grpc.add_NVMEGatewayServicer_to_server(gw_service, server)
        server.add_insecure_port("{}:{}".format(gateway_addr, gateway_port))

    server.start()
    server.wait_for_termination()


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
    serve(args.config)
