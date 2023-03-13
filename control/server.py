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
import socket
import subprocess
import grpc
import json
import logging
from concurrent import futures
from google.protobuf import json_format
from .generated import gateway_pb2 as pb2
from .generated import gateway_pb2_grpc as pb2_grpc
from .state import GatewayState, LocalGatewayState, OmapGatewayState, GatewayStateHandler
from .grpc import GatewayService

libc = ctypes.CDLL(ctypes.util.find_library("c"))
PR_SET_PDEATHSIG = 1


def set_pdeathsig(sig=signal.SIGTERM):

    def callable():
        return libc.prctl(PR_SET_PDEATHSIG, sig)

    return callable


class GatewayServer:
    """Runs SPDK and receives client requests for the gateway service.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_rpc: GatewayService implementation
        server: gRPC server instance to receive gateway client requests
        spdk_rpc: Module methods for SPDK
        spdk_rpc_client: Client of SPDK RPC server
        spdk_rpc_ping_client: Ping client of SPDK RPC server
        spdk_process: Subprocess running SPDK NVMEoF target application
    """

    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.spdk_process = None
        self.gateway_rpc = None
        self.server = None

        gateway_name = self.config.get("gateway", "name")
        if not gateway_name:
            gateway_name = socket.gethostname()
        self.logger.info(f"Starting gateway {gateway_name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleans up SPDK and server instances."""

        if self.spdk_process is not None:
            self.logger.info("Terminating SPDK...")
            self.spdk_process.terminate()
            try:
                timeout = self.config.getfloat("spdk", "timeout")
                self.spdk_process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.spdk_process.kill()

        if self.server is not None:
            self.logger.info("Stopping the server...")
            self.server.stop(None)

        self.logger.info("Exiting the gateway process.")

    def serve(self):
        """Starts gateway server."""

        # Start SPDK
        self._start_spdk()

        # Register service implementation with server
        omap_state = OmapGatewayState(self.config)
        local_state = LocalGatewayState(self.config)
        gateway_state = GatewayStateHandler(self.config, local_state,
                                            omap_state, self.gateway_rpc_caller)
        self.gateway_rpc = GatewayService(self.config, gateway_state,
                                          self.spdk_rpc, self.spdk_rpc_client)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        pb2_grpc.add_GatewayServicer_to_server(self.gateway_rpc, self.server)

        # Add listener port
        self._add_server_listener()

        # Check for existing NVMeoF target state
        gateway_state.start_update()

        # Start server
        self.server.start()

    def _add_server_listener(self):
        """Adds listener port to server."""

        enable_auth = self.config.getboolean("gateway", "enable_auth")
        gateway_addr = self.config.get("gateway", "addr")
        gateway_port = self.config.get("gateway", "port")
        if enable_auth:
            # Read in key and certificates for authentication
            server_key = self.config.get("mtls", "server_key")
            server_cert = self.config.get("mtls", "server_cert")
            client_cert = self.config.get("mtls", "client_cert")
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

    def _start_spdk(self):
        """Starts SPDK process."""

        # Get path and import SPDK's RPC modules
        spdk_path = self.config.get("spdk", "spdk_path")
        sys.path.append(os.path.join(spdk_path, "spdk/python"))
        self.logger.info(f"SPDK PATH: {spdk_path}")
        import spdk.rpc as spdk_rpc
        self.spdk_rpc = spdk_rpc

        # Start target
        tgt_path = self.config.get("spdk", "tgt_path")
        spdk_rpc_socket = self.config.get("spdk", "rpc_socket")
        spdk_tgt_cmd_extra_args = self.config.get_with_default(
            "spdk", "tgt_cmd_extra_args", "")
        spdk_cmd = os.path.join(spdk_path, tgt_path)
        cmd = [spdk_cmd, "-u", "-r", spdk_rpc_socket]
        if spdk_tgt_cmd_extra_args:
            cmd += shlex.split(spdk_tgt_cmd_extra_args)
        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            self.spdk_process = subprocess.Popen(cmd,
                                                 preexec_fn=set_pdeathsig(
                                                     signal.SIGTERM))
        except Exception as ex:
            self.logger.error(f"Unable to start SPDK: \n {ex}")
            raise

        # Initialization
        timeout = self.config.getfloat("spdk", "timeout")
        log_level = self.config.get("spdk", "log_level")
        conn_retries = self.config.getint_with_default("spdk",
                                                       "conn_retries", 10)
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

        # Implicitly create transports
        spdk_transports = self.config.get_with_default("spdk", "transports",
                                                       "tcp")
        for trtype in spdk_transports.split():
            self._create_transport(trtype.lower())

    def _create_transport(self, trtype):
        """Initializes a transport type."""
        args = {'trtype': trtype}
        name = "transport_" + trtype + "_options"
        options = self.config.get_with_default("spdk", name, "")

        self.logger.debug(f"create_transport: {trtype} options: {options}")

        if options:
            try:
                args.update(json.loads(options))
            except json.decoder.JSONDecodeError as ex:
                self.logger.error(
                    f"Failed to parse spdk {name} ({options}): \n {ex}")
                raise

        try:
            status = self.spdk_rpc.nvmf.nvmf_create_transport(
                self.spdk_rpc_client, **args)
        except Exception as ex:
            self.logger.error(
                f"Create Transport {trtype} returned with error: \n {ex}")
            raise

    def keep_alive(self):
        """Continuously confirms communication with SPDK process."""
        while True:
            timedout = self.server.wait_for_termination(timeout=1)
            if not timedout:
                break
            alive = self._ping()
            if not alive:
                break

    def _ping(self):
        """Confirms communication with SPDK process."""
        try:
            ret = self.spdk_rpc.spdk_get_version(self.spdk_rpc_ping_client)
            return True
        except Exception as ex:
            self.logger.error(f"spdk_get_version failed with: \n {ex}")
            return False

    def gateway_rpc_caller(self, requests, is_add_req):
        """Passes RPC requests to gateway service."""
        for key, val in requests.items():
            if key.startswith(GatewayState.BDEV_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_bdev_req())
                    self.gateway_rpc.create_bdev(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.delete_bdev_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.delete_bdev(req)
            elif key.startswith(GatewayState.SUBSYSTEM_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_subsystem_req())
                    self.gateway_rpc.create_subsystem(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.delete_subsystem_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.delete_subsystem(req)
            elif key.startswith(GatewayState.NAMESPACE_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.add_namespace_req())
                    self.gateway_rpc.add_namespace(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.remove_namespace_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.remove_namespace(req)
            elif key.startswith(GatewayState.HOST_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.add_host_req())
                    self.gateway_rpc.add_host(req)
                else:
                    req = json_format.Parse(val, pb2.remove_host_req())
                    self.gateway_rpc.remove_host(req)
            elif key.startswith(GatewayState.LISTENER_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_listener_req())
                    self.gateway_rpc.create_listener(req)
                else:
                    req = json_format.Parse(val, pb2.delete_listener_req())
                    self.gateway_rpc.delete_listener(req)
