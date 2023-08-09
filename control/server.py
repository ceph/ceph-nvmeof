#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import os
import shlex
import signal
import socket
import subprocess
import grpc
import json
import logging
import signal
import traceback
from concurrent import futures
from google.protobuf import json_format

import spdk.rpc
import spdk.rpc.client as rpc_client
import spdk.rpc.nvmf as rpc_nvmf

from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc
from .state import GatewayState, LocalGatewayState, OmapGatewayState, GatewayStateHandler
from .grpc import GatewayService

def sigchld_handler(signum, frame):
    """Handle SIGCHLD, runs when a spdk process terminates."""
    logger = logging.getLogger(__name__)
    logger.error(f"GatewayServer: GSIGCHLD received {signum=}")

    try:
        pid, wait_status = os.waitpid(-1, os.WNOHANG)
    except OSError:
        logger.exception(f"waitpid error:")
        # eat the exception, in signal handler context

    exit_code = os.waitstatus_to_exitcode(wait_status)

    # GW process should exit now
    raise SystemExit(f"spdk subprocess terminated {pid=} {exit_code=}")

class GatewayServer:
    """Runs SPDK and receives client requests for the gateway service.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_rpc: GatewayService implementation
        server: gRPC server instance to receive gateway client requests
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

        self.name = self.config.get("gateway", "name")
        if not self.name:
            self.name = socket.gethostname()
        self.logger.info(f"Starting gateway {self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleans up SPDK and server instances."""
        if exc_type is not None:
            self.logger.exception("GatewayServer exception occurred:")

        if self.spdk_process is not None:
            self._stop_spdk()

        if self.server is not None:
            self.logger.info("Stopping the server...")
            self.server.stop(None)

        self.logger.info("Exiting the gateway process.")

    def serve(self):
        """Starts gateway server."""
        self.logger.debug("Starting serve")

        # Start SPDK
        self._start_spdk()

        # Register service implementation with server
        omap_state = OmapGatewayState(self.config)
        local_state = LocalGatewayState()
        gateway_state = GatewayStateHandler(self.config, local_state,
                                            omap_state, self.gateway_rpc_caller)
        self.gateway_rpc = GatewayService(self.config, gateway_state,
                                          self.spdk_rpc_client)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        pb2_grpc.add_GatewayServicer_to_server(self.gateway_rpc, self.server)

        # Add listener port
        self._add_server_listener()

        # Check for existing NVMeoF target state
        gateway_state.start_update()

        # Start server
        self.server.start()
        enable_discovery_controller = self.config.getboolean_with_default("gateway", "enable_discovery_controller", False)
        if not enable_discovery_controller:
            try:
                rpc_nvmf.nvmf_delete_subsystem(self.spdk_rpc_ping_client, "nqn.2014-08.org.nvmexpress.discovery")
            except Exception as ex:
                self.logger.error(f"  Delete Discovery subsystem returned with error: \n {ex}")
                raise

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

            # Add secure port using credentials
            self.server.add_secure_port(
                "{}:{}".format(gateway_addr, gateway_port), server_credentials)
        else:
            # Authentication is not enabled
            self.server.add_insecure_port("{}:{}".format(
                gateway_addr, gateway_port))

    def _start_spdk(self):
        """Starts SPDK process."""

        # Start target
        self.logger.debug(f"Configuring server {self.name}")
        spdk_tgt_path = self.config.get("spdk", "tgt_path")
        self.logger.info(f"SPDK Target Path: {spdk_tgt_path}")
        spdk_rpc_socket = self.config.get("spdk", "rpc_socket")
        self.logger.info(f"SPDK Socket: {spdk_rpc_socket}")
        spdk_tgt_cmd_extra_args = self.config.get_with_default(
            "spdk", "tgt_cmd_extra_args", "")
        cmd = [spdk_tgt_path, "-u", "-r", spdk_rpc_socket]
        if spdk_tgt_cmd_extra_args:
            cmd += shlex.split(spdk_tgt_cmd_extra_args)
        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            # install SIGCHLD handler
            signal.signal(signal.SIGCHLD, sigchld_handler)

            # start spdk process
            self.spdk_process = subprocess.Popen(cmd)
        except Exception as ex:
            self.logger.error(f"Unable to start SPDK: \n {ex}")
            raise

        # Initialization
        timeout = self.config.getfloat("spdk", "timeout")
        log_level = self.config.get("spdk", "log_level")
        # connect timeout: spdk client retries 5 times per sec
        conn_retries = int(timeout * 5)
        self.logger.info(
            f"Attempting to initialize SPDK: rpc_socket: {spdk_rpc_socket},"
            f" conn_retries: {conn_retries}, timeout: {timeout}"
        )
        try:
            self.spdk_rpc_client = rpc_client.JSONRPCClient(
                spdk_rpc_socket,
                None,
                timeout,
                log_level=log_level,
                conn_retries=conn_retries,
            )
            self.spdk_rpc_ping_client = rpc_client.JSONRPCClient(
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

    def _stop_spdk(self):
        """Stops SPDK process."""
        assert self.spdk_process is not None # should be verified by the caller

        return_code = self.spdk_process.returncode
        rpc_socket = self.config.get("spdk", "rpc_socket")

        # Terminate spdk process
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        if return_code is not None:
            self.logger.error(f"SPDK({self.name}) pid {self.spdk_process.pid} "
                              f"already terminated, exit code: {return_code}")
        else:
            self.logger.info(f"Terminating SPDK({self.name}) pid {self.spdk_process.pid}...")
            self.spdk_process.terminate()
        try:
            timeout = self.config.getfloat("spdk", "timeout")
            self.spdk_process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.logger.exception(f"SPDK({self.name}) pid {self.spdk_process.pid} "
                                  f"timeout occurred while terminating spdk:")
            self.spdk_process.kill() # kill -9, send KILL signal

        # Clean spdk rpc socket
        if os.path.exists(rpc_socket):
            try:
                os.remove(rpc_socket)
            except Exception:
                self.logger.exception(f"An error occurred while removing "
                                      f"rpc socket {rpc_socket}:")

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
            status = rpc_nvmf.nvmf_create_transport(
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
            ret = spdk.rpc.spdk_get_version(self.spdk_rpc_ping_client)
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
