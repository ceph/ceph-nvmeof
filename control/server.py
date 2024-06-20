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
import threading
import contextlib
import time
from concurrent import futures
from google.protobuf import json_format

import spdk.rpc
import spdk.rpc.client as rpc_client
import spdk.rpc.nvmf as rpc_nvmf

from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc
from .proto import monitor_pb2_grpc
from .state import GatewayState, LocalGatewayState, OmapLock, OmapGatewayState, GatewayStateHandler
from .grpc import GatewayService, MonitorGroupService
from .discovery import DiscoveryService
from .config import GatewayConfig
from .utils import GatewayLogger
from .utils import GatewayUtils
from .cephutils import CephUtils
from .prometheus import start_exporter

def sigchld_handler(signum, frame):
    """Handle SIGCHLD, runs when a child process, like the spdk, terminates."""
    logger = GatewayLogger().logger
    logger.error(f"GatewayServer: SIGCHLD received {signum=}")

    try:
        pid, wait_status = os.waitpid(-1, os.WNOHANG)
        logger.error(f"PID of terminated child process is {pid}")
    except OSError:
        logger.exception(f"waitpid error")
        # eat the exception, in signal handler context
        pass

    exit_code = os.waitstatus_to_exitcode(wait_status)

    # GW process should exit now
    raise SystemExit(f"Gateway subprocess terminated {pid=} {exit_code=}")

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
        discovery_pid: Subprocess running Ceph nvmeof discovery service
    """

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.gw_logger_object = GatewayLogger(self.config)
        self.logger = self.gw_logger_object.logger
        self.spdk_process = None
        self.gateway_rpc = None
        self.server = None
        self.discovery_pid = None
        self.spdk_rpc_socket_path = None
        self.monitor_event = threading.Event()
        self.monitor_client_process = None
        self.ceph_utils = None
        self.rpc_lock = threading.Lock()
        self.group_id = 0
        self.monitor_client = '/usr/bin/ceph-nvmeof-monitor-client'
        self.omap_state = None

        self.name = self.config.get("gateway", "name")
        if not self.name:
            self.name = socket.gethostname()
        self.logger.info(f"Starting gateway {self.name}")

        self.allowed_consecutive_spdk_ping_failures = self.config.getint_with_default("gateway",
                                                                                      "allowed_consecutive_spdk_ping_failures", 1)
        self.spdk_ping_interval_in_seconds = self.config.getfloat_with_default("gateway", "spdk_ping_interval_in_seconds", 2.0)
        if self.spdk_ping_interval_in_seconds < 0.0:
            self.logger.warning(f"Invalid SPDK ping interval {self.spdk_ping_interval_in_seconds}, will reset to 0")
            self.spdk_ping_interval_in_seconds = 0.0
        self.ping_spdk_under_lock = self.config.getboolean_with_default("gateway", "ping_spdk_under_lock", False)
        if self.ping_spdk_under_lock:
            self.ping_lock = self.rpc_lock
        else:
            self.ping_lock = contextlib.suppress()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleans up SPDK and server instances."""
        if exc_type is not None:
            self.logger.exception("GatewayServer exception occurred:")

        gw_name = self.name
        gw_logger = self.gw_logger_object
        logger = gw_logger.logger
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        if self.monitor_client_process is not None:
            self._stop_monitor_client()

        if self.spdk_process is not None:
            self._stop_spdk()

        if self.server is not None:
            if logger:
                logger.info("Stopping the server...")
            self.server.stop(None)

        if self.discovery_pid:
            self._stop_discovery()

        if self.omap_state:
            self.omap_state.cleanup_omap()
            self.omap_state = None

        if logger:
            logger.info("Exiting the gateway process.")
        gw_logger.compress_final_log_file(gw_name)

    def set_group_id(self, id: int):
        self.logger.info(f"Gateway {self.name} group {id=}")
        assert id >= 0
        self.group_id = id
        self.monitor_event.set()

    def _wait_for_group_id(self):
        """Waits for the monitor notification of this gatway's group id"""
        #  Python 3.8: Default value of max_workers is  min(32, os.cpu_count() + 4).
        #  This default value preserves at least 5 workers for I/O bound tasks. It utilizes at
        #  most 32 CPU cores for CPU bound tasks which release the GIL. And it avoids using
        #  very large resources implicitly on many-core machines.
        self.monitor_server = grpc.server(futures.ThreadPoolExecutor())
        monitor_pb2_grpc.add_MonitorGroupServicer_to_server(MonitorGroupService(self.set_group_id), self.monitor_server)
        self.monitor_server.add_insecure_port(self._monitor_address())
        self.monitor_server.start()
        self.logger.info(f"MonitorGroup server is listening on {self._monitor_address()} for group id")
        self.monitor_event.wait()
        self.monitor_event = None
        self.logger.info("Stopping the MonitorGroup server...")
        grace = self.config.getfloat_with_default("gateway", "monitor_stop_grace", 1/1000)
        self.monitor_server.stop(grace).wait()
        self.logger.info("The MonitorGroup gRPC server stopped...")
        self.monitor_server = None

    def serve(self):
        """Starts gateway server."""
        self.logger.info(f"Starting serve, monitor client version: {self._monitor_client_version()}")

        omap_state = OmapGatewayState(self.config, f"gateway-{self.name}")
        self.omap_state = omap_state
        local_state = LocalGatewayState()
        omap_state.check_for_old_format_omap_files()

        # install SIGCHLD handler
        signal.signal(signal.SIGCHLD, sigchld_handler)

        # Start monitor client
        self._start_monitor_client()

        self.ceph_utils = CephUtils(self.config)

        # Start SPDK
        self._start_spdk(omap_state)

        # Start discovery service
        self._start_discovery_service()

        # Register service implementation with server
        gateway_state = GatewayStateHandler(self.config, local_state, omap_state, self.gateway_rpc_caller, f"gateway-{self.name}")
        omap_lock = OmapLock(omap_state, gateway_state, self.rpc_lock)
        self.gateway_rpc = GatewayService(self.config, gateway_state, self.rpc_lock, omap_lock, self.group_id, self.spdk_rpc_client, self.ceph_utils)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        pb2_grpc.add_GatewayServicer_to_server(self.gateway_rpc, self.server)

        # Add listener port
        self._add_server_listener()

        # Check for existing NVMeoF target state
        gateway_state.start_update()

        # Start server
        self.server.start()

        # Start the prometheus endpoint if enabled by the config
        if self.config.getboolean_with_default("gateway", "enable_prometheus_exporter", True):
            self.logger.info("Prometheus endpoint is enabled")
            start_exporter(self.spdk_rpc_client, self.config, self.gateway_rpc, self.logger)
        else:
            self.logger.info(f"Prometheus endpoint is disabled. To enable, set the config option 'enable_prometheus_exporter = True'")

    def _monitor_client_version(self) -> str:
        """Return monitor client version string."""
        # Get the current SIGCHLD handler
        original_sigchld_handler = signal.getsignal(signal.SIGCHLD)

        try:
            # Execute the command and capture its output
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
            completed_process = subprocess.run([self.monitor_client, "--version"], capture_output=True, text=True)
        finally:
            # Restore the original SIGCHLD handler
            signal.signal(signal.SIGCHLD, original_sigchld_handler)

        # Get the output
        output = completed_process.stdout.strip()
        return output

    def _start_monitor_client(self):
        """Runs CEPH NVMEOF Monitor Client."""
        enable_monitor_client = self.config.getboolean_with_default("gateway", "enable_monitor_client", True)
        if not enable_monitor_client:
            self.logger.info("CEPH monitor client is disabled")
            return
        client_prefix = "client."
        rados_id = self.config.get_with_default("ceph", "id", "client.admin")
        if not rados_id.startswith(client_prefix):
            rados_id = client_prefix + rados_id
        cmd = [ self.monitor_client,
                "--gateway-name", self.name,
                "--gateway-address", self._gateway_address(),
                "--gateway-pool", self.config.get("ceph", "pool"),
                "--gateway-group", self.config.get_with_default("gateway", "group", ""),
                "--monitor-group-address", self._monitor_address(),
                '-c', '/etc/ceph/ceph.conf',
                '-n', rados_id,
                '-k', '/etc/ceph/keyring']
        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            # start monitor client process
            self.monitor_client_process = subprocess.Popen(cmd)
            self.logger.info(f"monitor client process id: {self.monitor_client_process.pid}")
            # wait for monitor notification of the group id
            self._wait_for_group_id()
        except Exception:
            self.logger.exception(f"Unable to start CEPH monitor client:")
            raise

    def _start_discovery_service(self):
        """Runs either SPDK on CEPH NVMEOF Discovery Service."""
        enable_spdk_discovery_controller = self.config.getboolean_with_default("gateway", "enable_spdk_discovery_controller", False)
        if enable_spdk_discovery_controller:
            self.logger.info("Using SPDK discovery service")
            return

        try:
            rpc_nvmf.nvmf_delete_subsystem(self.spdk_rpc_ping_client, GatewayUtils.DISCOVERY_NQN)
        except Exception:
            self.logger.exception(f"Delete Discovery subsystem returned with error")
            raise

        # run ceph nvmeof discovery service in sub-process
        assert self.discovery_pid is None
        self.discovery_pid = os.fork()
        if self.discovery_pid == 0:
            self.logger.info("Starting ceph nvmeof discovery service")
            DiscoveryService(self.config).start_service()
            os._exit(0)
        else:
            self.logger.info(f"Discovery service process id: {self.discovery_pid}")

    def _gateway_address(self):
        """Calculate gateway gRPC address string."""
        gateway_addr = self.config.get("gateway", "addr")
        gateway_port = self.config.get("gateway", "port")
        # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
        gateway_addr = GatewayUtils.escape_address_if_ipv6(gateway_addr)
        return "{}:{}".format(gateway_addr, gateway_port)

    def _monitor_address(self):
        """Calculate gateway gRPC address string."""
        monitor_addr = self.config.get("gateway", "addr")
        monitor_port = self.config.getint_with_default("gateway", "port", 5500) - 1
        # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
        monitor_addr = GatewayUtils.escape_address_if_ipv6(monitor_addr)
        return "{}:{}".format(monitor_addr, monitor_port)

    def _add_server_listener(self):
        """Adds listener port to server."""

        enable_auth = self.config.getboolean("gateway", "enable_auth")
        if enable_auth:
            self.logger.info(f"mTLS authenciation has been enabled")
            # Read in key and certificates for authentication
            server_key = self.config.get("mtls", "server_key")
            server_cert = self.config.get("mtls", "server_cert")
            client_cert = self.config.get("mtls", "client_cert")
            self.logger.debug(f"Trying to open server key file: {server_key}")
            with open(server_key, "rb") as f:
                private_key = f.read()
            self.logger.debug(f"Trying to open server cert file: {server_cert}")
            with open(server_cert, "rb") as f:
                server_crt = f.read()
            self.logger.debug(f"Trying to open client cert file: {client_cert}")
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
                self._gateway_address(), server_credentials)
        else:
            # Authentication is not enabled
            self.server.add_insecure_port(self._gateway_address())

    def _get_spdk_rpc_socket_path(self, omap_state) -> str:
        # For backward compatibility, try first to get the old attribute
        spdk_rpc_socket = self.config.get_with_default("spdk", "rpc_socket", "")
        if spdk_rpc_socket:
            return spdk_rpc_socket

        spdk_rpc_socket_dir = self.config.get_with_default("spdk", "rpc_socket_dir", "")
        if not spdk_rpc_socket_dir:
            spdk_rpc_socket_dir = GatewayConfig.CEPH_RUN_DIRECTORY
            if self.ceph_utils:
                fsid = self.ceph_utils.fetch_ceph_fsid()
                if fsid:
                    spdk_rpc_socket_dir += fsid + "/"
        if not spdk_rpc_socket_dir.endswith("/"):
            spdk_rpc_socket_dir += "/"
        try:
            os.makedirs(spdk_rpc_socket_dir, 0o777, True)
        except Exception:
            logger.exception(f"makedirs({spdk_rpc_socket_dir}, 0o777, True) failed")
            pass
        spdk_rpc_socket = spdk_rpc_socket_dir + self.config.get_with_default("spdk", "rpc_socket_name", "spdk.sock")
        return spdk_rpc_socket

    def _start_spdk(self, omap_state):
        """Starts SPDK process."""

        # Start target
        self.logger.debug(f"Configuring server {self.name}")
        spdk_tgt_path = self.config.get("spdk", "tgt_path")
        self.logger.info(f"SPDK Target Path: {spdk_tgt_path}")
        self.spdk_rpc_socket_path = self._get_spdk_rpc_socket_path(omap_state)
        self.logger.info(f"SPDK Socket: {self.spdk_rpc_socket_path}")
        spdk_tgt_cmd_extra_args = self.config.get_with_default(
            "spdk", "tgt_cmd_extra_args", "")
        cmd = [spdk_tgt_path, "-u", "-r", self.spdk_rpc_socket_path]
        if spdk_tgt_cmd_extra_args:
            cmd += shlex.split(spdk_tgt_cmd_extra_args)
        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            # start spdk process
            self.spdk_process = subprocess.Popen(cmd)
        except Exception:
            self.logger.exception(f"Unable to start SPDK")
            raise

        # Initialization
        timeout = self.config.getfloat_with_default("spdk", "timeout", 60.0)
        log_level = self.config.get_with_default("spdk", "log_level", "WARNING")
        # connect timeout: spdk client retries 5 times per sec
        conn_retries = int(timeout * 5)
        self.logger.info(f"SPDK process id: {self.spdk_process.pid}")
        self.logger.info(
            f"Attempting to initialize SPDK: rpc_socket: {self.spdk_rpc_socket_path},"
            f" conn_retries: {conn_retries}, timeout: {timeout}"
        )
        try:
            self.spdk_rpc_client = rpc_client.JSONRPCClient(
                self.spdk_rpc_socket_path,
                None,
                timeout,
                log_level=log_level,
                conn_retries=conn_retries,
            )
            self.spdk_rpc_ping_client = rpc_client.JSONRPCClient(
                self.spdk_rpc_socket_path,
                None,
                timeout,
                log_level=log_level,
                conn_retries=conn_retries,
            )
        except Exception:
            self.logger.exception(f"Unable to initialize SPDK")
            raise

        # Implicitly create transports
        spdk_transports = self.config.get_with_default("spdk", "transports",
                                                       "tcp")
        for trtype in spdk_transports.split():
            self._create_transport(trtype.lower())

        with self.ping_lock:
            try:
                return_version = spdk.rpc.spdk_get_version(self.spdk_rpc_ping_client)
                try:
                    version_string = return_version["version"]
                    self.logger.info(f"Started SPDK with version \"{version_string}\"")
                except KeyError:
                    self.logger.error(f"Can't find SPDK version string in {return_version}")
            except Exception:
                self.logger.exception(f"Can't read SPDK version")
                pass

    def _stop_subprocess(self, proc, timeout):
        """Stops SPDK process."""
        assert proc is not None # should be verified by the caller

        return_code = proc.returncode

        # Terminate spdk process
        if return_code is not None:
            self.logger.error(f"{self.name} pid {proc.pid} "
                              f"already terminated, exit code: {return_code}")
        else:
            self.logger.info(f"Aborting ({self.name}) pid {proc.pid}...")
            proc.send_signal(signal.SIGABRT)

        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.logger.exception(f"({self.name}) pid {proc.pid} "
                                  f"timeout occurred while terminating sub process:")
            proc.kill() # kill -9, send KILL signal

    def _stop_monitor_client(self):
        """Stops Monitor client."""
        timeout = self.config.getfloat_with_default("monitor", "timeout", 1.0)
        self._stop_subprocess(self.monitor_client_process, timeout)

    def _stop_spdk(self):
        """Stops SPDK process."""
        # Terminate spdk process
        timeout = self.config.getfloat_with_default("spdk", "timeout", 60.0)
        self._stop_subprocess(self.spdk_process, timeout)

        # Clean spdk rpc socket
        if self.spdk_rpc_socket_path and os.path.exists(self.spdk_rpc_socket_path):
            try:
                os.remove(self.spdk_rpc_socket_path)
            except Exception:
                self.logger.exception(f"An error occurred while removing RPC socket {self.spdk_rpc_socket_path}")

    def _stop_discovery(self):
        """Stops Discovery service process."""
        assert self.discovery_pid is not None # should be verified by the caller

        self.logger.info("Terminating discovery service...")
        # discovery service selector loop should exit due to KeyboardInterrupt exception
        try:
            os.kill(self.discovery_pid, signal.SIGINT)
            os.waitpid(self.discovery_pid, 0)
        except ChildProcessError:
            pass # ignore
        self.logger.info("Discovery service terminated")

        self.discovery_pid = None

    def _create_transport(self, trtype):
        """Initializes a transport type."""
        args = {'trtype': trtype}
        name = "transport_" + trtype + "_options"
        options = self.config.get_with_default("spdk", name, "")

        self.logger.debug(f"create_transport: {trtype} options: {options}")

        if options:
            try:
                args.update(json.loads(options))
            except json.decoder.JSONDecodeError:
                self.logger.exception(f"Failed to parse spdk {name} ({options})")
                raise

        try:
            status = rpc_nvmf.nvmf_create_transport(
                self.spdk_rpc_client, **args)
        except Exception:
            self.logger.exception(f"Create Transport {trtype} returned with error")
            raise

    def keep_alive(self):
        """Continuously confirms communication with SPDK process."""

        consecutive_ping_failures = 0
        # we spend 1 second waiting for server termination so subtract it from ping interval
        if self.spdk_ping_interval_in_seconds >= 1.0:
            self.spdk_ping_interval_in_seconds -= 1.0
        else:
            self.spdk_ping_interval_in_seconds = 0.0

        while True:
            timedout = self.server.wait_for_termination(timeout=1)
            if not timedout:
                break
            if self.spdk_ping_interval_in_seconds > 0.0:
                time.sleep(self.spdk_ping_interval_in_seconds)
            alive = self._ping()
            if not alive:
                consecutive_ping_failures += 1
                if consecutive_ping_failures >= self.allowed_consecutive_spdk_ping_failures:
                    self.logger.critical(f"SPDK ping failed {consecutive_ping_failures} times, aborting")
                    break
            else:
                consecutive_ping_failures = 0

    def _ping(self):
        """Confirms communication with SPDK process."""
        try:
            with self.ping_lock:
                ret = spdk.rpc.spdk_get_version(self.spdk_rpc_ping_client)
            return True
        except Exception:
            self.logger.exception(f"spdk_get_version failed")
            return False

    def gateway_rpc_caller(self, requests, is_add_req):
        """Passes RPC requests to gateway service."""
        for key, val in requests.items():
            if key.startswith(GatewayState.SUBSYSTEM_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_subsystem_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.create_subsystem(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.delete_subsystem_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.delete_subsystem(req)
            elif key.startswith(GatewayState.NAMESPACE_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_add_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_add(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.namespace_delete_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_delete(req)
            elif key.startswith(GatewayState.NAMESPACE_QOS_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_set_qos_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_set_qos_limits(req)
                else:
                    # Do nothing, this is covered by the delete namespace code
                    pass
            elif key.startswith(GatewayState.HOST_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.add_host_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.add_host(req)
                else:
                    req = json_format.Parse(val, pb2.remove_host_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.remove_host(req)
            elif key.startswith(GatewayState.LISTENER_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_listener_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.create_listener(req)
                else:
                    req = json_format.Parse(val, pb2.delete_listener_req(), ignore_unknown_fields=True)
                    self.gateway_rpc.delete_listener(req)
