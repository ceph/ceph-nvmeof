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
import time
from concurrent import futures
from google.protobuf import json_format

import spdk.rpc
import spdk.rpc.client as rpc_client
import spdk.rpc.nvmf as rpc_nvmf
import spdk.rpc.iobuf as rpc_iobuf

from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc
from .proto import monitor_pb2_grpc
from .state import GatewayState, LocalGatewayState, OmapLock, OmapGatewayState, GatewayStateHandler
from .grpc import GatewayService, MonitorGroupService
from .discovery import DiscoveryService
from .config import GatewayConfig
from .utils import GatewayLogger
from .utils import GatewayUtils
from .utils import GatewayUtilsCrypto
from .cephutils import CephUtils
from .prometheus import start_exporter


def sigterm_handler(signum, frame):
    """Handle SIGTERM, runs when a gateway is terminated gracefully."""
    logger = GatewayLogger().logger
    logger.info(f"GatewayServer: SIGTERM received {signum=}")
    raise SystemExit(0)


def sigchld_handler(signum, frame):
    """Handle SIGCHLD, runs when a child process, like the spdk, terminates."""
    logger = GatewayLogger().logger
    logger.error(f"GatewayServer: SIGCHLD received {signum=}")

    try:
        pid, wait_status = os.waitpid(-1, os.WNOHANG)
        logger.error(f"PID of terminated child process is {pid}")
    except OSError:
        logger.exception("waitpid error")
        # eat the exception, in signal handler context
        pass

    exit_code = os.waitstatus_to_exitcode(wait_status)

    # GW process should exit now
    raise SystemExit(f"Gateway subprocess terminated {pid=} {exit_code=}")


def int_to_bitmask(n):
    """Converts an integer n to a bitmask string"""
    return f"0x{hex((1 << n) - 1)[2:].upper()}"


def cpumask_set(args):
    """Check if reactor cpu mask is set in command line args"""

    # Check if "-m" or "--cpumask" is in the arguments
    if "-m" in args or "--cpumask" in args:
        return True

    # Check for the presence of "--cpumask="
    for arg in args:
        if arg.startswith('--cpumask='):
            return True

    return False


class GatewayServer:
    """Runs SPDK and receives client requests for the gateway service.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_rpc: GatewayService implementation
        server: gRPC server instance to receive gateway client requests
        spdk_rpc_client: Client of SPDK RPC server
        spdk_rpc_ping_client: Ping client of SPDK RPC server
        spdk_rpc_subsystems_client: subsystems client of SPDK RPC server
        spdk_process: Subprocess running SPDK NVMEoF target application
        discovery_pid: Subprocess running Ceph nvmeof discovery service
    """

    MAX_TIME_TO_WAIT_FOR_GATEWAY_EXIT = 30

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.gw_logger_object = GatewayLogger(self.config)
        self.logger = self.gw_logger_object.logger
        self.spdk_process = None
        self.spdk_log_file = None
        self.spdk_log_file_path = None
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
        self.monitor_client_log_file = None
        self.monitor_client_log_file_path = None
        self.omap_state = None
        self.omap_lock = None
        self.crypto = None
        self.gateway_state = None
        self.exiting = False
        self.is_gateway_process = True
        enc_key = None
        enc_key_file = self.config.get_with_default("gateway", "encryption_key", "")
        if enc_key_file:
            try:
                enc_key = GatewayUtilsCrypto.read_encryption_key(enc_key_file)
            except Exception:
                self.logger.exception(f"Got an error trying to read encryption key "
                                      f"{enc_key_file}. Any attempt to encrypt or "
                                      f"decrypt keys would fail")
        if enc_key:
            self.logger.info(f"Read encryption key from {enc_key_file}")
            self.crypto = GatewayUtilsCrypto(enc_key)
        else:
            self.logger.warning("No valid encryption key file was set. Any attempt to "
                                "encrypt or decrypt keys would fail")

        self.name = self.config.get("gateway", "name")
        if not self.name:
            self.name = socket.gethostname()
        self.system_exit_message = None
        self.system_exit_message_lock = threading.Lock()
        self.gateway_exit_started = threading.Event()
        self.logger.info(f"Starting gateway {self.name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleans up SPDK and server instances."""

        gw_logger = self.gw_logger_object
        logger = None
        if gw_logger:
            logger = gw_logger.logger

        normalExit = False
        if exc_type is None and exc_value is None:
            normalExit = True
        elif isinstance(exc_value, SystemExit) and isinstance(exc_value.code, int):
            normalExit = exc_value.code == 0

        # In case we got here because the discovery exited, do nothing
        if not self.is_gateway_process:
            process_name = "discovery"
            if logger:
                logger.info(f"Exiting the {process_name} process.")
            return normalExit
        else:
            process_name = "gateway"
        if self.gateway_rpc:
            self.gateway_rpc.up_and_running = False
        if self.gateway_state:
            self.gateway_state.up_and_running = False
            if self.gateway_state.omap:
                self.gateway_state.omap.up_and_running = False
        gw_name = self.name
        if self.exiting:
            if logger:
                logger.debug("Already exiting, do nothing")
            return normalExit
        self.exiting = True

        if logger:
            if normalExit:
                logger.info("GatewayServer is terminating gracefully...")
            else:
                logger.exception("GatewayServer exception occurred")

        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        if self.monitor_client_process:
            self._stop_monitor_client()

        if self.spdk_process:
            self._stop_spdk()

        if self.spdk_log_file:
            try:
                self.spdk_log_file.close()
            except Exception:
                pass
            self.spdk_log_file = None

        if self.spdk_log_file_path:
            GatewayLogger.compress_file(self.spdk_log_file_path, f"{self.spdk_log_file_path}.gz")
            self.spdk_log_file_path = None

        if self.monitor_client_log_file:
            try:
                self.monitor_client_log_file.close()
            except Exception:
                pass
            self.monitor_client_log_file = None

        if self.monitor_client_log_file_path:
            GatewayLogger.compress_file(self.monitor_client_log_file_path,
                                        f"{self.monitor_client_log_file_path}.gz")
            self.monitor_client_log_file_path = None

        if self.server:
            if logger:
                logger.info("Stopping the server...")
            self.server.stop(None)
            self.server = None

        if self.discovery_pid:
            self._stop_discovery()

        if self.omap_state:
            self.omap_state.cleanup_omap(self.omap_lock)
            self.omap_state = None

        if logger:
            logger.info(f"Exiting the {process_name} process.")

        if gw_logger and gw_name:
            gw_logger.compress_final_log_file(gw_name)

        return normalExit

    def set_group_id(self, id: int):
        self.logger.info(f"Gateway {self.name} group {id=}")
        assert id >= 0
        self.group_id = id
        self.monitor_event.set()

    def _wait_for_group_id(self):
        """Waits for the monitor notification of this gatway's group id"""
        self.monitor_server = self._grpc_server(self._monitor_address())
        monitor_pb2_grpc.add_MonitorGroupServicer_to_server(MonitorGroupService(self.set_group_id),
                                                            self.monitor_server)
        self.monitor_server.start()
        self.logger.info(f"MonitorGroup server is listening on "
                         f"{self._monitor_address()} for group id")
        self.monitor_event.wait()
        self.monitor_event = None
        self.logger.info("Stopping the MonitorGroup server...")
        grace = self.config.getfloat_with_default("gateway", "monitor_stop_grace", 1 / 1000)
        self.monitor_server.stop(grace).wait()
        self.logger.info("The MonitorGroup gRPC server stopped...")
        self.monitor_server = None

    def start_prometheus(self):
        """Starts the prometheus endpoint if enabled by the config."""

        if self.config.getboolean_with_default("gateway", "enable_prometheus_exporter", True):
            self.logger.info("Prometheus endpoint is enabled")
            start_exporter(self.spdk_rpc_client, self.config, self.gateway_rpc, self.logger)
        else:
            self.logger.info("Prometheus endpoint is disabled. To enable, set the config "
                             "option 'enable_prometheus_exporter = True'")

    def serve(self):
        """Starts gateway server."""
        self.logger.info(f"Starting serve, monitor client version: "
                         f"{self._monitor_client_version()}")

        omap_state = OmapGatewayState(self.config, self.set_gateway_exit_message,
                                      f"gateway-{self.name}")
        self.omap_state = omap_state
        local_state = LocalGatewayState()

        # install SIGCHLD handler
        signal.signal(signal.SIGCHLD, sigchld_handler)

        # install SIGTERM handler
        signal.signal(signal.SIGTERM, sigterm_handler)

        # Start monitor client
        self._start_monitor_client()

        self.ceph_utils = CephUtils(self.config)

        # Start SPDK
        self._start_spdk()

        # Start discovery service
        self._start_discovery_service()

        # Register service implementation with server
        self.gateway_state = GatewayStateHandler(self.config, local_state, omap_state,
                                                 self.gateway_rpc_caller, self.crypto,
                                                 f"gateway-{self.name}")
        self.omap_lock = OmapLock(self.gateway_state, self.rpc_lock)
        self.gateway_rpc = GatewayService(self.config, self.gateway_state, self.rpc_lock,
                                          self.omap_lock, self.group_id, self.spdk_rpc_client,
                                          self.spdk_rpc_subsystems_client, self.ceph_utils,
                                          self.set_gateway_exit_message)
        self.server = self._grpc_server(self._gateway_address())
        pb2_grpc.add_GatewayServicer_to_server(self.gateway_rpc, self.server)

        # Check for existing NVMeoF target state
        self.gateway_state.start_update()

        # Start server
        self.server.start()

        # Set SPDK log level
        log_level = self.config.get_with_default("spdk", "log_level", None)
        if log_level and log_level.strip():
            log_level = log_level.strip().upper()
            log_req = pb2.set_spdk_nvmf_logs_req(log_level=log_level, print_level=log_level)
            self.gateway_rpc.set_spdk_nvmf_logs(log_req)

        self._register_service_map()

        # This should be at the end of the function, after the server is up
        self.start_prometheus()

    def _register_service_map(self):
        # show gateway in "ceph status" output
        conn = self.omap_state.conn
        metadata = {
            "id": self.name.removeprefix("client.nvmeof."),
            "pool_name": self.config.get("ceph", "pool"),
            "daemon_type": "gateway",          # "nvmeof: 3 <daemon_type> active (3 hosts)"
            "group": self.config.get_with_default("gateway", "group", ""),
        }
        metadata["id"] = metadata["id"].removeprefix(
            f"{metadata['pool_name']}.{metadata['group']}.")
        self.ceph_utils.service_daemon_register(conn, metadata)

    def _monitor_client_version(self) -> str:
        """Return monitor client version string."""
        # Get the current SIGCHLD handler
        original_sigchld_handler = signal.getsignal(signal.SIGCHLD)

        try:
            # Execute the command and capture its output
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
            completed_process = subprocess.run([self.monitor_client, "--version"],
                                               capture_output=True, text=True)
        finally:
            # Restore the original SIGCHLD handler
            signal.signal(signal.SIGCHLD, original_sigchld_handler)

        # Get the output
        output = completed_process.stdout.strip()
        return output

    def _start_monitor_client(self):
        """Runs CEPH NVMEOF Monitor Client."""
        enable_monitor_client = self.config.getboolean_with_default(
            "gateway",
            "enable_monitor_client",
            True)
        if not enable_monitor_client:
            self.logger.info("CEPH monitor client is disabled")
            return
        client_prefix = "client."
        rados_id = self.config.get_with_default("ceph", "id", "client.admin")
        if not rados_id.startswith(client_prefix):
            rados_id = client_prefix + rados_id
        cmd = [self.monitor_client,
               "--gateway-name", self.name,
               "--gateway-address", self._gateway_address(),
               "--gateway-pool", self.config.get("ceph", "pool"),
               "--gateway-group", self.config.get_with_default("gateway", "group", ""),
               "--monitor-group-address", self._monitor_address(),
               '-c', '/etc/ceph/ceph.conf',
               '-n', rados_id,
               '-k', '/etc/ceph/keyring']
        if self.config.getboolean("gateway", "enable_auth"):
            cmd += [
                "--server-cert", self.config.get("mtls", "server_cert"),
                "--client-key", self.config.get("mtls", "client_key"),
                "--client-cert", self.config.get("mtls", "client_cert")]

        self.monitor_client_log_file = None
        self.monitor_client_log_file_path = None
        log_stderr = None
        log_file_dir = self.config.get_with_default("monitor", "log_file_dir", None)
        self.monitor_client_log_file_path = self.handle_process_output_file(log_file_dir,
                                                                            "monitor-client")
        if self.monitor_client_log_file_path:
            try:
                self.monitor_client_log_file = open(self.monitor_client_log_file_path, "wt")
                log_stderr = subprocess.STDOUT
            except Exception:
                pass

        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            # start monitor client process
            self.monitor_client_process = subprocess.Popen(cmd,
                                                           stdout=self.monitor_client_log_file,
                                                           stderr=log_stderr)
            self.logger.info(f"monitor client process id: {self.monitor_client_process.pid}")
            if self.monitor_client_log_file and self.monitor_client_log_file_path:
                self.logger.info(f"Monitor log file is {self.monitor_client_log_file_path}")
            # wait for monitor notification of the group id
            self._wait_for_group_id()
        except Exception:
            self.logger.exception("Unable to start CEPH monitor client")
            raise

    def _start_discovery_service(self):
        """Runs either SPDK on CEPH NVMEOF Discovery Service."""
        enable_spdk_discovery_controller = self.config.getboolean_with_default(
            "gateway",
            "enable_spdk_discovery_controller",
            False)
        if enable_spdk_discovery_controller:
            self.logger.info("Using SPDK discovery service")
            return

        try:
            rpc_nvmf.nvmf_delete_subsystem(self.spdk_rpc_client, GatewayUtils.DISCOVERY_NQN)
        except Exception:
            self.logger.exception("Delete Discovery subsystem returned with error")
            raise

        # run ceph nvmeof discovery service in sub-process
        assert self.discovery_pid is None
        self.discovery_pid = os.fork()
        if self.discovery_pid == 0:
            self.logger.info("Starting ceph nvmeof discovery service")
            # disable inherited from gateway signal handlers
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            with DiscoveryService(self.config) as discovery:
                discovery.start_service()
            os._exit(0)
        else:
            self.logger.info(f"Discovery service process id: {self.discovery_pid}")

    def _gateway_address(self):
        """Calculate gateway gRPC address string."""
        gateway_addr = self.config.get("gateway", "addr")
        gateway_port = self.config.get("gateway", "port")
        # We need to enclose IPv6 addresses in brackets before concatenating
        # a colon and port number to it
        gateway_addr = GatewayUtils.escape_address_if_ipv6(gateway_addr)
        return "{}:{}".format(gateway_addr, gateway_port)

    def _monitor_address(self):
        """Calculate gateway gRPC address string."""
        monitor_addr = self.config.get("gateway", "addr")
        monitor_port = self.config.getint_with_default("gateway", "port", 5500) - 1
        # We need to enclose IPv6 addresses in brackets before concatenating
        # a colon and port number to it
        monitor_addr = GatewayUtils.escape_address_if_ipv6(monitor_addr)
        return "{}:{}".format(monitor_addr, monitor_port)

    def _grpc_server(self, address):
        """Construct grpc server"""

        #  Python 3.8: Default value of max_workers is  min(32, os.cpu_count() + 4).
        #  This default value preserves at least 5 workers for I/O bound tasks. It utilizes at
        #  most 32 CPU cores for CPU bound tasks which release the GIL. And it avoids using
        #  very large resources implicitly on many-core machines.
        server = grpc.server(futures.ThreadPoolExecutor())

        enable_auth = self.config.getboolean("gateway", "enable_auth")
        if enable_auth:
            self.logger.info("mTLS authentication has been enabled")
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
            server.add_secure_port(
                address, server_credentials)
        else:
            # Authentication is not enabled
            server.add_insecure_port(address)

        return server

    def handle_process_output_file(self, log_file_dir, prefix):
        if not log_file_dir or not log_file_dir.strip():
            return None

        try:
            os.makedirs(log_file_dir, 0o755, True)
        except Exception:
            return None

        if not log_file_dir.endswith("/"):
            log_file_dir += "/"
        log_file_path = f"{log_file_dir}{prefix}-{self.name}.log"

        log_files = [f"{log_file_path}{suffix}" for suffix in ['.bak', '.gz.bak']]
        for log_file in log_files:
            try:
                os.remove(log_file)
            except Exception:
                pass
        try:
            os.rename(log_file_path, f"{log_file_path}.bak")
        except FileNotFoundError:
            pass
        try:
            os.rename(f"{log_file_path}.gz", f"{log_file_path}.gz.bak")
        except FileNotFoundError:
            pass

        return log_file_path

    def _start_spdk(self):
        """Starts SPDK process."""

        # Start target
        self.logger.debug(f"Configuring server {self.name}")
        waiting_for_rpc = False
        spdk_tgt_path = self.config.get("spdk", "tgt_path")
        self.logger.info(f"SPDK Target Path: {spdk_tgt_path}")
        sockdir = self.config.get_with_default("spdk", "rpc_socket_dir", "/var/tmp/")
        if not os.path.isdir(sockdir):
            self.logger.warning(f"Directory {sockdir} does not exist, will create it")
            try:
                os.makedirs(sockdir, 0o755)
            except Exception:
                self.logger.exception(f"Error trying to create {sockdir}")
                raise
        if not sockdir.endswith("/"):
            sockdir += "/"
        sockname = self.config.get_with_default("spdk", "rpc_socket_name", "spdk.sock")
        if sockname.find("/") >= 0:
            self.logger.error(f"Invalid SPDK socket name \"{sockname}\". "
                              f"Name should not contain a \"/\".")
            raise RuntimeError("Invalid SPDK socket name.")
        self.spdk_rpc_socket_path = sockdir + sockname
        self.logger.info(f"SPDK Socket: {self.spdk_rpc_socket_path}")
        spdk_tgt_cmd_extra_args = self.config.get_with_default(
            "spdk", "tgt_cmd_extra_args", "")
        cmd = [spdk_tgt_path, "-u", "-r", self.spdk_rpc_socket_path]

        iobuf_options = self.config.get_with_default("spdk", "iobuf_options", "")
        max_subsystems = self.config.getint_with_default("gateway",
                                                         "max_subsystems",
                                                         GatewayService.MAX_SUBSYSTEMS_DEFAULT)
        if iobuf_options or max_subsystems > 0:
            waiting_for_rpc = True
            cmd += ["--wait-for-rpc"]

        # Add extra args from the conf file
        if spdk_tgt_cmd_extra_args:
            cmd += shlex.split(spdk_tgt_cmd_extra_args)

        # No huge pages configuration controlled by spdk.mem_size conf option
        spdk_memsize = self.config.getint_with_default("spdk", "mem_size", None)
        if spdk_memsize:
            self.logger.info(f"SPDK will not use huge pages, mem size: {spdk_memsize}")
            cmd += ["--no-huge", "-s", str(spdk_memsize)]
        else:
            self.logger.info("SPDK will use huge pages, probing...")
            self.probe_huge_pages()

        # If not provided in configuration,
        # calculate cpu mask available for spdk reactors
        if not cpumask_set(cmd):
            cpu_mask = f"-m {int_to_bitmask(min(4, os.cpu_count()))}"
            self.logger.info(f"SPDK autodetecting cpu_mask: {cpu_mask}")
            cmd += shlex.split(cpu_mask)

        self.spdk_log_file = None
        self.spdk_log_file_path = None
        log_stderr = None
        log_file_dir = self.config.get_with_default("spdk", "log_file_dir", None)
        self.spdk_log_file_path = self.handle_process_output_file(log_file_dir, "spdk")
        if self.spdk_log_file_path:
            try:
                self.spdk_log_file = open(self.spdk_log_file_path, "wt")
                log_stderr = subprocess.STDOUT
            except Exception:
                pass
        self.logger.info(f"Starting {' '.join(cmd)}")
        try:
            # start spdk process

            # This isleep()is a temporary hack, we have a timing issue here.
            # Once we solve it the sleep will be removed
            time.sleep(2)
            self.spdk_process = subprocess.Popen(cmd, stdout=self.spdk_log_file, stderr=log_stderr)
        except Exception:
            self.logger.exception("Unable to start SPDK")
            raise

        # Initialization
        timeout = self.config.getfloat_with_default("spdk", "timeout", 60.0)
        protocol_log_level = self.config.get_with_default("spdk", "protocol_log_level", None)
        if not protocol_log_level or not protocol_log_level.strip():
            protocol_log_level = self.config.get_with_default("spdk", "log_level", "WARNING")
        if not protocol_log_level or not protocol_log_level.strip():
            protocol_log_level = "WARNING"
        else:
            protocol_log_level = protocol_log_level.strip().upper()
        # connect timeout: spdk client retries 5 times per sec
        conn_retries = int(timeout * 5)
        self.logger.info(f"SPDK process id: {self.spdk_process.pid}")
        if self.spdk_log_file and self.spdk_log_file_path:
            self.logger.info(f"SPDK log file is {self.spdk_log_file_path}")
        self.logger.info(
            f"Attempting to initialize SPDK: rpc_socket: {self.spdk_rpc_socket_path},"
            f" conn_retries: {conn_retries}, timeout: {timeout}, log level: {protocol_log_level}"
        )
        try:
            self.spdk_rpc_client = rpc_client.JSONRPCClient(
                self.spdk_rpc_socket_path,
                None,
                timeout,
                log_level=protocol_log_level,
                conn_retries=conn_retries,
            )

            # Set max subsystems
            if max_subsystems > 0:
                self._set_max_subsystems(max_subsystems)

            # Initialize pool and buffer sizes
            if iobuf_options:
                self._initialize_iobuf_options(iobuf_options)

            if waiting_for_rpc:
                self._initialize_framework()

            self.spdk_rpc_ping_client = rpc_client.JSONRPCClient(
                self.spdk_rpc_socket_path,
                None,
                timeout,
                log_level=protocol_log_level,
                conn_retries=conn_retries,
            )
            self.spdk_rpc_subsystems_client = rpc_client.JSONRPCClient(
                self.spdk_rpc_socket_path,
                None,
                timeout,
                log_level=protocol_log_level,
                conn_retries=conn_retries,
            )
        except Exception:
            self.logger.exception("Unable to initialize SPDK")
            raise

        # Implicitly create transports
        spdk_transports = self.config.get_with_default("spdk", "transports",
                                                       "tcp")
        for trtype in spdk_transports.split():
            self._create_transport(trtype.lower())

        try:
            return_version = spdk.rpc.spdk_get_version(self.spdk_rpc_client)
            try:
                version_string = return_version["version"]
                self.logger.info(f"Started SPDK with version \"{version_string}\"")
            except KeyError:
                self.logger.error(f"Can't find SPDK version string in {return_version}")
        except Exception:
            self.logger.exception("Can't read SPDK version")
            pass

    def _stop_subprocess(self, proc, timeout):
        """Stops SPDK process."""
        assert proc is not None                # should be verified by the caller

        return_code = proc.returncode

        # Terminate spdk process
        if return_code is not None:
            self.logger.error(f"{self.name} pid {proc.pid} "
                              f"already terminated, exit code: {return_code}")
        else:
            self.logger.info(f"Terminating sub process of ({self.name}) "
                             f"pid {proc.pid} args {proc.args} ...")
            proc.terminate()

        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.logger.exception(f"({self.name}) pid {proc.pid} "
                                  f"timeout occurred while terminating sub process:")
            proc.kill()        # kill -9, send KILL signal

    def _stop_monitor_client(self):
        """Stops Monitor client."""
        timeout = self.config.getfloat_with_default("monitor", "timeout", 1.0)
        self._stop_subprocess(self.monitor_client_process, timeout)
        self.monitor_client_process = None
        if self.monitor_client_log_file:
            try:
                self.monitor_client_log_file.close()
            except Exception:
                pass
            self.monitor_client_log_file = None
        if self.monitor_client_log_file_path:
            GatewayLogger.compress_file(self.monitor_client_log_file_path,
                                        f"{self.monitor_client_log_file_path}.gz")
            self.monitor_client_log_file_path = None

    def _stop_spdk(self):
        """Stops SPDK process."""
        # Terminate spdk process
        timeout = self.config.getfloat_with_default("spdk", "timeout", 60.0)
        self._stop_subprocess(self.spdk_process, timeout)
        self.spdk_process = None
        if self.spdk_log_file:
            try:
                self.spdk_log_file.close()
            except Exception:
                pass
            self.spdk_log_file = None
        if self.spdk_log_file_path:
            GatewayLogger.compress_file(self.spdk_log_file_path, f"{self.spdk_log_file_path}.gz")
            self.spdk_log_file_path = None

        # Clean spdk rpc socket
        if self.spdk_rpc_socket_path and os.path.exists(self.spdk_rpc_socket_path):
            try:
                os.remove(self.spdk_rpc_socket_path)
            except Exception:
                self.logger.exception(f"An error occurred while removing RPC "
                                      f"socket {self.spdk_rpc_socket_path}")

    def _terminate_discovery(self, pid):
        def is_running(pid):
            try:
                os.kill(pid, 0)
                return True
            except ProcessLookupError:
                return False
            except PermissionError:
                self.logger.exception(f"Permission denied when checking status of discovery {pid}")
                return True

        def wait_for_exit(pid):
            WAIT_INTERVAL_SEC = 0.1
            MAX_WAIT_ATTEMPTS = 10
            for _ in range(MAX_WAIT_ATTEMPTS):
                if not is_running(pid):
                    return True
                time.sleep(WAIT_INTERVAL_SEC)
            return False

        try:
            # discovery service selector loop should exit
            # due to KeyboardInterrupt exception on SIGINT signal
            signals = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]
            for sig in signals:
                self.logger.info(f"Sending signal {sig.name} to Discovery service process {pid}")
                os.kill(pid, sig)
                if wait_for_exit(pid):
                    return True
            self.logger.warning(
                f"Discovery service process {pid} did not exit after all signals."
            )
            return False

        except ProcessLookupError:
            self.logger.info(f"Discovery service process {pid} already exited.")
            return True
        except Exception:
            self.logger.exception(f"Error terminating discovery service process {pid}")
            return False

    def _stop_discovery(self):
        """Stops Discovery service process."""
        assert self.discovery_pid is not None             # should be verified by the caller

        self.logger.info("Terminating discovery service...")
        if self._terminate_discovery(self.discovery_pid):
            self.logger.info("Discovery service terminated")

        self.discovery_pid = None

    def _set_max_subsystems(self, max_subsystems):
        """Sets SPDK's max subsystems attribute."""

        try:
            rpc_nvmf.nvmf_set_max_subsystems(self.spdk_rpc_client, max_subsystems=max_subsystems)
        except Exception:
            self.logger.exception(f"Failure setting max subsystems {max_subsystems}")
            pass

    def _initialize_iobuf_options(self, options):
        """Initialize pool and buffer sizes."""

        args = {}
        self.logger.debug(f"initialize_iobuf_options: options: {options}")

        try:
            args.update(json.loads(options))
        except json.decoder.JSONDecodeError:
            self.logger.exception(f"Failed to parse spdk iobuf_options ({options})")
            return

        try:
            rpc_iobuf.iobuf_set_options(self.spdk_rpc_client, **args)
        except Exception:
            self.logger.exception("IObuf set options returned with error")
            pass

    def _initialize_framework(self):
        """In case we started SPDK with the "wait for rpc" flag, we need to call this"""

        try:
            spdk.rpc.framework_start_init(self.spdk_rpc_client)
        except Exception:
            self.logger.exception("Framework start init returned with error")
            pass

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
            rpc_nvmf.nvmf_create_transport(self.spdk_rpc_client, **args)
        except Exception:
            self.logger.exception(f"Create Transport {trtype} returned with error")
            raise

    def set_gateway_exit_message(self, msg):
        with self.system_exit_message_lock:
            self.system_exit_message = msg
        return self.gateway_exit_started.wait(GatewayServer.MAX_TIME_TO_WAIT_FOR_GATEWAY_EXIT)

    def exit_gateway_if_needed(self):
        exit_msg = None
        with self.system_exit_message_lock:
            exit_msg = self.system_exit_message
        if exit_msg is not None:
            self.logger.error(f"System exit message was set to {exit_msg}")
            self.gateway_exit_started.set()
            raise SystemExit(exit_msg)

    def keep_alive(self):
        """Continuously confirms communication with SPDK process."""
        allowed_consecutive_spdk_ping_failures = self.config.getint_with_default(
            "gateway",
            "allowed_consecutive_spdk_ping_failures",
            1)
        spdk_ping_interval_in_seconds = self.config.getfloat_with_default(
            "gateway",
            "spdk_ping_interval_in_seconds",
            2.0)
        if spdk_ping_interval_in_seconds < 0.0:
            self.logger.warning(f"Invalid SPDK ping interval "
                                f"{spdk_ping_interval_in_seconds}, will reset to 0")
            spdk_ping_interval_in_seconds = 0.0

        consecutive_ping_failures = 0
        # we spend 1 second waiting for server termination so subtract it from ping interval
        if spdk_ping_interval_in_seconds >= 1.0:
            spdk_ping_interval_in_seconds -= 1.0
        else:
            spdk_ping_interval_in_seconds = 0.0

        while True:
            self.exit_gateway_if_needed()
            if self.gateway_rpc:
                if self.gateway_rpc.rebalance.rebalance_event.is_set():
                    self.logger.critical("Failure in rebalance, aborting")
                    raise SystemExit("Failure in rebalance, quitting gateway")
            timedout = self.server.wait_for_termination(timeout=1)
            if not timedout:
                break
            if spdk_ping_interval_in_seconds > 0.0:
                time.sleep(spdk_ping_interval_in_seconds)
            alive = self._ping()
            if not alive:
                consecutive_ping_failures += 1
                if consecutive_ping_failures >= allowed_consecutive_spdk_ping_failures:
                    self.logger.critical(f"SPDK ping failed {consecutive_ping_failures} "
                                         f"times, aborting")
                    raise SystemExit("SPDK ping failed, quitting gateway")
                else:
                    self.logger.warning(f"SPDK ping failed {consecutive_ping_failures} "
                                        f"times, will keep trying")
            else:
                consecutive_ping_failures = 0

    def _ping(self):
        """Confirms communication with SPDK process."""
        try:
            spdk.rpc.spdk_get_version(self.spdk_rpc_ping_client)
            return True
        except Exception:
            self.logger.exception("spdk_get_version failed")
            return False

    def probe_huge_pages(self):
        """Probe kernel's huge pages confiuguration"""
        requested_hugepages_val = os.getenv("HUGEPAGES", "")
        if not requested_hugepages_val:
            self.logger.warning("Can't get requested huge pages count")
        else:
            requested_hugepages_val = requested_hugepages_val.strip()
            try:
                requested_hugepages_val = int(requested_hugepages_val)
                self.logger.info(f"Requested huge pages count is {requested_hugepages_val}")
            except ValueError:
                self.logger.warning(f"Requested huge pages count value "
                                    f"{requested_hugepages_val} is not numeric")
                requested_hugepages_val = None
        hugepages_file = os.getenv("HUGEPAGES_DIR", "")
        if not hugepages_file:
            hugepages_file = "/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
            self.logger.warning(f"No huge pages file defined, will use {hugepages_file}")
        else:
            hugepages_file = hugepages_file.strip()
        if os.access(hugepages_file, os.F_OK):
            try:
                hugepages_val = ""
                with open(hugepages_file) as f:
                    hugepages_val = f.readline()
                hugepages_val = hugepages_val.strip()
                if hugepages_val:
                    try:
                        hugepages_val = int(hugepages_val)
                        self.logger.info(f"Actual huge pages count is {hugepages_val}")
                    except ValueError:
                        self.logger.warning(f"Actual huge pages count value "
                                            f"{hugepages_val} is not numeric")
                        hugepages_val = ""
                    if requested_hugepages_val:
                        if hugepages_val != "" and requested_hugepages_val > hugepages_val:
                            self.logger.warning(f"The actual huge page count "
                                                f"{hugepages_val} is smaller than the requested "
                                                f"value of {requested_hugepages_val}")
                else:
                    self.logger.warning(f"Can't read actual huge pages count value "
                                        f"from {hugepages_file}")
            except Exception:
                self.logger.exception(f"Can't read actual huge pages count "
                                      f"value from {hugepages_file}")
        else:
            self.logger.warning(f"Can't find huge pages file {hugepages_file}")

    def gateway_rpc_caller(self, requests, is_add_req):
        """Passes RPC requests to gateway service."""
        for key, val in requests.items():
            if key.startswith(GatewayState.SUBSYSTEM_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_subsystem_req(),
                                            ignore_unknown_fields=True)
                    if req.key_encrypted and req.dhchap_key:
                        dhchap_key_decrypted = None
                        if self.crypto:
                            dhchap_key_decrypted = self.crypto.decrypt_text(req.dhchap_key)
                            req.key_encrypted = False
                        if dhchap_key_decrypted:
                            req.dhchap_key = dhchap_key_decrypted
                        else:
                            # TODO: raise an alert
                            self.logger.warning(f"No encryption key or the wrong key was found "
                                                f"but we need to decrypt a subsystem "
                                                f"DH-HMAC-CHAP key. Any attempt to add host "
                                                f"access using a DH-HMAC-CHAP key to subsystem "
                                                f"{req.subsystem_nqn} would fail")
                            req.dhchap_key = GatewayUtilsCrypto.INVALID_KEY_VALUE
                            req.key_encrypted = False
                    self.gateway_rpc.create_subsystem(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.delete_subsystem_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.delete_subsystem(req)
            elif key.startswith(GatewayState.NAMESPACE_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_add_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_add(req)
                else:
                    req = json_format.Parse(val,
                                            pb2.namespace_delete_req(),
                                            ignore_unknown_fields=True)
                    req.i_am_sure = True
                    self.gateway_rpc.namespace_delete(req)
            elif key.startswith(GatewayState.NAMESPACE_QOS_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_set_qos_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_set_qos_limits(req)
                else:
                    # Do nothing, this is covered by the delete namespace code
                    pass
            elif key.startswith(GatewayState.HOST_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.add_host_req(),
                                            ignore_unknown_fields=True)
                    if req.key_encrypted and req.dhchap_key:
                        dhchap_key_decrypted = None
                        if self.crypto:
                            dhchap_key_decrypted = self.crypto.decrypt_text(req.dhchap_key)
                            req.key_encrypted = False
                        if dhchap_key_decrypted:
                            req.dhchap_key = dhchap_key_decrypted
                        else:
                            # TODO: raise an alert
                            self.logger.warning(f"No encryption key or the wrong key was found "
                                                f"but we need to decrypt host {req.host_nqn} "
                                                f"DH-HMAC-CHAP key")
                            req.dhchap_key = GatewayUtilsCrypto.INVALID_KEY_VALUE
                            req.key_encrypted = False
                    if req.psk_encrypted and req.psk:
                        psk_decrypted = None
                        if self.crypto:
                            psk_decrypted = self.crypto.decrypt_text(req.psk)
                            req.psk_encrypted = False
                        if psk_decrypted:
                            req.psk = psk_decrypted
                        else:
                            # TODO: raise an alert
                            self.logger.warning(f"No encryption key or the wrong key was found "
                                                f"but we need to decrypt host {req.host_nqn} "
                                                f"PSK key")
                            req.psk = GatewayUtilsCrypto.INVALID_KEY_VALUE
                            req.psk_encrypted = False
                    self.gateway_rpc.add_host(req)
                else:
                    req = json_format.Parse(val, pb2.remove_host_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.remove_host(req)
            elif key.startswith(GatewayState.LISTENER_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.create_listener_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.create_listener(req)
                else:
                    req = json_format.Parse(val, pb2.delete_listener_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.delete_listener(req)
            elif key.startswith(GatewayState.NAMESPACE_LB_GROUP_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_change_load_balancing_group_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_change_load_balancing_group(req)
            elif key.startswith(GatewayState.NAMESPACE_VISIBILITY_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_change_visibility_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_change_visibility(req)
            elif key.startswith(GatewayState.NAMESPACE_TRASH_IMAGE_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_set_rbd_trash_image_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_set_rbd_trash_image(req)
            elif key.startswith(GatewayState.NAMESPACE_HOST_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.namespace_add_host_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_add_host(req)
                else:
                    req = json_format.Parse(val, pb2.namespace_delete_host_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.namespace_delete_host(req)
            elif key.startswith(GatewayState.HOST_KEY_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.change_host_key_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.change_host_key(req)
            elif key.startswith(GatewayState.SUBSYSTEM_KEY_PREFIX):
                if is_add_req:
                    req = json_format.Parse(val, pb2.change_subsystem_key_req(),
                                            ignore_unknown_fields=True)
                    self.gateway_rpc.change_subsystem_key(req)
