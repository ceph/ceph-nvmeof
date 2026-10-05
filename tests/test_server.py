import copy
import pytest
import time
import re
import signal
import os
import socket
import threading
import unittest
from control.server import GatewayServer


class TestServer(unittest.TestCase):
    # Location of core files in test env.
    core_dir = "/tmp/coredump"

    @pytest.fixture(autouse=True)
    def _config(self, config):
        self.config = config

    def validate_exception(self, e):
        pattern = r'Gateway subprocess terminated pid=(\d+) exit_code=(-?\d+)'
        m = re.match(pattern, e.code)
        assert m
        pid = int(m.group(1))
        code = int(m.group(2))
        assert pid > 0
        assert code == 0 or code

    def remove_core_files(self, directory_path):
        # List all files starting with "core." in the core directory
        files = [
            f for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f)) and f.startswith("core.")
        ]

        # Remove each matching file
        for f in files:
            file_path = os.path.join(directory_path, f)
            os.remove(file_path)
            print(f"Removed: {file_path}")

    def assert_no_core_files(self, directory_path):
        assert os.path.exists(directory_path) and os.path.isdir(directory_path)
        files = [
            f for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f)) and f.startswith("core.")
        ]
        assert len(files) == 0

    def test_spdk_exception(self):
        """Tests spdk sub process exiting with error."""
        config_spdk_exception = copy.deepcopy(self.config)

        # invalid arg, spdk would exit with code 1 at start up
        config_spdk_exception.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-10000000)"

        with self.assertRaises(SystemExit) as cm:
            with GatewayServer(config_spdk_exception) as gateway:
                gateway.set_group_id(0)
                gateway.serve()
        self.validate_exception(cm.exception)

    def test_no_coredumps_on_gracefull_shutdown(self):
        """Tests gateway's sub processes do not dump cores on gracefull shutdown."""
        with GatewayServer(copy.deepcopy(self.config)) as gateway:
            gateway.set_group_id(0)
            gateway.serve()
            time.sleep(10)
        # exited context, sub processes should terminate gracefully
        time.sleep(10)     # let it dump
        self.assert_no_core_files(self.core_dir)

    def test_discovery_exit(self):
        """Tests discovery service sub process exiting when restarting it is disabled."""
        test_config = copy.deepcopy(self.config)
        test_config.config["discovery"]["restart_attempts_limit"] = "0"
        signals = [signal.SIGABRT, signal.SIGTERM, signal.SIGKILL, signal.SIGINT]

        for sig in signals:
            with self.assertRaises(SystemExit) as cm:
                with GatewayServer(test_config) as gateway:
                    gateway.set_group_id(0)
                    gateway.serve()

                    # Give the gateway some time to start
                    time.sleep(17)

                    # Send signal to the discovery service process
                    assert gateway.discovery_pid
                    os.kill(gateway.discovery_pid, sig)

                    # Block on running keep alive ping
                    gateway.keep_alive()

            # Assert error exit code
            self.validate_exception(cm.exception)

            # Clean up cores
            self.remove_core_files(self.core_dir)

    def connect_to_discovery(self, config, timeout):
        """Waits until we can open a connection to the discovery service."""
        addr = config.get("discovery", "addr")
        if addr == "0.0.0.0":
            addr = "127.0.0.1"
        elif addr == "::":
            addr = "::1"
        port = config.getint("discovery", "port")
        end_time = time.monotonic() + timeout
        while True:
            try:
                with socket.create_connection((addr, port), timeout=1):
                    return
            except OSError:
                if time.monotonic() >= end_time:
                    raise
                time.sleep(0.5)

    def test_discovery_restart(self):
        """Tests discovery service sub process is restarted, until there are too many failures."""
        test_config = copy.deepcopy(self.config)
        test_config.config["discovery"]["restart_attempts_limit"] = "1"
        test_config.config["discovery"]["restart_interval"] = "1"

        # As the gateway isn't defined in the monitor, the monitor client aborts after about
        # 30 seconds, so the whole test should take less than that
        with GatewayServer(test_config) as gateway:
            gateway.set_group_id(0)
            gateway.serve()

            self.connect_to_discovery(test_config, 10)
            first_pid = gateway.discovery_pid
            assert first_pid
            os.kill(first_pid, signal.SIGKILL)

            # Run keep alive until the discovery service is restarted
            def stop_server_after_restart():
                end_time = time.monotonic() + 8
                while time.monotonic() < end_time:
                    if gateway.discovery_pid not in (None, first_pid):
                        break
                    time.sleep(0.2)
                gateway.server.stop(None)

            stopper = threading.Thread(target=stop_server_after_restart)
            stopper.start()
            gateway.keep_alive()
            stopper.join()

            second_pid = gateway.discovery_pid
            assert second_pid
            assert second_pid != first_pid
            assert gateway.discovery_consecutive_failures == 1

            # The restarted discovery service should accept connections. The restart forks
            # the gateway while its other threads are running, so a new PID is not enough.
            self.connect_to_discovery(test_config, 5)
            assert gateway.discovery_pid == second_pid

            # A second consecutive failure is above the limit, the gateway should quit
            with self.assertRaises(SystemExit) as cm:
                os.kill(second_pid, signal.SIGKILL)
                time.sleep(10)
            self.validate_exception(cm.exception)

        self.remove_core_files(self.core_dir)

    def test_monc_exit(self):
        """Tests monitor client sub process abort."""
        config_monc_abort = copy.deepcopy(self.config)
        signals = [signal.SIGABRT, signal.SIGTERM, signal.SIGKILL]

        for sig in signals:
            with self.assertRaises(SystemExit) as cm:
                with GatewayServer(config_monc_abort) as gateway:
                    gateway.set_group_id(0)
                    gateway.serve()

                    # Give the gateway some time to start
                    time.sleep(2)

                    # Send SIGABRT (abort signal) to the monitor client process
                    assert gateway.monitor_client_process
                    gateway.monitor_client_process.send_signal(signal.SIGABRT)

                    # Block on running keep alive ping
                    gateway.keep_alive()

            # Assert error exit code
            self.validate_exception(cm.exception)

            # Clean up monc core
            self.remove_core_files(self.core_dir)

    def test_spdk_multi_gateway_exception(self):
        """Tests spdk sub process exiting with error, in multi gateway configuration."""
        configA = copy.deepcopy(self.config)
        configA.config["gateway"]["name"] = "GatewayA"
        configA.config["gateway"]["group"] = "Group1"

        configB = copy.deepcopy(configA)
        configB.config["gateway"]["name"] = "GatewayB"
        configB.config["gateway"]["port"] = str(configA.getint("gateway", "port") + 1)
        configB.config["spdk"]["rpc_socket_name"] = "spdk_GatewayB.sock"
        # invalid arg, spdk would exit with code 1 at start up
        configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x343435545"

        with self.assertRaises(SystemExit) as cm:
            with GatewayServer(configA) as gatewayA, GatewayServer(configB) as gatewayB:
                gatewayA.set_group_id(0)
                gatewayA.serve()
                gatewayB.set_group_id(1)
                gatewayB.serve()
        self.validate_exception(cm.exception)


if __name__ == '__main__':
    unittest.main()
