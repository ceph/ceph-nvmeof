import copy
import pytest
import time
import re
import signal
import os
import unittest
from control.server import GatewayServer

class TestServer(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _config(self, config):
        self.config = config

    def validate_exception(self, e):
        pattern = r'Gateway subprocess terminated pid=(\d+) exit_code=(-?\d+)'
        m = re.match(pattern, e.code)
        assert(m)
        pid = int(m.group(1))
        code = int(m.group(2))
        assert(pid > 0)
        assert(code)

    def assert_no_core_files(self, directory_path):
        assert(os.path.exists(directory_path) and os.path.isdir(directory_path))
        files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f)) and f.startswith("core.")]
        assert(len(files) == 0)

    def test_spdk_exception(self):
        """Tests spdk sub process exiting with error."""
        config_spdk_exception = copy.deepcopy(self.config)

        # invalid arg, spdk would exit with code 1 at start up
        config_spdk_exception.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x343435545"

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
        time.sleep(10) # let it dump
        self.assert_no_core_files("/tmp/coredump")

    def test_monc_abort(self):
        """Tests monitor client sub process abort."""
        config_monc_abort = copy.deepcopy(self.config)

        with self.assertRaises(SystemExit) as cm:
            with GatewayServer(config_monc_abort) as gateway:
                gateway.set_group_id(0)
                gateway.serve()

                # Give the gateway some time to start
                time.sleep(2)

                # Send SIGABRT (abort signal) to the monitor client process
                assert(gateway.monitor_client_process)
                gateway.monitor_client_process.send_signal(signal.SIGABRT)
                gateway.keep_alive()

        self.validate_exception(cm.exception)

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
            with (
                GatewayServer(configA) as gatewayA,
                GatewayServer(configB) as gatewayB,
             ):
                gatewayA.set_group_id(0)
                gatewayA.serve()
                gatewayB.set_group_id(1)
                gatewayB.serve()
        self.validate_exception(cm.exception)

if __name__ == '__main__':
    unittest.main()
