import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli

image = "mytestdevimage"
pool = "rbd"
bdev = "Ceph0"
subsystem = "nqn.2016-06.io.spdk:cnode1"
serial = "SPDK00000000000001"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
nsid = "1"
trtype = "TCP"
gateway_name = socket.gethostname()
addr = "127.0.0.1"
listener_list = [["-g", gateway_name, "-a", addr, "-s", "5001"], ["-g", gateway_name, "-a", addr,"-s", "5002"]]
config = "ceph-nvmeof.conf"

@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    # Start gateway
    gateway = GatewayServer(config)
    gateway.serve()

    yield

    # Stop gateway
    gateway.server.stop(grace=1)
    gateway.gateway_rpc.gateway_state.delete_state()

class TestGet:
    def test_get_subsystems(self, caplog, gateway):
        cli(["--server-address", "localhost", "get_subsystems"])
        assert "Failed to get" not in caplog.text


class TestCreate:
    def test_create_bdev(self, caplog, gateway):
        cli(["--server-address", "localhost", "create_bdev", "-i", image, "-p", pool, "-b", bdev])
        assert "Failed to create" not in caplog.text

    def test_create_subsystem(self, caplog, gateway):
        cli(["--server-address", "localhost", "create_subsystem", "-n", subsystem, "-s", serial])
        assert "Failed to create" not in caplog.text

    def test_add_namespace(self, caplog, gateway):
        cli(["--server-address", "localhost", "add_namespace", "-n", subsystem, "-b", bdev])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        cli(["--server-address", "localhost", "add_host", "-n", subsystem, "-t", host])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener, gateway):
        cli(["--server-address", "localhost", "create_listener", "-n", subsystem] + listener)
        assert "Failed to create" not in caplog.text


class TestDelete:
    @pytest.mark.parametrize("host", host_list)
    def test_remove_host(self, caplog, host, gateway):
        cli(["--server-address", "localhost", "remove_host", "-n", subsystem, "-t", host])
        assert "Failed to remove" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener, gateway):
        cli(["--server-address", "localhost", "delete_listener", "-n", subsystem] + listener)
        assert "Failed to delete" not in caplog.text

    def test_remove_namespace(self, caplog, gateway):
        cli(["--server-address", "localhost", "remove_namespace", "-n", subsystem, "-i", nsid])
        assert "Failed to remove" not in caplog.text

    def test_delete_bdev(self, caplog, gateway):
        cli(["--server-address", "localhost", "delete_bdev", "-b", bdev])
        assert "Failed to delete" not in caplog.text

    def test_delete_subsystem(self, caplog, gateway):
        cli(["--server-address", "localhost", "delete_subsystem", "-n", subsystem])
        assert "Failed to delete" not in caplog.text
