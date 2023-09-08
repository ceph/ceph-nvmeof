import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli

image = "mytestdevimage"
pool = "rbd"
bdev = "Ceph0"
bdev1 = "Ceph1"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
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

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.serve()
        yield

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.state.spdk.obj.delete()
        gateway.gateway_rpc.gateway_state.state.ranges.obj.delete()

class TestGet:
    def test_get_subsystems(self, caplog, gateway):
        cli(["get_subsystems"])
        assert "Failed to get" not in caplog.text


class TestCreate:
    def test_create_bdev(self, caplog, gateway):
        cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev])
        assert "Failed to create" not in caplog.text
        cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev1])
        assert "Failed to create" not in caplog.text

    def test_create_subsystem(self, caplog, gateway):
        cli(["create_subsystem", "-n", subsystem])
        assert "Failed to create" not in caplog.text
        cli(["get_subsystems"])
        assert serial not in caplog.text
        cli(["create_subsystem", "-n", subsystem2, "-s", serial])
        assert "Failed to create" not in caplog.text
        cli(["get_subsystems"])
        assert serial in caplog.text

    def test_add_namespace(self, caplog, gateway):
        cli(["add_namespace", "-n", subsystem, "-b", bdev])
        assert "Failed to add" not in caplog.text
        cli(["add_namespace", "-n", subsystem, "-b", bdev1])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        cli(["add_host", "-n", subsystem, "-t", host])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener, gateway):
        cli(["create_listener", "-n", subsystem] + listener)
        assert "Failed to create" not in caplog.text


class TestDelete:
    @pytest.mark.parametrize("host", host_list)
    def test_remove_host(self, caplog, host, gateway):
        cli(["remove_host", "-n", subsystem, "-t", host])
        assert "Failed to remove" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener, gateway):
        cli(["delete_listener", "-n", subsystem] + listener)
        assert "Failed to delete" not in caplog.text

    def test_remove_namespace(self, caplog, gateway):
        cli(["remove_namespace", "-n", subsystem, "-i", nsid])
        assert "Failed to remove" not in caplog.text

    def test_delete_bdev(self, caplog, gateway):
        cli(["delete_bdev", "-b", bdev, "-f"])
        assert "Failed to delete" not in caplog.text
        cli(["delete_bdev", "-b", bdev1, "--force"])
        assert "Failed to delete" not in caplog.text

    def test_delete_subsystem(self, caplog, gateway):
        cli(["delete_subsystem", "-n", subsystem])
        assert "Failed to delete" not in caplog.text
        cli(["delete_subsystem", "-n", subsystem2])
        assert "Failed to delete" not in caplog.text
