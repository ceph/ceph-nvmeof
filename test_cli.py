import pytest
import socket
from nvme_gw_cli import main as cli

image = "iscsidevimage"
pool = "rbd"
bdev = "Ceph0"
subsystem = "nqn.2016-06.io.spdk:cnode1"
serial = "SPDK00000000000001"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
nsid = "1"
trtype = "TCP"
gateway_name = socket.gethostname()
addr = "127.0.0.1"
listener_list = [["-g", gateway_name, "-a", addr, "-s", "5001"], ["-s", "5002"]]
config = "nvme_gw.config"

class TestGet:
    def test_get_subsystems(self, caplog):
        cli(["-c", config, "get_subsystems"])
        assert "Failed to get" not in caplog.text

class TestCreate:
    def test_create_bdev(self, caplog):
        cli(["-c", config, "create_bdev", "-i", image, "-p", pool, "-b", bdev])
        assert "Failed to create" not in caplog.text

    def test_create_subsystem(self, caplog):
        cli(["-c", config, "create_subsystem", "-n", subsystem, "-s", serial])
        assert "Failed to create" not in caplog.text

    def test_create_namespace(self, caplog):
        cli(["-c", config, "create_namespace", "-n", subsystem, "-b", bdev])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        cli(["-c", config, "add_host", "-n", subsystem, "-t", host])
        assert "Failed to add" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener):
        cli(["-c", config, "create_listener", "-n", subsystem] + listener)
        assert "Failed to create" not in caplog.text

class TestDelete:
    @pytest.mark.parametrize("host", host_list)
    def test_delete_host(self, caplog, host):
        cli(["-c", config, "delete_host", "-n", subsystem, "-t", host])
        assert "Failed to remove" not in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener):
        cli(["-c", config, "delete_listener", "-n", subsystem] + listener)
        assert "Failed to delete" not in caplog.text

    def test_delete_namespace(self, caplog):
        cli(["-c", config, "delete_namespace", "-n", subsystem, "-i", nsid])
        assert "Failed to remove" not in caplog.text

    def test_delete_bdev(self, caplog):
        cli(["-c", config, "delete_bdev", "-b", bdev])
        assert "Failed to delete" not in caplog.text

    def test_delete_subsystem(self, caplog):
        cli(["-c", config, "delete_subsystem", "-n", subsystem])
        assert "Failed to delete" not in caplog.text
