import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
import spdk.rpc.bdev as rpc_bdev

image = "mytestdevimage"
pool = "rbd"
bdev = "Ceph0"
bdev1 = "Ceph1"
bdev_ipv6 = bdev + "_ipv6"
bdev1_ipv6 = bdev1 + "_ipv6"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
serial = "SPDK00000000000001"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
nsid = "1"
nsid_ipv6 = "3"
anagrpid = "2"
trtype = "TCP"
gateway_name = socket.gethostname()
addr = "127.0.0.1"
addr_ipv6 = "::1"
server_addr_ipv6 = "2001:db8::3"
listener_list = [["-g", gateway_name, "-a", addr, "-s", "5001"], ["-g", gateway_name, "-a", addr,"-s", "5002"]]
listener_list_ipv6 = [["-g", gateway_name, "-a", addr_ipv6, "-s", "5003"], ["-g", gateway_name, "-a", addr_ipv6, "-s", "5004"]]
config = "ceph-nvmeof.conf"

@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.serve()
        yield gateway.gateway_rpc

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()

class TestGet:
    def test_get_subsystems(self, caplog, gateway):
        caplog.clear()
        cli(["get_subsystems"])
        assert "[]" in caplog.text

    def test_get_subsystems_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "get_subsystems"])
        assert "[]" in caplog.text


class TestCreate:
    def test_create_bdev(self, caplog, gateway):
        gw = gateway
        bdev_found = False
        caplog.clear()
        cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev])
        assert f"Created bdev {bdev}: True" in caplog.text
        bdev_list = rpc_bdev.bdev_get_bdevs(gw.spdk_rpc_client)
        for onedev in bdev_list:
            if onedev["name"] == bdev:
                bdev_found = True
                assert onedev["block_size"] == 512
                break
        assert bdev_found
        caplog.clear()
        bdev_found = False
        cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev1, "-s", "1024"])
        assert f"Created bdev {bdev1}: True" in caplog.text
        bdev_list = rpc_bdev.bdev_get_bdevs(gw.spdk_rpc_client)
        for onedev in bdev_list:
            if onedev["name"] == bdev1:
                bdev_found = True
                assert onedev["block_size"] == 1024
                break
        assert bdev_found

    def test_create_bdev_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "create_bdev", "-i", image, "-p", pool, "-b", bdev_ipv6])
        assert f"Created bdev {bdev_ipv6}: True" in caplog.text
        cli(["--server-address", server_addr_ipv6, "create_bdev", "-i", image, "-p", pool, "-b", bdev1_ipv6])
        assert f"Created bdev {bdev1_ipv6}: True" in caplog.text

    def test_resize_bdev(self, caplog, gateway):
        caplog.clear()
        bdev_found = False
        gw = gateway
        cli(["resize_bdev", "-b", bdev, "-s", "20"])
        assert f"Resized bdev {bdev}: True" in caplog.text
        bdev_list = rpc_bdev.bdev_get_bdevs(gw.spdk_rpc_client)
        for onedev in bdev_list:
            if onedev["name"] == bdev:
                bdev_found = True
                assert onedev["block_size"] == 512
                num_blocks = onedev["num_blocks"]
                # Should be 20M now
                assert num_blocks * 512 == 20971520
                break
        assert bdev_found

    def test_create_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["create_subsystem", "-n", subsystem])
        assert f"Created subsystem {subsystem}: True" in caplog.text
        assert "ana reporting: False" in caplog.text
        cli(["get_subsystems"])
        assert serial not in caplog.text
        caplog.clear()
        cli(["create_subsystem", "-n", subsystem2, "-s", serial])
        assert f"Created subsystem {subsystem2}: True" in caplog.text
        assert "ana reporting: False" in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert serial in caplog.text

    def test_add_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["add_namespace", "-n", subsystem, "-b", bdev])
        assert f"Added namespace 1 to {subsystem}, ANA group id None : True" in caplog.text
        cli(["add_namespace", "-n", subsystem, "-b", bdev1])
        assert f"Added namespace 2 to {subsystem}, ANA group id None : True" in caplog.text

    def test_add_namespace_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "add_namespace", "-n", subsystem, "-b", bdev_ipv6])
        assert f"Added namespace 3 to {subsystem}, ANA group id None : True" in caplog.text
        cli(["--server-address", server_addr_ipv6, "add_namespace", "-n", subsystem, "-b", bdev1_ipv6])
        assert f"Added namespace 4 to {subsystem}, ANA group id None : True" in caplog.text

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        caplog.clear()
        cli(["add_host", "-n", subsystem, "-t", host])
        if host == "*":
            assert f"Allowed open host access to {subsystem}: True" in caplog.text
        else:
            assert f"Added host {host} access to {subsystem}: True" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["create_listener", "-n", subsystem] + listener)
        assert "enable_ha: False" in caplog.text
        assert "ipv4" in caplog.text
        assert f"Created {subsystem} listener at {listener[3]}:{listener[5]}: True" in caplog.text

    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_create_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "create_listener", "-n", subsystem, "--adrfam", "IPV6"] + listener_ipv6)
        assert "enable_ha: False" in caplog.text
        assert "IPV6" in caplog.text
        assert f"Created {subsystem} listener at [{listener_ipv6[3]}]:{listener_ipv6[5]}: True" in caplog.text

class TestDelete:
    @pytest.mark.parametrize("host", host_list)
    def test_remove_host(self, caplog, host, gateway):
        caplog.clear()
        cli(["remove_host", "-n", subsystem, "-t", host])
        if host == "*":
            assert f"Disabled open host access to {subsystem}: True" in caplog.text
        else:
            assert f"Removed host {host} access from {subsystem}: True" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["delete_listener", "-n", subsystem] + listener)
        assert f"Deleted {listener[3]}:{listener[5]} from {subsystem}: True" in caplog.text

    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_delete_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "delete_listener", "-n", subsystem, "--adrfam", "IPV6"] + listener_ipv6)
        assert f"Deleted [{listener_ipv6[3]}]:{listener_ipv6[5]} from {subsystem}: True" in caplog.text

    def test_remove_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["remove_namespace", "-n", subsystem, "-i", nsid])
        assert f"Removed namespace {nsid} from {subsystem}: True" in caplog.text
        cli(["remove_namespace", "-n", subsystem, "-i", nsid_ipv6])
        assert f"Removed namespace {nsid_ipv6} from {subsystem}: True" in caplog.text

    def test_delete_bdev(self, caplog, gateway):
        caplog.clear()
        cli(["delete_bdev", "-b", bdev, "-f"])
        assert f"Deleted bdev {bdev}: True" in caplog.text
        assert "Will remove namespace" not in caplog.text
        caplog.clear()
        # Should fail as there is a namespace using the bdev
        with pytest.raises(Exception) as ex:
            try:
                cli(["delete_bdev", "-b", bdev1])
            except SystemExit as sysex:
                # should fail with non-zero return code
                assert sysex != 0
                pass
            assert "Device or resource busy" in str(ex.value)
        assert f"Namespace 2 from {subsystem} is still using bdev {bdev1}" in caplog.text
        caplog.clear()
        cli(["delete_bdev", "-b", bdev1, "--force"])
        assert f"Deleted bdev {bdev1}: True" in caplog.text
        assert f"Removed namespace 2 from {subsystem}" in caplog.text
        caplog.clear()
        cli(["delete_bdev", "-b", bdev_ipv6, "-f"])
        assert f"Deleted bdev {bdev_ipv6}: True" in caplog.text
        assert "Will remove namespace" not in caplog.text
        caplog.clear()
        cli(["delete_bdev", "-b", bdev1_ipv6, "--force"])
        assert f"Deleted bdev {bdev1_ipv6}: True" in caplog.text
        assert f"Removed namespace 4 from {subsystem}" in caplog.text

    def test_delete_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["delete_subsystem", "-n", subsystem])
        assert f"Deleted subsystem {subsystem}: True" in caplog.text
        caplog.clear()
        cli(["delete_subsystem", "-n", subsystem2])
        assert f"Deleted subsystem {subsystem2}: True" in caplog.text

class TestCreateWithAna:
    def test_create_bdev_ana(self, caplog, gateway):
        caplog.clear()
        cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev])
        assert f"Created bdev {bdev}: True" in caplog.text

    def test_create_bdev_ana_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "create_bdev", "-i", image, "-p", pool, "-b", bdev_ipv6])
        assert f"Created bdev {bdev_ipv6}: True" in caplog.text

    def test_create_subsystem_ana(self, caplog, gateway):
        caplog.clear()
        cli(["create_subsystem", "-n", subsystem, "-a", "-t"])
        assert f"Created subsystem {subsystem}: True" in caplog.text
        assert "ana reporting: True" in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert serial not in caplog.text

    def test_add_namespace_ana(self, caplog, gateway):
        caplog.clear()
        cli(["add_namespace", "-n", subsystem, "-b", bdev, "-a", anagrpid])
        assert f"Added namespace 1 to {subsystem}, ANA group id {anagrpid}" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["create_listener", "-n", subsystem] + listener)
        assert "enable_ha: True" in caplog.text
        assert "ipv4" in caplog.text
        assert f"Created {subsystem} listener at {listener[3]}:{listener[5]}: True" in caplog.text


class TestDeleteAna:

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["delete_listener", "-n", subsystem] + listener)
        assert f"Deleted {listener[3]}:{listener[5]} from {subsystem}: True" in caplog.text

    def test_remove_namespace_ana(self, caplog, gateway):
        caplog.clear()
        cli(["remove_namespace", "-n", subsystem, "-i", nsid])
        assert f"Removed namespace 1 from {subsystem}: True" in caplog.text

    def test_delete_bdev_ana(self, caplog, gateway):
        caplog.clear()
        cli(["delete_bdev", "-b", bdev, "-f"])
        assert f"Deleted bdev {bdev}: True" in caplog.text
        assert "Will remove namespace" not in caplog.text
        caplog.clear()
        cli(["delete_bdev", "-b", bdev_ipv6, "-f"])
        assert f"Deleted bdev {bdev_ipv6}: True" in caplog.text
        assert "Will remove namespace" not in caplog.text

    def test_delete_subsystem_ana(self, caplog, gateway):
        caplog.clear()
        cli(["delete_subsystem", "-n", subsystem])
        assert f"Deleted subsystem {subsystem}: True" in caplog.text

class TestSDKLOg:
    def test_log_flags(self, caplog, gateway):
        caplog.clear()
        cli(["get_spdk_nvmf_log_flags_and_level"])
        assert '"nvmf": false' in caplog.text
        assert '"nvmf_tcp": false' in caplog.text
        assert '"log_level": "NOTICE"' in caplog.text
        assert '"log_print_level": "INFO"' in caplog.text
        caplog.clear()
        cli(["set_spdk_nvmf_logs", "-f"])
        assert "Set SPDK nvmf logs : True" in caplog.text
        caplog.clear()
        cli(["get_spdk_nvmf_log_flags_and_level"])
        assert '"nvmf": true' in caplog.text
        assert '"nvmf_tcp": true' in caplog.text
        assert '"log_level": "NOTICE"' in caplog.text
        assert '"log_print_level": "INFO"' in caplog.text
        caplog.clear()
        cli(["set_spdk_nvmf_logs", "-f", "-l", "DEBUG"])
        assert "Set SPDK nvmf logs : True" in caplog.text
        caplog.clear()
        cli(["get_spdk_nvmf_log_flags_and_level"])
        assert '"nvmf": true' in caplog.text
        assert '"nvmf_tcp": true' in caplog.text
        assert '"log_level": "DEBUG"' in caplog.text
        assert '"log_print_level": "INFO"' in caplog.text
        caplog.clear()
        cli(["set_spdk_nvmf_logs", "-f", "-p", "ERROR"])
        assert "Set SPDK nvmf logs : True" in caplog.text
        caplog.clear()
        cli(["get_spdk_nvmf_log_flags_and_level"])
        assert '"nvmf": true' in caplog.text
        assert '"nvmf_tcp": true' in caplog.text
        assert '"log_level": "DEBUG"' in caplog.text
        assert '"log_print_level": "ERROR"' in caplog.text
        caplog.clear()
        cli(["disable_spdk_nvmf_logs"])
        assert "Disable SPDK nvmf logs: True" in caplog.text
        caplog.clear()
        cli(["get_spdk_nvmf_log_flags_and_level"])
        assert '"nvmf": false' in caplog.text
        assert '"nvmf_tcp": false' in caplog.text
        assert '"log_level": "NOTICE"' in caplog.text
        assert '"log_print_level": "INFO"' in caplog.text
