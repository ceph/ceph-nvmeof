import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import time

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
subsystem3 = "nqn.2016-06.io.spdk:cnode3"

host_name = socket.gethostname()
addr = "127.0.0.1"
addr_subnet = f'{addr}/24'
addr_ipv6 = "::1"
addr_ipv6_subnet = f'{addr_ipv6}/120'
config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        gateway.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{port}")
        stub = pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc, stub

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


class TestAutoListener:
    def test_subsystem_with_networks(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append",
             '--network-mask', addr_subnet, addr_ipv6_subnet])
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert (f"Automatically created listener at {addr}:4420 for {subsystem}"
                in caplog.text)
        assert "ipv6" in caplog.text.lower()
        assert (f"Automatically created listener at [{addr_ipv6}]:4420 for {subsystem}"
                in caplog.text)

    def test_listener_list(self, caplog, gateway):
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem])
        assert len(listeners.listeners) == 2
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].trsvcid == 4420
        assert listeners.listeners[0].active
        assert not listeners.listeners[0].secure
        assert not listeners.listeners[0].manual
        assert {listeners.listeners[0].traddr,
                listeners.listeners[1].traddr} == {addr, addr_ipv6}
        assert listeners.listeners[1].trtype == "TCP"
        assert listeners.listeners[1].trsvcid == 4420
        assert listeners.listeners[1].active
        assert not listeners.listeners[1].secure
        assert not listeners.listeners[1].manual

    def test_subsystem_list(self, caplog, gateway):
        subsystems = cli_test(["subsystem", "list", "--subsystem", subsystem])
        masks = subsystems.subsystems[0].network_mask
        assert len(masks) == 2
        assert set(masks) == {addr_subnet, addr_ipv6_subnet}

    def test_auto_listener_secure(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem2, "--no-group-append",
             '--network-mask', f'{addr}/24', '--secure-listeners'])
        assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert (f"Automatically created listener at {addr}:4420 for {subsystem2}"
                in caplog.text)

    def test_auto_listener_list_secure(self, caplog, gateway):
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem2])
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].traddr == addr
        assert listeners.listeners[0].trsvcid == 4420
        assert listeners.listeners[0].active
        assert listeners.listeners[0].secure
        assert not listeners.listeners[0].manual

    def test_del_network_mask(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "del_network", "--subsystem", subsystem,
             '--network-mask', f'{addr}/24'])
        assert (f"Removed network {addr}/24 for subsystem {subsystem}")
        assert (f"Automatically deleted listener at {addr}:4420 for {subsystem}"
                in caplog.text)
        assert (f"Automatically created listener at {addr}:4420 for {subsystem}"
                not in caplog.text)

    def test_del_network_subsystem_list(self, caplog, gateway):
        subsystems = cli_test(["subsystem", "list", "--subsystem", subsystem])
        masks = subsystems.subsystems[0].network_mask
        assert len(masks) == 1
        assert set(masks) == {addr_ipv6_subnet}

    def test_del_network_listener_list(self, caplog, gateway):
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem])
        assert len(listeners.listeners) == 1
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].traddr == addr_ipv6
        assert listeners.listeners[0].trsvcid == 4420
        assert listeners.listeners[0].active
        assert not listeners.listeners[0].secure
        assert not listeners.listeners[0].manual

    def test_add_network_mask(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "add_network", "--subsystem", subsystem,
             '--network-mask', f'{addr}/24'])
        assert (f"Added network {addr}/24 for subsystem {subsystem}")
        assert (f"Automatically created listener at {addr}:4420 for {subsystem}"
                in caplog.text)
        assert (f"Automatically deleted listener at {addr}:4420 for {subsystem}"
                not in caplog.text)

    def test_add_network_subsystem_list(self, caplog, gateway):
        subsystems = cli_test(["subsystem", "list", "--subsystem", subsystem])
        masks = subsystems.subsystems[0].network_mask
        assert len(masks) == 2
        assert set(masks) == {addr_ipv6_subnet, addr_subnet}

    def test_add_network_listener_list(self, caplog, gateway):
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem])
        assert len(listeners.listeners) == 2
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].traddr == addr
        assert listeners.listeners[0].trsvcid == 4420
        assert listeners.listeners[0].active
        assert not listeners.listeners[0].secure
        assert not listeners.listeners[0].manual
        assert listeners.listeners[1].trtype == "TCP"
        assert listeners.listeners[1].traddr == addr_ipv6
        assert listeners.listeners[1].trsvcid == 4420
        assert listeners.listeners[1].active
        assert not listeners.listeners[1].secure
        assert not listeners.listeners[1].manual

    def test_fail_delete_auto_listener(self, caplog, gateway):
        caplog.clear()
        cli(["listener", "del", "--subsystem", subsystem, "--host-name", host_name,
             "--traddr", addr, "--trsvcid", "4420"])
        assert f"Failed to delete listener {addr}:4420 from {subsystem}: " \
               f"Listener was created automatically as part of the subsystem's " \
               f"network mask. To remove it, modify the network mask." in caplog.text
