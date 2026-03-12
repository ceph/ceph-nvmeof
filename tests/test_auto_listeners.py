import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import time

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
subsystem3 = "nqn.2016-06.io.spdk:cnode3"
subsystem4 = "nqn.2016-06.io.spdk:cnode4"
subsystem5 = "nqn.2016-06.io.spdk:cnode5"

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
        assert f"Automatically created listener at {addr}:4420 for {subsystem}" in caplog.text
        assert "ipv6" in caplog.text.lower()
        assert f"Automatically created listener at [{addr_ipv6}]:4420 for " \
               f"{subsystem}" in caplog.text

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
             '--network-mask', addr_subnet, '--secure-listeners'])
        assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Automatically created listener at {addr}:4421 for {subsystem2}" in caplog.text

    def test_auto_listener_list_secure(self, caplog, gateway):
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem2])
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].traddr == addr
        assert listeners.listeners[0].trsvcid == 4421
        assert listeners.listeners[0].active
        assert listeners.listeners[0].secure
        assert not listeners.listeners[0].manual

    def test_create_subsystem_invalid_network_mask(self, caplog, gateway):
        _, stub = gateway
        caplog.clear()
        serial = "Ceph00000000000001"
        invalid_subnet = "nosubnet"
        req = pb2.create_subsystem_req(subsystem_nqn=subsystem, max_namespaces=256,
                                       serial_number=serial, enable_ha=True,
                                       network_mask=[invalid_subnet])
        ret = stub.create_subsystem(req)
        assert ret.status != 0
        assert f"Failure creating subsystem {subsystem}: Invalid subnet for " \
               f"network_mask \"{invalid_subnet}\"" in caplog.text

        caplog.clear()
        req = pb2.create_subsystem_req(subsystem_nqn=subsystem, max_namespaces=256,
                                       serial_number=serial, enable_ha=True,
                                       network_mask=[addr_subnet, invalid_subnet])
        ret = stub.create_subsystem(req)
        assert ret.status != 0
        assert f"Failure creating subsystem {subsystem}: Invalid subnet for " \
               f"network_mask \"{invalid_subnet}\"" in caplog.text
        caplog.clear()

    def test_del_network_mask_param_fail(self, caplog, gateway):
        _, stub = gateway
        caplog.clear()
        no_subsystem_param = pb2.del_subsystem_network_req(network_mask=addr_subnet)
        ret = stub.del_subsystem_network(no_subsystem_param)
        assert ret.status != 0
        assert "Failure deleting network_mask, missing subsystem NQN" in caplog.text

        caplog.clear()
        no_netmask_param = pb2.del_subsystem_network_req(subsystem_nqn=subsystem)
        ret = stub.del_subsystem_network(no_netmask_param)
        assert ret.status != 0
        assert f"Failure deleting network_mask for subsystem {subsystem}: " \
               "Missing network_mask" in caplog.text

        caplog.clear()
        invalid_subnet = "nosubnet"
        invalid_netmask_param = pb2.del_subsystem_network_req(subsystem_nqn=subsystem,
                                                              network_mask=invalid_subnet)
        ret = stub.del_subsystem_network(invalid_netmask_param)
        assert ret.status != 0
        assert f"Failure deleting network_mask for subsystem {subsystem}: " \
               f"Invalid subnet \"{invalid_subnet}\"" in caplog.text
        caplog.clear()

    def test_del_network_mask(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "del_network", "--subsystem", subsystem,
             '--network-mask', addr_subnet])
        assert f"Network mask {addr_subnet} deleted for subsystem {subsystem}: " \
               f"Successful" in caplog.text
        assert f"Automatically deleted listener at {addr}:4420 for {subsystem}" in caplog.text
        assert f"Automatically created listener at {addr}:4420 for {subsystem}" not in caplog.text

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

    def test_add_network_mask_param_fail(self, caplog, gateway):
        _, stub = gateway
        caplog.clear()
        no_subsystem_param = pb2.add_subsystem_network_req(network_mask=addr_subnet)
        ret = stub.add_subsystem_network(no_subsystem_param)
        assert ret.status != 0
        assert "Failure adding network_mask, missing subsystem NQN" in caplog.text

        caplog.clear()
        no_netmask_param = pb2.add_subsystem_network_req(subsystem_nqn=subsystem)
        ret = stub.add_subsystem_network(no_netmask_param)
        assert ret.status != 0
        assert f"Failure adding network_mask for subsystem {subsystem}: " \
               "Missing network_mask" in caplog.text

        caplog.clear()
        invalid_subnet = "nosubnet"
        invalid_netmask_param = pb2.add_subsystem_network_req(subsystem_nqn=subsystem,
                                                              network_mask=invalid_subnet)
        ret = stub.add_subsystem_network(invalid_netmask_param)
        assert ret.status != 0
        assert f"Failure adding network_mask for subsystem {subsystem}: " \
               f"Invalid subnet \"{invalid_subnet}\"" in caplog.text
        caplog.clear()

    def test_add_network_mask(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "add_network", "--subsystem", subsystem,
             '--network-mask', addr_subnet])
        assert f"Added network {addr_subnet} for subsystem {subsystem}" in caplog.text
        assert f"Automatically created listener at {addr}:4420 for {subsystem}" in caplog.text
        assert f"Automatically deleted listener at {addr}:4420 for {subsystem}" not in caplog.text

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

    def test_subsystem_with_networks_and_port(self, caplog, gateway):
        cli(["subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem3, "--no-group-append",
             '--network-mask', addr_subnet, addr_ipv6_subnet, "--port", "4500"])
        assert f"Adding subsystem {subsystem3}: Successful" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Automatically created listener at {addr}:4500 for {subsystem3}" in caplog.text
        assert "ipv6" in caplog.text.lower()
        assert f"Automatically created listener at [{addr_ipv6}]:4500 for " \
               f"{subsystem3}" in caplog.text
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem3])
        assert len(listeners.listeners) == 2
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].trsvcid == 4500
        assert listeners.listeners[0].active
        assert not listeners.listeners[0].secure
        assert not listeners.listeners[0].manual
        assert {listeners.listeners[0].traddr,
                listeners.listeners[1].traddr} == {addr, addr_ipv6}
        assert listeners.listeners[1].trtype == "TCP"
        assert listeners.listeners[1].trsvcid == 4500
        assert listeners.listeners[1].active
        assert not listeners.listeners[1].secure
        assert not listeners.listeners[1].manual

    def test_auto_listener_secure_with_port(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem4, "--no-group-append",
             '--network-mask', addr_subnet, '--secure-listeners', "--port", "4501"])
        assert f"Adding subsystem {subsystem4}: Successful" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Automatically created listener at {addr}:4501 for {subsystem4}" in caplog.text
        cli(["subsystem", "list"])
        time.sleep(30)
        caplog.clear()
        listeners = cli_test(["listener", "list", "--subsystem", subsystem4])
        assert listeners.listeners[0].trtype == "TCP"
        assert listeners.listeners[0].traddr == addr
        assert listeners.listeners[0].trsvcid == 4501
        assert listeners.listeners[0].active
        assert listeners.listeners[0].secure
        assert not listeners.listeners[0].manual

    def test_del_and_add_network_mask(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "del_network", "--subsystem", subsystem3, '--network-mask', addr_subnet])
        assert f"Network mask {addr_subnet} deleted for subsystem {subsystem3}: " \
               f"Successful" in caplog.text
        cli(["subsystem", "list"])
        time.sleep(30)
        listeners = cli_test(["listener", "list", "--subsystem", subsystem3])
        assert len(listeners.listeners) == 1
        caplog.clear()
        cli(["subsystem", "add_network", "--subsystem", subsystem3,
             '--network-mask', addr_subnet])
        assert f"Added network {addr_subnet} for subsystem {subsystem3}" in caplog.text
        assert f"Automatically created listener at {addr}:4500 for {subsystem3}" in caplog.text
        cli(["subsystem", "list"])
        time.sleep(30)
        listeners = cli_test(["listener", "list", "--subsystem", subsystem3])
        assert len(listeners.listeners) == 2
        assert listeners.listeners[0].trsvcid == 4500
        assert listeners.listeners[1].trsvcid == 4500

    def test_use_port_and_secure_without_network_mask(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["subsystem", "add", "--subsystem", subsystem5, "--no-group-append",
                 "--port", "4700"])
        except SystemExit as sysex:
            rc = sysex.code
        assert "Port cannot be set without a network mask" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["subsystem", "add", "--subsystem", subsystem5, "--no-group-append",
                 "--secure-listeners"])
        except SystemExit as sysex:
            rc = sysex.code
        assert "Secure listeners cannot be set without a network mask" in caplog.text
        assert rc == 2
