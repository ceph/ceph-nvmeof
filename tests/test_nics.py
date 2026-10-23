import pytest
import socket
import sys
from pyroute2 import IPRoute
from pyroute2.netlink.rtnl import rt_scope
from control.utils import NIC, NICS
from control.server import GatewayServer
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:nics1"
host_name = socket.gethostname()
labelled_addr = "192.0.2.10"
link_local_addr = "fe80::db8:10"
config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["verify_listener_ip"] = "True"
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


@pytest.fixture(scope="module")
def nic():
    """The first non-loopback NIC, as (index, name)"""
    return next((i, n) for i, n in socket.if_nameindex() if n != "lo")


@pytest.fixture(params=["{}:vip", "vip"])
def address_label(request):
    return request.param


@pytest.fixture(params=["universe", "link"])
def labelled_address(request, nic, address_label):
    """Adds an address labelled like a keepalived VIP, with and without the NIC name"""

    index, name = nic
    with IPRoute() as ipr:
        ipr.addr("add", index=index, address=labelled_addr, prefixlen=32,
                 label=address_label.format(name), scope=rt_scope[request.param])
    try:
        yield labelled_addr
    finally:
        with IPRoute() as ipr:
            ipr.addr("del", index=index, address=labelled_addr, prefixlen=32)


@pytest.fixture
def link_local_address(nic):
    index, _ = nic
    try:
        with IPRoute() as ipr:
            ipr.addr("add", index=index, address=link_local_addr, prefixlen=64)
    except Exception as ex:
        pytest.skip(f"cannot add an IPv6 address here: {ex}")
    yield link_local_addr
    with IPRoute() as ipr:
        ipr.addr("del", index=index, address=link_local_addr, prefixlen=64)


class TestNics:
    def test_normalize_raw_link_address(self):
        prefix = (12).to_bytes(2, byteorder=sys.byteorder)
        raw_address = prefix + b"\x01\x00\x00\x11\x22\x33\x44\x55\x66\x77"

        assert NIC._link_address(raw_address) == "00:11:22:33:44:55:66:77"

    def test_create_subsystem(self, caplog, gateway):
        ret = cli_test(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
        assert ret.status == 0

    def test_listener_on_labelled_address(self, caplog, gateway, labelled_address):
        nics = NICS()
        assert nics.verify_ip_address(labelled_address, "ipv4")
        assert labelled_address in nics.get_ips_in_subnet("192.0.2.0/24")
        caplog.clear()
        ret = cli_test(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                        "-a", labelled_address, "-s", "4420", "-f", "ipv4"])
        assert ret.status == 0
        assert f"Address {labelled_address} is not available" not in caplog.text
        ret = cli_test(["listener", "del", "--subsystem", subsystem, "--host-name", host_name,
                        "-a", labelled_address, "-s", "4420", "-f", "ipv4"])
        assert ret.status == 0

    def test_listener_on_link_local_address(self, caplog, gateway, link_local_address):
        caplog.clear()
        ret = cli_test(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                        "-a", link_local_address, "-s", "4420", "-f", "ipv6"])
        assert ret.status != 0
        assert f"Address {link_local_address} is not available" in caplog.text
