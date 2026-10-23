import pytest
import os
import socket
import struct
from control.server import GatewayServer
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:nics1"
host_name = socket.gethostname()
labelled_addr = "192.0.2.10"
config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"

# The gateway image has no iproute2, so addresses are added over netlink
RTM_NEWADDR = 20
RTM_DELADDR = 21
NLMSG_ERROR = 2
NLM_F_REQUEST = 0x1
NLM_F_ACK = 0x4
NLM_F_EXCL = 0x200
NLM_F_CREATE = 0x400
IFA_ADDRESS = 1
IFA_LOCAL = 2
IFA_LABEL = 3


def rtattr(kind, data):
    length = 4 + len(data)
    return struct.pack("HH", length, kind) + data + b"\0" * (-length % 4)


def change_address(cmd, flags, index, addr, label):
    ip = socket.inet_aton(addr)
    body = struct.pack("BBBBI", socket.AF_INET, 32, 0, 0, index)
    body += rtattr(IFA_LOCAL, ip) + rtattr(IFA_ADDRESS, ip)
    body += rtattr(IFA_LABEL, label.encode() + b"\0")
    header = struct.pack("IHHII", 16 + len(body), cmd, NLM_F_REQUEST | NLM_F_ACK | flags, 1, 0)
    with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE) as sock:
        sock.sendall(header + body)
        reply = sock.recv(4096)
    msg_type = struct.unpack_from("H", reply, 4)[0]
    err = struct.unpack_from("i", reply, 16)[0] if msg_type == NLMSG_ERROR else 0
    if err:
        raise OSError(-err, os.strerror(-err))


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


@pytest.fixture
def labelled_address():
    """Adds an address labelled like a keepalived VIP to the first non-loopback NIC"""

    index, name = next((i, n) for i, n in socket.if_nameindex() if n != "lo")
    label = f"{name}:vip"
    change_address(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL, index, labelled_addr, label)
    yield labelled_addr
    change_address(RTM_DELADDR, 0, index, labelled_addr, label)


class TestNics:
    def test_listener_on_labelled_address(self, caplog, gateway, labelled_address):
        ret = cli_test(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
        assert ret.status == 0
        caplog.clear()
        ret = cli_test(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                        "-a", labelled_address, "-s", "4420", "-f", "ipv4"])
        assert ret.status == 0
        assert f"Address {labelled_address} is not available" not in caplog.text
