import pytest
import socket
import grpc
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.proto import gateway_pb2_grpc as pb2_grpc

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
serial = "Ceph00000000000001"
host = "nqn.2016-06.io.spdk:host1"
config = "ceph-nvmeof.conf"
group_name = "group1"
addr = "127.0.0.1"
host_name = socket.gethostname()


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["force_tls"] = "true"
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
        pb2_grpc.GatewayStub(channel)
        yield gateway

        # Stop gateway
        gateway.gateway_rpc.gateway_state.delete_state()
        gateway.server.stop(grace=1)


class TestForceTls:
    def test_force_tls(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem,
             "--no-group-append"])
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host])
        assert f"Failure adding host {host} to {subsystem}: host must " \
               f"have a PSK key" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*"])
        assert f"Allowing open host access to {subsystem}: Successful" in caplog.text
        assert f"Open host access to subsystem {subsystem} might be a " \
               f"security breach" in caplog.text
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", "*"])
        assert f"Disabling open host access to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "-a", addr,
             "-s", "5001", "-t", host_name, "--verify-host-name"])
        assert f"Failure adding {subsystem} listener at {addr}:5001: " \
               f"Secure channel must be used" in caplog.text
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "-a", addr,
             "-s", "5002", "-t", host_name, "--verify-host-name", "--secure"])
        assert f"Adding {subsystem} listener at {addr}:5002: Successful" in caplog.text
