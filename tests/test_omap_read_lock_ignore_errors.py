import pytest
from control.server import GatewayServer
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import time

pool = "rbd"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    config.config["gateway"]["group"] = group_name
    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["omap_file_ignore_unlock_errors"] = "True"
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
        pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_ignore_unlock_errors(caplog, gateway):
    gw = gateway
    lookfor = "OMAP unlock errors will be ignored, the gateway will continue"
    found = 0
    time.sleep(10)
    for oneline in caplog.get_records("setup"):
        if oneline.message == lookfor:
            found += 1
    assert found == 1
    gw.rpc_lock.acquire()
    caplog.clear()
    gw.omap_lock.lock_omap()
    assert "Locked OMAP exclusive" in caplog.text
    time.sleep(25)     # A little more than omap_file_lock_duration
    caplog.clear()
    gw.omap_lock.unlock_omap()
    assert "No such lock, the exclusive lock might have expired" in caplog.text
