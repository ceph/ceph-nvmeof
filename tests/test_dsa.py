import pytest
from control.server import GatewayServer
from control.cephutils import CephUtils

pool = "rbd"
group_name = "GROUPNAME"


@pytest.fixture(scope="function")
def gateway(config):
    """Sets up and tears down Gateway"""

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
        yield gateway

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_dsa_is_enabled(caplog, gateway):
    _ = gateway
    found = False
    for line in caplog.get_records("setup"):
        if "dsa_scan_accel_module:" in line.message:
            found = True
            break
    assert found, "Didn't call SPDK DSA function"
