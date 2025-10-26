import pytest
import grpc
import re
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.proto import gateway_pb2_grpc as pb2_grpc

image = "ec_pool_image"
pool = "rbd"
ec_pool_no_overwrites = "ec_pool_no_overwrites"
ec_pool_overwrites = "ec_pool_overwrites"
subsystem = "nqn.2016-06.io.spdk:cnode1"
group_name = "mygroup"


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
        pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc, ceph_utils

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_setup_environment(caplog, gateway):
    gw, ceph_utils = gateway
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool create", "pool": "{ec_pool_no_overwrites}", '
        f'"pool_type": "erasure"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{ec_pool_no_overwrites}", "pool_type": "erasure"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{ec_pool_no_overwrites}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool create", "pool": "{ec_pool_overwrites}", '
        f'"pool_type": "erasure"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{ec_pool_overwrites}", "pool_type": "erasure"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{ec_pool_overwrites}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool set", "pool": "{ec_pool_overwrites}", '
        f'"var": "allow_ec_overwrites", "val": "true"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool set", "pool": ' \
           f'"{ec_pool_overwrites}", "var": "allow_ec_overwrites", ' \
           f'"val": "true"}}' in caplog.text
    pattern = re.compile(r"Monitor reply: \(0, b'', 'set pool \d+ allow_ec_overwrites to true'\)")
    assert pattern.search(caplog.text) is not None
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"Adding subsystem {subsystem}: Successful" in caplog.text


def test_pool_does_not_exist(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", "junk",
         "--rbd-image", "junkimage", "--size", "10MB", "--rbd-create-image"])
    assert f"Failure adding namespace to {subsystem}: RBD pool " \
           f"junk doesn't exist" in caplog.text


def test_data_pool_does_not_exist(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-pool", "junk",
         "--rbd-image", "junkimage", "--size", "10MB", "--rbd-create-image"])
    assert f"Failure adding namespace to {subsystem}: RBD data pool " \
           f"junk doesn't exist" in caplog.text


def test_use_erasure_pool_as_rbd_pool(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", ec_pool_overwrites,
         "--rbd-image", "junkimage", "--size", "10MB", "--rbd-create-image"])
    assert f"Failure adding namespace to {subsystem}: RBD pool " \
           f"{ec_pool_overwrites} is not a replicated pool" in caplog.text


def test_use_erasure_pool_with_no_overwrites(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-pool", ec_pool_no_overwrites,
         "--rbd-image", "junkimage", "--size", "10MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem}: RBD data pool ' \
           f'{ec_pool_no_overwrites} doesn\'t have "allow_ec_overwrites" set' in caplog.text


def test_use_erasure_pool_as_rbd_data_pool(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-pool", ec_pool_overwrites,
         "--rbd-image", image, "--size", "10MB", "--rbd-create-image"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    assert f"Image {pool}/{image} created, size is 10485760 bytes, " \
           f"data pool is {ec_pool_overwrites}" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert f'"rbd_image_name": "{image}"' in caplog.text
    assert f'"rbd_pool_name": "{pool}"' in caplog.text
    assert f'"rbd_data_pool_name": "{ec_pool_overwrites}"' in caplog.text
