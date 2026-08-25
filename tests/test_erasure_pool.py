import pytest
import grpc
import re
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.proto import gateway_pb2_grpc as pb2_grpc

image = "ec_pool_image"
image2 = "ec_pool_image2"
pool = "rbd"
ec_pool_no_overwrites = "ec_pool_no_overwrites"
ec_pool_overwrites = "ec_pool_overwrites"
ec_pool_supports_omap = "ec_pool_supports_omap"
ec_pool_omap_support_no_overwrites = "ec_pool_omap_support_no_overwrites"
subsystem = "nqn.2016-06.io.spdk:cnode1"
group_name = "mygroup"
supports_omap_implemented = True


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
    global supports_omap_implemented
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
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool create", "pool": "{ec_pool_supports_omap}", '
        f'"pool_type": "erasure"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{ec_pool_supports_omap}", "pool_type": "erasure"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{ec_pool_supports_omap}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool set", "pool": "{ec_pool_supports_omap}", '
        f'"var": "allow_ec_overwrites", "val": "true"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool set", "pool": ' \
           f'"{ec_pool_supports_omap}", "var": "allow_ec_overwrites", ' \
           f'"val": "true"}}' in caplog.text
    pattern = re.compile(r"Monitor reply: \(0, b'', 'set pool \d+ allow_ec_overwrites to true'\)")
    assert pattern.search(caplog.text) is not None
    caplog.clear()
    rc = ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool set", "pool": "{ec_pool_supports_omap}", '
        f'"var": "supports_omap", "val": "true"' + "}"
    )
    if rc[0]:
        supports_omap_implemented = False
        print('"supports_omap" attribute is not implemented in this version of Ceph')
    else:
        assert f'Execute monitor command: {{"prefix":"osd pool set", "pool": ' \
               f'"{ec_pool_supports_omap}", "var": "supports_omap", ' \
               f'"val": "true"}}' in caplog.text
        pattern = re.compile(r"Monitor reply: \(0, b'', 'set pool \d+ supports_omap to true'\)")
        assert pattern.search(caplog.text) is not None

    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool create", "pool": "{ec_pool_omap_support_no_overwrites}", '
        f'"pool_type": "erasure"' + "}"
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{ec_pool_omap_support_no_overwrites}", "pool_type": "erasure"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', ' \
           f'"pool \'{ec_pool_omap_support_no_overwrites}\' created")' in caplog.text
    caplog.clear()
    rc = ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"osd pool set", "pool": "{ec_pool_omap_support_no_overwrites}", '
        f'"var": "supports_omap", "val": "true"' + "}"
    )
    if rc[0]:
        supports_omap_implemented = False
        print('"supports_omap" attribute is not implemented in this version of Ceph')
    else:
        assert f'Execute monitor command: {{"prefix":"osd pool set", "pool": ' \
               f'"{ec_pool_omap_support_no_overwrites}", "var": "supports_omap", ' \
               f'"val": "true"}}' in caplog.text
        pattern = re.compile(r"Monitor reply: \(0, b'', 'set pool \d+ supports_omap to true'\)")
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
           f"{ec_pool_overwrites} is an erasure coded pool which does not " \
           f"support OMAP" in caplog.text


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


def test_use_omap_supporting_erasure_pool_as_rbd_pool(caplog, gateway):
    if not supports_omap_implemented:
        pytest.skip("supports_omap is not implemented in this version of Ceph")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", ec_pool_supports_omap,
         "--rbd-image", image2, "--size", "10MB", "--rbd-create-image"])
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
    assert f"Image {ec_pool_supports_omap}/{image2} created, size is 10485760 bytes" in caplog.text
    assert "data pool is" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "2"])
    assert f'"rbd_image_name": "{image2}"' in caplog.text
    assert f'"rbd_pool_name": "{ec_pool_supports_omap}"' in caplog.text
    assert '"rbd_data_pool_name": ""' in caplog.text


def test_use_erasure_pool_supporting_omap_with_no_overwrites(caplog, gateway):
    if not supports_omap_implemented:
        pytest.skip("supports_omap is not implemented in this version of Ceph")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem,
         "--rbd-pool", ec_pool_omap_support_no_overwrites,
         "--rbd-image", "junkimage", "--size", "10MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem}: Erasure coded RBD pool ' \
           f'{ec_pool_omap_support_no_overwrites} doesn\'t have ' \
           f'"allow_ec_overwrites" set' in caplog.text
