import pytest
import grpc
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.proto import gateway_pb2_grpc as pb2_grpc

image1 = "image1"
image2 = "image2"
pool = "rbd"
pool2 = "rbd2"
rados_ns1 = "rados_ns1"
rados_ns2 = "rados_ns2"
subsystem = "nqn.2016-06.io.spdk:cnode2"
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
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        gateway.serve()

        channel = grpc.insecure_channel(f"{addr}:{port}")
        pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc, ceph_utils

        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_setup_subsystem(caplog, gateway):
    """Create subsystem for tests"""
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"Adding subsystem {subsystem}: Successful" in caplog.text


def test_nonexistent_rados_namespace(caplog, gateway):
    """Test creating namespace with non-existent RADOS namespace"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", "nonexistent_ns",
         "--rbd-image", image1, "--size", "10MB", "--rbd-create-image"])
    assert f"Failure adding namespace to {subsystem}" in caplog.text
    assert f"Namespace nonexistent_ns doesn't exist in pool {pool}" in caplog.text


def test_create_image_in_rados_namespace(caplog, gateway):
    """Test using existing image in RADOS namespace"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", rados_ns1,
         "--rbd-image", image1])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text

    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert f'"rbd_image_name": "{image1}"' in caplog.text
    assert f'"rbd_pool_name": "{pool}"' in caplog.text
    assert f'"rados_namespace_name": "{rados_ns1}"' in caplog.text


def test_same_image_name_different_rados_namespace(caplog, gateway):
    """Test same image name in different RADOS namespace (should succeed)"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", rados_ns2,
         "--rbd-image", image1])
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text


def test_duplicate_image_in_same_rados_namespace(caplog, gateway):
    """Test duplicate image in same RADOS namespace (should fail)"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", rados_ns1,
         "--rbd-image", image1])
    assert f"RBD image {pool}/{rados_ns1}/{image1} is already used" in caplog.text


def test_image_without_rados_namespace(caplog, gateway):
    """Test image without RADOS namespace"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", "mytestdevimage"])
    assert f"Adding namespace 3 to {subsystem}: Successful" in caplog.text


def test_same_image_name_with_and_without_rados_namespace(caplog, gateway):
    """Test same image name with/without RADOS namespace (different images)"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", rados_ns1,
         "--rbd-image", image2])
    assert f"Adding namespace 4 to {subsystem}: Successful" in caplog.text

    caplog.clear()
    # Same image name but WITHOUT rados namespace - should succeed (different image)
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image2, "--size", "10MB", "--rbd-create-image"])
    assert f"Adding namespace 5 to {subsystem}: Successful" in caplog.text


def test_different_pool_same_image_and_rados_namespace(caplog, gateway):
    """Test same image and RADOS namespace name in different pool (should succeed)"""
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool2,
         "--rados-namespace", rados_ns1,
         "--rbd-image", "mytestdevimage2"])
    assert f"Adding namespace 6 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rados-namespace", rados_ns1,
         "--rbd-image", "mytestdevimage2"])
    assert f"Adding namespace 7 to {subsystem}: Successful" in caplog.text


def test_list_namespaces_with_rados_namespace(caplog, gateway):
    """Test namespace list shows RADOS namespace correctly"""
    caplog.clear()
    cli(["namespace", "list", "--subsystem", subsystem])
    assert f"{pool}/{rados_ns1}/{image1}" in caplog.text
    assert f"{pool}/{rados_ns2}/{image1}" in caplog.text
    assert f"{pool}/mytestdevimage" in caplog.text
    assert f"{pool}/{rados_ns1}/{image2}" in caplog.text
    assert f"{pool}/{image2}" in caplog.text
    assert f"{pool2}/{rados_ns1}/mytestdevimage2" in caplog.text
    assert f"{pool}/{rados_ns1}/mytestdevimage2" in caplog.text


def test_delete_namespace_with_rados_namespace(caplog, gateway):
    """Test deleting namespace with RADOS namespace"""
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text

    caplog.clear()
    cli(["namespace", "list", "--subsystem", subsystem])
    assert f"{pool}/{rados_ns1}/{image1}" not in caplog.text
