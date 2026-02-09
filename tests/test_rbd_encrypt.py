import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import os
import time

image = "enc_test_image"
pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    if os.cpu_count() >= 4:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-1)"
    else:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    portA = configA.getint("gateway", "port")
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["gateway"]["io_stats_enabled"] = "False"
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = configB.getint("discovery", "port") + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    if os.cpu_count() >= 4:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"
    else:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"

    ceph_utils = CephUtils(config)
    gatewayA = GatewayServer(configA)
    gatewayB = GatewayServer(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gatewayA.serve()
    gatewayB.serve()

    channelA = grpc.insecure_channel(f"{addr}:{portA}")
    stubA = pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    stubB = pb2_grpc.GatewayStub(channelB)

    return gatewayA, stubA, gatewayB, stubB


def test_create_resources(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_wrong_encryption_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "JUNK", "--key-id", "bla"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: argument --encryption-format/-f: invalid choice: 'junk' (choose from 'luks1'," \
           " 'LUKS1', 'luks2', 'LUKS2')" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format=5, key_id="bla")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Invalid encryption format 5" in caplog.text


def test_encryption_algorithm_with_no_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-algorithm", "AES256", "--key-id", "bla"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Encryption algorithm is only allowed when an encryption format " \
           "is specified" in caplog.text

    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=[],
                                       encryption_algorithm="aes256")
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Can\'t have an encryption algorithm " \
           f"without an encryption format" in caplog.text


def test_key_id_with_no_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--key-id", "bla"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Key IDs are only valid when an encryption format " \
           "is specified" in caplog.text

    enc_entries = [pb2.encryption_entry(key_id="bla")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Mustn\'t have a key ID when encryption " \
           f"format is not set" in caplog.text


def test_encryption_no_key_id(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "luks2"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: Must have a key ID when using encryption" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Must have a key ID when encryption " \
           f"format is set" in caplog.text


def test_number_of_formats_and_key_ids_mismatch(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks2", "luks1", "--key-id", "bla"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: The number of key IDs should match the number of encryption " \
           "formats" in caplog.text
    assert rc == 2
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks2", "--key-id", "bla", "junk"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: The number of key IDs should match the number of encryption " \
           "formats" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id="bla"),
                   pb2.encryption_entry(format="luks2")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Must have a key ID when encryption " \
           f"format is set" in caplog.text

    enc_entries = [pb2.encryption_entry(format="luks1", key_id="bla"),
                   pb2.encryption_entry(key_id="junk")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Mustn\'t have a key ID when encryption " \
           f"format is not set" in caplog.text


def test_multiple_formats_with_create(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "luks2", "luks1",
             "--key-id", "bla", "junk"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: at most one encryption format can be specified when creating a " \
           "new image" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id="bla"),
                   pb2.encryption_entry(format="luks2", key_id="junk")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: At most one encryption format can be " \
           f"specified when creating a new image" in caplog.text


def test_create_with_encryption(caplog, two_gateways):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
         "--rbd-create-image", "--encryption-format", "luks1",
         "--key-id", "bla"])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_encryption_algorithm_without_create(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks1", "--key-id", "bla",
             "--encryption-algorithm", "aes128"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: --encryption-algorithm argument is not allowed for add command when RBD " \
           "image creation is disabled" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id="bla")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries,
                                       encryption_algorithm="aes128")
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'aes128, context: <grpc._server' in caplog.text
    assert f"Failure adding namespace to {subsystem1}: Encryption algorithm is only allowed " \
           f"when creating a new image" in caplog.text


def test_open_with_encryption(caplog, two_gateways):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", "bla"])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_open_with_encryption_wrong_key_id(caplog, two_gateways):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", "wrong"])
    assert f"Failure adding namespace to {subsystem1}: Operation not permitted" in caplog.text


def test_list_namespaces(caplog, two_gateways):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", "bla"])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert 'encryption_entries: [(format: luks1, key_id: bla)], encryption_algorithm: ' \
           'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert '"nsid": 1' in caplog.text
    assert '"format": "luks1"' in caplog.text
    assert '"key_id": "bla"' in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["--format", "json", "--server-port", "5502", "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"nsid": 1' in caplog.text
    assert '"format": "luks1"' in caplog.text
    assert '"key_id": "bla"' in caplog.text
