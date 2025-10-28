import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os

image = "mytestdevimage"
image2 = "mytestdevimage2"
image3 = "mytestdevimage3"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
config = "ceph-nvmeof.conf"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    if os.cpu_count() >= 4:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x03"
    else:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    portA = configA.getint("gateway", "port")
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = configB.getint("discovery", "port") + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    if os.cpu_count() >= 4:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x0C"
    else:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"

    ceph_utils = CephUtils(config)
    with (GatewayServer(configA) as gatewayA, GatewayServer(configB) as gatewayB):
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", "group": ""' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", "group": ""' + "}"
        )
        gatewayA.serve()
        gatewayB.serve()

        channelA = grpc.insecure_channel(f"{addr}:{portA}")
        pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        pb2_grpc.GatewayStub(channelB)

        yield gatewayA.gateway_rpc, gatewayB.gateway_rpc
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def test_change_namespace_location(caplog, two_gateways):
    gatewayA, gatewayB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", "junk",
             "--rbd-image", "junk", "--location"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "error: argument --location: expected one argument" in caplog.text
    assert rc == 2
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-poo", pool,
         "--rbd-image", image, "--size", "16MB", "--rbd-create-image",
         "--location", "USA"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": "USA",' in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": "USA",' in caplog.text
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "1", "--location", "USA"])
    assert f'Setting location for namespace 1 in {subsystem} to "USA": ' \
           f'Successful' in caplog.text
    assert f"No change to namespace 1 in {subsystem} location, nothing to do" in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": "USA",' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-poo", pool,
         "--rbd-image", image2, "--size", "16MB", "--rbd-create-image",
         "--location", "USA"])
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert '"location": "USA",' in caplog.text
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("Junk")
    assert len(ns_list) == 0
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("USA")
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (2, subsystem)
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("USA",
                                                                                     subsystem)
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (2, subsystem)
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("USA",
                                                                                     subsystem2)
    assert len(ns_list) == 0
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "1", "--location", "China"])
    assert f'Setting location for namespace 1 in {subsystem} to "China": ' \
           f'Successful' in caplog.text
    assert f'Received request to change the location of namespace 1 in {subsystem} ' \
           f'to "China", context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to change the location of namespace 1 in {subsystem} ' \
           f'to "China", context: None' in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": "China",' in caplog.text
    assert f'"rbd_data_pool_name": "{pool}",' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": "China",' in caplog.text
    assert f'"rbd_data_pool_name": "{pool}",' in caplog.text
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("USA")
    assert len(ns_list) == 1
    assert ns_list[0] == (2, subsystem)
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "change_location",
         "--subsystem", subsystem, "--nsid", "1", "--location", ""])
    assert f'Unsetting location for namespace 1 in {subsystem}: ' \
           f'Successful' in caplog.text
    assert f'Received request to change the location of namespace 1 in {subsystem} to ' \
           f'"", context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to change the location of namespace 1 in {subsystem} to ' \
           f'"", context: None' in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"location": ""' in caplog.text
    assert f'"rbd_data_pool_name": "{pool}",' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image3, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 3 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "3"])
    assert '"nsid": 3,' in caplog.text
    assert '"location": ""' in caplog.text
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", "junk",
         "--nsid", "3", "--location", "Oz"])
    assert "Failure changing location for namespace 3 in junk: Can't find subsystem"
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "25", "--location", "Oz"])
    assert f"Failure changing location for namespace 25 in {subsystem}: Can't find namespace"
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location(None)
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (3, subsystem)
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("")
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (3, subsystem)
