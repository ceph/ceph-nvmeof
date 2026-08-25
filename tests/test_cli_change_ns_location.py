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
location = "USA"
location2 = "China"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
config = "ceph-nvmeof.conf"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    global location, location2
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
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-1)"
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
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"
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
        rc = ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw set-location", "id": "{nameA}", "pool": "{pool}", '
            f'"group": "", "location": "{location}"' + "}"
        )
        if rc[0]:
            location = ""
            location2 = ""
            print("set-location is not implemented in Ceph, will use default")
        else:
            rc = ceph_utils.execute_ceph_monitor_command(
                "{" + f'"prefix":"nvme-gw set-location", "id": "{nameB}", "pool": "{pool}", '
                f'"group": "", "location": "{location}"' + "}"
            )
            assert rc[0] == 0
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


def test_get_gw_info(caplog, two_gateways):
    _, _ = two_gateways
    caplog.clear()
    cli(["--format", "json", "gateway", "info"])
    assert f'"location": "{location}"' in caplog.text


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
         "--rbd-data-pool", pool,
         "--rbd-image", image, "--size", "16MB", "--rbd-create-image",
         "--location", location])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"location": "{location}",' in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"location": "{location}",' in caplog.text
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "1", "--location", location])
    assert f'Setting location for namespace 1 in {subsystem} to "{location}": ' \
           f'Successful' in caplog.text
    assert f"No change to namespace 1 in {subsystem} location, nothing to do" in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"location": "{location}",' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-data-pool", pool,
         "--rbd-image", image2, "--size", "16MB", "--rbd-create-image",
         "--location", location])
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert f'"location": "{location}",' in caplog.text
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location("Junk")
    assert len(ns_list) == 0
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location(location)
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (2, subsystem)
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location(location,
                                                                                     subsystem)
    assert len(ns_list) == 2
    assert ns_list[0] == (1, subsystem)
    assert ns_list[1] == (2, subsystem)
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location(location,
                                                                                     subsystem2)
    assert len(ns_list) == 0
    print("Disable change location calls until they work in Ceph")
    return
    assert location != location2
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "1", "--location", location2])
    assert f'Setting location for namespace 1 in {subsystem} to "{location2}": ' \
           f'Successful' in caplog.text
    assert f'Received request to change the location of namespace 1 in {subsystem} ' \
           f'to "{location2}", context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to change the location of namespace 1 in {subsystem} ' \
           f'to "{location2}", context: None' in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"location": "{location2}",' in caplog.text
    assert f'"rbd_data_pool_name": "{pool}",' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"location": "{location2}",' in caplog.text
    assert f'"rbd_data_pool_name": "{pool}",' in caplog.text
    ns_list = gatewayA.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_location(location)
    assert len(ns_list) == 1
    assert ns_list[0] == (2, subsystem)
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", "junk",
         "--nsid", "3", "--location", "Oz"])
    assert "Failure changing location for namespace 3 in junk: Can't find subsystem"
    caplog.clear()
    cli(["namespace", "change_location", "--subsystem", subsystem,
         "--nsid", "25", "--location", "Oz"])
    assert f"Failure changing location for namespace 25 in {subsystem}: Can't find namespace"
