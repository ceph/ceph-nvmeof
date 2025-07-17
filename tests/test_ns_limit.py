import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
import copy
import random
import os
import time
from control.proto import gateway_pb2_grpc as pb2_grpc

image_prefix = "testimage"
image_count = 0
pool = "rbd"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
subsystem_count = 8
namespace_count = 2048
namespace_delete_percentage = 25
group_name = "group1"
update_interval = 300


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    addr = config.get("gateway", "addr")
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["state_update_interval_sec"] = str(update_interval)
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
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", '
                  f'"group": "{group_name}"' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", '
                  f'"group": "{group_name}"' + "}"
        )
        gatewayA.serve()
        gatewayB.serve()

        # Bind the client and Gateway
        channelA = grpc.insecure_channel(f"{addr}:{portA}")
        pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        pb2_grpc.GatewayStub(channelB)

        yield gatewayA, gatewayB

        # Stop gateway
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def get_image_name():
    global image_count
    image = f"{image_prefix}_{image_count}"
    image_count += 1
    return image


def create_namespaces_for_subsystem(caplog, subsys, ns_cnt):
    for i in range(1, ns_cnt + 1):
        caplog.clear()
        image = get_image_name()
        cli(["namespace", "add", "--subsystem", subsys, "--rbd-pool", pool,
             "--rbd-image", image, "--size", "10MB", "--rbd-create-image"])
        assert "Adding namespace " in caplog.text
        assert f" to {subsys}: Successful" in caplog.text
        assert "Failure adding namespace" not in caplog.text


def create_namespaces_for_all_subsystems(caplog, subsys_cnt, ns_per_subsys):
    for subsys_id in range(1, subsys_cnt + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsys, "--no-group-append"])
        assert f"Adding subsystem {subsys}: Successful" in caplog.text
        create_namespaces_for_subsystem(caplog, subsys, ns_per_subsys)


def delete_one_namespace(caplog, subsys, nsid):
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsys, "--nsid", str(nsid)])
    assert f"Deleting namespace {nsid} from {subsys}: Successful" in caplog.text


def delete_namespaces_from_subsystem(caplog, subsys, subsys_ns_cnt, ns_count_to_delete):
    assert ns_count_to_delete <= subsys_ns_cnt
    nsids_to_delete = []
    random.seed()
    for i in range(1, ns_count_to_delete + 1):
        while True:
            ns_to_delete = random.randint(1, subsys_ns_cnt)
            if ns_to_delete not in nsids_to_delete:
                nsids_to_delete.insert(0, ns_to_delete)
                break
    assert len(nsids_to_delete) == ns_count_to_delete
    for nsid in nsids_to_delete:
        delete_one_namespace(caplog, subsys, nsid)


def set_qos_for_subsystem_namespaces(caplog, subsys, subsys_ns_cnt):
    for ns in range(1, subsys_ns_cnt + 1):
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsys, "--nsid", str(ns)])
        assert f'"nsid": {ns},' in caplog.text
        assert '"rw_ios_per_second": "0"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsys, "--nsid", str(ns),
             "--rw-ios-per-second", "2000"])
        assert f"Setting QOS limits of namespace {ns} in {subsys}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsys, "--nsid", str(ns)])
        assert f'"nsid": {ns},' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text


def verify_namespace_count(caplog, port, desired_ns_count):
    caplog.clear()
    ns_list = cli_test(["--server-port", port, "--format", "json", "namespace", "list"])
    if len(ns_list.namespaces) < desired_ns_count:
        # wait a little longer before giving up
        time.sleep(60)
        caplog.clear()
        ns_list = cli_test(["--server-port", port, "--format", "json", "namespace", "list"])
    assert len(ns_list.namespaces) == desired_ns_count


def test_ns_limit(caplog, two_gateways):
    gwA, gwB = two_gateways
    portA = gwA.config.config["gateway"]["port"]
    portB = gwB.config.config["gateway"]["port"]
    waitForUpdate = max(int(gwA.config.config["gateway"]["state_update_interval_sec"]),
                        int(gwB.config.config["gateway"]["state_update_interval_sec"]))
    waitForUpdate += 30
    ns_per_subsys = namespace_count // subsystem_count
    assert ns_per_subsys > 0
    assert ns_per_subsys * subsystem_count == namespace_count
    create_namespaces_for_all_subsystems(caplog, subsystem_count, ns_per_subsys)
    time.sleep(waitForUpdate)
    verify_namespace_count(caplog, portA, namespace_count)
    verify_namespace_count(caplog, portB, namespace_count)
    namespace_count_to_delete = (ns_per_subsys * namespace_delete_percentage) // 100
    assert namespace_count_to_delete > 0
    assert namespace_count_to_delete < ns_per_subsys
    for subsys_id in range(1, subsystem_count + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        delete_namespaces_from_subsystem(caplog, subsys, ns_per_subsys, namespace_count_to_delete)
    time.sleep(waitForUpdate)
    verify_namespace_count(caplog, portA,
                           namespace_count - (namespace_count_to_delete * subsystem_count))
    verify_namespace_count(caplog, portB,
                           namespace_count - (namespace_count_to_delete * subsystem_count))
    for subsys_id in range(1, subsystem_count + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        create_namespaces_for_subsystem(caplog, subsys, namespace_count_to_delete)
    time.sleep(waitForUpdate)
    verify_namespace_count(caplog, portA, namespace_count)
    verify_namespace_count(caplog, portB, namespace_count)
    for subsys_id in range(1, subsystem_count + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        set_qos_for_subsystem_namespaces(caplog, subsys, ns_per_subsys)
