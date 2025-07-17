import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import spdk.rpc.nvmf as rpc_nvmf
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os

image = "mytestdevimage"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
anagrpid = "1"
anagrpid2 = "2"
uuid = "9dee1f89-e950-4a2f-b984-244ea73f1851"
uuid2 = "9dee1f89-e950-4a2f-b984-244ea73f1852"
config = "ceph-nvmeof.conf"
namespace_count = 20


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    config.config["gateway"]["rebalance_period_sec"] = "0"
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
        stubA = pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channelB)

        yield gatewayA, stubA, gatewayB, stubB
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def verify_one_namespace_lb_group(caplog, gw_port, subsys, nsid_to_verify, grp):
    caplog.clear()
    cli(["--server-port", gw_port, "--format", "json", "namespace", "list",
         "--subsystem", subsys, "--nsid", nsid_to_verify])
    assert f'"nsid": {nsid_to_verify},' in caplog.text
    assert f'"load_balancing_group": {grp},' in caplog.text


def verify_namespaces(caplog, gw_port, subsys, first_nsid, last_nsid, grp):
    for ns in range(first_nsid, last_nsid + 1):
        verify_one_namespace_lb_group(caplog, gw_port, subsys, str(ns), grp)


def verify_namespaces_using_get_subsystems(caplog, gw_port, subsys, first_nsid, last_nsid, grp):
    caplog.clear()
    subsys_info = cli_test(["--server-port", gw_port, "get_subsystems"])
    assert len(subsys_info.subsystems) == 1
    assert subsys_info.subsystems[0].nqn == subsys
    assert len(subsys_info.subsystems[0].namespaces) >= last_nsid
    for ns in range(first_nsid, last_nsid + 1):
        assert subsys_info.subsystems[0].namespaces[ns - 1].nsid == ns
        assert subsys_info.subsystems[0].namespaces[ns - 1].anagrpid == grp


def verify_namespaces_using_spdk_get_subsystems(caplog, gw, subsys, first_nsid, last_nsid, grp):
    caplog.clear()
    with gw.rpc_lock:
        subsys_info = rpc_nvmf.nvmf_get_subsystems(gw.gateway_rpc.spdk_rpc_client)
    assert len(subsys_info) == 1
    assert subsys_info[0]["nqn"] == subsys
    assert len(subsys_info[0]["namespaces"]) >= last_nsid
    for ns in range(first_nsid, last_nsid + 1):
        assert subsys_info[0]["namespaces"][ns - 1]["nsid"] == ns
        assert subsys_info[0]["namespaces"][ns - 1]["anagrpid"] == grp


def create_namespaces(caplog, ns_count, subsys):
    for i in range(1, 1 + (ns_count // 2)):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsys, "--rbd-pool", pool,
             "--rbd-image", f"{image}{i}", "--size", "16MB", "--rbd-create-image",
             "--load-balancing-group", anagrpid])
        assert f"Adding namespace {i} to {subsys}: Successful" in caplog.text
    for i in range(1 + (ns_count // 2), 1 + ns_count):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsys, "--rbd-pool", pool, "--rbd-image",
             f"{image}{i}", "--size", "16MB", "--rbd-create-image",
             "--load-balancing-group", anagrpid2])
        assert f"Adding namespace {i} to {subsys}: Successful" in caplog.text


def try_change_one_namespace_lb_group_no_listeners(caplog, subsys, nsid_to_change, new_group):
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "change_load_balancing_group",
         "--subsystem", subsys, "--nsid", nsid_to_change, "--load-balancing-group", new_group])
    time.sleep(15)
    assert f"Changing load balancing group of namespace {nsid_to_change} in {subsys} " \
           f"to {new_group}: Successful" in caplog.text
    assert "try running the command from there" not in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"{nsid_to_change} in {subsys} to {new_group}, context: <grpc._server" in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"{nsid_to_change} in {subsys} to {new_group}, context: None" in caplog.text


def change_one_namespace_lb_group(caplog, subsys, nsid_to_change, new_group):
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "change_load_balancing_group",
         "--subsystem", subsys, "--nsid", nsid_to_change, "--load-balancing-group", new_group])
    time.sleep(15)
    if "try running the command from there" in caplog.text:
        caplog.clear()
        cli(["namespace", "change_load_balancing_group", "--subsystem", subsys,
             "--nsid", nsid_to_change, "--load-balancing-group", new_group])
        time.sleep(15)

    assert f"Changing load balancing group of namespace {nsid_to_change} in {subsys} " \
           f"to {new_group}: Successful" in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"{nsid_to_change} in {subsys} to {new_group}, context: <grpc._server" in caplog.text
    assert "Received request to delete namespace" not in caplog.text
    assert "Received request to remove namespace" not in caplog.text
    assert "Received request to add a namespace" not in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"{nsid_to_change} in {subsys} to {new_group}, context: None" in caplog.text


def switch_namespaces_lb_group(caplog, ns_count, subsys):
    for i in range(1, 1 + (ns_count // 2)):
        change_one_namespace_lb_group(caplog, subsys, f"{i}", anagrpid2)
    for i in range(1 + (ns_count // 2), 1 + ns_count):
        change_one_namespace_lb_group(caplog, subsys, f"{i}", anagrpid)


def test_change_namespace_lb_group(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image, "--size", "16MB", "--rbd-create-image", "--uuid", uuid,
         "--load-balancing-group", anagrpid, "--force"])
    time.sleep(15)
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
    assert f"Received request to add namespace to {subsystem}, ana group {anagrpid}, " \
           f"no_auto_visible: False, disable_auto_resize: False, " \
           f"read_only: False, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}, ana group {anagrpid}, " \
           f"no_auto_visible: False, disable_auto_resize: False, " \
           f"read_only: False, " \
           f"context: None" in caplog.text
    caplog.clear()
    cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "1",
         "--rw-ios-per-second", "2000"])
    assert f"Setting QOS limits of namespace 1 in {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' not in caplog.text
    assert '"rw_ios_per_second": "2000",' in caplog.text
    assert '"rw_mbytes_per_second": "0",' in caplog.text
    assert '"r_mbytes_per_second": "0",' in caplog.text
    assert '"w_mbytes_per_second": "0",' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' not in caplog.text
    assert '"rw_ios_per_second": "2000",' in caplog.text
    assert '"rw_mbytes_per_second": "0",' in caplog.text
    assert '"r_mbytes_per_second": "0",' in caplog.text
    assert '"w_mbytes_per_second": "0",' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    try_change_one_namespace_lb_group_no_listeners(caplog, subsystem, "1", anagrpid2)
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' not in caplog.text
    try_change_one_namespace_lb_group_no_listeners(caplog, subsystem, "1", anagrpid)
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "listener", "add", "--subsystem", subsystem,
         "--host-name", "GatewayBB", "--traddr", "127.0.0.1", "--trsvcid", "4420"])
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", "GatewayAA",
         "--traddr", "127.0.0.1", "--trsvcid", "4430"])
    change_one_namespace_lb_group(caplog, subsystem, "1", anagrpid2)
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' not in caplog.text
    assert '"rw_ios_per_second": "2000",' in caplog.text
    assert '"rw_mbytes_per_second": "0",' in caplog.text
    assert '"r_mbytes_per_second": "0",' in caplog.text
    assert '"w_mbytes_per_second": "0",' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"uuid": "{uuid}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' not in caplog.text
    assert '"rw_ios_per_second": "2000",' in caplog.text
    assert '"rw_mbytes_per_second": "0",' in caplog.text
    assert '"r_mbytes_per_second": "0",' in caplog.text
    assert '"w_mbytes_per_second": "0",' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--uuid", uuid2,
         "--rbd-pool", pool, "--rbd-image", f"{image}2", "--size", "16MB", "--rbd-create-image",
         "--load-balancing-group", anagrpid2, "--force"])
    time.sleep(15)
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
    assert f"get_cluster cluster_name='cluster_context_{anagrpid2}_0'" in caplog.text
    assert f"Received request to add namespace to {subsystem}, ana group {anagrpid2}, " \
           f"no_auto_visible: False, disable_auto_resize: False, " \
           f"read_only: False, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to add namespace 2 to {subsystem}, ana group {anagrpid2}, " \
           f"no_auto_visible: False, disable_auto_resize: False, " \
           f"read_only: False, " \
           f"context: None" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert f'"uuid": "{uuid2}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert f'"uuid": "{uuid2}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' not in caplog.text
    change_one_namespace_lb_group(caplog, subsystem, "2", anagrpid)
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert f'"uuid": "{uuid2}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "2"])
    assert '"nsid": 2,' in caplog.text
    assert f'"uuid": "{uuid2}",' in caplog.text
    assert f'"load_balancing_group": {anagrpid},' in caplog.text
    assert f'"load_balancing_group": {anagrpid2},' not in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "2"])
    assert f"Deleting namespace 2 from {subsystem}: Successful" in caplog.text
    time.sleep(15)
    create_namespaces(caplog, namespace_count, subsystem)
    time.sleep(15)
    verify_namespaces(caplog, "5500", subsystem, 1, namespace_count // 2, anagrpid)
    verify_namespaces(caplog, "5500", subsystem, 1 + (namespace_count // 2),
                      namespace_count, anagrpid2)
    verify_namespaces(caplog, "5502", subsystem, 1, namespace_count // 2, anagrpid)
    verify_namespaces(caplog, "5502", subsystem, 1 + (namespace_count // 2),
                      namespace_count, anagrpid2)

    verify_namespaces_using_get_subsystems(caplog, "5500", subsystem, 1, namespace_count // 2,
                                           int(anagrpid))
    verify_namespaces_using_get_subsystems(caplog, "5500", subsystem, 1 + (namespace_count // 2),
                                           namespace_count, int(anagrpid2))
    verify_namespaces_using_get_subsystems(caplog, "5502", subsystem, 1, namespace_count // 2,
                                           int(anagrpid))
    verify_namespaces_using_get_subsystems(caplog, "5502", subsystem, 1 + (namespace_count // 2),
                                           namespace_count, int(anagrpid2))

    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayA, subsystem, 1,
                                                namespace_count // 2, int(anagrpid))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayA, subsystem,
                                                1 + (namespace_count // 2), namespace_count,
                                                int(anagrpid2))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayB, subsystem, 1,
                                                namespace_count // 2, int(anagrpid))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayB, subsystem,
                                                1 + (namespace_count // 2), namespace_count,
                                                int(anagrpid2))

    switch_namespaces_lb_group(caplog, namespace_count, subsystem)
    time.sleep(15)
    verify_namespaces(caplog, "5500", subsystem, 1, namespace_count // 2, anagrpid2)
    verify_namespaces(caplog, "5500", subsystem, 1 + (namespace_count // 2),
                      namespace_count, anagrpid)
    verify_namespaces(caplog, "5502", subsystem, 1, namespace_count // 2, anagrpid2)
    verify_namespaces(caplog, "5502", subsystem, 1 + (namespace_count // 2),
                      namespace_count, anagrpid)

    verify_namespaces_using_get_subsystems(caplog, "5500", subsystem, 1, namespace_count // 2,
                                           int(anagrpid2))
    verify_namespaces_using_get_subsystems(caplog, "5500", subsystem, 1 + (namespace_count // 2),
                                           namespace_count, int(anagrpid))
    verify_namespaces_using_get_subsystems(caplog, "5502", subsystem, 1, namespace_count // 2,
                                           int(anagrpid2))
    verify_namespaces_using_get_subsystems(caplog, "5502", subsystem, 1 + (namespace_count // 2),
                                           namespace_count, int(anagrpid))

    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayA, subsystem, 1,
                                                namespace_count // 2, int(anagrpid2))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayA, subsystem,
                                                1 + (namespace_count // 2), namespace_count,
                                                int(anagrpid))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayB, subsystem, 1,
                                                namespace_count // 2, int(anagrpid2))
    verify_namespaces_using_spdk_get_subsystems(caplog, gatewayB, subsystem,
                                                1 + (namespace_count // 2), namespace_count,
                                                int(anagrpid))
