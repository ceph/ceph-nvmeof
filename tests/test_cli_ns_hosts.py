import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.state import GatewayState
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os

image = "mytestdevimage"
pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
hostnqn1 = "nqn.2016-06.io.spdk:host1"
hostnqn2 = "nqn.2016-06.io.spdk:host2"
hostnqn3 = "nqn.2016-06.io.spdk:host3"
hostnqn4 = "nqn.2016-06.io.spdk:host4"
hostnqn5 = "nqn.2016-06.io.spdk:host5"
config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["state_update_notify"] = "False"
    config.config["gateway"]["state_update_interval_sec"] = "5"
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
    pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    pb2_grpc.GatewayStub(channelB)

    return gatewayA, gatewayB


def test_create_resources(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
         "--rbd-create-image", "--no-auto-visible"])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text


def test_add_host_no_access(caplog, two_gateways):
    caplog.clear()
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn1])
    assert f"Failure adding host {hostnqn1} to namespace 1 on {subsystem1}: " \
           f"Host is not allowed to access the subsystem, " \
           f"use the \"force\" parameter to add the host anyway" in caplog.text
    caplog.clear()
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn1, "--force"])
    assert f"Adding host {hostnqn1} to namespace 1 on {subsystem1}: Successful" in caplog.text
    assert f"Received request to add host {hostnqn1} to namespace 1 on {subsystem1}, " \
           f"force: False, context: <grpc._server" not in caplog.text
    assert f"Received request to add host {hostnqn1} to namespace 1 on {subsystem1}, " \
           f"force: True, context: <grpc._server" in caplog.text
    time.sleep(20)
    assert f"Received request to add host {hostnqn1} to namespace 1 on {subsystem1}, " \
           f"force: False, context: None" not in caplog.text
    assert f"Received request to add host {hostnqn1} to namespace 1 on {subsystem1}, " \
           f"force: True, context: None" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn1 in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list_hosts", "--subsystem",
         subsystem1, "--nsid", "1"])
    assert hostnqn1 in caplog.text
    caplog.clear()
    cli(["namespace", "del_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn1])
    assert f"Deleting host {hostnqn1} from namespace 1 on {subsystem1}: " \
           f"Successful" in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn1 not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list_hosts", "--subsystem",
         subsystem1, "--nsid", "1"])
    assert hostnqn1 not in caplog.text


def test_add_host_success(caplog, two_gateways):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn2])
    assert f"Adding host {hostnqn2} to {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem1])
    assert hostnqn2 in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "host", "list", "--subsystem", subsystem1])
    assert hostnqn2 in caplog.text
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn2])
    assert f"Adding host {hostnqn2} to namespace 1 on {subsystem1}: Successful" in caplog.text
    assert f"Received request to add host {hostnqn2} to namespace 1 on {subsystem1}, " \
           f"force: False, context: <grpc._server" in caplog.text
    assert f"Received request to add host {hostnqn2} to namespace 1 on {subsystem1}, " \
           f"force: True, context: <grpc._server" not in caplog.text
    time.sleep(20)
    assert f"Received request to add host {hostnqn2} to namespace 1 on {subsystem1}, " \
           f"force: False, context: None" not in caplog.text
    assert f"Received request to add host {hostnqn2} to namespace 1 on {subsystem1}, " \
           f"force: True, context: None" in caplog.text


def test_remove_host_included_in_netmask(caplog, two_gateways):
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", hostnqn2])
    assert f"Failure removing host {hostnqn2} access from {subsystem1}: " \
           f"Host is included in the netmask of namespace 1. Either remove it or use " \
           f"the \"force\" parameter." in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem1])
    assert hostnqn2 in caplog.text
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", hostnqn2, "--force"])
    assert f"Removing host {hostnqn2} access from {subsystem1}: Successful" in caplog.text
    assert f"Host {hostnqn2} is included in the netmask of namespace 1 in subsystem " \
           f"{subsystem1}. Will continue as the \"force\" parameter was used"
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem1])
    assert hostnqn2 not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn2 in caplog.text
    caplog.clear()
    cli(["namespace", "del_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn2])
    assert f"Deleting host {hostnqn2} from namespace 1 on {subsystem1}: " \
           f"Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn2 not in caplog.text


def test_add_host_open_access_success(caplog, two_gateways):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", "*"])
    assert f"Subsystem {subsystem1} will be opened to be accessed from any " \
           f"host. This might be a security breach" in caplog.text
    assert f"Allowing open host access to {subsystem1}: Successful" in caplog.text
    assert f"Open host access to subsystem {subsystem1} might be a " \
           f"security breach" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem1])
    assert hostnqn3 not in caplog.text
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn3])
    assert f"Adding host {hostnqn3} to namespace 1 on {subsystem1}: Successful" in caplog.text
    assert f"Received request to add host {hostnqn3} to namespace 1 on {subsystem1}, " \
           f"force: False, context: <grpc._server" in caplog.text
    assert f"Received request to add host {hostnqn3} to namespace 1 on {subsystem1}, " \
           f"force: True, context: <grpc._server" not in caplog.text
    time.sleep(20)
    assert f"Received request to add host {hostnqn3} to namespace 1 on {subsystem1}, " \
           f"force: False, context: None" not in caplog.text
    assert f"Received request to add host {hostnqn3} to namespace 1 on {subsystem1}, " \
           f"force: True, context: None" in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn3 in caplog.text
    assert "One of the hosts relied on the subsystem being open for all hosts" in caplog.text


def test_del_open_access(caplog, two_gateways):
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", "*"])
    assert f"Failure disabling open host access to {subsystem1}: One of the hosts in the " \
           f"netmask of namespace 1 relies on the subsystem being open for all hosts. Either " \
           f"clear the netmask or use the \"force\" parameter." in caplog.text
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", "*", "--force"])
    assert f"Disabling open host access to {subsystem1}: Successful" in caplog.text
    assert "One of the hosts in the netmask of namespace 1 relies on the subsystem being " \
           "open for all hosts. Will continue as the \"force\" parameter was used" in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn3 in caplog.text
    assert "One of the hosts relied on the subsystem being open for all hosts" in caplog.text
    caplog.clear()
    cli(["namespace", "del_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn3])
    assert f"Deleting host {hostnqn3} from namespace 1 on {subsystem1}: " \
           f"Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn3 not in caplog.text
    assert "One of the hosts relied on the subsystem being open for all hosts" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert '"hosts": []' in caplog.text
    assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text


def test_change_visibility_with_hosts_fail(caplog, two_gateways):
    gwA, _ = two_gateways
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", "*"])
    assert f"Allowing open host access to {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn4])
    assert f"Adding host {hostnqn4} to namespace 1 on {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "add_host", "--subsystem", subsystem1, "--nsid", "1",
         "--host-nqn", hostnqn5])
    assert f"Adding host {hostnqn5} to namespace 1 on {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn4 in caplog.text
    assert hostnqn5 in caplog.text
    state = gwA.gateway_rpc.gateway_state.omap.get_state()
    found_host4 = False
    found_host5 = False
    for key, val in state.items():
        if key.startswith(GatewayState.NAMESPACE_HOST_PREFIX):
            if hostnqn4 in key:
                found_host4 = True
            if hostnqn5 in key:
                found_host5 = True
    assert found_host4
    assert found_host5
    caplog.clear()
    cli(["namespace", "change_visibility", "--subsystem", subsystem1, "--nsid", "1",
         "--auto-visible", "yes"])
    assert f"Failure changing visibility for namespace 1 in {subsystem1}: Asking to change " \
           f"visibility of namespace to be visible to all hosts while there are already hosts " \
           f"added to it. Either remove these hosts or use the \"force\" parameter" in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn4 in caplog.text
    assert hostnqn5 in caplog.text
    state = gwA.gateway_rpc.gateway_state.omap.get_state()
    found_host4 = False
    found_host5 = False
    for key, val in state.items():
        if key.startswith(GatewayState.NAMESPACE_HOST_PREFIX):
            if hostnqn4 in key:
                found_host4 = True
            if hostnqn5 in key:
                found_host5 = True
    assert found_host4
    assert found_host5


def test_change_visibility_with_hosts_force(caplog, two_gateways):
    gwA, _ = two_gateways
    caplog.clear()
    cli(["namespace", "change_visibility", "--subsystem", subsystem1, "--nsid", "1",
         "--auto-visible", "yes", "--force"])
    assert f'Changing visibility of namespace 1 in {subsystem1} to "visible to all hosts": ' \
           f'Successful' in caplog.text
    assert f"Asking to change visibility of namespace 1 in {subsystem1} to be visible to all " \
           f"hosts while there are already hosts added to it. Will continue as the \"force\" " \
           f"parameter was used but these hosts will be removed from the namespace." in caplog.text
    caplog.clear()
    cli(["namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert hostnqn4 not in caplog.text
    assert hostnqn5 not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list_hosts", "--subsystem", subsystem1, "--nsid", "1"])
    assert '"hosts": []' in caplog.text
    state = gwA.gateway_rpc.gateway_state.omap.get_state()
    for key, val in state.items():
        assert not key.startswith(GatewayState.NAMESPACE_HOST_PREFIX)
