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
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
hostnqn = "nqn.2016-06.io.spdk:host1"
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
    config.config["gateway"]["state_update_notify"] = "False"
    config.config["gateway"]["state_update_interval_sec"] = "60"
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

        yield gatewayA, gatewayB
        if gatewayA.server:
            gatewayA.server.stop(grace=1)
        if gatewayB.server:
            gatewayB.server.stop(grace=1)


def test_change_namespace_visibility(caplog, two_gateways):
    gatewayA, gatewayB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", f"{image}", "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    time.sleep(90)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    caplog.clear()
    cli(["namespace", "change_visibility", "--subsystem", subsystem,
         "--nsid", "1", "--auto-visible", "no"])
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to selected hosts": ' \
           f'Successful' in caplog.text
    assert f'Received request to change the visibility of namespace 1 in {subsystem} ' \
           f'to "visible to selected hosts", force: False, context: <grpc._server' in caplog.text
    time.sleep(90)
    assert f'Received request to change the visibility of namespace 1 in {subsystem} ' \
           f'to "visible to selected hosts", force: True, context: None' in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "change_visibility",
         "--subsystem", subsystem, "--nsid", "1", "--auto-visible", "yes"])
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to all hosts": ' \
           f'Successful' in caplog.text
    assert f'Received request to change the visibility of namespace 1 in {subsystem} to ' \
           f'"visible to all hosts", force: False, context: <grpc._server' in caplog.text
    time.sleep(90)
    assert f'Received request to change the visibility of namespace 1 in {subsystem} to ' \
           f'"visible to all hosts", force: True, context: None' in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text

    gwB = gatewayB.gateway_rpc
    configB = gwB.config
    portB = gwB.config.config["gateway"]["port"]
    addrB = gwB.config.config["gateway"]["addr"]
    assert portB == "5502"
    gatewayB.__exit__(None, None, None)
    print("Restarting gateway B")
    time.sleep(90)
    gatewayB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gatewayB.name}", "pool": "{pool}", '
        f'"group": ""' + "}"
    )
    gatewayB.serve()
    gwB = gatewayB.gateway_rpc
    assert gwB.up_and_running
    channelB = grpc.insecure_channel(f"{addrB}:{portB}")
    pb2_grpc.GatewayStub(channelB)
    time.sleep(90)
    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text


def test_change_namespace_visibility_with_hosts(caplog, two_gateways):
    gatewayA, gatewayB = two_gateways
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*"])
    assert f"Allowing open host access to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "subsystem", "list"])
    assert '"allow_any_host": true,' in caplog.text
    time.sleep(90)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "subsystem", "list"])
    assert '"allow_any_host": true,' in caplog.text
    caplog.clear()
    cli(["namespace", "add_host", "--subsystem", subsystem,
         "--nsid", "1", "--host-nqn", hostnqn])
    assert f"Failure adding host {hostnqn} to namespace 1 on {subsystem}: " \
           f"Namespace is visible to all hosts"
    caplog.clear()
    cli(["namespace", "change_visibility", "--subsystem", subsystem,
         "--nsid", "1", "--auto-visible", "no"])
    cli(["namespace", "add_host", "--subsystem", subsystem,
         "--nsid", "1", "--host-nqn", hostnqn])
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to selected hosts": ' \
           f'Successful' in caplog.text
    assert f"Adding host {hostnqn} to namespace 1 on {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    assert '"hosts": []' in caplog.text
    assert hostnqn not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": false' in caplog.text
    assert '"hosts": []' not in caplog.text
    assert hostnqn in caplog.text
    time.sleep(90)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": false' in caplog.text
    assert '"hosts": []' not in caplog.text
    assert hostnqn in caplog.text
    caplog.clear()
    cli(["namespace", "del_host", "--subsystem", subsystem,
         "--nsid", "1", "--host-nqn", hostnqn])
    cli(["namespace", "change_visibility", "--subsystem", subsystem,
         "--nsid", "1", "--auto-visible", "yes"])
    assert f"Deleting host {hostnqn} from namespace 1 on {subsystem}: Successful" in caplog.text
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to all hosts": ' \
           f'Successful' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    assert '"hosts": []' in caplog.text
    assert hostnqn not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": false' in caplog.text
    assert '"hosts": []' not in caplog.text
    assert hostnqn in caplog.text
    time.sleep(90)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true' in caplog.text
    assert '"hosts": []' in caplog.text
    assert hostnqn not in caplog.text
