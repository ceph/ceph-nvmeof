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
        stubA = pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channelB)

        yield gatewayA, stubA, gatewayB, stubB
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def test_change_namespace_visibility(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", f"{image}", "--size", "16MB", "--rbd-create-image",
         "--load-balancing-group", "1"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"load_balancing_group": 1,' in caplog.text
    time.sleep(15)
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    caplog.clear()
    cli(["namespace", "change_visibility", "--subsystem", subsystem,
         "--nsid", "1", "--auto-visible", "no"])
    cli(["namespace", "change_load_balancing_group", "--subsystem", subsystem,
         "--nsid", "1", "--load-balancing-group", "2"])
    cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
         "--nsid", "1", "--rbd-trash-image-on-delete", "yes"])
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to selected hosts": ' \
           f'Successful' in caplog.text
    assert f"Changing load balancing group of namespace 1 in {subsystem} " \
           f"to 2: Successful" in caplog.text
    assert f'Setting RBD trash image flag for namespace 1 in {subsystem} to ' \
           f'"trash on namespace deletion": Successful' in caplog.text
    assert f'Received request to change the visibility of namespace 1 in {subsystem} ' \
           f'to "visible to selected hosts", force: False, context: <grpc._server' in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"1 in {subsystem} to 2, context: <grpc._server" in caplog.text
    assert f'Received request to set the RBD trash image flag of namespace 1 in ' \
           f'{subsystem} to "trash on namespace deletion", context: ' \
           f'<grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to change the visibility of namespace 1 in {subsystem} ' \
           f'to "visible to selected hosts", force: True, context: None' in caplog.text
    assert f"Received manual request to change load balancing group for namespace with ID " \
           f"1 in {subsystem} to 2, context: None" in caplog.text
    assert f'Received request to set the RBD trash image flag of namespace 1 in ' \
           f'{subsystem} to "trash on namespace deletion", context: ' \
           f'None' in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible":' not in caplog.text or '"auto_visible": false,' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "change_visibility",
         "--subsystem", subsystem, "--nsid", "1", "--auto-visible", "yes"])
    assert f'Changing visibility of namespace 1 in {subsystem} to "visible to all hosts": ' \
           f'Successful' in caplog.text
    assert f'Received request to change the visibility of namespace 1 in {subsystem} to ' \
           f'"visible to all hosts", force: False, context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to change the visibility of namespace 1 in {subsystem} to ' \
           f'"visible to all hosts", force: True, context: None' in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"auto_visible": true,' in caplog.text
