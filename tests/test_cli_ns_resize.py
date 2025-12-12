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
        if gatewayA and gatewayA.server:
            gatewayA.server.stop(grace=1)
        if gatewayB and gatewayB.server:
            gatewayB.server.stop(grace=1)


def test_namespace_resize(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image, "--size", "16MB", "--rbd-create-image",
         "--disable-auto-resize"])
    time.sleep(15)
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    assert f"Received request to add namespace to {subsystem}, ana group 0, " \
           f"no_auto_visible: False, disable_auto_resize: True, " \
           f"read_only: False, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}, ana group 1, " \
           f"no_auto_visible: False, disable_auto_resize: True, " \
           f"read_only: False, " \
           f"context: None" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"rbd_image_name": "{image}",' in caplog.text
    assert '"rbd_image_size": "16777216",' in caplog.text
    assert '"load_balancing_group": 1,' in caplog.text
    assert '"disable_auto_resize": true,' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert f'"rbd_image_name": "{image}",' in caplog.text
    assert '"rbd_image_size": "16777216",' in caplog.text
    assert '"load_balancing_group": 1,' in caplog.text
    assert '"disable_auto_resize": true,' in caplog.text
    assert '"auto_visible": true,' in caplog.text
    assert '"hosts": []' in caplog.text
    caplog.clear()
    cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "1", "--size", "18MB"])
    assert f"Resizing namespace 1 in {subsystem} to 18 MiB: Successful" in caplog.text
    time.sleep(15)
    assert f"Received request to resize namespace 1 on {subsystem} to 18 MiB, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to resize namespace 1 on {subsystem} to 0 MiB, " \
           f"context: None" in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "18874368",' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "18874368",' in caplog.text
    state = gatewayA.gateway_rpc.gateway_state.omap.get_state()
    found = False
    for key, val in state.items():
        if key.startswith(GatewayState.NAMESPACE_REFRESH_SIZE_PREFIX):
            found = True
            break
    assert found, "No namespace refresh size entry in OMAP"
    caplog.clear()
    cli(["namespace", "set_auto_resize", "--subsystem", subsystem, "--nsid", "1",
         "--auto-resize-enabled", "yes"])
    assert f'Setting auto resize flag for namespace 1 in {subsystem} to ' \
           f'"auto resize namespace": Successful' in caplog.text
    time.sleep(15)
    assert f'Received request to set the auto resize flag of namespace 1 in {subsystem} to ' \
           f'"auto resize namespace", context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to set the auto resize flag of namespace 1 in {subsystem} to ' \
           f'"auto resize namespace", context: None' in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    state = gatewayA.gateway_rpc.gateway_state.omap.get_state()
    found = False
    for key, val in state.items():
        if key.startswith(GatewayState.NAMESPACE_REFRESH_SIZE_PREFIX):
            found = True
            break
    assert not found, "Shouldn't have namespace refresh size entry in OMAP " \
                      "when auto-resize is enabled"
    caplog.clear()
    cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "1", "--size", "20MB"])
    assert f"Resizing namespace 1 in {subsystem} to 20 MiB: Successful" in caplog.text
    time.sleep(15)
    assert f"Received request to resize namespace 1 on {subsystem} to 20 MiB, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to resize namespace 1 on {subsystem} to 0 MiB, " \
           f"context: None" not in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "20971520",' in caplog.text
    assert '"disable_auto_resize": false,' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "20971520",' in caplog.text
    assert '"disable_auto_resize": false,' in caplog.text
    caplog.clear()
    cli(["namespace", "set_auto_resize", "--subsystem", subsystem, "--nsid", "1",
         "--auto-resize-enabled", "no"])
    assert f'Setting auto resize flag for namespace 1 in {subsystem} to ' \
           f'"do not auto resize namespace": Successful' in caplog.text
    time.sleep(15)
    assert f'Received request to set the auto resize flag of namespace 1 in {subsystem} to ' \
           f'"do not auto resize namespace", context: <grpc._server' in caplog.text
    time.sleep(15)
    assert f'Received request to set the auto resize flag of namespace 1 in {subsystem} to ' \
           f'"do not auto resize namespace", context: None' in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "1", "--size", "22MB"])
    assert f"Resizing namespace 1 in {subsystem} to 22 MiB: Successful" in caplog.text
    time.sleep(15)
    assert f"Received request to resize namespace 1 on {subsystem} to 22 MiB, " \
           f"context: <grpc._server" in caplog.text
    assert f"Received request to resize namespace 1 on {subsystem} to 0 MiB, " \
           f"context: None" in caplog.text
    assert f"Received request to delete namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "23068672",' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "23068672",' in caplog.text


def test_gateway_restart(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    caplog.clear()
    gwB = gatewayB.gateway_rpc
    configB = gwB.config
    portB = gwB.config.config["gateway"]["port"]
    gatewayB.__exit__(None, None, None)
    print("Restarting gateway B")
    time.sleep(15)
    gatewayB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gatewayB.name}", "pool": "{pool}", '
        f'"group": ""' + "}"
    )
    gatewayB.serve()
    retries = 5
    while retries > 0:
        if gatewayB.gateway_rpc.gateway_state.is_initialization_over():
            break
        print("Gateway B still initializing...")
        retries -= 1
        time.sleep(15)
    assert retries > 0, "Gateway is not fully initialized after restart"
    assert f"Will not refresh size of namespace 1 in subsystem {subsystem} as the " \
           f"gateway is coming up" in caplog.text

    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "23068672",' in caplog.text

    state = gatewayB.gateway_rpc.gateway_state.omap.get_state()
    found = False
    for key, val in state.items():
        if key.startswith(gwB.gateway_state.local.NAMESPACE_REFRESH_SIZE_PREFIX):
            found = True
            break
    assert found, "No namespace refresh size entry in OMAP"
    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "namespace", "list",
         "--subsystem", subsystem, "--nsid", "1"])
    assert '"nsid": 1,' in caplog.text
    assert '"rbd_image_size": "23068672",' in caplog.text
