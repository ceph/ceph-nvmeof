import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time

image = "mytestdevimage"
image2 = "mytestdevimage2"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
anagrpid = "1"
anagrpid2 = "2"
config = "ceph-nvmeof.conf"

@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = "spdk_GatewayAA.sock"
    sockB = "spdk_GatewayBB.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    portA = configA.getint("gateway", "port") + 1
    configA.config["gateway"]["port"] = str(portA)
    discPortA = configA.getint("discovery", "port") + 1
    configA.config["discovery"]["port"] = str(discPortA)
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = discPortA + 1
    configB.config["gateway"]["port"] = str(portB)
    discPort = configB.getint("discovery", "port") + 1
    configB.config["discovery"]["port"] = str(discPortB)
    configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x02"

    ceph_utils = CephUtils(config)
    with (GatewayServer(configA) as gatewayA, GatewayServer(configB) as gatewayB):
        ceph_utils.execute_ceph_monitor_command("{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", "group": ""' + "}")
        ceph_utils.execute_ceph_monitor_command("{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", "group": ""' + "}")
        gatewayA.serve()
        gatewayB.serve()

        channelA = grpc.insecure_channel(f"{addr}:{portA}")
        stubA = pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channelB)

        yield gatewayA.gateway_rpc, stubA, gatewayB.gateway_rpc, stubB
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)

def test_change_namespace_lb_group(caplog, two_gateways):
    gwA, stubA, gwB, stubB = two_gateways
    caplog.clear()
    cli(["--server-port", "5501", "subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--size", "16MB", "--rbd-create-image", "--load-balancing-group", anagrpid, "--force"])
    time.sleep(5)
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "namespace", "set_qos", "--subsystem", subsystem, "--nsid", "1", "--rw-ios-per-second", "2000"])
    assert f"Setting QOS limits of namespace 1 in {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert f'"nsid": 1' in caplog.text
    assert f'"load_balancing_group": {anagrpid}' in caplog.text
    assert f'"load_balancing_group": {anagrpid2}' not in caplog.text
    assert f'"rw_ios_per_second": "2000"' in caplog.text
    assert f'"rw_mbytes_per_second": "0"' in caplog.text
    assert f'"r_mbytes_per_second": "0"' in caplog.text
    assert f'"w_mbytes_per_second": "0"' in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "namespace", "change_load_balancing_group", "--subsystem", subsystem, "--nsid", "1", "--load-balancing-group", anagrpid2])
    time.sleep(5)
    assert f"Changing load balancing group of namespace 1 in {subsystem} to {anagrpid2}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert f'"nsid": 1' in caplog.text
    assert f'"load_balancing_group": {anagrpid2}' in caplog.text
    assert f'"load_balancing_group": {anagrpid}' not in caplog.text
    assert f'"rw_ios_per_second": "2000"' in caplog.text
    assert f'"rw_mbytes_per_second": "0"' in caplog.text
    assert f'"r_mbytes_per_second": "0"' in caplog.text
    assert f'"w_mbytes_per_second": "0"' in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "namespace", "add", "--subsystem", subsystem, "--nsid", "2", "--rbd-pool", pool, "--rbd-image", image2, "--size", "16MB", "--rbd-create-image", "--load-balancing-group", anagrpid2, "--force"])
    time.sleep(5)
    assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
    assert f"get_cluster cluster_name='cluster_context_{anagrpid2}_0'" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "2"])
    assert f'"nsid": 2' in caplog.text
    assert f'"load_balancing_group": {anagrpid2}' in caplog.text
    assert f'"load_balancing_group": {anagrpid}' not in caplog.text
