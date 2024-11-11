import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import spdk.rpc.nvmf as rpc_nvmf
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time

image = "mytestdevimage"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
hostnqn1 = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288ff"
hostnqn2 = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288fe"
hostnqn3 = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288fd"
hostnqn4 = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288fc"
hostnqn5 = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288fb"
discovery_nqn = "nqn.2014-08.org.nvmexpress.discovery"
key1 = "DHHC-1:01:rPTE0Q73nd3hEqqEuQNaPL11G/aFXpOHtldWXz9vNCeef4WV:"
key2 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
key3 = "DHHC-1:01:KD+sfH3/o2bRQoV0ESjBUywQlMnSaYpZISUbVa0k0nsWpNST:"
key4 = "DHHC-1:01:x7ecfGgIdOEl+J5cJ9JcZHOS2By2Me6eDJUnrsT9MVrCWRYV:"
hostpsk1 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
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
    configA.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x03"
    portA = configA.getint("gateway", "port") + 1
    configA.config["gateway"]["port"] = str(portA)
    discPortA = configA.getint("discovery", "port") + 1
    configA.config["discovery"]["port"] = str(discPortA)
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 1
    discPortB = discPortA + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x0C"

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

        yield gatewayA, stubA, gatewayB, stubB
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)

def test_change_host_key(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    gwA = gatewayA.gateway_rpc
    gwB = gatewayB.gateway_rpc
    caplog.clear()
    cli(["--server-port", "5501", "subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn1])
    assert f"Adding host {hostnqn1} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn2, "--dhchap-key", key1, "--force"])
    assert f"Adding host {hostnqn2} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn1, "--dhchap-key", key2, "--force"])
    assert f"Changing key for host {hostnqn1} on subsystem {subsystem}: Successful" in caplog.text
    time.sleep(15)
    assert f"Received request to change inband authentication key for host {hostnqn1} on subsystem {subsystem}, dhchap: {key2}, force: True, context: <grpc._server" in caplog.text
    assert f"Received request to change inband authentication key for host {hostnqn1} on subsystem {subsystem}, dhchap: {key2}, force: True, context: None" in caplog.text
    assert f"Received request to remove host {hostnqn1} access from {subsystem}" not in caplog.text
    assert f"Received request to add host {hostnqn1} to {subsystem}" not in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn2, "--dhchap-key", key3, "--force"])
    time.sleep(15)
    assert f"Received request to change inband authentication key for host {hostnqn2} on subsystem {subsystem}, dhchap: {key3}, force: True, context: <grpc._server" in caplog.text
    assert f"Received request to change inband authentication key for host {hostnqn2} on subsystem {subsystem}, dhchap: {key3}, force: True, context: None" in caplog.text
    assert f"Received request to remove host {hostnqn2} access from {subsystem}" not in caplog.text
    assert f"Received request to add host {hostnqn2} to {subsystem}" not in caplog.text

def test_change_key_with_psk(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    gwA = gatewayA.gateway_rpc
    caplog.clear()
    cli(["--server-port", "5501", "host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", hostpsk1])
    assert f"Adding host {hostnqn3} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--dhchap-key", key4, "--force"])
    assert f"Changing key for host {hostnqn3} on subsystem {subsystem}: Successful" in caplog.text

def test_change_key_host_not_exist(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    gwA = gatewayA.gateway_rpc
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn4, "--dhchap-key", "junk", "--force"])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn4} on subsystem {subsystem}: Can't find host on subsystem" in caplog.text

def test_change_key_host_on_discovery(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    gwA = gatewayA.gateway_rpc
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", discovery_nqn, "--dhchap-key", "junk", "--force"])
    assert f"Failure changing DH-HMAC-CHAP key for host {discovery_nqn} on subsystem {subsystem}: Can't use a discovery NQN as host's" in caplog.text
    caplog.clear()
    cli(["--server-port", "5501", "host", "change_key", "--subsystem", subsystem, "--host-nqn", "bad_nqn", "--dhchap-key", "junk", "--force"])
    assert f"Failure changing DH-HMAC-CHAP key for host bad_nqn on subsystem {subsystem}: Invalid host NQN" in caplog.text
