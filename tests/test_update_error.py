import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os

pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
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


def test_error_on_update(caplog, two_gateways):
    gatewayA, gatewayB = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem2, "--no-group-append"])
    assert f"create_subsystem {subsystem2}: True" in caplog.text

    gwB = gatewayB.gateway_rpc
    configB = gwB.config
    configB.config["gateway"]["max_subsystems"] = "1"
    configB.config["gateway"]["abort_on_update_error"] = "False"
    configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    portB = gwB.config.config["gateway"]["port"]
    assert portB == "5502"
    gatewayB.__exit__(None, None, None)
    print("Restarting gateway B")
    time.sleep(20)
    caplog.clear()
    gatewayB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gatewayB.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gatewayB.serve()
    time.sleep(20)
    assert "Got error 7 while updating gateway GatewayBB state, will not abort" in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "subsystem", "list"])
    s1 = f'"nqn": "{subsystem}"'
    s2 = f'"nqn": "{subsystem2}"'
    cpt = caplog.text
    assert (s1 in cpt and s2 not in cpt) or (s1 not in cpt and s2 in cpt)
    gwB = gatewayB.gateway_rpc
    configB = gwB.config
    configB.config["gateway"]["max_subsystems"] = "1"
    portB = gwB.config.config["gateway"]["port"]
    configB.config["gateway"]["abort_on_update_error"] = "True"
    configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    assert portB == "5502"
    gatewayB.__exit__(None, None, None)
    print("Restarting gateway B again")
    time.sleep(20)
    caplog.clear()
    gatewayB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gatewayB.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    found_exit = False
    caplog.clear()
    try:
        gatewayB.serve()
        gatewayB.keep_alive()
    except SystemExit as sysex:
        assert sysex.code.find("Got error 7 while updating gateway GatewayBB state, "
                               "aborting gateway") >= 0
        found_exit = True
    assert found_exit
