import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.utils import GatewayUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import os
import time

image = "mytestdevimage"
pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
hostnqn1 = "nqn.2016-06.io.spdk:host1"
hostnqn2 = "nqn.2016-06.io.spdk:host2"
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
    configB.config["gateway"]["io_stats_enabled"] = "False"
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
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Adding host {hostnqn1} to {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_get_stats_wrong_subsystem(caplog, two_gateways):
    caplog.clear()
    cli(["connection", "get_io_statistics", "--subsystem", subsystem2, "--host-nqn", hostnqn1])
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem {subsystem2}: " \
           f"No such subsystem" in caplog.text
    caplog.clear()
    cli(["connection", "get_io_statistics", "--subsystem", GatewayUtils.DISCOVERY_NQN,
         "--host-nqn", hostnqn1])
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem " \
           f"{GatewayUtils.DISCOVERY_NQN}: No such subsystem" in caplog.text


def test_get_stats_wrong_host(caplog, two_gateways):
    caplog.clear()
    cli(["connection", "get_io_statistics", "--subsystem", subsystem1, "--host-nqn", hostnqn2])
    assert f"Failure getting IO statistics for host {hostnqn2} on subsystem {subsystem1}: " \
           f"Host is not allowed to access subsystem" in caplog.text
    caplog.clear()
    cli(["connection", "get_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", GatewayUtils.DISCOVERY_NQN])
    assert f"Failure getting IO statistics for host {GatewayUtils.DISCOVERY_NQN} on subsystem " \
           f"{subsystem1}: Host is not allowed to access subsystem" in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["connection", "get_io_statistics", "--subsystem", subsystem1, "--host-nqn", "*"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Must specify a specific host NQN" in caplog.text
    assert rc == 2


def test_reset_stats_wrong_subsystem(caplog, two_gateways):
    caplog.clear()
    cli(["connection", "reset_io_statistics", "--subsystem", subsystem2, "--host-nqn", hostnqn1])
    assert f"Failure resetting IO statistics for host {hostnqn1} on subsystem {subsystem2}: " \
           f"No such subsystem" in caplog.text
    caplog.clear()
    cli(["connection", "reset_io_statistics", "--subsystem", GatewayUtils.DISCOVERY_NQN,
         "--host-nqn", hostnqn1])
    assert f"Failure resetting IO statistics for host {hostnqn1} on subsystem " \
           f"{GatewayUtils.DISCOVERY_NQN}: No such subsystem" in caplog.text


def test_reset_stats_wrong_host(caplog, two_gateways):
    caplog.clear()
    cli(["connection", "reset_io_statistics", "--subsystem", subsystem1, "--host-nqn", hostnqn2])
    assert f"Failure resetting IO statistics for host {hostnqn2} on subsystem {subsystem1}: " \
           f"Host is not allowed to access subsystem" in caplog.text
    caplog.clear()
    cli(["connection", "reset_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", GatewayUtils.DISCOVERY_NQN])
    assert f"Failure resetting IO statistics for host {GatewayUtils.DISCOVERY_NQN} on subsystem " \
           f"{subsystem1}: Host is not allowed to access subsystem" in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["connection", "reset_io_statistics", "--subsystem", subsystem1, "--host-nqn", "*"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Must specify a specific host NQN" in caplog.text
    assert rc == 2


def test_set_stats_mode_wrong_params(caplog, two_gateways):
    caplog.clear()
    rc = 0
    try:
        cli(["gateway", "set_io_stats_mode"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert 'One of "--enable" or "--disable" must be specified' in caplog.text
    assert rc == 2
    caplog.clear()
    rc = 0
    try:
        cli(["gateway", "set_io_stats_mode", "--enable", "--disable"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert '"--enable" and "--disable" are mutually exclusive' in caplog.text
    assert rc == 2


def test_gw_info(caplog, two_gateways):
    caplog.clear()
    cli(["gateway", "info"])
    assert "Gateway's IO statistics is disabled" not in caplog.text
    assert "Gateway's IO statistics is enabled" in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "gateway", "info"])
    assert "Gateway's IO statistics is disabled" in caplog.text
    assert "Gateway's IO statistics is enabled" not in caplog.text


def test_get_stats_host_not_connected(caplog, two_gateways):
    caplog.clear()
    cli(["connection", "get_io_statistics", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem {subsystem1}: " \
           f"Host is not connected" in caplog.text


def test_get_stats_host_when_disabled(caplog, two_gateways):
    caplog.clear()
    cli(["--server-port", "5502", "connection", "get_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", hostnqn1])
    assert f"Received request to get IO statistics for host {hostnqn1} on " \
           f"{subsystem1}" in caplog.text
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem {subsystem1}: " \
           f"IO statistics is disabled or not supported" in caplog.text


def test_reset_stats_host_when_disabled(caplog, two_gateways):
    caplog.clear()
    cli(["--server-port", "5502", "connection", "reset_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", hostnqn1])
    assert f"Received request to reset IO statistics for host {hostnqn1} on " \
           f"{subsystem1}" in caplog.text
    assert f"Failure resetting IO statistics for host {hostnqn1} on subsystem {subsystem1}: " \
           f"IO statistics is disabled or not supported" in caplog.text


def test_get_stats_host_after_enable(caplog, two_gateways):
    caplog.clear()
    cli(["--server-port", "5502", "gateway", "set_io_stats_mode", "--enable"])
    assert 'Set gateway IO statistics mode to "enabled": Successful' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "connection", "get_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", hostnqn1])
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem {subsystem1}: " \
           f"Host is not connected" in caplog.text


def test_get_stats_host_after_disable_again(caplog, two_gateways):
    caplog.clear()
    cli(["--server-port", "5502", "gateway", "set_io_stats_mode", "--disable"])
    assert 'Set gateway IO statistics mode to "disabled": Successful' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "connection", "get_io_statistics", "--subsystem", subsystem1,
         "--host-nqn", hostnqn1])
    assert f"Failure getting IO statistics for host {hostnqn1} on subsystem {subsystem1}: " \
           f"IO statistics is disabled or not supported" in caplog.text
