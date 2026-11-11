import pytest
from control.server import GatewayServer
import copy
import os
import control.utils as utils
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import time

pool = "rbd"
config = "ceph-nvmeof.conf"

multi_gw_subsystem = "nqn.2016-06.io.spdk:cnode1"
multi_gw_host_nqn = "nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-0000000000a1"
gw_a_mask_subnet = "127.1.0.0/16"
gw_b_mask_subnet = "127.2.0.0/16"
gw_a_fake_ip = "127.1.0.1"
gw_b_fake_ip = "127.2.0.1"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayA"
    nameB = "GatewayB"
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


def test_group_config_network_masks_per_gateway_listener(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB = two_gateways
    gatewayA.gateway_rpc.listener_network_masks = [gw_a_mask_subnet]
    gatewayB.gateway_rpc.listener_network_masks = [gw_b_mask_subnet]

    fake_ips_by_subnet = {gw_a_mask_subnet: gw_a_fake_ip, gw_b_mask_subnet: gw_b_fake_ip}

    def fake_get_ips_in_subnet(self, subnet):
        ip = fake_ips_by_subnet.get(subnet)
        return [ip] if ip else []

    def fake_verify_ip_address(self, addr, family):
        return addr in fake_ips_by_subnet.values()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(utils.NICS, "get_ips_in_subnet", fake_get_ips_in_subnet)
        mp.setattr(utils.NICS, "verify_ip_address", fake_verify_ip_address)

        caplog.clear()
        cli(["subsystem", "add", "--subsystem", multi_gw_subsystem, "--no-group-append"])
        assert f"create_subsystem {multi_gw_subsystem}: True" in caplog.text
        cli(["host", "add", "--subsystem", multi_gw_subsystem, "--host-nqn", multi_gw_host_nqn])

        # Give gateway B time to replay the bare subsystem (and host) first, so the
        # upcoming group-config-mask enable is guaranteed to show up on B as an
        # incremental change, not folded into its very first sync of the subsystem.
        time.sleep(15)
        subsystemsB = stubB.list_subsystems(pb2.list_subsystems_req())
        assert any(s.nqn == multi_gw_subsystem for s in subsystemsB.subsystems)
        hostsB = stubB.list_hosts(pb2.list_hosts_req(subsystem=multi_gw_subsystem))
        assert any(h.nqn == multi_gw_host_nqn for h in hostsB.hosts)

        caplog.clear()
        cli(["subsystem", "set_use_group_config_network_masks",
             "--subsystem", multi_gw_subsystem,
             "--use-group-config-network-masks", "true"])
        assert f"Automatically created listener at {gw_a_fake_ip}:4420 for " \
               f"{multi_gw_subsystem}" in caplog.text
        assert f"Enabled group config network masks for subsystem " \
               f"{multi_gw_subsystem}" in caplog.text
        assert f"Received request to enable group config network masks for " \
               f"subsystem {multi_gw_subsystem}, context: <grpc._server" in caplog.text

        caplog.clear()
        time.sleep(15)
        assert f"Received request to enable group config network masks for " \
               f"subsystem {multi_gw_subsystem}, context: None" in caplog.text
        assert f"Automatically created listener at {gw_b_fake_ip}:4420 for " \
               f"{multi_gw_subsystem}" in caplog.text
        assert f"Automatically created listener at {gw_a_fake_ip}:4420 for " \
               f"{multi_gw_subsystem}" not in caplog.text

        hostsB_after = stubB.list_hosts(pb2.list_hosts_req(subsystem=multi_gw_subsystem))
        assert any(h.nqn == multi_gw_host_nqn for h in hostsB_after.hosts)
