import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import os
import time
import re

image = "mytestdevimage1"
image2 = "mytestdevimage2"
image3 = "mytestdevimage3"
image4 = "mytestdevimage4"
image5 = "mytestdevimage5"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
group_name = "GROUPNAME"
location = "USA"
location2 = "France"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two gateways"""

    global location, location2
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    addr = config.get("gateway", "addr")
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
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

        # Start gateway
        gatewayA.gw_logger_object.set_log_level("debug")
        gatewayB.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        rc = ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw set-location", "id": "{nameA}", "pool": "{pool}", '
            f'"group": "{group_name}", "location": "{location}"' + "}"
        )
        if rc[0]:
            location = ""
            location2 = ""
            print("set-location is not implemented in Ceph, will use default")
        else:
            rc = ceph_utils.execute_ceph_monitor_command(
                "{" + f'"prefix":"nvme-gw set-location", "id": "{nameB}", "pool": "{pool}", '
                f'"group": "{group_name}", "location": "{location2}"' + "}"
            )
            assert rc[0] == 0

        assert (location and location2) or (not location and not location2)

        gatewayA.serve()
        gatewayB.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{portA}")
        pb2_grpc.GatewayStub(channel)
        channel = grpc.insecure_channel(f"{addr}:{portB}")
        pb2_grpc.GatewayStub(channel)
        yield gatewayA.gateway_rpc, gatewayB.gateway_rpc

        # Stop gateway
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()


@pytest.fixture(scope="module", autouse=True)
def create_resources(two_gateways):
    _, gwB = two_gateways
    rc = cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert rc == 0
    rc = cli(["subsystem", "add", "--subsystem", subsystem2, "--no-group-append"])
    assert rc == 0
    rc = cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
              "--rbd-image", image, "--size", "16MB", "--load-balancing-group", "1",
              "--rbd-create-image", "--location", location])
    assert rc == 0
    rc = cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
              "--rbd-image", image2, "--size", "16MB", "--load-balancing-group", "1",
              "--rbd-create-image", "--location", location])
    assert rc == 0
    rc = cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
              "--rbd-image", image3, "--size", "16MB", "--load-balancing-group", "1",
              "--rbd-create-image", "--location", location])
    assert rc == 0
    rc = cli(["namespace", "add", "--subsystem", subsystem2, "--rbd-pool", pool,
              "--rbd-image", image4, "--size", "16MB", "--load-balancing-group", "1",
              "--rbd-create-image", "--location", location])
    assert rc == 0
    time.sleep(20)
    rc = cli(["--server-port", gwB.config.config["gateway"]["port"],
              "namespace", "add", "--subsystem", subsystem2,
              "--rbd-pool", pool, "--rbd-image", image5, "--size", "16MB",
              "--rbd-create-image", "--location", location2, "--load-balancing-group", "2"])
    assert rc == 0
    time.sleep(20)


def find_in_caplog(lookfor, captext) -> bool:
    regex = re.compile(lookfor, re.MULTILINE)
    if re.search(regex, captext) is not None:
        return True
    return False


def test_list_namespace_locations_one_subsystem(caplog, two_gateways):
    caplog.clear()
    cli(["--format", "plain", "namespace", "list_locations", "--subsystem", subsystem2])
    if location and location2:
        assert find_in_caplog(rf"^\s*{re.escape(subsystem2)}\s*1\s*{re.escape(location)}\s*1\s*$",
                              caplog.text)
        assert find_in_caplog(rf"^\s*2\s*{re.escape(location2)}\s*1\s*$", caplog.text)
    else:
        assert find_in_caplog(rf"^\s*{re.escape(subsystem2)}\s*1\s*<default>\s*1\s*$",
                              caplog.text)
        assert find_in_caplog(r"^\s*2\s*<default>\s*1\s*$", caplog.text)
    assert subsystem not in caplog.text


def test_list_namespace_locations_all_subsystems(caplog, two_gateways):
    caplog.clear()
    cli(["--format", "plain", "namespace", "list_locations"])
    if location and location2:
        assert find_in_caplog(rf"^\s*{re.escape(subsystem)}\s*1\s*{re.escape(location)}\s*3\s*$",
                              caplog.text)
        assert find_in_caplog(rf"^\s*{re.escape(subsystem2)}\s*1\s*{re.escape(location)}\s*1\s*$",
                              caplog.text)
        assert find_in_caplog(rf"^\s*2\s*{re.escape(location2)}\s*1\s*$", caplog.text)
    else:
        assert find_in_caplog(rf"^\s*{re.escape(subsystem)}\s*1\s*<default>\s*3\s*$", caplog.text)
        assert find_in_caplog(rf"^\s*{re.escape(subsystem2)}\s*1\s*<default>\s*1\s*$", caplog.text)
        assert find_in_caplog(r"^\s*2\s*<default>\s*1\s*$", caplog.text)
