import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os

image_prefix = "mytestdevimage"
pool = "rbd"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
host_prefix = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288"
subsystem_count = 128
namespace_count = 8
host_count = 12
anagrpid = "1"
anagrpid2 = "2"
group_name = "group1"
max_subsystems = 1024
max_namespaces = 5120
max_hosts = 5000
update_interval = 300


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["max_subsystems"] = f"{max_subsystems}"
    config.config["gateway"]["max_namespaces"] = f"{max_namespaces}"
    config.config["gateway"]["max_hosts"] = f"{max_hosts}"
    config.config["gateway"]["rebalance_period_sec"] = "0"
    config.config["gateway"]["state_update_notify"] = "False"
    config.config["gateway"]["state_update_interval_sec"] = f"{update_interval}"
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

        yield gatewayA, gatewayB
        if gatewayA and gatewayA.server:
            gatewayA.server.stop(grace=1)
        if gatewayB and gatewayB.server:
            gatewayB.server.stop(grace=1)


def verify_one_namespace_lb_group(caplog, gw_port, subsys, nsid_to_verify, grp):
    caplog.clear()
    cli(["--server-port", gw_port, "--format", "json", "namespace", "list",
         "--subsystem", subsys, "--nsid", str(nsid_to_verify)])
    if f'"nsid": {nsid_to_verify},' not in caplog.text:
        return f'Couldn\'t find \'"nsid": {nsid_to_verify},\' ' \
               f'port {gw_port}, subsystem {subsys}'
    if f'"load_balancing_group": {grp},' not in caplog.text:
        return f'Couldn\'t find \'"load_balancing_group": {grp},\' ' \
               f'port {gw_port}, subsystem {subsys}'
    return None


def verify_namespaces(caplog, gw_port, subsys, first_nsid, last_nsid, grp):
    for ns in range(first_nsid, last_nsid + 1):
        rc = verify_one_namespace_lb_group(caplog, gw_port, subsys, ns, grp)
        if rc is not None:
            return rc
    return None


def verify_resources(caplog, gw_port, subsys_cnt, ns_cnt, grp):
    for subsys_id in range(1, subsys_cnt + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        rc = verify_namespaces(caplog, gw_port, subsys, 1, ns_cnt, grp)
        if rc is not None:
            return rc
    return None


def create_resources(caplog, subsys_cnt, host_cnt, ns_cnt, grp):
    img_id = 1
    for subsys_id in range(1, subsys_cnt + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsys, "--no-group-append",
             "--max-namespaces", f"{2 * ns_cnt}"])
        assert f"Adding subsystem {subsys}: Successful" in caplog.text
        for ns_id in range(1, ns_cnt + 1):
            caplog.clear()
            image = f"{image_prefix}{ns_id}"
            cli(["namespace", "add", "--subsystem", subsys, "--rbd-pool", pool,
                 "--rbd-image", f"{image}{img_id}", "--size", "10MB", "--rbd-create-image",
                 "--load-balancing-group", grp])
            assert f"Adding namespace {ns_id} to {subsys}: Successful" in caplog.text
            img_id += 1
        for host_id in range(1, host_cnt + 1):
            caplog.clear()
            host_nqn = f"{host_prefix}{host_id:02x}"
            cli(["host", "add", "--subsystem", subsys, "--host-nqn", host_nqn])
            assert f"Adding host {host_nqn} to {subsys}: Successful" in caplog.text


def create_listeners(caplog, gw_name, gw_port, subsys_cnt, addr, start_port):
    port = int(start_port)
    for subsys_id in range(1, subsys_cnt + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        caplog.clear()
        cli(["--server-port", gw_port, "listener", "add", "--subsystem", subsys,
             "--host-name", gw_name, "--traddr", addr, "--trsvcid", str(port)])
        assert f"Adding {subsys} listener at {addr}:{port}: Successful" in caplog.text
        port += 1


def change_namespace_lb_group(caplog, gw_port1, gw_port2, subsys, nsid, grp):
    cli(["--server-port", gw_port1, "namespace", "change_load_balancing_group",
         "--subsystem", subsys, "--nsid", str(nsid), "--load-balancing-group", grp])
    cli(["--server-port", gw_port2, "namespace", "change_load_balancing_group",
         "--subsystem", subsys, "--nsid", str(nsid), "--load-balancing-group", grp])


def change_all_namespaces_lb_group(caplog, gw_port1, gw_port2, subsys, grp):
    for ns in range(1, namespace_count + 1):
        change_namespace_lb_group(caplog, gw_port1, gw_port2, subsys, ns, grp)


def change_lb_group_for_all_subsystems(caplog, gw_port1, gw_port2, grp):
    for subsys_id in range(1, subsystem_count + 1):
        subsys = f"{subsystem_prefix}{subsys_id}"
        change_all_namespaces_lb_group(caplog, gw_port1, gw_port2, subsys, grp)


def test_big_omap(caplog, two_gateways):
    gatewayA, gatewayB = two_gateways
    gwA = gatewayA.gateway_rpc
    gwB = gatewayB.gateway_rpc

    sleep_delta = 10
    print("About to create the resources")
    create_resources(caplog, subsystem_count, host_count, namespace_count, anagrpid)
    waitForUpdate = max(int(gwA.config.config["gateway"]["state_update_interval_sec"]),
                        int(gwB.config.config["gateway"]["state_update_interval_sec"]))
    time.sleep(waitForUpdate + sleep_delta)
    print("Verify the resources were created")
    for port in [gwA.config.config["gateway"]["port"],
                 gwB.config.config["gateway"]["port"]]:
        retries = 3
        while True:
            rc = verify_resources(caplog, port, subsystem_count, namespace_count, anagrpid)
            if rc is None:
                break
            retries -= 1
            assert retries > 0, f"Failed verifying created resources: {rc}"
            time.sleep(sleep_delta)

    print("Create listeners")
    create_listeners(caplog, gwA.host_name, gwA.config.config["gateway"]["port"],
                     subsystem_count, "127.0.0.1", 3000)
    create_listeners(caplog, gwB.host_name, gwB.config.config["gateway"]["port"],
                     subsystem_count, "127.0.0.1", 4000)

    time.sleep(waitForUpdate + sleep_delta)
    print("Change load balancing group")
    change_lb_group_for_all_subsystems(caplog,
                                       gwA.config.config["gateway"]["port"],
                                       gwB.config.config["gateway"]["port"],
                                       anagrpid2)
    time.sleep(waitForUpdate + sleep_delta)
    print("Verify resources after load balancing group change")
    for port in [gwA.config.config["gateway"]["port"],
                 gwB.config.config["gateway"]["port"]]:
        retries = 3
        while True:
            rc = verify_resources(caplog, port, subsystem_count, namespace_count, anagrpid2)
            if rc is None:
                break
            retries -= 1
            assert retries > 0, f"Failed verifying resources after load " \
                                f"balancing group change: {rc}"
            time.sleep(sleep_delta)

    configB = gwB.config
    portB = gwB.config.config["gateway"]["port"]
    gatewayB.__exit__(None, None, None)
    print("Restarting gateway B")
    time.sleep(15)
    gatewayB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gatewayB.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gatewayB.serve()
    caplog.clear()
    time.sleep(1)
    cli(["--server-port", portB, "--format", "json", "gateway", "info"])
    assert '"gateway_initialization_over": false,' in caplog.text
    time.sleep(waitForUpdate + sleep_delta)
    while True:
        retries = 5
        caplog.clear()
        cli(["--server-port", portB, "--format", "json", "gateway", "info"])
        if '"gateway_initialization_over": true,' in caplog.text:
            break
        retries -= 1
        assert retries > 0, "Gateway is not fully initialized after restart"
        time.sleep(sleep_delta)
    print("Verify resources after gateway restart")
    while True:
        retries = 3
        rc = verify_resources(caplog, portB, subsystem_count, namespace_count, anagrpid2)
        if rc is None:
            break
        retries -= 1
        assert retries > 0, f"Failed verifying resources after gateway restart: {rc}"
        time.sleep(sleep_delta)
