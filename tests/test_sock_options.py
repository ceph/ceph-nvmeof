import pytest
from control.server import GatewayServer
from control.cephutils import CephUtils
import copy
import os

subsystem1 = "nqn.2016-06.io.spdk:cnode1"
config = "ceph-nvmeof.conf"
pool = "rbd"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def tw_gateways(config):
    """Sets up and tears down two Gateways"""

    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["spdk"]["sock_impl_options"] = '{"ssl": {"recv_buf_size": 1048576, ' \
                                                  '"send_buf_size": 1048576, ' \
                                                  '"ssl_tickets_number": 5}, "posix": ' \
                                                  '{"recv_buf_size": 1572864, ' \
                                                  '"send_buf_size": 1572864}}'
    configB.config["spdk"]["sock_impl_options"] = '{"ssl": "stam"}'
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
    with GatewayServer(configA) as gatewayA, GatewayServer(configB) as gatewayB:
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

        yield gatewayA.gateway_rpc, gatewayB.gateway_rpc

        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def test_sock_impl_options(caplog, tw_gateways):
    gwA, gwB = tw_gateways
    found = False
    for line in caplog.get_records("setup"):
        linemsg = line.message
        if linemsg == 'Failed to parse spdk sock_impl_options ({"ssl": "stam"})':
            found = True
            break
    assert found, "Parsing should fail for non-dictionary object"
    with gwA.rpc_lock:
        opts = gwA.spdk_rpc_client.sock_impl_get_options(impl_name="posix")
        assert opts["recv_buf_size"] == 1572864
        assert opts["send_buf_size"] == 1572864
        opts = gwA.spdk_rpc_client.sock_impl_get_options(impl_name="ssl")
        assert opts["recv_buf_size"] == 1048576
        assert opts["send_buf_size"] == 1048576
        assert opts["ssl_tickets_number"] == 5
    with gwB.rpc_lock:
        opts = gwB.spdk_rpc_client.sock_impl_get_options(impl_name="ssl")
        assert opts["recv_buf_size"] == 4096
        assert opts["ssl_tickets_number"] == 0
