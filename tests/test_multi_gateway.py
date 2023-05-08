import pytest
import copy
import grpc
import json
import time
from control.server import GatewayServer
from control.generated import gateway_pb2 as pb2
from control.generated import gateway_pb2_grpc as pb2_grpc

update_notify = True
update_interval_sec = 5


@pytest.fixture(scope="module")
def conn(config):
    """Sets up and tears down Gateways A and B."""
    # Setup GatewayA and GatewayB configs
    configA = copy.deepcopy(config)
    configA.gateway.name = "GatewayA"
    configA.gateway.group = "Group1"
    configA.gateway.state_update_notify = update_notify
    configB = copy.deepcopy(configA)
    addr = configA.gateway.addr
    portA = configA.gateway.port
    portB = portA + 1
    configB.gateway.name = "GatewayB"
    configB.gateway.port = portB
    configB.gateway.state_update_interval_sec = update_interval_sec
    configB.spdk.rpc_socket = "/var/tmp/spdk_GatewayB.sock"
    configB.spdk.tgt_cmd_extra_args = "-m 0x02"

    # Start servers
    gatewayA = GatewayServer(configA)
    gatewayA.serve()
    # Delete existing OMAP state
    gatewayA.gateway_rpc.gateway_state.delete_state()
    # Create new
    gatewayB = GatewayServer(configB)
    gatewayB.serve()

    # Bind the client and Gateways A & B
    channelA = grpc.insecure_channel(f"{addr}:{portA}")
    stubA = pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    stubB = pb2_grpc.GatewayStub(channelB)
    yield stubA, stubB

    # Stop gateways
    gatewayA.server.stop(grace=1)
    gatewayB.server.stop(grace=1)
    gatewayB.gateway_rpc.gateway_state.delete_state()


def test_multi_gateway_coordination(config, image, conn):
    """Tests state coordination in a gateway group.
    
    Sends requests to GatewayA to set up a subsystem with a single namespace
    and checks if GatewayB has the the identical state after watch/notify and/or
    periodic polling.
    """
    stubA, stubB = conn

    bdev = "Ceph0"
    nqn = "nqn.2016-06.io.spdk:cnode1"
    serial = "SPDK00000000000001"
    nsid = 10
    pool = config.ceph.pool

    # Send requests to create a subsytem with one namespace to GatewayA
    bdev_req = pb2.create_bdev_req(bdev_name=bdev,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=nqn,
                                             serial_number=serial)
    namespace_req = pb2.add_namespace_req(subsystem_nqn=nqn,
                                          bdev_name=bdev,
                                          nsid=nsid)
    get_subsystems_req = pb2.get_subsystems_req()
    ret_bdev = stubA.create_bdev(bdev_req)
    ret_subsystem = stubA.create_subsystem(subsystem_req)
    ret_namespace = stubA.add_namespace(namespace_req)
    assert ret_bdev.status is True
    assert ret_subsystem.status is True
    assert ret_namespace.status is True

    # Watch/Notify
    if update_notify:
        time.sleep(1)
        watchB = stubB.get_subsystems(get_subsystems_req)
        listB = json.loads(watchB.subsystems)
        assert len(listB) == 2
        assert listB[1]["nqn"] == nqn
        assert listB[1]["serial_number"] == serial
        assert listB[1]["namespaces"][0]["nsid"] == nsid
        assert listB[1]["namespaces"][0]["bdev_name"] == bdev

    # Periodic update
    time.sleep(update_interval_sec + 1)
    pollB = stubB.get_subsystems(get_subsystems_req)
    listB = json.loads(pollB.subsystems)
    assert len(listB) == 2
    assert listB[1]["nqn"] == nqn
    assert listB[1]["serial_number"] == serial
    assert listB[1]["namespaces"][0]["nsid"] == nsid
    assert listB[1]["namespaces"][0]["bdev_name"] == bdev
