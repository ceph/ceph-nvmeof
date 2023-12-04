import pytest
import copy
import grpc
import json
import time
from google.protobuf import json_format
from control.server import GatewayServer
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
from . import set_group_id

update_notify = True
update_interval_sec = 5

@pytest.fixture(scope="module")
def conn(config):
    """Sets up and tears down Gateways A and B."""
    # Setup GatewayA and GatewayB configs
    configA = copy.deepcopy(config)
    configA.config["gateway"]["name"] = "GatewayA"
    configA.config["gateway"]["group"] = "Group1"
    configA.config["gateway"]["state_update_notify"] = str(update_notify)
    configA.config["gateway"]["enable_spdk_discovery_controller"] = "True"
    configA.config["spdk"]["rpc_socket_name"] = "spdk_GatewayA.sock"
    configB = copy.deepcopy(configA)
    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = portA + 2
    configB.config["gateway"]["name"] = "GatewayB"
    configB.config["gateway"]["port"] = str(portB)
    configB.config["gateway"]["state_update_interval_sec"] = str(
        update_interval_sec)
    configB.config["spdk"]["rpc_socket_name"] = "spdk_GatewayB.sock"
    configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x02"

    # Start servers
    with (
       GatewayServer(configA) as gatewayA,
       GatewayServer(configB) as gatewayB,
    ):
        gatewayA.serve()
        set_group_id(1, gatewayA)
        # Delete existing OMAP state
        gatewayA.gateway_rpc.gateway_state.delete_state()
        # Create new
        gatewayB.serve()
        set_group_id(2, gatewayB)

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
    and checks if GatewayB has the identical state after watch/notify and/or
    periodic polling.
    """
    stubA, stubB = conn
    bdev = "Ceph0"
    nqn = "nqn.2016-06.io.spdk:cnode1"
    serial = "SPDK00000000000001"
    nsid = 10
    num_subsystems = 2

    pool = config.get("ceph", "pool")

    # Send requests to create a subsystem with one namespace to GatewayA
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
        listB = json.loads(json_format.MessageToJson(
            stubB.get_subsystems(get_subsystems_req),
            preserving_proto_field_name=True))['subsystems']
        assert len(listB) == num_subsystems
        assert listB[num_subsystems-1]["nqn"] == nqn
        assert listB[num_subsystems-1]["serial_number"] == serial
        assert listB[num_subsystems-1]["namespaces"][0]["nsid"] == nsid
        assert listB[num_subsystems-1]["namespaces"][0]["bdev_name"] == bdev

    # Periodic update
    time.sleep(update_interval_sec + 1)
    listB = json.loads(json_format.MessageToJson(
        stubB.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True))['subsystems']
    assert len(listB) == num_subsystems
    assert listB[num_subsystems-1]["nqn"] == nqn
    assert listB[num_subsystems-1]["serial_number"] == serial
    assert listB[num_subsystems-1]["namespaces"][0]["nsid"] == nsid
    assert listB[num_subsystems-1]["namespaces"][0]["bdev_name"] == bdev

