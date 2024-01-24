import pytest
import copy
import grpc
import json
import time
from google.protobuf import json_format
from control.server import GatewayServer
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc

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
    configA.config["gateway"]["min_controller_id"] = "1"
    configA.config["gateway"]["max_controller_id"] = "20000"
    configA.config["gateway"]["enable_spdk_discovery_controller"] = "True"
    configA.config["spdk"]["rpc_socket_name"] = "spdk_GatewayA.sock"
    configB = copy.deepcopy(configA)
    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = portA + 1
    configB.config["gateway"]["name"] = "GatewayB"
    configB.config["gateway"]["port"] = str(portB)
    configA.config["gateway"]["min_controller_id"] = "20001"
    configA.config["gateway"]["max_controller_id"] = "40000"
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
        # Delete existing OMAP state
        gatewayA.gateway_rpc.gateway_state.delete_state()
        # Create new
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
    and checks if GatewayB has the identical state after watch/notify and/or
    periodic polling.
    """
    stubA, stubB = conn
    nqn = "nqn.2016-06.io.spdk:cnode1"
    serial = "SPDK00000000000001"
    nsid = 10
    num_subsystems = 2

    pool = config.get("ceph", "pool")

    # Send requests to create a subsystem with one namespace to GatewayA
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=nqn,
                                             serial_number=serial)
    namespace_req = pb2.namespace_add_req(subsystem_nqn=nqn,
                                          rbd_pool_name=pool,
                                          rbd_image_name=image,
                                          block_size=4096,
                                          nsid=nsid, create_image=True, size=16*1024*1024)
    list_subsystems_req = pb2.list_subsystems_req()
    list_namespaces_req = pb2.list_namespaces_req(subsystem=nqn)
    ret_subsystem = stubA.create_subsystem(subsystem_req)
    ret_namespace = stubA.namespace_add(namespace_req)
    assert ret_subsystem.status == 0
    assert ret_namespace.status == 0

    nsListA = json.loads(json_format.MessageToJson(
        stubA.list_namespaces(list_namespaces_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
    assert len(nsListA) == 1
    assert nsListA[0]["nsid"] == nsid
    uuid = nsListA[0]["uuid"]

    # Watch/Notify
    if update_notify:
        time.sleep(1)
        listB = json.loads(json_format.MessageToJson(
            stubB.list_subsystems(list_subsystems_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
        assert len(listB) == num_subsystems
        assert listB[num_subsystems-1]["nqn"] == nqn
        assert listB[num_subsystems-1]["serial_number"] == serial
        assert listB[num_subsystems-1]["namespace_count"] == 1

        nsListB = json.loads(json_format.MessageToJson(
            stubB.list_namespaces(list_namespaces_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
        assert len(nsListB) == 1
        assert nsListB[0]["nsid"] == nsid
        assert nsListB[0]["uuid"] == uuid
        assert nsListB[0]["rbd_image_name"] == image
        assert nsListB[0]["rbd_pool_name"] == pool

    # Periodic update
    time.sleep(update_interval_sec + 1)
    listB = json.loads(json_format.MessageToJson(
        stubB.list_subsystems(list_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listB) == num_subsystems
    assert listB[num_subsystems-1]["nqn"] == nqn
    assert listB[num_subsystems-1]["serial_number"] == serial
    assert listB[num_subsystems-1]["namespace_count"] == 1
    nsListB = json.loads(json_format.MessageToJson(
        stubB.list_namespaces(list_namespaces_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
    assert len(nsListB) == 1
    assert nsListB[0]["nsid"] == nsid
    assert nsListB[0]["uuid"] == uuid
    assert nsListB[0]["rbd_image_name"] == image
    assert nsListB[0]["rbd_pool_name"] == pool

