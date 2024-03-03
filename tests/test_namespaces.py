import pytest
import copy
import grpc
import json
import time
import errno
from google.protobuf import json_format
from control.server import GatewayServer
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc

update_notify = True
update_interval_sec = 5
pool = "rbd"
image = "mytestdevimage"
subsystem_nqn = "nqn.2016-06.io.spdk:cnode1"
run_count = 3
namespace_count = 500
namespace_del_range=range(101,201)

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

def create_namespace(stub, rbd_pool, rbd_image, nsid):
    retries = 5
    while retries >= 0:
        namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem_nqn,
                                          rbd_pool_name=rbd_pool,
                                          rbd_image_name=rbd_image,
                                          nsid=nsid,
                                          block_size=4096,
                                          force=True)
        ret_namespace = stub.namespace_add(namespace_req)
        if ret_namespace.status != errno.ETIMEDOUT:
            assert ret_namespace.status == 0
            return
        time.sleep(1)
        retries -= 1
    assert False

def delete_namespace(stub, nsid):
    del_ns_req = pb2.namespace_delete_req(subsystem_nqn=subsystem_nqn, nsid=nsid)
    ret_del_namespace = stub.namespace_delete(del_ns_req)
    assert ret_del_namespace.status == 0

def create_subsystem(stub, nqn, max_ns):
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=nqn, max_namespaces=max_ns, enable_ha=False)
    ret_subsystem = stub.create_subsystem(subsystem_req)
    assert ret_subsystem.status == 0

def create_listener(stub, nqn, name, addr, port):
    listener_req = pb2.create_listener_req(nqn=nqn,
                                           gateway_name=name,
                                           adrfam="ipv4",
                                           traddr=addr,
                                           trsvcid=port)
    listener_ret = stub.create_listener(listener_req)
    assert listener_ret.status == 0

def wait_for_update():
    time_to_sleep = 1
    if not update_notify:
        # Periodic update
        time_to_sleep += update_interval_sec

    time.sleep(time_to_sleep)

def test_create_subsystem_and_namespaces(config, image, conn):
    """Tests state coordination in a gateway group.

    Sends requests to GatewayA to set up a subsystem
    and checks if GatewayB has the identical state after watch/notify and/or
    periodic polling.
    """
    stubA, stubB = conn

    # Send requests to create a subsystem to GatewayA
    max_ns = namespace_count * run_count + 10
    create_subsystem(stubA, subsystem_nqn, max_ns)
    list_namespaces_req = pb2.list_namespaces_req(subsystem=subsystem_nqn)
    listA = json.loads(json_format.MessageToJson(
        stubA.list_subsystems(pb2.list_subsystems_req()),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listA) == 2
    assert listA[1]["nqn"] == subsystem_nqn

    wait_for_update()
    listB = json.loads(json_format.MessageToJson(
        stubB.list_subsystems(pb2.list_subsystems_req()),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']

    assert len(listB) == 2
    assert listB[1]["nqn"] == subsystem_nqn
    assert listB[1]["serial_number"] == listA[1]["serial_number"]

    wait_for_update()

    create_listener(stubA, subsystem_nqn, "GatewayA", "127.0.0.1", 5101)
    create_listener(stubB, subsystem_nqn, "GatewayB", "127.0.0.1", 5102)

    wait_for_update()

    nsid = 0
    ns_count = 0
    for run in range(run_count):
        for ns in range(int(namespace_count / 2)):
            nsid += 1
            ns_count += 1
            create_namespace(stubB, pool, image, nsid)

        time.sleep(0.5)
        create_subsystem(stubA, f"{subsystem_nqn}A{nsid}", 256)

        for ns in range(int(namespace_count / 2), namespace_count):
            nsid += 1
            ns_count += 1
            create_namespace(stubB, f"{pool}2", f"{image}2", nsid)

        time.sleep(0.5)
        create_subsystem(stubA, f"{subsystem_nqn}B{nsid}", 256)

        listB = json.loads(json_format.MessageToJson(
            stubB.list_subsystems(pb2.list_subsystems_req()),
            preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
        assert listB[1]["namespace_count"] == ns_count
        nsListB = json.loads(json_format.MessageToJson(
            stubB.list_namespaces(list_namespaces_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
        assert len(nsListB) == ns_count
        wait_for_update()
        listA = json.loads(json_format.MessageToJson(
            stubA.list_subsystems(pb2.list_subsystems_req()),
            preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
        assert listA[1]["namespace_count"] == ns_count
        nsListA = json.loads(json_format.MessageToJson(
            stubA.list_namespaces(list_namespaces_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
        assert len(nsListA) == ns_count

        for ns in namespace_del_range:
            ns_count -= 1
            delete_namespace(stubB, run * namespace_count + ns)

        time.sleep(0.5)
        create_subsystem(stubA, f"{subsystem_nqn}C{nsid}", 256)

        listB = json.loads(json_format.MessageToJson(
            stubB.list_subsystems(pb2.list_subsystems_req()),
            preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
        assert listB[1]["namespace_count"] == ns_count
        nsListB = json.loads(json_format.MessageToJson(
            stubB.list_namespaces(list_namespaces_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
        assert len(nsListB) == ns_count
        wait_for_update()
        listA = json.loads(json_format.MessageToJson(
            stubA.list_subsystems(pb2.list_subsystems_req()),
            preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
        assert listA[1]["namespace_count"] == ns_count
        nsListA = json.loads(json_format.MessageToJson(
            stubA.list_namespaces(list_namespaces_req),
            preserving_proto_field_name=True, including_default_value_fields=True))['namespaces']
        assert len(nsListA) == ns_count
