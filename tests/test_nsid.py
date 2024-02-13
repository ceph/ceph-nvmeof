import pytest
import copy
import grpc
import json
import time
from google.protobuf import json_format
from control.server import GatewayServer
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import spdk.rpc.bdev as rpc_bdev

image = "mytestdevimage"
pool = "rbd"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
host_nqn_prefix = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b"

def setup_config(config, gw1_name, gw2_name, gw_group, update_notify, update_interval_sec, disable_unlock, lock_duration,
                 sock1_name, sock2_name):
    """Sets up the config objects for gateways A and B """

    configA = copy.deepcopy(config)
    configA.config["gateway"]["name"] = gw1_name
    configA.config["gateway"]["group"] = gw_group
    configA.config["gateway"]["state_update_notify"] = str(update_notify)
    configA.config["gateway"]["state_update_interval_sec"] = str(update_interval_sec)
    configA.config["gateway"]["omap_file_disable_unlock"] = str(disable_unlock)
    configA.config["gateway"]["omap_file_lock_duration"] = str(lock_duration)
    configA.config["gateway"]["enable_spdk_discovery_controller"] = "True"
    configA.config["spdk"]["rpc_socket_name"] = sock1_name
    configB = copy.deepcopy(configA)
    portA = configA.getint("gateway", "port")
    configA.config["gateway"]["port"] = str(portA)
    portB = portA + 1
    configB.config["gateway"]["name"] = gw2_name
    configB.config["gateway"]["port"] = str(portB)
    configB.config["spdk"]["rpc_socket_name"] = sock2_name
    configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x02"

    return configA, configB

def start_servers(gatewayA, gatewayB, addr, portA, portB):
    gatewayA.serve()
    # Delete existing OMAP state
    gatewayA.gateway_rpc.gateway_state.delete_state()
    # Create new
    gatewayB.serve()
    gatewayB.gateway_rpc.gateway_state.delete_state()

    # Bind the client and Gateways A & B
    channelA = grpc.insecure_channel(f"{addr}:{portA}")
    stubA = pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    stubB = pb2_grpc.GatewayStub(channelB)

    return stubA, stubB

def test_multi_gateway_namespace_ids(config, image, caplog):
    """Tests NSID are OK after a gateway restart
    """
    configA, configB = setup_config(config, "GatewayAAA", "GatewayBBB", "Group1", True, 5, False, 60,
                                    "spdk_GatewayAAA.sock", "spdk_GatewayBBB.sock")

    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = configB.getint("gateway", "port")
    # Start servers
    with (
       GatewayServer(configA) as gatewayA,
       GatewayServer(configB) as gatewayB,
    ):
        stubA, stubB = start_servers(gatewayA, gatewayB, addr, portA, portB)

        # Send requests to create a subsystem on GatewayA
        caplog.clear()
        subsystem = f"{subsystem_prefix}WWW"
        subsystem_add_req = pb2.create_subsystem_req(subsystem_nqn=subsystem)
        ret_subsystem = stubA.create_subsystem(subsystem_add_req)
        assert ret_subsystem.status == 0
        assert f"create_subsystem {subsystem}: True" in caplog.text
        assert f"Failure creating subsystem {subsystem}" not in caplog.text
        time.sleep(10)
        caplog.clear()
        # Send requests to create a namespace on GatewayA
        namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                              rbd_pool_name=pool, rbd_image_name=f"{image}WWW", block_size=4096,
                                              create_image=True, size=16*1024*1024, force=True)
        ret_ns = stubA.namespace_add(namespace_req)
        assert ret_ns.status == 0
        time.sleep(10)
        namespace_req2 = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                              rbd_pool_name=pool, rbd_image_name=f"{image}EEE", block_size=4096,
                                              create_image=True, size=16*1024*1024, force=True)
        ret_ns = stubA.namespace_add(namespace_req2)
        assert ret_ns.status == 0
        time.sleep(10)

        namespace_list_req = pb2.list_namespaces_req(subsystem=subsystem)
        listA = json.loads(json_format.MessageToJson(
            stubA.list_namespaces(namespace_list_req),
            preserving_proto_field_name=True, including_default_value_fields=True))
        assert listA["status"] == 0
        assert len(listA["namespaces"]) == 2
        nsidA1 = listA["namespaces"][0]["nsid"]
        nsidA2 = listA["namespaces"][1]["nsid"]
        bdevA1 = listA["namespaces"][0]["bdev_name"]
        bdevA2 = listA["namespaces"][1]["bdev_name"]
        uuidA1 = listA["namespaces"][0]["uuid"]
        uuidA2 = listA["namespaces"][1]["uuid"]
        imgA1 = listA["namespaces"][0]["rbd_image_name"]
        imgA2 = listA["namespaces"][1]["rbd_image_name"]
        time.sleep(10)
        listB = json.loads(json_format.MessageToJson(
            stubB.list_namespaces(namespace_list_req),
            preserving_proto_field_name=True, including_default_value_fields=True))
        assert listB["status"] == 0
        assert len(listB["namespaces"]) == 2
        nsidB1 = listB["namespaces"][0]["nsid"]
        nsidB2 = listB["namespaces"][1]["nsid"]
        bdevB1 = listB["namespaces"][0]["bdev_name"]
        bdevB2 = listB["namespaces"][1]["bdev_name"]
        uuidB1 = listB["namespaces"][0]["uuid"]
        uuidB2 = listB["namespaces"][1]["uuid"]
        imgB1 = listB["namespaces"][0]["rbd_image_name"]
        imgB2 = listB["namespaces"][1]["rbd_image_name"]
        if nsidA1 == nsidB1:
            assert nsidA2 == nsidB2
            assert bdevA1 == bdevB1
            assert bdevA2 == bdevB2
            assert uuidA1 == uuidB1
            assert uuidA2 == uuidB2
            assert imgA1 == imgB1
            assert imgA2 == imgB2
        elif nsidA1 == nsidB2:
            assert nsidA2 == nsidB1
            assert bdevA1 == bdevB2
            assert bdevA2 == bdevB1
            assert uuidA1 == uuidB2
            assert uuidA2 == uuidB1
            assert imgA1 == imgB2
            assert imgA2 == imgB1
        else:
            assert False
        gatewayB.__exit__(None, None, None)
        gatewayB = GatewayServer(configB)
        gatewayB.serve()
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channelB)
        time.sleep(10)
        listB = json.loads(json_format.MessageToJson(
            stubB.list_namespaces(namespace_list_req),
            preserving_proto_field_name=True, including_default_value_fields=True))
        assert listB["status"] == 0
        assert len(listB["namespaces"]) == 2
        nsidB1 = listB["namespaces"][0]["nsid"]
        nsidB2 = listB["namespaces"][1]["nsid"]
        bdevB1 = listB["namespaces"][0]["bdev_name"]
        bdevB2 = listB["namespaces"][1]["bdev_name"]
        uuidB1 = listB["namespaces"][0]["uuid"]
        uuidB2 = listB["namespaces"][1]["uuid"]
        imgB1 = listB["namespaces"][0]["rbd_image_name"]
        imgB2 = listB["namespaces"][1]["rbd_image_name"]
        if nsidA1 == nsidB1:
            assert nsidA2 == nsidB2
            assert bdevA1 == bdevB1
            assert bdevA2 == bdevB2
            assert uuidA1 == uuidB1
            assert uuidA2 == uuidB2
            assert imgA1 == imgB1
            assert imgA2 == imgB2
        elif nsidA1 == nsidB2:
            assert nsidA2 == nsidB1
            assert bdevA1 == bdevB2
            assert bdevA2 == bdevB1
            assert uuidA1 == uuidB2
            assert uuidA2 == uuidB1
            assert imgA1 == imgB2
            assert imgA2 == imgB1
        else:
            assert False
