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
import spdk.rpc.bdev as rpc_bdev

image = "mytestdevimage"
pool = "rbd"
bdev_prefix = "Ceph_"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
host_nqn_prefix = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b"
created_resource_count = 200

def setup_config(config, gw1_name, gw2_name, gw_group, update_notify ,update_interval_sec, disable_unlock, lock_duration,
                 sock1_name, sock2_name, port_inc):
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
    addr = configA.get("gateway", "addr") + port_inc
    portA = configA.getint("gateway", "port")
    portB = portA + 2
    configB.config["gateway"]["name"] = gw2_name
    configB.config["gateway"]["port"] = str(portB)
    configB.config["spdk"]["rpc_socket_name"] = sock2_name
    configB.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x02"

    return configA, configB

def start_servers(gatewayA, gatewayB, addr, portA, portB):
    gatewayA.serve()
    set_group_id(1, gatewayA)
    # Delete existing OMAP state
    gatewayA.gateway_rpc.gateway_state.delete_state()
    # Create new
    gatewayB.serve()
    set_group_id(2, gatewayB)
    gatewayB.gateway_rpc.gateway_state.delete_state()

    # Bind the client and Gateways A & B
    channelA = grpc.insecure_channel(f"{addr}:{portA}")
    stubA = pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    stubB = pb2_grpc.GatewayStub(channelB)

    return stubA, stubB

def stop_servers(gatewayA, gatewayB):
    # Stop gateways
    gatewayA.gateway_rpc.gateway_state.delete_state()
    gatewayB.gateway_rpc.gateway_state.delete_state()
    gatewayA.server.stop(grace=1)
    gatewayB.server.stop(grace=1)

@pytest.fixture(scope="function")
def conn_omap_reread(config, request):
    """Sets up and tears down Gateways A and B."""

    # Setup GatewayA and GatewayB configs
    configA, configB = setup_config(config, "GatewayA", "GatewayB", "Group1", False, 300, False, 60,
                                    "spdk_GatewayA.sock", "spdk_GatewayB.sock", 0)
    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = configB.getint("gateway", "port")
    # Start servers
    with (
       GatewayServer(configA) as gatewayA,
       GatewayServer(configB) as gatewayB,
    ):
        stubA, stubB = start_servers(gatewayA, gatewayB, addr, portA, portB)
        yield stubA, stubB, gatewayA.gateway_rpc, gatewayB.gateway_rpc
        stop_servers(gatewayA, gatewayB)

@pytest.fixture(scope="function")
def conn_lock_twice(config, request):
    """Sets up and tears down Gateways A and B."""

    # Setup GatewayA and GatewayB configs
    configA, configB = setup_config(config, "GatewayAA", "GatewayBB", "Group2", True, 5, True, 100,
                                    "spdk_GatewayAA.sock", "spdk_GatewayBB.sock", 2)
    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = configB.getint("gateway", "port")
    # Start servers
    with (
       GatewayServer(configA) as gatewayA,
       GatewayServer(configB) as gatewayB,
    ):
        stubA, stubB = start_servers(gatewayA, gatewayB, addr, portA, portB)
        yield stubA, stubB
        stop_servers(gatewayA, gatewayB)

@pytest.fixture(scope="function")
def conn_concurrent(config, request):
    """Sets up and tears down Gateways A and B."""
    update_notify = True
    update_interval_sec = 5
    disable_unlock = False
    lock_duration = 60

    # Setup GatewayA and GatewayB configs
    configA, configB = setup_config(config, "GatewayAAA", "GatewayBBB", "Group3", True, 5, False, 60,
                                    "spdk_GatewayAAA.sock", "spdk_GatewayBBB.sock", 4)
    addr = configA.get("gateway", "addr")
    portA = configA.getint("gateway", "port")
    portB = configB.getint("gateway", "port")
    # Start servers
    with (
       GatewayServer(configA) as gatewayA,
       GatewayServer(configB) as gatewayB,
    ):
        stubA, stubB = start_servers(gatewayA, gatewayB, addr, portA, portB)
        yield stubA, stubB
        stop_servers(gatewayA, gatewayB)

def build_host_nqn(i):
    ihex = hex(i).split("x")[1]
    hostnqn = f"{host_nqn_prefix}{ihex:{0}>6}"
    return hostnqn

def create_resource_by_index(stub, i, caplog):
    bdev = f"{bdev_prefix}{i}"
    bdev_req = pb2.create_bdev_req(bdev_name=bdev,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    ret_bdev = stub.create_bdev(bdev_req)
    assert ret_bdev.status
    if caplog != None:
        assert f"create_bdev: {bdev}" in caplog.text
        assert "create_bdev failed" not in caplog.text
    subsystem = f"{subsystem_prefix}{i}"
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=subsystem)
    ret_subsystem = stub.create_subsystem(subsystem_req)
    assert ret_subsystem.status
    if caplog != None:
        assert f"create_subsystem {subsystem}: True" in caplog.text
        assert "create_subsystem failed" not in caplog.text
    namespace_req = pb2.add_namespace_req(subsystem_nqn=subsystem,
                                          bdev_name=bdev)
    ret_namespace = stub.add_namespace(namespace_req)
    assert ret_namespace.status
    hostnqn = build_host_nqn(i)
    host_req = pb2.add_host_req(subsystem_nqn=subsystem, host_nqn=hostnqn)
    ret_host = stub.add_host(host_req)
    assert ret_host.status
    host_req = pb2.add_host_req(subsystem_nqn=subsystem, host_nqn="*")
    ret_host = stub.add_host(host_req)
    assert ret_host.status
    if caplog != None:
        assert f"add_host {hostnqn}: True" in caplog.text
        assert "add_host *: True" in caplog.text
        assert "add_host failed" not in caplog.text

def check_resource_by_index(i, resource_list):
    # notice that this also verifies the namespace as the bdev name is in the namespaces section
    bdev = f"{bdev_prefix}{i}"
    subsystem = f"{subsystem_prefix}{i}"
    hostnqn = build_host_nqn(i)
    found_bdev = False
    found_host = False
    for res in resource_list:
        try:
            if res["nqn"] != subsystem:
                continue
            assert res["allow_any_host"]
            for host in res["hosts"]:
                if host["nqn"] == hostnqn:
                    found_host = True
            for ns in res["namespaces"]:
                if ns["bdev_name"] == bdev:
                    found_bdev = True
                    break
            break
        except Exception:
            pass
    assert found_bdev and found_host

def test_multi_gateway_omap_reread(config, conn_omap_reread, caplog):
    """Tests reading out of date OMAP file
    """
    stubA, stubB, gatewayA, gatewayB = conn_omap_reread
    bdev = bdev_prefix + "X0"
    bdev2 = bdev_prefix + "X1"
    bdev3 = bdev_prefix + "X2"
    nqn = subsystem_prefix + "X1"
    serial = "SPDK00000000000001"
    nsid = 10
    num_subsystems = 2

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

    # Until we create some resource on GW-B it shouldn't still have the resrouces created on GW-A, only the discovery subsystem
    listB = json.loads(json_format.MessageToJson(
        stubB.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listB) == 1

    listA = json.loads(json_format.MessageToJson(
        stubA.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listA) == num_subsystems

    bdev2_req = pb2.create_bdev_req(bdev_name=bdev2,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    ret_bdev2 = stubB.create_bdev(bdev2_req)
    assert ret_bdev2.status is True
    assert "The file is not current, will reload it and try again" in caplog.text

    # Make sure that after reading the OMAP file GW-B has the subsystem and namespace created on GW-A
    listB = json.loads(json_format.MessageToJson(
        stubB.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listB) == num_subsystems
    assert listB[num_subsystems-1]["nqn"] == nqn
    assert listB[num_subsystems-1]["serial_number"] == serial
    assert listB[num_subsystems-1]["namespaces"][0]["nsid"] == nsid
    assert listB[num_subsystems-1]["namespaces"][0]["bdev_name"] == bdev

    caplog.clear()
    bdev3_req = pb2.create_bdev_req(bdev_name=bdev3,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    ret_bdev3 = stubB.create_bdev(bdev3_req)
    assert ret_bdev3.status is True
    assert "The file is not current, will reload it and try again" not in caplog.text

    bdevsA = rpc_bdev.bdev_get_bdevs(gatewayA.spdk_rpc_client)
    bdevsB = rpc_bdev.bdev_get_bdevs(gatewayB.spdk_rpc_client)
    # GW-B should have the bdev created on GW-A after reading the OMAP file plus the two we created on it
    # GW-A should only have the bdev created on it as we didn't update it after creating the bdev on GW-B
    assert len(bdevsA) == 1
    assert len(bdevsB) == 3
    assert bdevsA[0]["name"] == bdev
    assert bdevsB[0]["name"] == bdev
    assert bdevsB[1]["name"] == bdev2
    assert bdevsB[2]["name"] == bdev3

def test_trying_to_lock_twice(config, image, conn_lock_twice, caplog):
    """Tests an attempt to lock the OMAP file from two gateways at the same time
    """
    caplog.clear()
    stubA, stubB = conn_lock_twice

    with pytest.raises(Exception) as ex:
        create_resource_by_index(stubA, 100000, None)
        create_resource_by_index(stubB, 100001, None)
    assert "OMAP file unlock was disabled, will not unlock file" in caplog.text
    assert "The OMAP file is locked, will try again in" in caplog.text
    assert "Unable to lock OMAP file" in caplog.text
    time.sleep(120) # Wait enough time for OMAP lock to be released

def test_multi_gateway_concurrent_changes(config, image, conn_concurrent, caplog):
    """Tests concurrent changes to the OMAP from two gateways
    """
    caplog.clear()
    stubA, stubB = conn_concurrent

    for i in range(created_resource_count):
        if i % 2 == 0:
            create_resource_by_index(stubA, i, caplog)
        else:
            create_resource_by_index(stubB, i, caplog)
        assert "failed" not in caplog.text.lower()

    # Let the update some time to bring both gateways to the same page
    time.sleep(15)
    caplog.clear()
    get_subsystems_req = pb2.get_subsystems_req()
    listA = json.loads(json_format.MessageToJson(
        stubA.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    listB = json.loads(json_format.MessageToJson(
        stubB.get_subsystems(get_subsystems_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    for i in range(created_resource_count):
        check_resource_by_index(i, listA)
        check_resource_by_index(i, listB)
