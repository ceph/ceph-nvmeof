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
created_resource_count = 10

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
    portA = configA.getint("gateway", "port") + port_inc
    configA.config["gateway"]["port"] = str(portA)
    portB = portA + 2
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
    subsystem = f"{subsystem_prefix}{i}"
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=subsystem)
    ret_subsystem = stub.create_subsystem(subsystem_req)
    assert ret_subsystem.status == 0
    if caplog != None:
        assert f"create_subsystem {subsystem}: True" in caplog.text
        assert f"Failure creating subsystem {subsystem}" not in caplog.text
    namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                          rbd_pool_name=pool, rbd_image_name=image, block_size=4096,
                                          create_image=True, size=16*1024*1024, force=True)
    ret_namespace = stub.namespace_add(namespace_req)
    assert ret_namespace.status == 0
    hostnqn = build_host_nqn(i)
    host_req = pb2.add_host_req(subsystem_nqn=subsystem, host_nqn=hostnqn)
    ret_host = stub.add_host(host_req)
    assert ret_host.status == 0
    host_req = pb2.add_host_req(subsystem_nqn=subsystem, host_nqn="*")
    ret_host = stub.add_host(host_req)
    assert ret_host.status == 0
    if caplog != None:
        assert f"add_host {hostnqn}: True" in caplog.text
        assert "add_host *: True" in caplog.text
        assert f"Failure allowing open host access to {subsystem}" not in caplog.text
        assert f"Failure adding host {hostnqn} to {subsystem}" not in caplog.text

def check_resource_by_index(i, subsys_list, hosts_info):
    subsystem = f"{subsystem_prefix}{i}"
    hostnqn = build_host_nqn(i)
    found_host = False
    for subsys in subsys_list:
        try:
            if subsys["nqn"] != subsystem:
                continue
            assert subsys["namespace_count"] == 1
            assert hosts_info["allow_any_host"]
            for host in hosts_info["hosts"]:
                if host["nqn"] == hostnqn:
                    found_host = True
            break
        except Exception:
            pass
    assert found_host

def test_multi_gateway_omap_reread(config, conn_omap_reread, caplog):
    """Tests reading out of date OMAP file
    """
    stubA, stubB, gatewayA, gatewayB = conn_omap_reread
    nqn = subsystem_prefix + "X1"
    serial = "SPDK00000000000001"
    nsid = 10
    num_subsystems = 2

    # Send requests to create a subsystem with one namespace to GatewayA
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=nqn, serial_number=serial)
    namespace_req = pb2.namespace_add_req(subsystem_nqn=nqn, nsid=nsid,
                                          rbd_pool_name=pool, rbd_image_name=image, block_size=4096,
                                          create_image=True, size=16*1024*1024, force=True)

    subsystem_list_req = pb2.list_subsystems_req()
    ret_subsystem = stubA.create_subsystem(subsystem_req)
    assert ret_subsystem.status == 0
    ret_namespace = stubA.namespace_add(namespace_req)
    assert ret_namespace.status == 0

    # Until we create some resource on GW-B it shouldn't still have the resrouces created on GW-A, only the discovery subsystem
    listB = json.loads(json_format.MessageToJson(
        stubB.list_subsystems(subsystem_list_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listB) == 1

    listA = json.loads(json_format.MessageToJson(
        stubA.list_subsystems(subsystem_list_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listA) == num_subsystems

    ns2_req = pb2.namespace_add_req(subsystem_nqn=nqn,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096, create_image=True, size=16*1024*1024, force=True)
    ret_ns2 = stubB.namespace_add(ns2_req)
    assert ret_ns2.status == 0
    assert "The file is not current, will reload it and try again" in caplog.text

    # Make sure that after reading the OMAP file GW-B has the subsystem and namespace created on GW-A
    listB = json.loads(json_format.MessageToJson(
        stubB.list_subsystems(subsystem_list_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    assert len(listB) == num_subsystems
    assert listB[num_subsystems-1]["nqn"] == nqn
    assert listB[num_subsystems-1]["serial_number"] == serial
    assert listB[num_subsystems-1]["namespace_count"] == num_subsystems    # We created one namespace on each subsystem

    caplog.clear()
    ns3_req = pb2.namespace_add_req(subsystem_nqn=nqn,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096, create_image=True, size=16*1024*1024, force=True)
    ret_ns3 = stubB.namespace_add(ns3_req)
    assert ret_ns3.status == 0
    assert "The file is not current, will reload it and try again" not in caplog.text

    bdevsA = rpc_bdev.bdev_get_bdevs(gatewayA.spdk_rpc_client)
    bdevsB = rpc_bdev.bdev_get_bdevs(gatewayB.spdk_rpc_client)
    # GW-B should have the bdev created on GW-A after reading the OMAP file plus the two we created on it
    # GW-A should only have the bdev created on it as we didn't update it after creating the bdev on GW-B
    assert len(bdevsA) == 1
    assert len(bdevsB) == 3
    assert bdevsA[0]["uuid"] == bdevsB[0]["uuid"]

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
    listener_req = pb2.create_listener_req(nqn=f"{subsystem_prefix}0",
                                           gateway_name="GatewayAAA",
                                           adrfam="ipv4",
                                           traddr="127.0.0.1",
                                           trsvcid=5001)
    listener_ret = stubA.create_listener(listener_req)
    assert listener_ret.status == 0
    assert f"Received request to create GatewayAAA TCP ipv4 listener for {subsystem_prefix}0 at 127.0.0.1:5001" in caplog.text
    assert f"create_listener: True" in caplog.text

    timeout = 15  # Maximum time to wait (in seconds)
    start_time = time.time()
    expected_warning_other_gw = "Listener not created as gateway GatewayBBB differs from requested gateway GatewayAAA"

    while expected_warning_other_gw not in caplog.text:
        if time.time() - start_time > timeout:
            pytest.fail(f"Timeout: '{expected_warning_other_gw}' not found in caplog.text within {timeout} seconds.")
        time.sleep(0.1)

    assert expected_warning_other_gw in caplog.text
    caplog.clear()
    subsystem_list_req = pb2.list_subsystems_req()
    subListA = json.loads(json_format.MessageToJson(
        stubA.list_subsystems(subsystem_list_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    subListB = json.loads(json_format.MessageToJson(
        stubB.list_subsystems(subsystem_list_req),
        preserving_proto_field_name=True, including_default_value_fields=True))['subsystems']
    for i in range(created_resource_count):
        subsystem = f"{subsystem_prefix}{i}"
        host_list_req = pb2.list_hosts_req(subsystem=subsystem)
        hostListA = json.loads(json_format.MessageToJson(
            stubA.list_hosts(host_list_req),
            preserving_proto_field_name=True, including_default_value_fields=True))
        hostListB = json.loads(json_format.MessageToJson(
            stubB.list_hosts(host_list_req),
            preserving_proto_field_name=True, including_default_value_fields=True))
        check_resource_by_index(i, subListA, hostListA)
        check_resource_by_index(i, subListB, hostListB)

def test_multi_gateway_listener_update(config, image, conn_concurrent, caplog):
    """Tests listener update after subsystem deletion
    """
    stubA, stubB = conn_concurrent

    caplog.clear()
    subsystem = f"{subsystem_prefix}QQQ"
    subsystem_add_req = pb2.create_subsystem_req(subsystem_nqn=subsystem)
    ret_subsystem = stubA.create_subsystem(subsystem_add_req)
    assert ret_subsystem.status == 0
    assert f"create_subsystem {subsystem}: True" in caplog.text
    assert f"Failure creating subsystem {subsystem}" not in caplog.text
    caplog.clear()
    listenerA_req = pb2.create_listener_req(nqn=subsystem,
                                           gateway_name="GatewayAAA",
                                           adrfam="ipv4",
                                           traddr="127.0.0.1",
                                           trsvcid=5101)
    listener_ret = stubA.create_listener(listenerA_req)
    assert listener_ret.status == 0
    assert f"Received request to create GatewayAAA TCP ipv4 listener for {subsystem} at 127.0.0.1:5101" in caplog.text
    assert f"create_listener: True" in caplog.text
    caplog.clear()
    listenerB_req = pb2.create_listener_req(nqn=subsystem,
                                           gateway_name="GatewayBBB",
                                           adrfam="ipv4",
                                           traddr="127.0.0.1",
                                           trsvcid=5102)
    listener_ret = stubB.create_listener(listenerB_req)
    assert listener_ret.status == 0
    assert f"Received request to create GatewayBBB TCP ipv4 listener for {subsystem} at 127.0.0.1:5102" in caplog.text
    assert f"create_listener: True" in caplog.text
    caplog.clear()
    subsystem_del_req = pb2.delete_subsystem_req(subsystem_nqn=subsystem)
    ret_subsystem = stubA.delete_subsystem(subsystem_del_req)
    assert ret_subsystem.status == 0
    assert f"delete_subsystem {subsystem}: True" in caplog.text
    assert f"Failure deleting subsystem {subsystem}" not in caplog.text
    caplog.clear()
    ret_subsystem = stubA.create_subsystem(subsystem_add_req)
    assert ret_subsystem.status == 0
    assert f"create_subsystem {subsystem}: True" in caplog.text
    assert f"Failure creating subsystem {subsystem}" not in caplog.text
    caplog.clear()
    listener_ret = stubA.create_listener(listenerA_req)
    assert listener_ret.status == 0
    assert f"Received request to create GatewayAAA TCP ipv4 listener for {subsystem} at 127.0.0.1:5101" in caplog.text
    assert f"create_listener: True" in caplog.text
    assert f"Failure adding {subsystem} listener at 127.0.0.1:5101" not in caplog.text
    caplog.clear()
    listener_ret = stubB.create_listener(listenerB_req)
    assert listener_ret.status == 0
    assert f"Received request to create GatewayBBB TCP ipv4 listener for {subsystem} at 127.0.0.1:5102" in caplog.text
    assert f"create_listener: True" in caplog.text
    assert f"Failure adding {subsystem} listener at 127.0.0.1:5102" not in caplog.text
