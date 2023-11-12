import pytest
import copy
import grpc
import json
import time
from control.server import GatewayServer
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import spdk.rpc.bdev as rpc_bdev

image = "mytestdevimage"
pool = "rbd"
bdev_prefix = "Ceph_"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
created_resource_count = 500

@pytest.fixture(scope="function")
def conn(config, request):
    """Sets up and tears down Gateways A and B."""
    update_notify = True
    update_interval_sec = 5
    disable_unlock = False
    lock_duration = 60
    if request.node.name == "test_multi_gateway_omap_reread":
        update_notify = False
        update_interval_sec = 300
    elif request.node.name == "test_trying_to_lock_twice":
        disable_unlock = True
        lock_duration = 100    # This should be bigger than lock retries * retry sleep interval

    # Setup GatewayA and GatewayB configs
    configA = copy.deepcopy(config)
    configA.config["gateway"]["name"] = "GatewayA"
    configA.config["gateway"]["group"] = "Group1"
    configA.config["gateway"]["state_update_notify"] = str(update_notify)
    configA.config["gateway"]["state_update_interval_sec"] = str(update_interval_sec)
    configA.config["gateway"]["omap_file_disable_unlock"] = str(disable_unlock)
    configA.config["gateway"]["omap_file_lock_duration"] = str(lock_duration)
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
    configB.config["gateway"]["min_controller_id"] = "20001"
    configB.config["gateway"]["max_controller_id"] = "40000"
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
        yield stubA, stubB, gatewayA.gateway_rpc, gatewayB.gateway_rpc

        # Stop gateways
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)
        gatewayB.gateway_rpc.gateway_state.delete_state()

def test_multi_gateway_omap_reread(config, conn, caplog):
    """Tests reading out of date OMAP file
    """
    stubA, stubB, gatewayA, gatewayB = conn
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
    watchB = stubB.get_subsystems(get_subsystems_req)
    listB = json.loads(watchB.subsystems)
    assert len(listB) == 1

    watchA = stubA.get_subsystems(get_subsystems_req)
    listA = json.loads(watchA.subsystems)
    assert len(listA) == num_subsystems

    bdev2_req = pb2.create_bdev_req(bdev_name=bdev2,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    ret_bdev2 = stubB.create_bdev(bdev2_req)
    assert ret_bdev2.status is True
    assert "The file is not current, will reload it and try again" in caplog.text

    # Make sure that after reading the OMAP file GW-B has the subsystem and namespace created on GW-A
    watchB = stubB.get_subsystems(get_subsystems_req)
    listB = json.loads(watchB.subsystems)
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

def test_trying_to_lock_twice(config, image, conn, caplog):
    """Tests an attempt to lock the OMAP file from two gateways at the same time
    """
    caplog.clear()
    stubA, stubB, gatewayA, gatewayB = conn

    with pytest.raises(Exception) as ex:
        create_resource_by_index(stubA, 0)
        create_resource_by_index(stubB, 1)
    assert "OMAP file unlock was disabled, will not unlock file" in caplog.text
    assert "The OMAP file is locked, will try again in" in caplog.text
    assert "Unable to lock OMAP file" in caplog.text
    time.sleep(120) # Wait enough time for OMAP lock to be released

def create_resource_by_index(stub, i):
    bdev = f"{bdev_prefix}{i}"
    bdev_req = pb2.create_bdev_req(bdev_name=bdev,
                                   rbd_pool_name=pool,
                                   rbd_image_name=image,
                                   block_size=4096)
    ret_bdev = stub.create_bdev(bdev_req)
    assert ret_bdev
    subsystem = f"{subsystem_prefix}{i}"
    subsystem_req = pb2.create_subsystem_req(subsystem_nqn=subsystem)
    ret_subsystem = stub.create_subsystem(subsystem_req)
    assert ret_subsystem
    namespace_req = pb2.add_namespace_req(subsystem_nqn=subsystem,
                                          bdev_name=bdev)
    ret_namespace = stub.add_namespace(namespace_req)
    assert ret_namespace

def check_resource_by_index(i, caplog):
    bdev = f"{bdev_prefix}{i}"
    # notice that this also verifies the namespace as the bdev name is in the namespaces section
    assert f"{bdev}" in caplog.text
    subsystem = f"{subsystem_prefix}{i}"
    assert f"{subsystem}" in caplog.text

def test_multi_gateway_concurrent_changes(config, image, conn, caplog):
    """Tests concurrent changes to the OMAP from two gateways
    """
    caplog.clear()
    stubA, stubB, gatewayA, gatewayB = conn

    for i in range(created_resource_count):
        if i % 2:
            stub = stubA
        else:
            stub = stubB
        create_resource_by_index(stub, i)
        assert "Failed" not in caplog.text

    # Let the update some time to bring both gateways to the same page
    time.sleep(15)
    for i in range(created_resource_count):
        check_resource_by_index(i, caplog)
