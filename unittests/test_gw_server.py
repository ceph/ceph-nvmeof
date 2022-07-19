import grpc
import pytest
import pdb
import logging
from control.proto.gateway_pb2 import create_bdev_req
from unittest.mock import patch
from unittest import mock
from unittest.mock import Mock
import unittest
import spdk.scripts.rpc
from control.grpc import GatewayService

gwService  = None

@pytest.fixture(scope='module')
def object_instance():
    with patch('control.config.GatewayConfig') as MockClass:
        instance = MockClass.return_value
        yield instance

@pytest.fixture(scope='module')
def grpc_add_to_server():
    from control.proto.gateway_pb2_grpc import add_GatewayServicer_to_server

    return add_GatewayServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer(object_instance):
    
    global gwService 
    gwService =  GatewayService(object_instance, Mock(), Mock(), Mock())
    gwService.client = mock.Mock()
    # gwService.spdk_rpc = mock.Mock()
    #gwService.spdk_rpc.bdev = mock.Mock()
    # gwService.spdk_rpc.bdev.bdev_rbd_create.return_value = "test"
    return gwService


@pytest.fixture(scope='module')
def grpc_stub(grpc_channel):
    from control.proto.gateway_pb2_grpc import GatewayStub

    return GatewayStub(grpc_channel)

def test_bdev_rbd_create_pass(grpc_stub):
    # Configure the mock to return a response with an OK status code.
    gwService.spdk_rpc = mock.Mock()
    gwService.spdk_rpc.bdev.bdev_rbd_create.return_value = "test"
    # response = None
    # try: 
    bdev_req = create_bdev_req(ceph_pool_name="rbd", rbd_name='rbd_name',block_size=4096)
    response = grpc_stub.create_bdev(bdev_req)

    assert response.status == True , "Not matching name"

def test_bdev_rbd_create_fail(grpc_stub):
    # Configure the mock to return a response with an OK status code.
    gwService.spdk_rpc.bdev.bdev_rbd_create.side_effect = Exception()
    try: 
        bdev_req = create_bdev_req(ceph_pool_name="rbd", rbd_name='rbd_name',block_size=4096)
        response = grpc_stub.create_bdev(bdev_req)
    except Exception as ex:
        assert ex.args[0].code == grpc.StatusCode.INTERNAL , "Failed to get right name"
