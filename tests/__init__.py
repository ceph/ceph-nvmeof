from control.proto import monitor_pb2
from control.proto.monitor_pb2_grpc import MonitorGroupStub
from control.server import GatewayServer
import grpc

def set_group_id(group_id: int, server: GatewayServer):
    """Set group ID od the gateway, mocking nvmeof gateway monitor client"""
    channel = grpc.insecure_channel(server._monitor_address())
    stub = MonitorGroupStub(channel)
    request = monitor_pb2.group_id_req(id=group_id)
    stub.group_id(request)
    print(f"Set group id {group_id=} using address {server._monitor_address()} successfully.")
