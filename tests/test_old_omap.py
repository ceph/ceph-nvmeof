import pytest
from control.server import GatewayServer
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc

def test_old_omap(caplog, config):
    with GatewayServer(config) as gateway:
        gateway.serve()
        gateway.gateway_rpc.gateway_state.omap._add_key("bdev_dummy", "dummy")

    caplog.clear()
    with GatewayServer(config) as gateway:
        with pytest.raises(Exception) as ex:
            gateway.serve()
            assert f"Old OMAP file format, still contains bdevs, please remove file and try again" in str(ex.value)
