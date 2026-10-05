import logging
import tempfile
import sys
import types
from pathlib import Path

# Provide a lightweight dummy grpc module for test environment if not installed.
if 'grpc' not in sys.modules:
    sys.modules['grpc'] = types.ModuleType('grpc')

# Stub minimal spdk.rpc.client module so imports succeed in test environment.
if 'spdk.rpc.client' not in sys.modules:
    spdk_mod = types.ModuleType('spdk')
    spdk_rpc_mod = types.ModuleType('spdk.rpc')
    spdk_rpc_client_mod = types.ModuleType('spdk.rpc.client')
    # Provide a JSONRPCClient factory that returns None (we replace the client later)
    def JSONRPCClient(*args, **kwargs):
        return None
    spdk_rpc_client_mod.JSONRPCClient = JSONRPCClient
    # Minimal exception type used by codepaths during import
    spdk_rpc_client_mod.JSONRPCException = Exception
    sys.modules['spdk'] = spdk_mod
    sys.modules['spdk.rpc'] = spdk_rpc_mod
    sys.modules['spdk.rpc.client'] = spdk_rpc_client_mod

# Stub generated protobuf modules under control.proto so imports succeed.
for mod in (
    'control.proto.gateway_pb2',
    'control.proto.gateway_pb2_grpc',
    'control.proto.monitor_pb2',
    'control.proto.monitor_pb2_grpc',
):
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

# Stub Ceph Python modules used by control.state and cephutils.
for mod in ('rados', 'rbd'):
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

# Stub prometheus_client.core used by control.prometheus
if 'prometheus_client.core' not in sys.modules:
    prom = types.ModuleType('prometheus_client')
    prom_core = types.ModuleType('prometheus_client.core')
    class _DummyRegistry:
        def unregister(self, *a, **kw):
            return None

    prom_core.REGISTRY = _DummyRegistry()
    class GaugeMetricFamily:
        pass
    prom_core.GaugeMetricFamily = GaugeMetricFamily
    class CounterMetricFamily:
        pass
    prom_core.CounterMetricFamily = CounterMetricFamily
    class InfoMetricFamily:
        pass
    prom_core.InfoMetricFamily = InfoMetricFamily
    # minimal functions/consts expected
    prom.start_http_server = lambda *a, **kw: None
    prom.GC_COLLECTOR = object()
    sys.modules['prometheus_client'] = prom
    sys.modules['prometheus_client.core'] = prom_core

# Provide lightweight stubs for control.grpc and control.state to avoid
# importing heavy dependencies during unit tests.
if 'control.grpc' not in sys.modules:
    mod = types.ModuleType('control.grpc')
    class DummyService: pass
    mod.GatewayService = DummyService
    mod.MonitorGroupService = DummyService
    sys.modules['control.grpc'] = mod

if 'control.state' not in sys.modules:
    mod = types.ModuleType('control.state')
    # minimal placeholders referenced by control.server
    class GatewayState: pass
    class LocalGatewayState: pass
    class OmapLock: pass
    class OmapGatewayState: pass
    class GatewayStateHandler: pass
    mod.GatewayState = GatewayState
    mod.LocalGatewayState = LocalGatewayState
    mod.OmapLock = OmapLock
    mod.OmapGatewayState = OmapGatewayState
    mod.GatewayStateHandler = GatewayStateHandler
    sys.modules['control.state'] = mod

from control.config import GatewayConfig
from control.server import GatewayServer


class DummyRpcClient:
    def __init__(self):
        self.last_enable = None

    def bdev_rbd_set_with_crc32c(self, enable):
        self.last_enable = bool(enable)


def make_conf(contents: str) -> str:
    tf = tempfile.NamedTemporaryFile(mode="w", delete=False)
    tf.write(contents)
    tf.flush()
    tf.close()
    return tf.name


def make_gateway_from_conf(conf_text: str):
    conf_path = make_conf(conf_text)
    cfg = GatewayConfig(conf_path)
    gw = GatewayServer.__new__(GatewayServer)
    gw.config = cfg
    gw.logger = logging.getLogger("test_rbd_crc")
    gw.spdk_rpc_client = DummyRpcClient()
    return gw


def test_default_disables_rbd_crc():
    conf = """[spdk]
"""
    gw = make_gateway_from_conf(conf)
    gw._initialize_rbd_crc32c()
    assert gw.spdk_rpc_client.last_enable is False


def test_skip_flag_disables_rbd_crc():
    conf = """[spdk]
skip_rbd_crc_if_transport_digest = True
"""
    gw = make_gateway_from_conf(conf)
    gw._initialize_rbd_crc32c()
    assert gw.spdk_rpc_client.last_enable is False


def test_explicit_enable_respected():
    conf = """[spdk]
rbd_with_crc32c = True
"""
    gw = make_gateway_from_conf(conf)
    gw._initialize_rbd_crc32c()
    assert gw.spdk_rpc_client.last_enable is True


def test_skip_overrides_explicit_enable():
    conf = """[spdk]
rbd_with_crc32c = True
skip_rbd_crc_if_transport_digest = True
"""
    gw = make_gateway_from_conf(conf)
    gw._initialize_rbd_crc32c()
    assert gw.spdk_rpc_client.last_enable is False
