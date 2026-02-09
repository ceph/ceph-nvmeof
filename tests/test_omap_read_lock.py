import pytest
from control.server import GatewayServer
from control.cephutils import CephUtils
from control.state import OmapLock
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time
import os
import rados

pool = "rbd"
host_prefix = "nqn.2014-08.org.nvmexpress:uuid:893a6752-fe9b-ca48-aa93-e4565f3288"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    grp = "group2"
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = grp
    config.config["gateway"]["max_subsystems"] = "1024"
    config.config["gateway"]["max_namespaces"] = "5120"
    config.config["gateway"]["max_hosts"] = "5000"
    config.config["gateway"]["rebalance_period_sec"] = "0"
    config.config["gateway"]["state_update_notify"] = "False"
    config.config["gateway"]["state_update_interval_sec"] = "300"
    config.config["gateway"]["omap_file_lock_duration"] = "20"
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    if os.cpu_count() >= 4:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--lcores 1"
    else:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    portA = configA.getint("gateway", "port")
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = configB.getint("discovery", "port") + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    if os.cpu_count() >= 4:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"
    else:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"

    ceph_utils = CephUtils(config)
    with (GatewayServer(configA) as gatewayA, GatewayServer(configB) as gatewayB):
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", '
                  f'"group": "{grp}"' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", '
                  f'"group": "{grp}"' + "}"
        )
        gatewayA.serve()
        gatewayB.serve()

        channelA = grpc.insecure_channel(f"{addr}:{portA}")
        pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        pb2_grpc.GatewayStub(channelB)

        yield gatewayA.gateway_rpc, gatewayB.gateway_rpc
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def test_mixing_locks(caplog, two_gateways):
    gwA, gwB = two_gateways

    caplog.clear()
    gwA.gateway_state.omap.get_state()
    assert "Locked OMAP file before reading its content" in caplog.text
    assert "Released OMAP file lock after reading content" in caplog.text
    gwA.rpc_lock.acquire()
    gwB.rpc_lock.acquire()
    caplog.clear()
    gwA.omap_lock.lock_omap()
    assert "Locked OMAP exclusive" in caplog.text
    assert "Locked OMAP shared" not in caplog.text
    time.sleep(19)     # A little less than omap_file_lock_duration
    caplog.clear()
    gwB.gateway_state.omap.get_state()
    assert "The OMAP file is locked, will try again in" in caplog.text
    assert "Succeeded to lock OMAP file (shared) after" in caplog.text
    caplog.clear()
    with pytest.raises(rados.ObjectNotFound):
        gwA.omap_lock.unlock_omap()
    assert "OMAP was unlocked" not in caplog.text
    assert "No such lock, the exclusive lock might have expired" in caplog.text
    OmapLock.reset_lock_markers()
    time.sleep(25)
    caplog.clear()
    gwA.omap_lock.lock_omap(False, False, 1)
    assert "Locked OMAP shared" in caplog.text
    caplog.clear()
    gwB.omap_lock.lock_omap(False, False, 2)
    assert "Locked OMAP shared" in caplog.text
    assert "We already locked the OMAP file" not in caplog.text
    caplog.clear()
    gwA.omap_lock.unlock_omap(False, 1)
    assert "OMAP was unlocked" in caplog.text
    caplog.clear()
    gwB.omap_lock.unlock_omap(False, 2)
    assert "OMAP was unlocked" in caplog.text
    caplog.clear()
    with pytest.raises(rados.ObjectNotFound):
        gwA.omap_lock.unlock_omap(False, 1)
    assert "OMAP was unlocked" not in caplog.text
    assert "No such lock, the shared lock might have expired" in caplog.text
    OmapLock.reset_lock_markers()
    time.sleep(25)
    caplog.clear()
    gwA.omap_lock.lock_omap()
    assert "Locked OMAP exclusive" in caplog.text
    caplog.clear()
    try:
        gwA.omap_lock.lock_omap()
    except RuntimeError as ex:
        assert str(ex) == "An attempt to lock OMAP exclusively twice from the same thread"
        pass
    caplog.clear()
    gwA.omap_lock.unlock_omap()
    assert "OMAP was unlocked" in caplog.text
    caplog.clear()
    with pytest.raises(rados.ObjectNotFound):
        gwA.omap_lock.unlock_omap()
    assert "OMAP was unlocked" not in caplog.text
    assert "No such lock, the exclusive lock might have expired" in caplog.text
    OmapLock.reset_lock_markers()
    time.sleep(25)
    caplog.clear()
    gwA.omap_lock.lock_omap()
    assert "Locked OMAP exclusive" in caplog.text
    gotFileExists = False
    caplog.clear()
    try:
        gwA.omap_lock.lock_omap(False, False, 3)
    except FileExistsError:
        gotFileExists = True
    assert "No need to lock OMAP for read as we already have it locked for write" in caplog.text
    assert "Locked OMAP shared" not in caplog.text
    assert gotFileExists
    gwA.omap_lock.unlock_omap()
    gwA.rpc_lock.release()
    gwB.rpc_lock.release()
