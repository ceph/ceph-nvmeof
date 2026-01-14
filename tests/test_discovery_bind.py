import pytest
from control.cli import main as cli
from control.server import GatewayServer
import socket
from control.cephutils import CephUtils
import time


config = "ceph-nvmeof.conf"
pool = "rbd"
group_name = "GROUPNAME"


@pytest.fixture(scope="function")
def gateway(config, request):
    """Sets up and tears down Gateway"""

    discAddr = config.get("discovery", "addr")
    discPort = config.getint("discovery", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )

        if request.function.__name__ == "test_discovery_bind_abort":
            print(f"Will bind {discAddr}:{discPort} before starting server")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((discAddr, discPort))
        else:
            print(f"Will not bind {discAddr}:{discPort} before starting server")
        gateway.serve()

        yield gateway

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_discovery_bind_abort(caplog, gateway):
    _ = gateway
    aborted = False
    discovery_aborted = False
    discovery_pid = None
    lookfor = "Discovery service process id: "
    try:
        _ = gateway
        time.sleep(30)
    except SystemExit:
        aborted = True
        for line in caplog.get_records("setup"):
            linemsg = line.message
            pos = linemsg.find(lookfor)
            if pos >= 0:
                pos += len(lookfor)
                discovery_pid = linemsg[pos:].split('"')[0]
        assert discovery_pid
        lookfor = "PID of terminated child process is " + discovery_pid
        for line in caplog.get_records("call"):
            linemsg = line.message
            pos = linemsg.find(lookfor)
            if pos >= 0:
                discovery_aborted = True
    assert aborted
    assert discovery_aborted


def test_discovery_bind_ok(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["--format", "json", "gateway", "info"])
    assert f'"gw-id": "{gw.name}"' in caplog.text
