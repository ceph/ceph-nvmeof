import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
from control.utils import GatewayUtils
from control.config import GatewayConfig
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import os
import os.path

image = "mytestdevimage"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
hostnqn = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7eb"
hostnqn2 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ec"
hostnqn3 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ee"
hostnqn4 = "nqn.2014-08.org.nvmexpress:uuid:6488a49c-dfa3-11d4-ac31-b232c6c68a8a"
hostnqn5 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ef"
hostnqn6 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f0"
hostnqn7 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f1"

hostpsk = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
hostpsk2 = "NVMeTLSkey-1:02:FTFds4vH4utVcfrOforxbrWIgv+Qq4GQHgMdWwzDdDxE1bAqK2mOoyXxmbJxGeueEVVa/Q==:"
hostpsk3 = "junk" 
hostpsk4 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"

host_name = socket.gethostname()
addr = "127.0.0.1"
config = "ceph-nvmeof.conf"

@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway-logs"]["log_level"] = "debug"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command("{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", "group": ""' + "}")
        gateway.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{port}")
        yield gateway.gateway_rpc

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()

def write_file(filepath, content):
    with open(filepath, "w") as f:
        f.write(content)
    os.chmod(filepath, 0o600)

def test_setup(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--rbd-create-image", "--size", "16MB"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text

def test_allow_any_host(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", "*"])
    assert f"Allowing open host access to {subsystem}: Successful" in caplog.text

def test_create_secure_with_any_host(caplog, gateway):
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name, "-a", addr, "-s", "5001", "--secure"])
    assert f"Secure channel is only allowed for subsystems in which \"allow any host\" is off" in caplog.text

def test_remove_any_host_access(caplog, gateway):
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem, "--host", "*"])
    assert f"Disabling open host access to {subsystem}: Successful" in caplog.text

def test_create_secure(caplog, gateway):
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name, "-a", addr, "-s", "5001", "--secure"])
    assert f"Adding {subsystem} listener at {addr}:5001: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn, "--psk", hostpsk])
    assert f"Adding host {hostnqn} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn2, "--psk", hostpsk2])
    assert f"Adding host {hostnqn2} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn4, "--psk", hostpsk4])
    assert f"Adding host {hostnqn4} to {subsystem}: Successful" in caplog.text

def test_create_not_secure(caplog, gateway):
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name, "-a", addr, "-s", "5002"])
    assert f"Adding {subsystem} listener at {addr}:5002: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn6])
    assert f"Adding host {hostnqn6} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn7])
    assert f"Adding host {hostnqn7} to {subsystem}: Successful" in caplog.text

def test_create_secure_junk_key(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn3, "--psk", hostpsk3])
    assert f"Failure adding host {hostnqn3} to {subsystem}" in caplog.text

def test_create_secure_no_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host", hostnqn5, "--psk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"error: argument --psk: expected one argument" in caplog.text

def test_list_psk_hosts(caplog, gateway):
    caplog.clear()
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    found = 0
    assert len(hosts.hosts) == 5
    for h in hosts.hosts:
        assert h.nqn != hostnqn3
        assert h.nqn != hostnqn5
        if h.nqn == hostnqn:
            found += 1
            assert h.use_psk
        elif h.nqn == hostnqn2:
            found += 1
            assert h.use_psk
        elif h.nqn == hostnqn4:
            found += 1
            assert h.use_psk
        elif h.nqn == hostnqn6:
            found += 1
            assert not h.use_psk
        elif h.nqn == hostnqn7:
            found += 1
            assert not h.use_psk
        else:
            assert False
    assert found == 5

def test_allow_any_host_with_psk(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host", "*", "--psk", hostpsk])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"PSK is only allowed for specific hosts" in caplog.text

def test_list_listeners(caplog, gateway):
    caplog.clear()
    listeners = cli_test(["listener", "list", "--subsystem", subsystem])
    assert len(listeners.listeners) == 2
    found = 0
    for l in listeners.listeners:
        if l.trsvcid == 5001:
            found += 1
            assert l.secure
        elif l.trsvcid == 5002:
            found += 1
            assert not l.secure
        else:
            assert False
    assert found == 2
