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
hostnqn1 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7eb"
hostnqn2 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ec"
hostnqn4 = "nqn.2014-08.org.nvmexpress:uuid:6488a49c-dfa3-11d4-ac31-b232c6c68a8a"
hostnqn5 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ef"
hostnqn6 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f0"
hostnqn7 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f1"
hostnqn8 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f2"
hostnqn9 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f3"
hostnqn10 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f4"
hostnqn11 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f5"

hostdhchap1 = "DHHC-1:00:MWPqcx1Ug1debg8fPIGpkqbQhLcYUt39k7UWirkblaKEH1kE:"
hostdhchap2 = "DHHC-1:00:ojmm6ISA2DBpZEldEJqJvA1/cAl1wDmeolS0fCIn2qZpK0b7gpx3qu76yHpjlOOXNyjqf0oFYCWxUqkXGN2xEBOVxoA=:"
hostdhchap4 = "DHHC-1:01:Ei7kUrD7iyrjzXDwIZ666sSPBswUk4wDjSQtodytVB5YiBru:"
hostdhchap5 = "DHHC-1:03:6EKZcEL86u4vzTE8sCETvskE3pLKE+qOorl9QxrRfLvfOEQ5GvqCzM41U8fFVAz1cs+T14PjSpd1J641rfeCC1x2VNg=:"
hostdhchap6 = "DHHC-1:02:ULMaRuJ40ui58nK4Qk5b0J89G3unbGb8mBUbt/XSrf18PBPvyL3sivZOINNh2o/fPpXbGg==:"

host_name = socket.gethostname()
addr = "127.0.0.1"
config = "ceph-nvmeof.conf"

@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
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

def test_setup(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--rbd-create-image", "--size", "16MB"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text

def test_create_secure(caplog, gateway):
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name, "-a", addr, "-s", "5001", "--secure"])
    assert f"Adding {subsystem} listener at {addr}:5001: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn1, "--dhchap-key", hostdhchap1])
    assert f"Adding host {hostnqn1} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn2, "--dhchap-key", hostdhchap2])
    assert f"Adding host {hostnqn2} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn4, "--dhchap-key", hostdhchap4])
    assert f"Adding host {hostnqn4} to {subsystem}: Successful" in caplog.text

def test_create_not_secure(caplog, gateway):
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name, "-a", addr, "-s", "5002"])
    assert f"Adding {subsystem} listener at {addr}:5002: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn6])
    assert f"Adding host {hostnqn6} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn7])
    assert f"Adding host {hostnqn7} to {subsystem}: Successful" in caplog.text

def test_create_secure_list(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn8, hostnqn9, hostnqn10, "--dhchap-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"error: Can't have more than one host NQN when DH-HMAC-CHAP keys are used" in caplog.text

def test_create_secure_no_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn5, "--dhchap-key"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"error: argument --dhchap-key: expected one argument" in caplog.text

def test_dhchap_controller_key(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn11, "--dhchap-key", hostdhchap5, "--dhchap-ctrlr-key", hostdhchap6])
    assert f"Adding host {hostnqn11} to {subsystem}: Successful" in caplog.text

def test_list_dhchap_hosts(caplog, gateway):
    caplog.clear()
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    found = 0
    assert len(hosts.hosts) == 6
    for h in hosts.hosts:
        if h.nqn == hostnqn1:
            found += 1
            assert h.use_dhchap
        elif h.nqn == hostnqn2:
            found += 1
            assert h.use_dhchap
        elif h.nqn == hostnqn4:
            found += 1
            assert h.use_dhchap
        elif h.nqn == hostnqn6:
            found += 1
            assert not h.use_dhchap
        elif h.nqn == hostnqn7:
            found += 1
            assert not h.use_dhchap
        elif h.nqn == hostnqn11:
            found += 1
            assert h.use_dhchap
        else:
            assert False
    assert found == 6

def test_allow_any_host_with_dhchap(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*", "--dhchap-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"error: DH-HMAC-CHAP key is only allowed for specific hosts" in caplog.text

def test_dhchap_controller_with_no_dhchap_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn10, "--dhchap-ctrlr-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert f"error: DH-HMAC-CHAP controller keys can not be used without DH-HMAC-CHAP keys" in caplog.text

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
