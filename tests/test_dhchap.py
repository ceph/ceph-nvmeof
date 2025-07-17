import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
import time
import os

image = "mytestdevimage"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
subsystem3 = "nqn.2016-06.io.spdk:cnode3"
subsystem4 = "nqn.2016-06.io.spdk:cnode4"
subsystem5 = "nqn.2016-06.io.spdk:cnode5"
subsystem6 = "nqn.2016-06.io.spdk:cnode6"
subsystem7 = "nqn.2016-06.io.spdk:cnode7"
subsystem8 = "nqn.2016-06.io.spdk:cnode8"
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
hostnqn12 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f6"
hostnqn13 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f7"
hostnqn14 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f8"
hostnqn15 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7f9"
hostnqn16 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7fa"
hostnqn17 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7fb"
hostnqn18 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7fc"

discovery_nqn = "nqn.2014-08.org.nvmexpress.discovery"

hostdhchap1 = "DHHC-1:00:MWPqcx1Ug1debg8fPIGpkqbQhLcYUt39k7UWirkblaKEH1kE:"
hostdhchap2 = "DHHC-1:00:ojmm6ISA2DBpZEldEJqJvA1/cAl1wDmeolS0fCIn2qZpK0b7gpx3qu76yHpjlOOXN" \
              "yjqf0oFYCWxUqkXGN2xEBOVxoA=:"
hostdhchap4 = "DHHC-1:01:Ei7kUrD7iyrjzXDwIZ666sSPBswUk4wDjSQtodytVB5YiBru:"
hostdhchap5 = "DHHC-1:03:6EKZcEL86u4vzTE8sCETvskE3pLKE+qOorl9QxrRfLvfOEQ5GvqCzM41U8fFVAz1c" \
              "s+T14PjSpd1J641rfeCC1x2VNg=:"
hostdhchap6 = "DHHC-1:02:ULMaRuJ40ui58nK4Qk5b0J89G3unbGb8mBUbt/XSrf18PBPvyL3sivZOINNh2o/fPpXbGg==:"
hostdhchap7 = "DHHC-1:01:x7ecfGgIdOEl+J5cJ9JcZHOS2By2Me6eDJUnrsT9MVrCWRYV:"
hostdhchap8 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
hostdhchap9 = "DHHC-1:01:KD+sfH3/o2bRQoV0ESjBUywQlMnSaYpZISUbVa0k0nsWpNST:"
hostdhchap10 = "DHHC-1:00:rWf0ZFYO7IgWGttM8w6jUrAY4cTQyqyXPdmxHeOSve3w5QU9:"
hostdhchap11 = "DHHC-1:02:j3uUz05r5aQy42vX4tDXqVf9HgUPPdEp3kXTgUWl9EphsG7jwpr9KSIt3bmRLXBijPTIDQ==:"

badhostdhchap1 = "xDHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap2 = "DHHC-1:01eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap3 = "DHHC-1:07:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap4 = "DHHC-1::eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap5 = "DHHC-1:tt:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap6 = "DHHC-1:01::"
badhostdhchap7 = "DHHC-1:01:xxxxxx:"
badhostdhchap8 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz"
badhostdhchap9 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hzYzrP:"
badhostdhchap10 = "DHHC-1:02:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz:"
badhostdhchap11 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327h*:"
badhostdhchap12 = "DHHC-1:01:eNNXGjidEHHStbUi2Gmpps0JcnofReFfy+NaulguGgt327hz::"
badhostdhchap13 = "DHHC-1:00:MWPrcx1Ug1debg8fPIGpkqbQhLcYUt39k7UWirkblaKEH1kE:"

hostpsk1 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
hostpsk2 = "NVMeTLSkey-1:01:vUrPe33Auz/sgAAcYctjI0oOOEFM5lheeLy7U+yTsD/LHm9q:"

addr = "127.0.0.1"
config = "ceph-nvmeof.conf"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["name"] = "GW1"
    config.config["gateway"]["override_hostname"] = "GW1"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    if os.cpu_count() >= 4:
        config.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x01"
    else:
        config.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", '
            f'"pool": "{pool}", "group": ""' + "}"
        )
        gateway.serve()

        # Bind the client and Gateway
        grpc.insecure_channel(f"{addr}:{port}")
        yield gateway.gateway_rpc

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


@pytest.fixture(scope="function")
def gateway_encryption_disabled(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port") + 2
    discport = config.getint("discovery", "port") + 1
    config.config["gateway"]["name"] = "GW2"
    config.config["gateway"]["override_hostname"] = "GW2"
    config.config["gateway"]["port"] = f"{port}"
    config.config["discovery"]["port"] = f"{discport}"
    config.config["spdk"]["rpc_socket_name"] = "spdk2.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    config.config["gateway"]["enable_key_encryption"] = "False"
    if os.cpu_count() >= 4:
        config.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x02"
    else:
        config.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway_encryption_disabled:

        # Start gateway
        gateway_encryption_disabled.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway_encryption_disabled.name}", '
            f'"pool": "{pool}", "group": ""' + "}"
        )
        gateway_encryption_disabled.serve()

        # Bind the client and Gateway
        grpc.insecure_channel(f"{addr}:{port}")
        yield gateway_encryption_disabled.gateway_rpc

        # Stop gateway
        gateway_encryption_disabled.server.stop(grace=1)


@pytest.fixture(scope="function")
def gateway_no_encryption_key(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port") + 2
    discport = config.getint("discovery", "port") + 1
    config.config["gateway"]["name"] = "GW3"
    config.config["gateway"]["override_hostname"] = "GW3"
    config.config["gateway"]["port"] = f"{port}"
    config.config["discovery"]["port"] = f"{discport}"
    config.config["spdk"]["rpc_socket_name"] = "spdk3.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    config.config["gateway"]["enable_key_encryption"] = "True"
    config.config["gateway"]["encryption_key"] = "/etc/ceph/NOencryption.key"
    if os.cpu_count() >= 4:
        config.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x04"
    else:
        config.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway_no_encryption_key:

        # Start gateway
        gateway_no_encryption_key.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway_no_encryption_key.name}", '
            f'"pool": "{pool}", "group": ""' + "}"
        )
        gateway_no_encryption_key.serve()

        # Bind the client and Gateway
        grpc.insecure_channel(f"{addr}:{port}")
        yield gateway_no_encryption_key.gateway_rpc

        # Stop gateway
        gateway_no_encryption_key.server.stop(grace=1)


@pytest.fixture(scope="function")
def gateway_no_key_encryption_disabled(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port") + 2
    discport = config.getint("discovery", "port") + 1
    config.config["gateway"]["name"] = "GW4"
    config.config["gateway"]["override_hostname"] = "GW4"
    config.config["gateway"]["port"] = f"{port}"
    config.config["discovery"]["port"] = f"{discport}"
    config.config["spdk"]["rpc_socket_name"] = "spdk4.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = ""
    config.config["gateway"]["enable_key_encryption"] = "False"
    config.config["gateway"]["encryption_key"] = "/etc/ceph/NOencryption.key"
    if os.cpu_count() >= 4:
        config.config["spdk"]["tgt_cmd_extra_args"] = "-m 0x08"
    else:
        config.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway_no_key_encryption_disabled:

        # Start gateway
        gateway_no_key_encryption_disabled.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway_no_key_encryption_disabled.name}", '
            f'"pool": "{pool}", "group": ""' + "}"
        )
        gateway_no_key_encryption_disabled.serve()

        # Bind the client and Gateway
        grpc.insecure_channel(f"{addr}:{port}")
        yield gateway_no_key_encryption_disabled.gateway_rpc

        # Stop gateway
        gateway_no_key_encryption_disabled.server.stop(grace=1)


def test_setup(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image, "--rbd-create-image", "--size", "16MB"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem2, "--dhchap-key", hostdhchap6,
         "--no-group-append"])
    assert f"create_subsystem {subsystem2}: True" in caplog.text
    caplog.clear()
    state = gw.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gw.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem in key:
            assert '"dhchap_key":' not in valstr
            assert '"key_encrypted": true' not in valstr
        elif subsystem2 in key:
            assert hostdhchap6 not in valstr
            assert '"dhchap_key": "' in valstr
            assert '"dhchap_key": ""' not in valstr
            assert '"key_encrypted": true' in valstr


def test_create_secure(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", gw.gateway_name,
         "-a", addr, "-s", "5001", "--secure"])
    assert f"Adding {subsystem} listener at {addr}:5001: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn1,
         "--dhchap-key", hostdhchap1])
    assert f"Adding host {hostnqn1} to {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn1} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn2,
         "--dhchap-key", hostdhchap2])
    assert f"Adding host {hostnqn2} to {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn2} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn4,
         "--dhchap-key", hostdhchap4])
    assert f"Adding host {hostnqn4} to {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn4} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text


def test_create_not_secure(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem, "--host-name", gw.gateway_name,
         "-a", addr, "-s", "5002"])
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
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn8, hostnqn9, hostnqn10,
             "--dhchap-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: Can't have more than one host NQN when DH-HMAC-CHAP keys are used" in caplog.text


def test_create_secure_no_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn5, "--dhchap-key"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: argument --dhchap-key/-k: expected one argument" in caplog.text


def test_create_secure_empty_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn5, "--dhchap-key", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key can't be empty" in caplog.text


def test_add_host_bad_keys(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap1])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap1}": key must start with "DHHC-1:' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap2])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap2}": should contain a ":" delimiter' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap3])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap3}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap4])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap4}": missing hash' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap5])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap5}": non numeric hash "' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap6])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap6}": base64 part is missing' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap7])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap7}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap8])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap8}": key must end with ":"' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap9])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap9}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap10])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap10}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap11])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap11}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap12])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap12}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn15,
         "--dhchap-key", badhostdhchap13])
    assert f'Failure adding host {hostnqn15} to {subsystem}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap13}": CRC-32 checksums mismatch' in caplog.text


def test_dhchap_subsystem_key(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem2, "--host-nqn", hostnqn11,
         "--dhchap-key", hostdhchap5])
    assert f"Adding host {hostnqn11} to {subsystem2}: Successful" in caplog.text


def test_list_dhchap_hosts(caplog, gateway):
    caplog.clear()
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    found = 0
    assert len(hosts.hosts) == 5
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
        else:
            assert False
    assert found == 5

    caplog.clear()
    hosts = cli_test(["host", "list", "--subsystem", subsystem2])
    found = 0
    assert len(hosts.hosts) == 1
    for h in hosts.hosts:
        if h.nqn == hostnqn11:
            found += 1
            assert h.use_dhchap
        else:
            assert False
    assert found == 1


def test_allow_any_host_with_dhchap(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*",
             "--dhchap-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key is only allowed for specific hosts" in caplog.text


def test_empty_dhchap_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn17, "--dhchap-key", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key can't be empty" in caplog.text


def test_dhchap_controller_with_no_dhchap_key(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem2, "--host-nqn", hostnqn10])
    assert f"Failure adding host {hostnqn10} to {subsystem2}: Host must have a DH-HMAC-CHAP " \
           f"key if the subsystem has one" in caplog.text


def test_list_listeners(caplog, gateway):
    caplog.clear()
    listeners = cli_test(["listener", "list", "--subsystem", subsystem])
    assert len(listeners.listeners) == 2
    found = 0
    for lstnr in listeners.listeners:
        if lstnr.trsvcid == 5001:
            found += 1
            assert lstnr.secure
        elif lstnr.trsvcid == 5002:
            found += 1
            assert not lstnr.secure
        else:
            assert False
    assert found == 2


def test_add_key_to_host(caplog, gateway):
    gw = gateway
    caplog.clear()
    found = False
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    for h in hosts.hosts:
        if h.nqn == hostnqn7:
            found = True
            assert not h.use_dhchap
            break
    assert found
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn7,
         "--dhchap-key", hostdhchap6])
    assert f"Changing key for host {hostnqn7} on subsystem {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn7} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text
    caplog.clear()
    found = False
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    for h in hosts.hosts:
        if h.nqn == hostnqn7:
            found = True
            assert h.use_dhchap
            break
    assert found
    found = 0
    state = gw.gateway_state.omap.get_state()
    for key, val in state.items():
        if hostnqn7 in key:
            valstr = val.decode()
            assert '"dhchap_key": "' in valstr
            assert '"dhchap_key": ""' not in valstr
            assert '"key_encrypted": true' in valstr
            assert f"{hostdhchap6}" not in valstr
            found += 1
            break
    assert found == 1


def test_change_key_to_all_hosts(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", "*",
             "--dhchap-key", hostdhchap1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: Can't change key for host NQN '*', please use a real NQN" in caplog.text


def test_change_key_for_host(caplog, gateway):
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn7,
         "--dhchap-key", hostdhchap1])
    assert f"Changing key for host {hostnqn7} on subsystem {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn7} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text
    rc = 0
    try:
        cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn7])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: the following arguments are required: --dhchap-key" in caplog.text


def test_change_key_for_host_empty_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn7,
             "--dhchap-key", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key can't be empty" in caplog.text


def test_change_key_with_psk(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn12, "--psk", hostpsk1])
    assert f"Adding host {hostnqn12} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", hostdhchap7])
    assert f"Changing key for host {hostnqn12} on subsystem {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn12} has a DH-HMAC-CHAP key but subsystem {subsystem} has none, " \
           f"a unidirectional authentication will be used" in caplog.text


def test_change_key_host_not_exist(caplog, gateway):
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn13,
         "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn13} on " \
           f"subsystem {subsystem}: Can't find host on subsystem" in caplog.text


def test_change_key_host_on_discovery(caplog, gateway):
    caplog.clear()
    cli(["host", "change_key", "--subsystem", discovery_nqn,
         "--host-nqn", hostnqn12, "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn12} on " \
           f"subsystem {discovery_nqn}: Can't use a discovery NQN as subsystem's" in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", discovery_nqn,
         "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {discovery_nqn} on " \
           f"subsystem {subsystem}: Can't use a discovery NQN as host's" in caplog.text


def test_change_key_host_invalid_nqn(caplog, gateway):
    caplog.clear()
    badsubsystem = subsystem.replace("-", "_")
    cli(["host", "change_key", "--subsystem", badsubsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn12} on " \
           f"subsystem {badsubsystem}: Invalid subsystem NQN \"{badsubsystem}\", " \
           f"contains invalid characters" in caplog.text
    bad_hostnqn = hostnqn12.replace("-", "_")
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", bad_hostnqn,
         "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {bad_hostnqn} on subsystem {subsystem}: " \
           f"Invalid host NQN \"{bad_hostnqn}\", contains invalid characters" in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", "badnqn", "--host-nqn", hostnqn12,
         "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem badnqn: " \
           f"NQN \"badnqn\" is too short, minimal length is 11" in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem,
         "--host-nqn", "badnqn", "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host badnqn on subsystem {subsystem}: " \
           f"NQN \"badnqn\" is too short, minimal length is 11" in caplog.text


def test_change_key_host_bad_keys(caplog, gateway):
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap1])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap1}": key must start with "DHHC-1:' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap2])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap2}": should contain a ":" delimiter' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap3])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap3}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap4])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap4}": missing hash' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap5])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap5}": non numeric hash "' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap6])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap6}": base64 part is missing' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap7])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap7}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap8])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap8}": key must end with ":"' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap9])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap9}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap10])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap10}": invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap11])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap11}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap12])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap12}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", hostnqn12,
         "--dhchap-key", badhostdhchap13])
    assert f'Failure changing DH-HMAC-CHAP key for host {hostnqn12} on subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap13}": CRC-32 checksums mismatch' in caplog.text


def test_change_key_host_on_all_hosts(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "change_key", "--subsystem", subsystem, "--host-nqn", "*",
             "--dhchap-key", "junk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Can't change key for host NQN '*', please use a real NQN" in caplog.text
    assert rc == 2


def test_add_host_with_key_host_list(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn13, hostnqn14,
             "--dhchap-key", "junk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Can't have more than one host NQN when DH-HMAC-CHAP keys are used" in caplog.text
    assert rc == 2


def test_set_subsystem_key_with_empty_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key can't be empty" in caplog.text


def test_set_subsystem_key_with_bad_keys(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap1])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap1}": key must start with "DHHC-1:' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap2])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap2}": should contain a ":" delimiter' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap3])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap3}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap4])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap4}": missing hash' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap5])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap5}": non numeric hash "' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap6])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap6}": base64 part is missing' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap7])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap7}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap8])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap8}": key must end with ":"' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap9])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap9}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap10])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap10}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap11])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap11}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap12])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap12}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", badhostdhchap13])
    assert f'Failure creating subsystem {subsystem8}: Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap13}": CRC-32 checksums mismatch' in caplog.text


def test_set_subsystem_key_with_non_key_hosts(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", hostdhchap8])
    assert f"Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: " \
           f"Can't set a subsystem's DH-HMAC-CHAP key when it has hosts with " \
           f"no key, like host " in caplog.text


def test_change_subsystem_key(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem2, "--dhchap-key", hostdhchap8])
    assert f"Changing key for subsystem {subsystem2}: Successful" in caplog.text
    assert f"Received request to change inband authentication key for host " \
           f"{hostnqn11} on subsystem {subsystem2}" in caplog.text
    rc = 0
    try:
        cli(["subsystem", "change_key", "--subsystem", subsystem2])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: the following arguments are required: --dhchap-key" in caplog.text


def test_change_key_for_subsystem_empty_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: DH-HMAC-CHAP key can't be empty" in caplog.text


def test_change_key_for_subsystem_with_bad_keys(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap1])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap1}": key must start with "DHHC-1:' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap2])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap2}": should contain a ":" delimiter' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap3])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap3}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap4])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap4}": missing hash' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap5])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap5}": non numeric hash "' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap6])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap6}": base64 part is missing' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap7])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap7}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap8])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap8}": key must end with ":"' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap9])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap9}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap10])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap10}": invalid key length' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap11])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap11}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap12])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap12}": base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem, "--dhchap-key", badhostdhchap13])
    assert f'Failure changing DH-HMAC-CHAP key for subsystem {subsystem}: ' \
           f'Invalid DH-HMAC-CHAP key ' \
           f'"{badhostdhchap13}": CRC-32 checksums mismatch' in caplog.text


def test_delete_key(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem8, "--dhchap-key", hostdhchap6,
         "--no-group-append"])
    assert f"Adding subsystem {subsystem8}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "subsystem", "list", "--subsystem", subsystem8])
    assert '"status": 0' in caplog.text
    assert f'"nqn": "{subsystem8}"' in caplog.text
    assert '"has_dhchap_key": true' in caplog.text
    assert '"created_without_key": false' in caplog.text
    cli(["host", "add", "--subsystem", subsystem8, "--host-nqn", hostnqn1,
         "--dhchap-key", hostdhchap1])
    assert f"Adding host {hostnqn1} to {subsystem8}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem8])
    assert '"status": 0,' in caplog.text
    assert f'"subsystem_nqn": "{subsystem8}",' in caplog.text
    assert f'"nqn": "{hostnqn1}",' in caplog.text
    assert '"use_dhchap": true,' in caplog.text
    caplog.clear()
    cli(["host", "del_key", "--subsystem", subsystem8, "--host-nqn", hostnqn1])
    assert f"Failure deleting DH-HMAC-CHAP key for host {hostnqn1} on subsystem {subsystem8}: " \
           f"Host must have a DH-HMAC-CHAP key if the subsystem has one" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem8])
    assert '"status": 0,' in caplog.text
    assert f'"subsystem_nqn": "{subsystem8}",' in caplog.text
    assert f'"nqn": "{hostnqn1}",' in caplog.text
    assert '"use_dhchap": true,' in caplog.text
    caplog.clear()
    cli(["subsystem", "del_key", "--subsystem", subsystem8])
    assert f"Received request to delete inband authentication key for " \
           f"subsystem {subsystem8}" in caplog.text
    assert f"Deleting key for subsystem {subsystem8}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "subsystem", "list", "--subsystem", subsystem8])
    assert '"status": 0' in caplog.text
    assert f'"nqn": "{subsystem8}"' in caplog.text
    assert '"has_dhchap_key": false' in caplog.text
    caplog.clear()
    cli(["host", "del_key", "--subsystem", subsystem8, "--host-nqn", hostnqn1])
    assert f"Received request to delete inband authentication key for host {hostnqn1} " \
           f"on subsystem {subsystem8}" in caplog.text
    assert f"Deleting key for host {hostnqn1} on subsystem {subsystem8}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem8])
    assert '"status": 0,' in caplog.text
    assert f'"subsystem_nqn": "{subsystem8}",' in caplog.text
    assert f'"nqn": "{hostnqn1}",' in caplog.text
    assert '"use_dhchap": false,' in caplog.text


def test_encryption_disabled(caplog, gateway_encryption_disabled):
    gw_encryption_disabled = gateway_encryption_disabled
    cli(["subsystem", "add", "--subsystem", subsystem3, "--dhchap-key", hostdhchap9,
         "--no-group-append"])
    assert f"Received request to create subsystem {subsystem3}, enable_ha: True, " \
           f"max_namespaces: 0, no group append: True, context: <grpc._server" in caplog.text
    assert f"create_subsystem {subsystem3}: True" in caplog.text
    time.sleep(15)
    lookfor = f"Received request to create subsystem {subsystem3}, enable_ha: True, " \
              f"max_namespaces: {gw_encryption_disabled.MAX_NAMESPACES_PER_SUBSYSTEM_DEFAULT}" \
              f", no group append: True, context: None"
    assert lookfor in caplog.text
    pos = caplog.text.find(lookfor)
    assert pos >= 0
    pos += len(lookfor)
    partial_text = caplog.text[pos:]
    # see that subsystem was created successfully on the update too
    assert f"create_subsystem {subsystem3}: True" in partial_text
    caplog.clear()
    cli(["--server-port", "5502", "subsystem", "add", "--subsystem", subsystem4,
         "--dhchap-key", hostdhchap10])
    assert f"create_subsystem {subsystem4}: True" in caplog.text
    found = 0
    state = gw_encryption_disabled.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gw_encryption_disabled.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem in key:
            assert '"dhchap_key":' not in valstr
            assert '"key_encrypted":' not in valstr
            found += 1
        elif subsystem2 in key:
            assert '"dhchap_key": "' in valstr
            assert '"dhchap_key": ""' not in valstr
            assert '"key_encrypted": true' in valstr
            found += 1
        elif subsystem3 in key:
            assert '"dhchap_key": "' in valstr
            assert '"dhchap_key": ""' not in valstr
            assert '"key_encrypted": true' in valstr
            found += 1
        elif subsystem4 in key:
            found += 1
        elif subsystem4 in key:
            assert f'"dhchap_key": "{hostdhchap10}"' in valstr
            assert '"key_encrypted": false' in valstr or "key_encrypted" not in valstr
            found += 1
    assert found == 4
    caplog.clear()
    cli(["--server-port", "5502", "subsystem", "change_key", "--subsystem", subsystem4,
         "--dhchap-key", hostdhchap8])
    assert f"Changing key for subsystem {subsystem4}: Successful" in caplog.text
    found = 0
    state = gw_encryption_disabled.gateway_state.omap.get_state()
    for key, val in state.items():
        if subsystem4 in key:
            valstr = val.decode()
            assert f'"dhchap_key": "{hostdhchap8}"' in valstr
            assert '"key_encrypted": false' in valstr
            found += 1
            break
    assert found == 1
    found = 0
    state = gw_encryption_disabled.gateway_state.omap.get_state()
    for key, val in state.items():
        if hostnqn7 in key:
            valstr = val.decode()
            assert '"dhchap_key": "' in valstr
            assert '"dhchap_key": ""' not in valstr
            assert '"key_encrypted": true' in valstr
            found += 1
            break
    assert found == 1
    caplog.clear()
    cli(["--server-port", "5502", "host", "change_key", "--subsystem", subsystem,
         "--host-nqn", hostnqn7, "--dhchap-key", hostdhchap11])
    assert f"Received request to change inband authentication key for host {hostnqn7} " \
           f"on subsystem {subsystem}" in caplog.text
    assert f"Changing key for host {hostnqn7} on subsystem {subsystem}: Successful" in caplog.text
    found = 0
    state = gw_encryption_disabled.gateway_state.omap.get_state()
    for key, val in state.items():
        if hostnqn7 in key:
            valstr = val.decode()
            assert f'"dhchap_key": "{hostdhchap11}"' in valstr
            assert '"key_encrypted": false' in valstr
            assert f'"dhchap_key": "{hostdhchap11}"' in valstr
            assert '"key_encrypted": false' in valstr
            found += 1
            break
    assert found == 1


def test_no_encryption_key(caplog, gateway_no_encryption_key):
    _ = gateway_no_encryption_key
    found = False
    lookfor = "No valid encryption key file was set. Any attempt to encrypt or " \
              "decrypt keys would fail"
    for oneline in caplog.get_records("setup"):
        if oneline.message == lookfor:
            found = True
            break
    assert found
    time.sleep(15)
    found = False
    lookfor = "No encryption key or the wrong key was found but we need to decrypt a subsystem " \
              "DH-HMAC-CHAP key. Any attempt to add host access using a DH-HMAC-CHAP key " \
              "to subsystem"
    for oneline in caplog.get_records("setup"):
        if oneline.message.startswith(lookfor):
            found = True
            break
    assert found or lookfor in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "subsystem", "add", "--subsystem", subsystem5, "--dhchap-key",
         hostdhchap9, "--no-group-append"])
    assert f"Adding subsystem {subsystem5}: Successful" in caplog.text
    assert f"No encryption key or the wrong key was found but we need to encrypt subsystem " \
           f"{subsystem5} DH-HMAC-CHAP key. Any attempt to add host access using a " \
           f"DH-HMAC-CHAP key to the subsystem would fail" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5504", "subsystem", "list",
         "--subsystem", subsystem5])
    assert f'"nqn": "{subsystem5}"' in caplog.text
    assert '"created_without_key": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "subsystem", "change_key", "--subsystem", subsystem5,
         "--dhchap-key", hostdhchap9])
    assert f"Received request to change inband authentication key for " \
           f"subsystem {subsystem5}" in caplog.text
    assert f"Changing key for subsystem {subsystem5}: Successful" in caplog.text
    assert f"No encryption key or the wrong key was found but we need to encrypt subsystem " \
           f"{subsystem5} DH-HMAC-CHAP key. Any attempt to add host access using a " \
           f"DH-HMAC-CHAP key to the subsystem would fail" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5504", "subsystem", "list",
         "--subsystem", subsystem5])
    assert f'"nqn": "{subsystem5}"' in caplog.text
    assert '"created_without_key": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "host", "add", "--subsystem", subsystem5, "--host-nqn",
         hostnqn15, "--dhchap-key", hostdhchap1])
    assert f"Failure adding host {hostnqn15} to {subsystem5}: No encryption key or the wrong " \
           f"key was found but we need to encrypt host {hostnqn15} DH-HMAC-CHAP key" in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "host", "add", "--subsystem", subsystem5, "--host-nqn",
         hostnqn16, "--psk", hostpsk2])
    assert f"Failure adding host {hostnqn16} to {subsystem5}: No encryption key or the wrong " \
           f"key was found but we need to encrypt host {hostnqn16} PSK key" in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "host", "change_key", "--subsystem", subsystem, "--host-nqn",
         hostnqn7, "--dhchap-key", hostdhchap1])
    assert f"Failure changing DH-HMAC-CHAP key for host {hostnqn7} on subsystem {subsystem}: " \
           f"No encryption key or the wrong key was found but we need to encrypt host " \
           f"{hostnqn7} DH-HMAC-CHAP key" in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "subsystem", "add", "--subsystem", subsystem6,
         "--no-group-append"])
    assert f"Adding subsystem {subsystem6}: Successful" in caplog.text
    assert f"No encryption key or the wrong key was found but we need to encrypt subsystem " \
           f"{subsystem6} DH-HMAC-CHAP key. Any attempt to add host access using a " \
           f"DH-HMAC-CHAP key to the subsystem would fail" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5504", "subsystem", "list",
         "--subsystem", subsystem6])
    assert f'"nqn": "{subsystem6}"' in caplog.text
    assert '"created_without_key": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5504", "subsystem", "change_key", "--subsystem", subsystem6,
         "--dhchap-key", hostdhchap1])
    assert f"Changing key for subsystem {subsystem6}: Successful" in caplog.text
    assert f"No encryption key or the wrong key was found but we need to encrypt subsystem " \
           f"{subsystem6} DH-HMAC-CHAP key. Any attempt to add host access using a " \
           f"DH-HMAC-CHAP key to the subsystem would fail" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5504", "subsystem", "list",
         "--subsystem", subsystem6])
    assert f'"nqn": "{subsystem6}"' in caplog.text
    assert '"created_without_key": true' in caplog.text


def test_no_encryption_key_when_encryption_disabled(caplog, gateway_no_key_encryption_disabled):
    _ = gateway_no_key_encryption_disabled
    found = False
    lookfor = "No valid encryption key file was set. Any attempt to encrypt or " \
              "decrypt keys would fail"
    for oneline in caplog.get_records("setup"):
        if oneline.message == lookfor:
            found = True
            break
    assert found
    time.sleep(15)
    found = False
    lookfor = "No encryption key or the wrong key was found but we need to decrypt a subsystem " \
              "DH-HMAC-CHAP key. Any attempt to add host access using a DH-HMAC-CHAP key " \
              "to subsystem"
    for oneline in caplog.get_records("setup"):
        if oneline.message.startswith(lookfor):
            found = True
            break
    assert found or lookfor in caplog.text
    caplog.clear()
    cli(["--server-port", "5506", "subsystem", "add", "--subsystem", subsystem7, "--dhchap-key",
         hostdhchap9, "--no-group-append"])
    assert f"Adding subsystem {subsystem7}: Successful" in caplog.text
    assert f"No encryption key or the wrong key was found but we need to encrypt subsystem " \
           f"{subsystem7} DH-HMAC-CHAP key. Any attempt to add host access using a " \
           f"DH-HMAC-CHAP key to the subsystem would fail" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5506", "subsystem", "list",
         "--subsystem", subsystem7])
    assert f'"nqn": "{subsystem7}"' in caplog.text
    assert '"created_without_key": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5506", "subsystem", "change_key", "--subsystem", subsystem7,
         "--dhchap-key", hostdhchap9])
    assert f"Changing key for subsystem {subsystem7}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", "5506", "subsystem", "list",
         "--subsystem", subsystem7])
    assert f'"nqn": "{subsystem7}"' in caplog.text
    assert '"created_without_key": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5506", "host", "add", "--subsystem", subsystem7, "--host-nqn",
         hostnqn18, "--dhchap-key", hostdhchap1])
    assert f"Adding host {hostnqn18} to {subsystem7}: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", "5506", "host", "change_key", "--subsystem", subsystem7, "--host-nqn",
         hostnqn18, "--dhchap-key", hostdhchap2])
    assert f"Changing key for host {hostnqn18} on subsystem {subsystem7}: Successful" in caplog.text
