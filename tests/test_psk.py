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
hostnqn1 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7eb"
hostnqn2 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ec"
hostnqn3 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7ee"
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

hostpsk1 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
hostpsk2 = \
    "NVMeTLSkey-1:02:FTFds4vH4utVcfrOforxbrWIgv+Qq4GQHgMdWwzDdDxE1bAqK2mOoyXxmbJxGeueEVVa/Q==:"
hostpsk3 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
hostpsk4 = \
    "NVMeTLSkey-1:02:AP/STOH7Z/V9wGU2Pmh01rhBpaQfxY+WIlGxCUd9UWVagpDMDaSoujOP/nFfgzgOnTgu1g==:"

badhostpsk0 = "junk"
badhostpsk1 = "xVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk2 = "NVMeTLSkey-1:01YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk3 = "NVMeTLSkey-1:07:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk4 = "NVMeTLSkey-1::YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk5 = "NVMeTLSkey-1:tt:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk6 = "NVMeTLSkey-1:01::"
badhostpsk7 = "NVMeTLSkey-1:01:xxxx:"
badhostpsk8 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT"
badhostpsk9 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuTYzrP:"
badhostpsk10 = "NVMeTLSkey-1:02:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
badhostpsk11 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBu*:"
badhostpsk12 = "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT::"
badhostpsk13 = "NVMeTLSkey-1:01:YztPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"

hostdhchap1 = "DHHC-1:00:MWPqcx1Ug1debg8fPIGpkqbQhLcYUt39k7UWirkblaKEH1kE:"

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
        config.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-1)"
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
def gateway_no_encryption_key(config):
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
    config.config["gateway"]["encryption_key"] = "/etc/ceph/NOencryption.key"
    config.config["gateway"]["abort_on_update_error"] = "False"
    if os.cpu_count() >= 4:
        config.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"
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


def test_setup(caplog, gateway):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem])
    assert f"Adding subsystem {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image, "--rbd-create-image", "--size", "16MB"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text


def test_create_secure_with_any_host(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*"])
    assert f"Allowing open host access to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5001", "--secure"])
    assert "Secure channel is only allowed for subsystems in which " \
           "\"allow any host\" is off" in caplog.text
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem, "--host-nqn", "*"])
    assert f"Disabling open host access to {subsystem}: Successful" in caplog.text


def test_create_secure(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5001", "--secure"])
    assert f"Adding {subsystem} listener at {addr}:5001: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn1, "--psk", hostpsk1])
    assert f"Adding host {hostnqn1} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn2, "--psk", hostpsk2])
    assert f"Adding host {hostnqn2} to {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn4, "--psk", hostpsk3])
    assert f"Adding host {hostnqn4} to {subsystem}: Successful" in caplog.text
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn13, "--psk", hostpsk4])
    assert f"Adding host {hostnqn13} to {subsystem}: Successful" in caplog.text


def test_create_not_secure(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5002"])
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
        cli(["host", "add", "--subsystem", subsystem,
             "--host-nqn", hostnqn8, hostnqn9, hostnqn10, "--psk", hostpsk1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: Can't have more than one host NQN when PSK keys are used" in caplog.text


def test_create_secure_bad_key(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk0])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': key must start with "NVMeTLSkey-1:' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk1])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': key must start with "NVMeTLSkey-1:' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk2])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': should contain a ":" delimiter' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk3])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk4])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': missing hash' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk5])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': non numeric hash "' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk6])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': base64 part is missing' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk7])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk8])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': key must end with ":"' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk9])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk10])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': invalid key length' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk11])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk12])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': base64 part is invalid' in caplog.text
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn3, "--psk", badhostpsk13])
    assert f'Failure adding host {hostnqn3} to {subsystem}: Invalid PSK key' \
           f': CRC-32 checksums mismatch' in caplog.text


def test_create_secure_no_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn5, "--psk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: argument --psk/-p: expected one argument" in caplog.text


def test_create_secure_empty_key(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn5, "--psk", ""])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: PSK key can't be empty" in caplog.text


def test_list_psk_hosts(caplog, gateway):
    caplog.clear()
    hosts = cli_test(["host", "list", "--subsystem", subsystem])
    found = 0
    assert len(hosts.hosts) == 6
    for h in hosts.hosts:
        if h.nqn == hostnqn1:
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
        elif h.nqn == hostnqn13:
            found += 1
            assert h.use_psk
        else:
            assert False
    assert found == 6


def test_allow_any_host_with_psk(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "*", "--psk", hostpsk1])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert rc == 2
    assert "error: PSK key is only allowed for specific hosts" in caplog.text


def test_psk_with_dhchap(caplog, gateway):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem, "--host-nqn", hostnqn14,
         "--psk", hostpsk1, "--dhchap-key", hostdhchap1])
    assert f"Adding host {hostnqn14} to {subsystem}: Successful" in caplog.text
    assert f"Host {hostnqn14} has a DH-HMAC-CHAP key but no controller key, " \
           f"and subsystem {subsystem} has no key, a unidirectional " \
           f"authentication will be used" in caplog.text


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


def test_add_host_with_key_host_list(caplog, gateway):
    caplog.clear()
    rc = 0
    try:
        cli(["host", "add", "--subsystem", subsystem,
             "--host-nqn", hostnqn11, hostnqn12, "--psk", "junk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Can't have more than one host NQN when PSK keys are used" in caplog.text
    assert rc == 2


def test_add_host_with_no_encryption_key(caplog, gateway_no_encryption_key):
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
    lookfor = f"No encryption key or the wrong key was found but we need to decrypt host " \
              f"{hostnqn13} PSK key"
    for oneline in caplog.get_records("setup"):
        if oneline.message.startswith(lookfor):
            found = True
            break
    assert found or lookfor in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "host", "list", "--subsystem", subsystem])
    assert f"{hostnqn13}" not in caplog.text
    assert f"{hostnqn6}" in caplog.text
    assert f"{hostnqn7}" in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "host", "add", "--subsystem", subsystem,
         "--host-nqn", hostnqn15, "--psk", hostpsk1])
    assert f"Failure adding host {hostnqn15} to {subsystem}: No encryption key or the wrong " \
           f"key was found but we need to encrypt host {hostnqn15} PSK key" in caplog.text


def test_listener_security_clash(caplog, gateway):
    gw = gateway
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem2])
    assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem3])
    assert f"Adding subsystem {subsystem3}: Successful" in caplog.text
    cli(["listener", "add", "--subsystem", subsystem2,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5100", "--secure"])
    assert f"Adding {subsystem2} listener at {addr}:5100: Successful" in caplog.text
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem3,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5100"])
    assert f'Failure adding {subsystem3} listener at {addr}:5100: The listener clashes with ' \
           f'the existing secure listener on {subsystem2}, address {addr}:5100, either ' \
           f'remove that listener or use the "force" parameter' in caplog.text
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem3,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5100", "--force"])
    assert f"Adding {subsystem3} listener at {addr}:5100: Successful" in caplog.text
    assert f'The listener clashes with the existing secure listener on {subsystem2}, address ' \
           f'{addr}:5100, will continue as the "force" parameter was used' in caplog.text
    caplog.clear()
    cli(["listener", "del", "--subsystem", subsystem3,
         "--host-name", gw.gateway_name, "-a", addr, "-s", "5100"])
    assert f"Deleting listener {addr}:5100 from {subsystem3} " \
           f"for host {gw.gateway_name}: Successful" in caplog.text
    caplog.clear()
    cli(["listener", "add", "--subsystem", subsystem3,
         "--host-name", gw.gateway_name, "-a", "0.0.0.0", "-s", "5100"])
    assert f'Failure adding {subsystem3} listener at 0.0.0.0:5100: The listener clashes with ' \
           f'the existing secure listener on {subsystem2}, address {addr}:5100, either ' \
           f'remove that listener or use the "force" parameter' in caplog.text
