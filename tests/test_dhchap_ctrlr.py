import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import grpc
import copy
import os
import errno
import re

pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
hostnqn1 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7e0"
hostnqn2 = "nqn.2014-08.org.nvmexpress:uuid:22207d09-d8af-4ed2-84ec-a6d80b0cf7e1"
dhchapkey1 = "DHHC-1:00:RbgmpCGRaCwY1m4SexVxhf7tsJCXuAg77iH0Z18Ys25TgI/K:"
dhchapkey2 = "DHHC-1:00:bvr7w9iA2tLUs/XcwYHbwhlHjnCuNMt6S/1hBbvlBO94NKji:"
dhchapkey3 = "DHHC-1:01:vYcm5QemILkz5zSIeZidb4nPGcxongu2l/OaWMRgM9iIk/2A:"
dhchapkey4 = "DHHC-1:02:Hi9ha9idssh3FgURRbfMUvOY7NlHiApDCmHhmIRCdhdlNQwebHEt/7DHoag3SmCHFvei/Q==:"
dhchapkey5 = "DHHC-1:00:JqJKQvifiLh5Udedny7GoXusbrouWzzhpwonrlRAh7AzF6Vj:"
dhchapkey6 = "DHHC-1:01:s+U9TVgALTumIqiD4zodRVqMNGc9cbdEMWzV3ShbyeMeLhSb:"
dhchapkey7 = "DHHC-1:02:7CRgseaehU2SWsm7t7/ZZttCs1GO2XUkxvuLohyhhG8AkuGTnmf91nAvPOiN0W1IM+J9Jg==:"
dhchapkey8 = "DHHC-1:00:vEPBLyYQA+njnSdiXfI63FApPxCihcgYy4b1IlbHUjaLFqEs:"

config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two gateways"""

    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway"]["group"] = group_name
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["enable_key_encryption"] = "False"
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    if os.cpu_count() >= 4:
        configA.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-1)"
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
        # Start gateway
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", '
            f'"pool": "{pool}", "group": "{group_name}"' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", '
            f'"pool": "{pool}", "group": "{group_name}"' + "}"
        )
        gatewayA.serve()
        gatewayB.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{portA}")
        stubA = pb2_grpc.GatewayStub(channel)
        channel = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channel)

        yield gatewayA.gateway_rpc, gatewayB.gateway_rpc, stubA, stubB

        # Stop gateways
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


@pytest.fixture(scope="module", autouse=True)
def create_subsystem(two_gateways):
    gatewayA, _, _, _ = two_gateways
    rc = cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert rc == 0
    rc = cli(["subsystem", "add", "--subsystem", subsystem2, "--dhchap-key", dhchapkey1,
              "--no-group-append"])
    assert rc == 0
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert '"dhchap_key":' not in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
        elif subsystem2 in key:
            assert f'"dhchap_key": "{dhchapkey1}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr


def test_mix_host_ctrlr_key_with_subsystem_key(caplog, two_gateways):
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem2, "--host-nqn", hostnqn1,
         "--dhchap-key", dhchapkey2, "--dhchap-controller-key", dhchapkey3])
    assert f"Failure adding host {hostnqn1} to {subsystem2}: Host DH-HMAC-CHAP controller keys " \
           f"and subsystem DH-HMAC-CHAP keys are mutually exclusive" in caplog.text


def test_add_host_with_only_ctrlr_key_using_cli(caplog, two_gateways):
    rc = 0
    caplog.clear()
    try:
        cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1,
             "--dhchap-controller-key", dhchapkey3])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "Controller's DH-HMAC-CHAP key is not allowed without a " \
           "host DH-HMAC-CHAP key" in caplog.text
    assert rc == 2


def test_add_host_with_only_ctrlr_key_using_grpc(caplog, two_gateways):
    _, _, stubA, _ = two_gateways
    add_host_req = pb2.add_host_req(subsystem_nqn=subsystem1, host_nqn=hostnqn1,
                                    dhchap_ctrlr_key=dhchapkey3)
    caplog.clear()
    ret = stubA.add_host(add_host_req)
    assert ret.status == errno.ENOKEY
    assert f"Failure adding host {hostnqn1} to {subsystem1}: Host must have a DH-HMAC-CHAP " \
           f"key if the controller or subsystem has one" in caplog.text


def test_set_subsystem_key_with_nokey_host(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Adding host {hostnqn1} to {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem1, "--dhchap-key", dhchapkey3])
    assert f"Failure changing DH-HMAC-CHAP key for subsystem {subsystem1}: " \
           f"Can't set a subsystem's DH-HMAC-CHAP key when it has hosts with no key, like host " \
           f"{hostnqn1}" in caplog.text
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Removing host {hostnqn1} access from {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem1, "--dhchap-key", dhchapkey3])
    assert f"Changing DH-HMAC-CHAP key for subsystem {subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert f'"dhchap_key": "{dhchapkey3}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
        elif subsystem2 in key:
            assert f'"dhchap_key": "{dhchapkey1}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Failure adding host {hostnqn1} to {subsystem1}: Host must have a DH-HMAC-CHAP " \
           f"key if the controller or subsystem has one" in caplog.text
    caplog.clear()
    cli(["subsystem", "del_key", "--subsystem", subsystem1])
    assert f"Deleting DH-HMAC-CHAP key for subsystem {subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert '"dhchap_key":' not in valstr or '"dhchap_key": ""' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            break
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Adding host {hostnqn1} to {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["host", "del", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Removing host {hostnqn1} access from {subsystem1}: Successful" in caplog.text


def find_in_caplog(lookfor, captext) -> bool:
    regex = re.compile(lookfor, re.MULTILINE)
    if re.search(regex, captext) is not None:
        return True
    return False


def test_host_with_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem1, "--host-nqn", hostnqn1,
         "--dhchap-key", dhchapkey2, "--dhchap-controller-key", dhchapkey3])
    assert f"Adding host {hostnqn1} to {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem1])
    assert f'"nqn": "{hostnqn1}"' in caplog.text
    assert '"use_psk": false' in caplog.text
    assert '"use_psk": true' not in caplog.text
    assert '"use_dhchap": false' not in caplog.text
    assert '"use_dhchap": true' in caplog.text
    assert '"dhchap_controller_origin": "host_specific"' in caplog.text
    assert '"dhchap_controller_origin": "no_key"' not in caplog.text
    assert '"dhchap_controller_origin": "subsystem_implicit"' not in caplog.text
    caplog.clear()
    cli(["--format", "plain", "host", "list", "--subsystem", subsystem1])
    assert find_in_caplog(rf"^\s*{re.escape(hostnqn1)}\s*No\s*Yes\s*Host Specific\s*$", caplog.text)
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert '"dhchap_key":' not in valstr or '"dhchap_key": ""' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            break
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey2}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert f'"dhchap_ctrlr_key": "{dhchapkey3}"' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break


def test_del_host_key_and_keeping_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "del_key", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Failure deleting DH-HMAC-CHAP key for host {hostnqn1} on subsystem {subsystem1}: " \
           f"Host must have a DH-HMAC-CHAP key if the controller has one" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey2}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert f'"dhchap_ctrlr_key": "{dhchapkey3}"' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break


def test_change_host_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "change_key", "--subsystem", subsystem1, "--host-nqn", hostnqn1,
         "--dhchap-key", dhchapkey4])
    assert f"Changing DH-HMAC-CHAP key for host {hostnqn1} on subsystem {subsystem1}: " \
           f"Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey4}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert f'"dhchap_ctrlr_key": "{dhchapkey3}"' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break


def test_change_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "change_controller_key", "--subsystem", subsystem1, "--host-nqn", hostnqn1,
         "--dhchap-controller-key", dhchapkey5])
    assert f"Changing DH-HMAC-CHAP key for controller of host {hostnqn1} on subsystem " \
           f"{subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey4}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert f'"dhchap_ctrlr_key": "{dhchapkey5}"' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break


def test_change_both_keys(caplog, two_gateways):
    gatewayA, _, stubA, _ = two_gateways
    change_host_key_req = pb2.change_host_key_req(subsystem_nqn=subsystem1, host_nqn=hostnqn1,
                                                  dhchap_key=dhchapkey5,
                                                  dhchap_ctrlr_key=dhchapkey6)
    caplog.clear()
    ret = stubA.change_host_key(change_host_key_req)
    assert ret.status == 0
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey5}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert f'"dhchap_ctrlr_key": "{dhchapkey6}"' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break


def test_set_subsystem_key_with_host_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem1, "--dhchap-key", dhchapkey7])
    assert f"Failure changing DH-HMAC-CHAP key for subsystem {subsystem1}: DH-HMAC-CHAP key " \
           f"is not allowed for subsystems which have a host with a DH-HMAC-CHAP controller key. " \
           f"You need to remove host {hostnqn1} DH-HMAC-CHAP controller key in order to set a " \
           f"key for the subsystem" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert '"dhchap_key":' not in valstr or '"dhchap_key": ""' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            break


def test_del_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "del_controller_key", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Deleting DH-HMAC-CHAP key for controller of host {hostnqn1} on subsystem " \
           f"{subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn1 in key:
            assert f'"dhchap_key": "{dhchapkey5}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert '"dhchap_ctrlr_key":' not in valstr or '"dhchap_ctrlr_key": ""' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break
    caplog.clear()
    cli(["host", "del_controller_key", "--subsystem", subsystem1, "--host-nqn", hostnqn1])
    assert f"Deleting DH-HMAC-CHAP key for controller of host {hostnqn1} on subsystem " \
           f"{subsystem1}: Successful" in caplog.text


def test_set_subsystem_key_with_deleted_host_controller_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["subsystem", "change_key", "--subsystem", subsystem1, "--dhchap-key", dhchapkey7])
    assert f"Changing DH-HMAC-CHAP key for subsystem {subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert f'"dhchap_key": "{dhchapkey7}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            break
    caplog.clear()
    cli(["subsystem", "del_key", "--subsystem", subsystem1])
    assert f"Deleting DH-HMAC-CHAP key for subsystem {subsystem1}: Successful" in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.SUBSYSTEM_PREFIX):
            continue
        valstr = val.decode()
        if subsystem1 in key:
            assert '"dhchap_key":' not in valstr or '"dhchap_key": ""' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            break


def test_del_controller_key_instead_of_subsystem_key(caplog, two_gateways):
    gatewayA, _, _, _ = two_gateways
    caplog.clear()
    cli(["host", "add", "--subsystem", subsystem2, "--host-nqn", hostnqn2,
         "--dhchap-key", dhchapkey8])
    assert f"Adding host {hostnqn2} to {subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem2])
    assert f'"nqn": "{hostnqn2}"' in caplog.text
    assert '"use_psk": false' in caplog.text
    assert '"use_psk": true' not in caplog.text
    assert '"use_dhchap": false' not in caplog.text
    assert '"use_dhchap": true' in caplog.text
    assert '"dhchap_controller_origin": "host_specific"' not in caplog.text
    assert '"dhchap_controller_origin": "no_key"' not in caplog.text
    assert '"dhchap_controller_origin": "subsystem_implicit"' in caplog.text
    state = gatewayA.gateway_state.omap.get_state()
    for key, val in state.items():
        if not key.startswith(gatewayA.gateway_state.local.HOST_PREFIX):
            continue
        valstr = val.decode()
        if hostnqn2 in key:
            assert f'"dhchap_key": "{dhchapkey8}"' in valstr
            assert '"key_encrypted": false' in valstr or '"key_encrypted":' not in valstr
            assert '"dhchap_ctrlr_key":' not in valstr or '"dhchap_ctrlr_key": ""' in valstr
            assert '"ctrlr_key_encrypted": false' in valstr or '"ctrlr_key_encrypted"' \
                   ':' not in valstr
            break
    caplog.clear()
    cli(["host", "del_controller_key", "--subsystem", subsystem2, "--host-nqn", hostnqn2])
    assert f"Failure deleting DH-HMAC-CHAP controller key for host {hostnqn2} on " \
           f"subsystem {subsystem2}: Can't delete host DH-HMAC-CHAP controller key " \
           f"as it was defined in the subsystem" in caplog.text


def test_del_subsystem_key(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "del_key", "--subsystem", subsystem2])
    assert f"Deleting DH-HMAC-CHAP key for subsystem {subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem2])
    assert f'"nqn": "{hostnqn2}"' in caplog.text
    assert '"use_psk": false' in caplog.text
    assert '"use_psk": true' not in caplog.text
    assert '"use_dhchap": false' not in caplog.text
    assert '"use_dhchap": true' in caplog.text
    assert '"dhchap_controller_origin": "host_specific"' not in caplog.text
    assert '"dhchap_controller_origin": "no_key"' in caplog.text
    assert '"dhchap_controller_origin": "subsystem_implicit"' not in caplog.text


def test_add_controller_key_after_subsystem_del_key(caplog, two_gateways):
    caplog.clear()
    cli(["host", "change_controller_key", "--subsystem", subsystem2, "--host-nqn", hostnqn2,
         "--dhchap-controller-key", dhchapkey5])
    assert f"Changing DH-HMAC-CHAP key for controller of host {hostnqn2} on subsystem " \
           f"{subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "host", "list", "--subsystem", subsystem2])
    assert f'"nqn": "{hostnqn2}"' in caplog.text
    assert '"use_psk": false' in caplog.text
    assert '"use_psk": true' not in caplog.text
    assert '"use_dhchap": false' not in caplog.text
    assert '"use_dhchap": true' in caplog.text
    assert '"dhchap_controller_origin": "host_specific"' in caplog.text
    assert '"dhchap_controller_origin": "no_key"' not in caplog.text
    assert '"dhchap_controller_origin": "subsystem_implicit"' not in caplog.text
