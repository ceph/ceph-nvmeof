import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import os
import time
import subprocess
import sys
from kmip.pie import client
from kmip.pie import objects
from kmip import enums

image = "enc_test_image"
pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
group_name = "GROUPNAME"
kmip_dir_prefix = "/tmp/kmip/"
kmip_dir1 = ""
kmip_dir2 = ""
kmip_addr = "127.0.0.1"
kmip_port = 5700
kmip_port2 = 5750
kmip_port3 = 5800
kmip_key_ids = {}
kmip_server_name1 = "blabla"
kmip_server_name2 = "stam"


def start_kmip_server_endpoint(base_dir, addr, port, create_cert):
    """Sets up a KMIP server endpoint"""
    if create_cert:
        setup_path = os.path.join(".", "tests", "kmip", "setup_kmip_test.sh")
        subprocess.run([setup_path, base_dir], check=True,
                       capture_output=True, text=True)
    srvr_path = os.path.join(".", "tests", "kmip", "dummy_kmip_server.py")
    subprocess.Popen(
        [
            sys.executable,
            srvr_path,
            '--address', addr,
            '--port', str(port),
            '--base-dir', base_dir
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    time.sleep(15)


def add_key_to_kmip_server_endpoint(base_dir, addr, port, val):
    """Add a key to a KMIP server endpoint and returns its id"""

    key = f"{base_dir}_{addr}_{port}_{val}"
    # If the key was already added return the existing id
    sec_id = kmip_key_ids.get(key)
    if sec_id:
        return sec_id

    kmip_client = client.ProxyKmipClient(hostname=addr, port=port,
                                         cert=os.path.join(base_dir, "client_cert.pem"),
                                         key=os.path.join(base_dir, "client_key.pem"),
                                         ca=os.path.join(base_dir, "ca_cert.pem"))

    kmip_client.open()
    secret = objects.SecretData(val.encode(),
                                enums.SecretDataType.PASSWORD,
                                masks=[enums.CryptographicUsageMask.DERIVE_KEY])
    sec_id = kmip_client.register(secret)
    kmip_client.activate(sec_id)
    kmip_client.close()
    kmip_key_ids[key] = sec_id
    return sec_id


def clear_kmip_server_endpoint_keys_cache(base_dir, addr, port):
    k_list = []
    prefix = f"{base_dir}_{addr}_{port}_"
    for k in list(kmip_key_ids.keys()):
        if k.startswith(prefix):
            k_list.append(k)
    for k in k_list:
        kmip_key_ids.pop(k, None)


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up two Gateways"""
    global kmip_dir1, kmip_dir2
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    config.config["kmip"]["cert_dir"] = kmip_dir_prefix + "{server_name}/certs"
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
    configB.config["gateway"]["io_stats_enabled"] = "False"
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = configB.getint("discovery", "port") + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    if os.cpu_count() >= 4:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"
    else:
        configB.config["spdk"]["tgt_cmd_extra_args"] = "--disable-cpumask-locks"

    kmip_dir1 = os.path.join(kmip_dir_prefix, kmip_server_name1)
    kmip_dir2 = os.path.join(kmip_dir_prefix, kmip_server_name2)
    start_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, True)
    start_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port2, False)
    start_kmip_server_endpoint(kmip_dir2, kmip_addr, kmip_port3, True)
    kmip_dir1 = os.path.join(kmip_dir1, "certs")
    kmip_dir2 = os.path.join(kmip_dir2, "certs")
    ceph_utils = CephUtils(config)
    gatewayA = GatewayServer(configA)
    gatewayB = GatewayServer(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gatewayA.serve()
    gatewayB.serve()

    channelA = grpc.insecure_channel(f"{addr}:{portA}")
    stubA = pb2_grpc.GatewayStub(channelA)
    channelB = grpc.insecure_channel(f"{addr}:{portB}")
    stubB = pb2_grpc.GatewayStub(channelB)

    return gatewayA, stubA, gatewayB, stubB


def test_create_resources(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_use_encryption_without_kmip_server_endpoint(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
         "--rbd-create-image", "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Failure adding namespace to {subsystem1}: No KMIP server endpoints were added " \
           f"to the subsystem but encryption was requested" in caplog.text


def test_add_kmip_server_endpoint_negative_port(caplog, two_gateways):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", kmip_server_name1,
             "--address", kmip_addr, "--port", "-20"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Endpoint's port must be positive" in caplog.text


def test_add_kmip_server_endpoint_zero_port(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", kmip_server_name1,
             "--address", kmip_addr, "--port", "0"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Endpoint's port must be positive" in caplog.text

    endpoint = pb2.kmip_server_endpoint(address="junk", port=0)
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[endpoint])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f'Failure adding an endpoint, with address junk:0, to ' \
           f'KMIP server "{kmip_server_name1}" on subsystem {subsystem1}: ' \
           f'Server endpoint\'s port must be between 1 and 65535' in caplog.text


def test_add_kmip_server_endpoint_non_numeric_port(caplog, two_gateways):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--address", kmip_addr, "--server-name", kmip_server_name1,
             "--port", "ABC"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: argument --port/-p: invalid int value: 'ABC'" in caplog.text


def test_add_kmip_server_endpoint_no_endpoints(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f'Failure adding endpoints to KMIP server "{kmip_server_name1}" ' \
           f'on subsystem {subsystem1}: ' \
           f'No endpoints were specified' in caplog.text


def test_del_kmip_server_endpoint_negative_port(caplog, two_gateways):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
             "--address", kmip_addr, "--server-name", kmip_server_name1,
             "--port", "-20"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Endpoint's port must be positive" in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
             "--address", kmip_addr, "--server-name", kmip_server_name1,
             "--port", "0"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Endpoint's port must be positive" in caplog.text


def test_del_kmip_server_endpoint_zero_port(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", kmip_server_name1,
             "--address", kmip_addr, "--port", "0"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Endpoint's port must be positive" in caplog.text

    endpoint = pb2.kmip_server_endpoint(address="junk", port=0)
    req = pb2.del_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[endpoint])
    caplog.clear()
    stub.del_kmip_server_endpoints(req)
    assert f'Failure deleting endpoint, with address junk:0, from ' \
           f'KMIP server "{kmip_server_name1}" on subsystem {subsystem1}: ' \
           f'Server endpoint\'s port must be between 1 and 65535' in caplog.text


def test_del_kmip_server_endpoint_non_numeric_port(caplog, two_gateways):
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
             "--address", kmip_addr,
             "--server-name", kmip_server_name1,
             "--port", "ABC"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: argument --port/-p: invalid int value: 'ABC'" in caplog.text


def test_del_kmip_server_endpoint_no_endpoints(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    req = pb2.del_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[])
    caplog.clear()
    stub.del_kmip_server_endpoints(req)
    assert f'Failure deleting endpoints from KMIP server "{kmip_server_name1}" ' \
           f'on subsystem {subsystem1}: ' \
           f'No endpoints were specified' in caplog.text


def test_add_kmip_server_endpoint(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port}, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text


def test_list_kmip_server_endpoints(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "list_kmip_server_endpoints"])
    assert kmip_addr in caplog.text
    assert str(kmip_port) in caplog.text
    assert kmip_server_name1 in caplog.text


def test_del_kmip_server_endpoint(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Deleting endpoint, with address {kmip_addr}:{kmip_port}, from KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    assert f"Last server endpoint for subsystem {subsystem1} was deleted" in caplog.text
    clear_kmip_server_endpoint_keys_cache(kmip_dir1, kmip_addr, kmip_port)


def test_list_kmip_server_endpoints_after_delete(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "list_kmip_server_endpoints"])
    assert kmip_addr not in caplog.text
    assert str(kmip_port) not in caplog.text
    assert kmip_server_name1 not in caplog.text
    assert "No KMIP server endpoints on" in caplog.text


def test_use_encryption_after_kmip_server_endpoint_deletion(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
         "--rbd-create-image", "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Failure adding namespace to {subsystem1}: No KMIP server endpoints were added " \
           f"to the subsystem but encryption was requested" in caplog.text


def test_re_add_kmip_server_endpoint_after_deletion(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1, "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port}, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text


def test_re_add_kmip_server_endpoint(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"The endpoint, with address {kmip_addr}:{kmip_port}, was not added to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1} as it's already there" in caplog.text
    assert f"Failure adding an endpoint, with address " \
           f"{kmip_addr}:{kmip_port}, to KMIP server \"{kmip_server_name1}\" on subsystem " \
           f"{subsystem1}: Server endpoint already exists" in caplog.text


def test_add_kmip_server_endpoint_default_port(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", kmip_server_name1,
         "--address", "junk"])
    assert f"Adding an endpoint, with address junk, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    assert f"KMIP server {kmip_server_name1} endpoint's port wasn't specified, will use " \
           f"default port 5696" in caplog.text
    caplog.clear()
    cli(["subsystem", "list_kmip_server_endpoints"])
    assert kmip_addr in caplog.text
    assert str(kmip_port) in caplog.text
    assert "junk" in caplog.text
    assert "5696" in caplog.text
    assert kmip_server_name1 in caplog.text

    endpoint = pb2.kmip_server_endpoint(address="junk")
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[endpoint])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f"KMIP server {kmip_server_name1} endpoint's port wasn't specified, will use " \
           f"default port 5696" in caplog.text
    assert f"Failure adding an endpoint, with address " \
           f"junk:5696, to KMIP server \"{kmip_server_name1}\" on subsystem " \
           f"{subsystem1}: Server endpoint already exists" in caplog.text


def test_del_non_existing_kmip_server_endpoint(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", "junk",
         "--server-name", kmip_server_name1,
         "--port", "1234"])
    assert f"Failure deleting endpoint, with address junk:1234, from " \
           f"KMIP server \"{kmip_server_name1}\" on subsystem {subsystem1}: server endpoint " \
           f"not found" in caplog.text
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1, "--address", "junk",
         "--server-name", kmip_server_name1,
         "--port", "5696"])
    assert f"Deleting endpoint, with address junk:5696, from KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "list_kmip_server_endpoints"])
    assert kmip_addr in caplog.text
    assert str(kmip_port) in caplog.text
    assert "junk" not in caplog.text
    assert "5696" not in caplog.text


def test_del_kmip_server_endpoint_default_port(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", kmip_server_name1,
         "--address", "notthere"])
    assert f"KMIP server {kmip_server_name1} endpoint's port wasn't specified, will use " \
           f"default port 5696" in caplog.text

    endpoint = pb2.kmip_server_endpoint(address="notthere")
    req = pb2.del_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[endpoint])
    caplog.clear()
    stub.del_kmip_server_endpoints(req)
    assert f"KMIP server {kmip_server_name1} endpoint's port wasn't specified, will use " \
           f"default port 5696" in caplog.text


def test_add_kmip_server_endpoint_invalid_host_name(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", kmip_server_name1,
             "--address", "junk#junk"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "Invalid endpoint address junk#junk" in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", kmip_server_name1,
             "--address", "junk-"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "Invalid endpoint address junk-" in caplog.text

    endpoint = pb2.kmip_server_endpoint(address="junk-", port=6789)
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=kmip_server_name1,
                                            endpoints=[endpoint])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f'Failure adding an endpoint, with address junk-:6789, to ' \
           f'KMIP server "{kmip_server_name1}" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server endpoint address "junk-"' in caplog.text


def test_add_kmip_server_endpoint_invalid_subsystem(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem2,
         "--server-name", kmip_server_name2,
         "--address", "junk", "-p", "1234"])
    assert f"Failure adding an endpoint, with address junk:1234, to " \
           f"KMIP server \"{kmip_server_name2}\" on subsystem {subsystem2}: " \
           f"Can't find subsystem {subsystem2}" in caplog.text


def test_add_kmip_server_endpoint_no_name(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--address", "junk", "-p", "1234"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: the following arguments are required: --server-name/-s" in caplog.text
    assert rc == 2

    endpoint = pb2.kmip_server_endpoint(address="junk", port=2345)
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1, endpoints=[endpoint])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f'Failure adding an endpoint, with address junk:2345, to ' \
           f'KMIP server "" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "", name can\'t be empty' in caplog.text


def test_add_kmip_server_endpoint_empty_name(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
             "--server-name", " ", "--address", "junk", "-p", "1234"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: Server's name can't be empty" in caplog.text
    assert rc == 2

    endpoint = pb2.kmip_server_endpoint(address="junk", port=3456)
    req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsystem1,
                                            server_name=" ", endpoints=[endpoint])
    caplog.clear()
    stub.add_kmip_server_endpoints(req)
    assert f'Failure adding an endpoint, with address junk:3456, to ' \
           f'KMIP server " " on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name " ", name can\'t be empty' in caplog.text


def test_add_kmip_server_endpoint_invalid_server_name(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "..", "--address", "junk", "-p", "1234"])
    assert f'Failure adding an endpoint, with address junk:1234, to ' \
           f'KMIP server ".." on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name ".."' in caplog.text
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", ".", "--address", "junk", "-p", "1234"])
    assert f'Failure adding an endpoint, with address junk:1234, to ' \
           f'KMIP server "." on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "."' in caplog.text
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "not_valid", "--address", "junk", "-p", "1234"])
    assert f'Failure adding an endpoint, with address junk:1234, to ' \
           f'KMIP server "not_valid" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "not_valid", contains invalid characters' in caplog.text
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "not/valid", "--address", "junk", "-p", "1234"])
    assert f'Failure adding an endpoint, with address junk:1234, to ' \
           f'KMIP server "not/valid" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "not/valid", contains invalid characters' in caplog.text


def test_del_kmip_server_endpoint_invalid_server_name(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "..", "--address", "junk", "-p", "1234"])
    assert f'Failure deleting endpoint, with address junk:1234, from ' \
           f'KMIP server ".." on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name ".."' in caplog.text
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", ".", "--address", "junk", "-p", "1234"])
    assert f'Failure deleting endpoint, with address junk:1234, from ' \
           f'KMIP server "." on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "."' in caplog.text
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "not_valid", "--address", "junk", "-p", "1234"])
    assert f'Failure deleting endpoint, with address junk:1234, from ' \
           f'KMIP server "not_valid" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "not_valid", contains invalid characters' in caplog.text
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", "not/valid", "--address", "junk", "-p", "1234"])
    assert f'Failure deleting endpoint, with address junk:1234, from ' \
           f'KMIP server "not/valid" on subsystem {subsystem1}: ' \
           f'Invalid KMIP server name "not/valid", contains invalid characters' in caplog.text


def test_add_kmip_server_endpoint_missing_client_key(caplog, two_gateways):
    os.rename(f"{kmip_dir1}/client_key.pem", f"{kmip_dir1}/client_key.XXX")
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", kmip_server_name1,
         "--address", "junk", "-p", "1234"])
    os.rename(f"{kmip_dir1}/client_key.XXX", f"{kmip_dir1}/client_key.pem")
    assert f"Failure adding an endpoint, with address junk:1234, to " \
           f"KMIP server \"{kmip_server_name1}\" on subsystem {subsystem1}: " \
           f"Missing client key {kmip_dir_prefix}{kmip_server_name1}/certs/" \
           f"client_key.pem" in caplog.text


def test_add_kmip_server_endpoint_with_same_attributes_different_subsys(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem2, "--no-group-append"])
    assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem2,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port}, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem2}: Successful" in caplog.text


def test_add_kmip_server_endpoint_with_different_server_name(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem2,
         "--address", "junkjunk",
         "--server-name", kmip_server_name2,
         "--port", "9876"])
    assert f"Failure adding an endpoint, with address junkjunk:9876, to KMIP server " \
           f"\"{kmip_server_name2}\" on subsystem {subsystem2}: Subsystem already uses KMIP " \
           f"server \"{kmip_server_name1}\", no other server is allowed" in caplog.text


def test_list_kmip_server_endpoints_two_subsystems(caplog, two_gateways):
    caplog.clear()
    endpoints = cli_test(["subsystem", "list_kmip_server_endpoints"])
    assert endpoints.status == 0
    assert len(endpoints.endpoints) == 2
    actual = {
        (ep.subsystem_nqn, ep.server_name, ep.address, ep.port)
        for ep in endpoints.endpoints
    }
    expected = {
        (subsystem1, kmip_server_name1, kmip_addr, kmip_port),
        (subsystem2, kmip_server_name1, kmip_addr, kmip_port),
    }
    assert actual == expected
    caplog.clear()
    endpoints = cli_test(["subsystem", "list_kmip_server_endpoints", "--subsystem", subsystem2])
    assert endpoints.status == 0
    assert len(endpoints.endpoints) == 1
    assert endpoints.endpoints[0].subsystem_nqn == subsystem2
    assert endpoints.endpoints[0].server_name == kmip_server_name1
    assert endpoints.endpoints[0].address == kmip_addr
    assert endpoints.endpoints[0].port == kmip_port
    caplog.clear()
    cli(["subsystem", "del", "--subsystem", subsystem2])
    assert f"Deleting subsystem {subsystem2}: Successful" in caplog.text


def test_wrong_encryption_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "JUNK", "--key-id", key_id])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: argument --encryption-format/-f: invalid choice: 'junk' (choose from 'luks1'," \
           " 'LUKS1', 'luks2', 'LUKS2')" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format=5, key_id=key_id)]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Invalid encryption format 5" in caplog.text


def test_encryption_algorithm_with_no_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-algorithm", "AES256", "--key-id", key_id])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Encryption algorithm is only allowed when an encryption format " \
           "is specified" in caplog.text

    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=[],
                                       encryption_algorithm="aes256")
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Can\'t have an encryption algorithm " \
           f"without an encryption format" in caplog.text


def test_key_id_with_no_format(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--key-id", key_id])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert rc == 2
    assert "error: Key IDs are only valid when an encryption format " \
           "is specified" in caplog.text

    enc_entries = [pb2.encryption_entry(key_id=key_id)]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Mustn\'t have a key ID when encryption " \
           f"format is not set" in caplog.text


def test_encryption_no_key_id(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "luks2"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: Must have a key ID when using encryption" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Must have a key ID when encryption " \
           f"format is set" in caplog.text


def test_number_of_formats_and_key_ids_mismatch(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    key_id2 = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "junk")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks2", "luks1", "--key-id", key_id])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: The number of key IDs should match the number of encryption " \
           "formats" in caplog.text
    assert rc == 2
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks2", "--key-id", key_id, key_id2])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: The number of key IDs should match the number of encryption " \
           "formats" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id=key_id),
                   pb2.encryption_entry(format="luks2")]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Must have a key ID when encryption " \
           f"format is set" in caplog.text

    enc_entries = [pb2.encryption_entry(format="luks1", key_id=key_id),
                   pb2.encryption_entry(key_id=key_id2)]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: Mustn\'t have a key ID when encryption " \
           f"format is not set" in caplog.text


def test_multiple_formats_with_create(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    key_id2 = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "junk")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
             "--rbd-create-image", "--encryption-format", "luks2", "luks1",
             "--key-id", key_id, key_id2])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: at most one encryption format can be specified when creating a " \
           "new image" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id=key_id),
                   pb2.encryption_entry(format="luks2", key_id=key_id2)]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       create_image=True,
                                       size=16777216,
                                       encryption_entries=enc_entries)
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f"Failure adding namespace to {subsystem1}: At most one encryption format can be " \
           f"specified when creating a new image" in caplog.text


def test_create_with_encryption(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image, "--size", "16MB",
         "--rbd-create-image", "--encryption-format", "luks1",
         "--key-id", key_id])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_encryption_algorithm_without_create(caplog, two_gateways):
    _, stub, _, _ = two_gateways
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
             "--rbd-data-pool", pool, "--rbd-image", image,
             "--encryption-format", "luks1", "--key-id", key_id,
             "--encryption-algorithm", "aes128"])
    except SystemExit as sysex:
        rc = sysex.code
        pass
    assert "error: --encryption-algorithm argument is not allowed for add command when RBD " \
           "image creation is disabled" in caplog.text
    assert rc == 2

    enc_entries = [pb2.encryption_entry(format="luks1", key_id=key_id)]
    ns_add_req = pb2.namespace_add_req(rbd_pool_name=pool,
                                       rbd_image_name=image,
                                       subsystem_nqn=subsystem1,
                                       block_size=512,
                                       encryption_entries=enc_entries,
                                       encryption_algorithm="aes128")
    caplog.clear()
    ret = stub.namespace_add(ns_add_req)
    assert ret.status != 0
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'aes128, context: <grpc._server' in caplog.text
    assert f"Failure adding namespace to {subsystem1}: Encryption algorithm is only allowed " \
           f"when creating a new image" in caplog.text


def test_open_with_encryption(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_open_with_encryption_wrong_key_id(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "wrong")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Failure adding namespace to {subsystem1}: Operation not permitted" in caplog.text


def test_list_namespaces(caplog, two_gateways):
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(20)
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert '"nsid": 1' in caplog.text
    assert '"format": "luks1"' in caplog.text
    assert f'"key_id": "{key_id}"' in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["--format", "json", "--server-port", "5502", "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"nsid": 1' in caplog.text
    assert '"format": "luks1"' in caplog.text
    assert f'"key_id": "{key_id}"' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    time.sleep(20)


def test_open_with_encryption_second_endpoint(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port2)])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port2}, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    time.sleep(10)
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    key_id2 = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port2, "bla")
    assert key_id != key_id2
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Deleting endpoint, with address {kmip_addr}:{kmip_port}, from KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    clear_kmip_server_endpoint_keys_cache(kmip_dir1, kmip_addr, kmip_port)
    time.sleep(20)
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Failed to get key {key_id} from {kmip_addr}:{kmip_port2}" in caplog.text
    assert f"Failure adding namespace to {subsystem1}: Can't fetch passphrase for id " \
           f"{key_id}" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id2])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(30)
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text


def test_open_with_encryption_second_server(caplog, two_gateways):
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem2, "--no-group-append"])
    assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem2,
         "--address", kmip_addr,
         "--server-name", kmip_server_name2,
         "--port", str(kmip_port3)])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port3}, to KMIP server " \
           f"{kmip_server_name2} on subsystem {subsystem2}: Successful" in caplog.text
    time.sleep(10)
    caplog.clear()
    cli(["subsystem", "list_kmip_server_endpoints", "--server-name", kmip_server_name2])
    assert kmip_addr in caplog.text
    assert str(kmip_port3) in caplog.text
    assert str(kmip_port) not in caplog.text
    assert str(kmip_port2) not in caplog.text
    assert kmip_server_name2 in caplog.text
    assert kmip_server_name1 not in caplog.text
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port2, "bla")
    add_key_to_kmip_server_endpoint(kmip_dir2, kmip_addr, kmip_port3, "dummy1")
    add_key_to_kmip_server_endpoint(kmip_dir2, kmip_addr, kmip_port3, "dummy2")
    key_id2 = add_key_to_kmip_server_endpoint(kmip_dir2, kmip_addr, kmip_port3, "bla")
    assert key_id != key_id2
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1, "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port2)])
    assert f"Deleting endpoint, with address {kmip_addr}:{kmip_port2}, from KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    clear_kmip_server_endpoint_keys_cache(kmip_dir1, kmip_addr, kmip_port2)
    time.sleep(20)
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem2, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Failure adding namespace to {subsystem2}: Operation not permitted" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem2, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image,
         "--encryption-format", "luks1", "--key-id", key_id2])
    assert f"Adding namespace 1 to {subsystem2}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: <grpc._server' in caplog.text
    time.sleep(30)
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem2, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem2}: Successful" in caplog.text
    time.sleep(20)


def test_delete_subsystem(caplog, two_gateways):
    gw, _, _, _ = two_gateways
    found = False
    state = gw.gateway_state.omap.get_state()
    for key, val in state.items():
        if key.startswith(gw.gateway_state.local.KMIP_SERVER_ENDPOINT_PREFIX):
            found = True
            break
    assert found
    caplog.clear()
    cli(["subsystem", "del", "--subsystem", subsystem1, "--force"])
    assert f"Deleting subsystem {subsystem1}: Successful" in caplog.text
    cli(["subsystem", "del", "--subsystem", subsystem2, "--force"])
    assert f"Deleting subsystem {subsystem2}: Successful" in caplog.text
    state = gw.gateway_state.omap.get_state()
    for key, val in state.items():
        assert not key.startswith(gw.gateway_state.local.KMIP_SERVER_ENDPOINT_PREFIX)


def test_no_certificate_in_config(caplog, two_gateways):
    gwA, _, gwB, _ = two_gateways
    configA = gwA.gateway_rpc.config
    configA.config["kmip"]["client_cert"] = ""
    gwA.__exit__(None, None, None)
    gwB.__exit__(None, None, None)
    print("Restarting gateway A")
    time.sleep(20)
    gwA = GatewayServer(configA)
    ceph_utils = CephUtils(configA)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gwA.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gwA.serve()

    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--server-name", kmip_server_name1,
         "--address", kmip_addr, "--port", str(kmip_port)])
    assert f"Failure adding an endpoint, with address " \
           f"{kmip_addr}:{kmip_port}, to KMIP server \"{kmip_server_name1}\" on " \
           f"subsystem {subsystem1}: Client certificate name is undefined" in caplog.text
    caplog.clear()
    cli(["subsystem", "del", "--subsystem", subsystem1])
    assert f"Deleting subsystem {subsystem1}: Successful" in caplog.text
