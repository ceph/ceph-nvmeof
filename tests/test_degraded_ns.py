import copy
import os
import socket
import ssl
import subprocess
import sys
import time
from functools import wraps

import signal
import grpc
import pytest

from control.cephutils import CephUtils
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
from control.server import GatewayServer
from control.state import GatewayState
from google.protobuf import json_format

kmip = pytest.importorskip("kmip")
from kmip import enums  # noqa: E402
from kmip.pie import client  # noqa: E402
from kmip.pie import objects  # noqa: E402

image1 = "enc_test_image1"
image2 = "enc_test_image2"
image3 = "enc_test_image3"
pool = "rbd"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
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
kmip_procs = {}
restarted_gw_b = None


def _install_ssl_wrap_socket_compat():
    if hasattr(ssl, "wrap_socket"):
        return

    @wraps(ssl.SSLContext.wrap_socket)
    def _wrap_socket(sock, keyfile=None, certfile=None, server_side=False,
                     cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_TLS,
                     ca_certs=None, do_handshake_on_connect=True,
                     suppress_ragged_eofs=True, ciphers=None):
        if ssl_version in (None, ssl.PROTOCOL_TLS):
            protocol = ssl.PROTOCOL_TLS_SERVER if server_side else getattr(
                ssl, "PROTOCOL_TLS_CLIENT", ssl.PROTOCOL_TLS
            )
        else:
            protocol = ssl_version

        context = ssl.SSLContext(protocol)
        # Only set minimum_version for generic TLS protocols, not for specific versions
        if hasattr(context, "minimum_version") and ssl_version in (None, ssl.PROTOCOL_TLS):
            context.minimum_version = ssl.TLSVersion.TLSv1_2

        if hasattr(context, "check_hostname"):
            context.check_hostname = False

        context.verify_mode = cert_reqs
        if ca_certs:
            context.load_verify_locations(ca_certs)
        if certfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        if ciphers:
            context.set_ciphers(ciphers)
        return context.wrap_socket(
            sock,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
            suppress_ragged_eofs=suppress_ragged_eofs,
        )

    ssl.wrap_socket = _wrap_socket


_install_ssl_wrap_socket_compat()


def start_kmip_server_endpoint(base_dir, addr, port, create_cert):
    """Sets up a KMIP server endpoint"""
    certs_dir = os.path.join(base_dir, "certs")
    required_certs = ("ca_cert.pem", "client_cert.pem", "client_key.pem",
                      "server_cert.pem", "server_key.pem")
    missing = any(not os.path.exists(os.path.join(certs_dir, f)) for f in required_certs)
    if create_cert or missing:
        setup_path = os.path.join(".", "tests", "kmip", "setup_kmip_test.sh")
        subprocess.run([setup_path, base_dir], check=True,
                       capture_output=True, text=True)
    srvr_path = os.path.join(".", "tests", "kmip", "dummy_kmip_server.py")

    # Start server process
    proc = subprocess.Popen(
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

    # Wait for server to start and verify it's listening
    max_retries = 30  # 30 seconds total
    for _ in range(max_retries):
        time.sleep(1)
        try:
            # Try to connect to verify server is up
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((addr, port))
            sock.close()
            if result == 0:
                print(f"KMIP server started successfully on {addr}:{port}")
                return proc
        except Exception:
            pass

        # Check if process crashed
        if proc.poll() is not None:
            stdout, _ = proc.communicate()
            raise RuntimeError(
                f"KMIP server failed to start on {addr}:{port}. "
                f"Output:\n{stdout}"
            )

    # Timeout - kill process and raise error
    proc.kill()
    stdout, _ = proc.communicate()
    raise RuntimeError(
        f"KMIP server did not start within {max_retries} seconds "
        f"on {addr}:{port}. Output:\n{stdout}"
    )


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


def wait_for_string(caplog, needle, timeout):
    for _ in range(timeout):
        if needle in caplog.text:
            return
        time.sleep(1)

    raise AssertionError(f"Couldn't find string \"{needle}\" in {timeout} seconds")


def look_for_string_from_file(lines, filename, lookfor):
    assert lookfor in lines
    broken_lines = lines.split("\n")
    for line in broken_lines:
        if lookfor not in line:
            continue
        if f":{filename}:" not in line:
            continue
        return
    raise AssertionError(f"Didn't find \"{lookfor}\" from file {filename} in {broken_lines}")


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
    kmip_procs[(kmip_addr, kmip_port)] = start_kmip_server_endpoint(
        kmip_dir1, kmip_addr, kmip_port, True)
    kmip_procs[(kmip_addr, kmip_port2)] = start_kmip_server_endpoint(
        kmip_dir1, kmip_addr, kmip_port2, False)
    kmip_procs[(kmip_addr, kmip_port3)] = start_kmip_server_endpoint(
        kmip_dir2, kmip_addr, kmip_port3, True)
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


def test_degraded_namespace(caplog, two_gateways):
    global kmip_dir1, restarted_gw_b
    gwA, _, gwB, _ = two_gateways
    configA = gwA.gateway_rpc.config
    configB = gwB.gateway_rpc.config
    portA = configA.config["gateway"]["port"]
    portB = configB.config["gateway"]["port"]
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["--server-port", portA, "listener", "add", "--subsystem", subsystem1,
         "--host-name", gwA.name, "--traddr", "127.0.0.1", "--trsvcid", "4420",
         "--verify-host-name"])
    assert f"Adding {subsystem1} listener at 127.0.0.1:4420: Successful" in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "listener", "add", "--subsystem", subsystem1,
         "--host-name", gwB.name, "--traddr", "127.0.0.1", "--trsvcid", "4421",
         "--verify-host-name"])
    assert f"Adding {subsystem1} listener at 127.0.0.1:4421: Successful" in caplog.text
    time.sleep(20)
    caplog.clear()
    cli(["subsystem", "add_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr, "--port", str(kmip_port),
         "--server-name", kmip_server_name1])
    assert f"Adding an endpoint, with address {kmip_addr}:{kmip_port}, to KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    key_id = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "bla")
    key_id2 = add_key_to_kmip_server_endpoint(kmip_dir1, kmip_addr, kmip_port, "junk")
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image1, "--rbd-create-image",
         "--load-balancing-group", "2",
         "--size", "16MB", "--encryption-format", "luks1", "--key-id", key_id])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: <' in caplog.text
    assert f"to {subsystem1} with load balancing group id 2" in caplog.text
    time.sleep(20)
    assert f'encryption_entries: [(format: luks1, key id: {key_id})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image2, "--rbd-create-image",
         "--load-balancing-group", "1",
         "--size", "16MB"])
    assert f"Adding namespace 2 to {subsystem1}: Successful" in caplog.text
    assert f"to {subsystem1} with load balancing group id 1" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", image3, "--rbd-create-image",
         "--load-balancing-group", "2",
         "--size", "16MB", "--encryption-format", "luks1", "--key-id", key_id2])
    assert f"Adding namespace 3 to {subsystem1}: Successful" in caplog.text
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: <' in caplog.text
    assert f"to {subsystem1} with load balancing group id 2" in caplog.text
    time.sleep(20)
    assert f'encryption_entries: [(format: luks1, key id: {key_id2})], encryption_algorithm: ' \
           f'no_algorithm, context: None' in caplog.text
    # refresh the subsystem cache on both gateways
    cli(["--server-port", portA, "subsystem", "list"])
    cli(["--server-port", portB, "subsystem", "list"])
    time.sleep(30)
    print("Stop KMIP server to simulate key unavailability")
    proc = kmip_procs[(kmip_addr, kmip_port)]

    # Temporarily disable the gateway's SIGCHLD handler so killing
    # the KMIP server doesn't trigger a false gateway-death SystemExit
    old_handler = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    try:
        proc.kill()
        proc.wait(timeout=10)
    finally:
        signal.signal(signal.SIGCHLD, old_handler)

    caplog.clear()
    gwB.__exit__(None, None, None)
    print("Restarting gateway B")
    time.sleep(90)
    gwB = GatewayServer(configB)
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gwB.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    caplog.clear()
    gwB.serve()
    time.sleep(30)
    assert gwB.gateway_rpc.up_and_running
    assert f"Failed to retrieve key {key_id} from any KMIP server endpoint" in caplog.text
    assert f"Can\'t fetch passphrase for id {key_id}, will create " \
           f"a degraded namespace" in caplog.text
    assert f"Failed to retrieve key {key_id2} from any KMIP server endpoint" in caplog.text
    assert f"Can\'t fetch passphrase for id {key_id2}, will create " \
           f"a degraded namespace" in caplog.text
    cli(["--server-port", portB, "subsystem", "list"])
    caplog.clear()
    cli(["--server-port", portA, "--format", "json", "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": false' in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": true' in caplog.text
    with gwB.gateway_rpc.rpc_lock:
        bdev_list = gwB.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    found = 0
    for b in bdev_list:
        if b.get("product_name") == "Null disk":
            found += 1
    assert found == 2, "Should have 2 null bdevs on gateway B"
    with gwA.gateway_rpc.rpc_lock:
        bdev_list = gwA.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    found = any(b.get("product_name") == "Null disk" for b in bdev_list)
    assert not found, "Null bdev found on gateway A"
    caplog.clear()
    cli(["--server-port", portB, "namespace", "resize", "--subsystem", subsystem1,
         "--nsid", "1", "--size", "20MB"])
    assert f"Namespace 1 on {subsystem1} is degraded, no resize was done" in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "namespace", "resize", "--subsystem", subsystem1,
         "--nsid", "3", "--size", "20MB"])
    assert f"Namespace 3 on {subsystem1} is degraded, no resize was done" in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "namespace", "list", "--subsystem", subsystem1, "--nsid", "3"])
    assert "Degraded" in caplog.text, "Should see degraded namespace on gateway B"
    caplog.clear()
    cli(["--server-port", portA, "namespace", "list", "--subsystem", subsystem1, "--nsid", "3"])
    assert "Degraded" not in caplog.text, "Shouldn't see degraded namespace on gateway A"
    caplog.clear()
    ns_info = cli_test(["namespace", "list", "--subsystem", subsystem1, "--nsid", "3"])
    assert len(ns_info.namespaces) == 1
    assert ns_info.namespaces[0].nsid == 3
    bdev_name = ns_info.namespaces[0].bdev_name
    ns_info = cli_test(["--server-port", portB, "namespace", "list", "--subsystem", subsystem1])
    assert ns_info.status == 0
    assert len(ns_info.namespaces) == 3
    for n in ns_info.namespaces:
        assert not n.encryption_entries, "Shouldn't have a namespace with encryption"
    eps = cli_test(["--server-port", portB, "subsystem", "list_kmip_server_endpoints"])
    assert len(eps.endpoints) == 1
    caplog.clear()
    cli(["--server-port", portB, "subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr, "--server-name", kmip_server_name1, "--port", str(kmip_port)])
    assert f"Failure deleting endpoints from KMIP server \"{kmip_server_name1}\" on subsystem " \
           f"{subsystem1}: There are encrypted (or degraded) " \
           f"namespaces in the subsystem. Either delete these namespaces or use the \"force\" " \
           f"parameter." in caplog.text
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "3"])
    eps = cli_test(["--server-port", portB, "subsystem", "list_kmip_server_endpoints"])
    assert len(eps.endpoints) == 1
    assert f"Deleting namespace 3 from {subsystem1}: Successful" in caplog.text
    time.sleep(30)
    with gwB.gateway_rpc.rpc_lock:
        bdev_list = gwB.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    found = 0
    for b in bdev_list:
        if b.get("product_name") == "Null disk":
            found += 1
        assert b.get("name") != bdev_name, f"Bdev {bdev_name} shouldn't exist on gateway B"
    assert found == 1, "Should have 1 null bdevs on gateway B after namespace deletion"
    with gwA.gateway_rpc.rpc_lock:
        bdev_list = gwA.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    found = False
    for b in bdev_list:
        if b.get("product_name") == "Null disk":
            found = True
            break
        assert b.get("name") != bdev_name, f"Bdev {bdev_name} shouldn't exist on gateway A"
    assert not found, "Null bdev found on gateway A"
    look_for = f"Received auto request to change load balancing group for namespace with ID 1 " \
               f"in {subsystem1} to {gwB.gateway_rpc.MAINTENANCE_ANA_GROUP}, " \
               f"persistent: False, maintenance: True, context: context"
    wait_for_string(caplog, look_for, 300)
    look_for = f"Received manual request to change load balancing group for namespace with ID 1 " \
               f"in {subsystem1} to {gwB.gateway_rpc.MAINTENANCE_ANA_GROUP}, " \
               f"persistent: False, maintenance: False, context: None"
    wait_for_string(caplog, look_for, 300)
    time.sleep(20)
    look_for = f"Received auto request to change load balancing group for namespace with ID 1 " \
               f"in {subsystem1} to 1, " \
               f"persistent: True, maintenance: False, context: context"
    wait_for_string(caplog, look_for, 300)
    look_for = f"Received manual request to change load balancing group for namespace with ID 1 " \
               f"in {subsystem1} to -1, " \
               f"persistent: False, maintenance: False, context: None"
    wait_for_string(caplog, look_for, 300)
    look_for_key = GatewayState.build_namespace_key(subsystem1, "1")
    state = gwB.gateway_state.omap.get_state()
    assert look_for_key in state.keys()
    val = state[look_for_key]
    req = json_format.Parse(val, pb2.namespace_add_req(), ignore_unknown_fields=True)
    assert req.anagrpid == -1 or req.anagrpid == gwB.gateway_rpc.MAINTENANCE_ANA_GROUP
    if req.anagrpid == gwB.gateway_rpc.MAINTENANCE_ANA_GROUP:
        print("Namespace has a maintenance load balancing group ID, will wait for persistent one")
        for _ in range(120):
            state = gwB.gateway_state.omap.get_state()
            assert look_for_key in state.keys()
            val = state[look_for_key]
            req = json_format.Parse(val, pb2.namespace_add_req(), ignore_unknown_fields=True)
            if req.anagrpid == -1:
                break
            time.sleep(1)
        assert req.anagrpid == -1
    assert f"Received auto request to change load balancing group for namespace with ID 1 " \
           f"in {subsystem1} to -1" not in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", portA, "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": false' in caplog.text
    assert '"pinned": true' in caplog.text
    caplog.clear()
    cli(["--server-port", portA, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "(Pinned)" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", portB, "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": true' in caplog.text
    assert '"pinned": true' in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "Degraded" in caplog.text
    assert "(Pinned)" in caplog.text
    print("Run the KMIP server again")
    kmip_dir1 = os.path.join(kmip_dir_prefix, kmip_server_name1)
    kmip_procs[(kmip_addr, kmip_port)] = start_kmip_server_endpoint(
        kmip_dir1, kmip_addr, kmip_port, False)
    kmip_dir1 = os.path.join(kmip_dir1, "certs")
    time.sleep(5)
    caplog.clear()
    gwB.__exit__(None, None, None)
    print("Restarting gateway B again")
    time.sleep(90)
    gwB = GatewayServer(configB)
    restarted_gw_b = gwB
    ceph_utils = CephUtils(configB)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gwB.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    caplog.clear()
    gwB.serve()
    time.sleep(30)
    assert f"Failed to retrieve key {key_id} from any KMIP server endpoint" not in caplog.text
    assert f"Can\'t fetch passphrase for id {key_id}, will create " \
           f"a degraded namespace" not in caplog.text
    cli(["--server-port", portB, "subsystem", "list"])   # refresh subsystem cache
    caplog.clear()
    ns = cli_test(["--server-port", portB, "namespace", "list"])
    assert ns.status == 0
    assert len(ns.namespaces) == 2
    assert ns.namespaces[0].nsid == 1 or ns.namespaces[0].nsid == 2
    assert ns.namespaces[1].nsid == 1 or ns.namespaces[1].nsid == 2
    assert ns.namespaces[0].nsid != ns.namespaces[1].nsid
    caplog.clear()
    cli(["--server-port", portB, "--format", "json", "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": true' not in caplog.text
    assert '"degraded": false' in caplog.text
    with gwB.gateway_rpc.rpc_lock:
        bdev_list = gwB.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    found = any(b.get("product_name") == "Null disk" for b in bdev_list)
    assert not found, "Null bdev found on gateway B after KMIP server restart"
    state = gwB.gateway_state.omap.get_state()
    assert look_for_key in state.keys()
    val = state[look_for_key]
    req = json_format.Parse(val, pb2.namespace_add_req(), ignore_unknown_fields=True)
    assert req.anagrpid == -1


def test_list_pinned_not_degraded(caplog, two_gateways):
    gwA, _, _, _ = two_gateways
    configA = gwA.gateway_rpc.config
    assert restarted_gw_b is not None
    configB = restarted_gw_b.gateway_rpc.config
    portA = configA.config["gateway"]["port"]
    portB = configB.config["gateway"]["port"]
    caplog.clear()
    cli(["--format", "json", "--server-port", portA, "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": false' in caplog.text
    assert '"pinned": true' in caplog.text
    caplog.clear()
    cli(["--server-port", portA, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "(Pinned)" in caplog.text
    caplog.clear()
    cli(["--format", "json", "--server-port", portB, "namespace", "list",
         "--subsystem", subsystem1, "--nsid", "1"])
    assert '"degraded": false' in caplog.text
    assert '"pinned": true' in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "(Pinned)" in caplog.text


def test_unpin(caplog, two_gateways):
    gwA, _, _, _ = two_gateways
    configA = gwA.gateway_rpc.config
    assert restarted_gw_b is not None
    configB = restarted_gw_b.gateway_rpc.config
    portA = configA.config["gateway"]["port"]
    portB = configB.config["gateway"]["port"]
    caplog.clear()
    cli(["namespace", "unpin", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Unpinning load balancing group for namespace 1 in {subsystem1}: " \
           f"Successful" in caplog.text
    look_for_key = GatewayState.build_namespace_key(subsystem1, "1")
    state = restarted_gw_b.gateway_state.omap.get_state()
    assert look_for_key in state.keys()
    val = state[look_for_key]
    req = json_format.Parse(val, pb2.namespace_add_req(), ignore_unknown_fields=True)
    assert req.anagrpid == 1
    look_for = f"Received manual request to change load balancing group for namespace with " \
               f"ID 1 in {subsystem1} to 1, persistent: False, maintenance: False, " \
               f"context: None"
    wait_for_string(caplog, look_for, 300)
    assert f"Received request to delete namespace 1 from {subsystem1}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem1}" not in caplog.text
    ns = cli_test(["--server-port", portA, "namespace", "list",
                   "--subsystem", subsystem1, "--nsid", "1"])
    assert ns.status == 0
    assert len(ns.namespaces) == 1
    assert ns.namespaces[0].nsid == 1
    assert ns.namespaces[0].rbd_image_name == image1
    assert ns.namespaces[0].rbd_pool_name == pool
    assert not ns.namespaces[0].degraded
    assert not ns.namespaces[0].pinned
    ns = cli_test(["--server-port", portB, "namespace", "list",
                   "--subsystem", subsystem1, "--nsid", "1"])
    assert ns.status == 0
    assert len(ns.namespaces) == 1
    assert ns.namespaces[0].nsid == 1
    assert ns.namespaces[0].rbd_image_name == image1
    assert ns.namespaces[0].rbd_pool_name == pool
    assert not ns.namespaces[0].degraded
    assert not ns.namespaces[0].pinned
    caplog.clear()
    cli(["--server-port", portA, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "(Pinned)" not in caplog.text
    assert "Degraded" not in caplog.text
    caplog.clear()
    cli(["--server-port", portB, "namespace", "list", "--subsystem", subsystem1, "--nsid", "1"])
    assert "(Pinned)" not in caplog.text
    assert "Degraded" not in caplog.text


def test_delete_resources(caplog, two_gateways):
    gwA, _, gwB, _ = two_gateways
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem1}: Successful" in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem1, "--nsid", "2"])
    assert f"Deleting namespace 2 from {subsystem1}: Successful" in caplog.text
    ns = cli_test(["namespace", "list"])
    assert ns.status == 0
    assert len(ns.namespaces) == 0
    time.sleep(30)
    with gwA.gateway_rpc.rpc_lock:
        bdev_list = gwA.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    assert len(bdev_list) == 0, "There shouldn't be bdevs left on Gateway A"
    assert restarted_gw_b is not None
    with restarted_gw_b.gateway_rpc.rpc_lock:
        bdev_list = restarted_gw_b.gateway_rpc.spdk_rpc_client.bdev_get_bdevs()
    assert len(bdev_list) == 0, "There shouldn't be bdevs left on Gateway B"
    caplog.clear()
    cli(["subsystem", "del_kmip_server_endpoint", "--subsystem", subsystem1,
         "--address", kmip_addr,
         "--server-name", kmip_server_name1,
         "--port", str(kmip_port)])
    assert f"Deleting endpoint, with address {kmip_addr}:{kmip_port}, from KMIP server " \
           f"{kmip_server_name1} on subsystem {subsystem1}: Successful" in caplog.text
    look_for_string_from_file(caplog.text, "grpc.py",
                              f"Last endpoint of server \"{kmip_server_name1}\" on "
                              f"subsystem {subsystem1} was deleted.")
    look_for_string_from_file(caplog.text, "cli.py",
                              f"Last endpoint of server \"{kmip_server_name1}\" on "
                              f"subsystem {subsystem1} was deleted.")
    clear_kmip_server_endpoint_keys_cache(kmip_dir1, kmip_addr, kmip_port)
    caplog.clear()
    cli(["subsystem", "del", "--subsystem", subsystem1])
    assert f"Deleting subsystem {subsystem1}: Successful" in caplog.text
