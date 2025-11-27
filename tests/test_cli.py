import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
from control.utils import GatewayUtils
import spdk.rpc.bdev as rpc_bdev
from spdk.rpc import spdk_get_version
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import os

image = "mytestdevimage1"
image2 = "mytestdevimage2"
image3 = "mytestdevimage3"
image4 = "mytestdevimage4"
image5 = "mytestdevimage5"
image6 = "mytestdevimage6"
image7 = "mytestdevimage7"
image8 = "mytestdevimage8"
image9 = "mytestdevimage9"
image10 = "mytestdevimage10"
image11 = "mytestdevimage11"
image12 = "mytestdevimage12"
image13 = "mytestdevimage13"
image14 = "mytestdevimage14"
image15 = "mytestdevimage15"
image16 = "mytestdevimage16"
image17 = "mytestdevimage17"
image18 = "mytestdevimage18"
image19 = "mytestdevimage19"
image20 = "mytestdevimage20"
image21 = "mytestdevimage21"
image22 = "mytestdevimage22"
image23 = "mytestdevimage23"
image24 = "mytestdevimage24"
image25 = "mytestdevimage25"
image26 = "mytestdevimage26"
image27 = "mytestdevimage27"
image28 = "mytestdevimage28"
image29 = "mytestdevimage29"
image30 = "mytestdevimage30"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
subsystem3 = "nqn.2016-06.io.spdk:cnode3"
subsystem4 = "nqn.2016-06.io.spdk:cnode4"
subsystem5 = "nqn.2016-06.io.spdk:cnode5"
subsystem6 = "nqn.2016-06.io.spdk:cnode6"
subsystem7 = "nqn.2016-06.io.spdk:cnode7"
subsystem8 = "nqn.2016-06.io.spdk:cnode8"
subsystem9 = "nqn.2016-06.io.spdk:cnode9"
subsystem10 = "nqn.2016-06.io.spdk:cnode10"
subsystem11 = "nqn.2016-06.io.spdk:cnode11"
subsystem12 = "nqn.2016-06.io.spdk:cnode12"
subsystem13 = "nqn.2016-06.io.spdk:cnode13"
subsystem14 = "nqn.2016-06.io.spdk:cnode14"
subsystem15 = "nqn.2016-06.io.spdk:cnode15"
subsystem16 = "nqn.2016-06.io.spdk:cnode16"
subsystem17 = "nqn.2016-06.io.spdk:cnode17"
subsystem18 = "nqn.2016-06.io.spdk:cnode18"
subsystemX = "nqn.2016-06.io.spdk:cnodeX"
discovery_nqn = "nqn.2014-08.org.nvmexpress.discovery"
serial = "Ceph00000000000001"
serial2 = "Ceph00000000000002"
uuid = "948878ee-c3b2-4d58-a29b-2cff713fc02d"
uuid2 = "948878ee-c3b2-4d58-a29b-2cff713fc02e"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
subsystemX = "nqn.2016-06.io.spdk:cnodeX"
discovery_nqn = "nqn.2014-08.org.nvmexpress.discovery"
uuid = "948878ee-c3b2-4d58-a29b-2cff713fc02d"
uuid2 = "948878ee-c3b2-4d58-a29b-2cff713fc02e"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
host1 = "nqn.2016-06.io.spdk:host1"
host2 = "nqn.2016-06.io.spdk:host2"
host3 = "nqn.2016-06.io.spdk:host3"
host4 = "nqn.2016-06.io.spdk:host4"
host5 = "nqn.2016-06.io.spdk:host5"
host6 = "nqn.2016-06.io.spdk:host6"
host7 = "nqn.2016-06.io.spdk:host7"
host8 = "nqn.2016-06.io.spdk:host8"
host9 = "nqn.2016-06.io.spdk:host9"
host10 = "nqn.2016-06.io.spdk:host10"
host11 = "nqn.2016-06.io.spdk:host11"
host12 = "nqn.2016-06.io.spdk:host12"
host13 = "nqn.2016-06.io.spdk:host13"
host14 = "nqn.2016-06.io.spdk:host14"
host15 = "nqn.2016-06.io.spdk:host15"
hostxx = "nqn.2016-06.io.spdk:hostXX"
nsid = "1"
anagrpid = "1"
anagrpid2 = "2"
host_name = socket.gethostname()
addr = "127.0.0.1"
addr_ipv6 = "::1"
server_addr_ipv6 = "2001:db8::3"
listener_list = [["-a", addr, "-s", "5001", "-f", "ipv4"], ["-a", addr, "-s", "5002"]]
listener_list_no_port = [["-a", addr]]
listener_list_invalid_adrfam = [["-a", addr, "-s", "5013", "--adrfam", "JUNK"]]
listener_list_ipv6 = [["-a", addr_ipv6, "-s", "5003", "--adrfam", "ipv6"],
                      ["-a", addr_ipv6, "-s", "5004", "--adrfam", "IPV6"]]
listener_list_discovery = [["-n", discovery_nqn, "-t", host_name, "-a", addr, "-s", "5012"]]
listener_list_negative_port = [["-t", host_name, "-a", addr, "-s", "-2000"]]
listener_list_big_port = [["-t", host_name, "-a", addr, "-s", "70000"]]
listener_list_wrong_host = [["-t", "WRONG", "-a", addr, "-s", "5015", "-f", "ipv4"]]
listener_list_bad_ips = [["127.1.1.1", 5011, "ipv4"],
                         ["[fe80::a00:27ff:fe38:1d48]", 5022, "ipv6"],
                         [addr, 5033, "ipv6"],
                         [f"[{addr_ipv6}]", 5044, "ipv4"]]
config = "ceph-nvmeof.conf"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["max_namespaces_with_netmask"] = "3"
    config.config["gateway"]["max_hosts_per_namespace"] = "3"
    config.config["gateway"]["max_subsystems"] = "4"
    config.config["gateway"]["max_namespaces"] = "12"
    config.config["gateway"]["max_namespaces_per_subsystem"] = "11"
    config.config["gateway"]["max_hosts_per_subsystem"] = "4"
    config.config["gateway"]["max_hosts"] = "6"
    config.config["gateway-logs"]["log_level"] = "debug"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        gateway.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{port}")
        stub = pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc, stub

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


class TestGet:
    def test_get_subsystems(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text

    def test_get_subsystems_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "subsystem", "list"])
        assert "No subsystems" in caplog.text

    def test_get_gateway_info(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        gw_info_req = pb2.get_gateway_info_req(cli_version="0.0.1")
        ret = stub.get_gateway_info(gw_info_req)
        assert ret.status != 0
        assert "is older than gateway" in caplog.text
        caplog.clear()
        gw_info_req = pb2.get_gateway_info_req()
        ret = stub.get_gateway_info(gw_info_req)
        assert "No CLI version specified" in caplog.text
        assert ret.status == 0
        caplog.clear()
        gw_info_req = pb2.get_gateway_info_req(cli_version="0.0.1.4")
        ret = stub.get_gateway_info(gw_info_req)
        assert "Can't parse version" in caplog.text
        assert "Invalid CLI version" in caplog.text
        assert ret.status == 0
        caplog.clear()
        gw_info_req = pb2.get_gateway_info_req(cli_version="0.X.4")
        ret = stub.get_gateway_info(gw_info_req)
        assert "Can't parse version" in caplog.text
        assert "Invalid CLI version" in caplog.text
        assert ret.status == 0
        caplog.clear()
        cli_ver = os.getenv("NVMEOF_VERSION")
        save_port = gw.config.config["gateway"]["port"]
        save_addr = gw.config.config["gateway"]["addr"]
        gw.config.config["gateway"]["port"] = "6789"
        gw.config.config["gateway"]["addr"] = "10.10.10.10"
        gw_info_req = pb2.get_gateway_info_req(cli_version=cli_ver)
        ret = stub.get_gateway_info(gw_info_req)
        assert ret.status == 0
        assert f'version: "{cli_ver}"' in caplog.text
        assert 'port: "6789"' in caplog.text
        assert 'addr: "10.10.10.10"' in caplog.text
        assert f'name: "{gw.gateway_name}"' in caplog.text
        assert f'hostname: "{gw.host_name}"' in caplog.text
        gw.config.config["gateway"]["port"] = save_port
        gw.config.config["gateway"]["addr"] = save_addr
        caplog.clear()
        cli(["version"])
        assert f"CLI version: {cli_ver}" in caplog.text
        caplog.clear()
        spdk_ver = None
        with gw.rpc_lock:
            try:
                spdk_ver = spdk_get_version(gw.spdk_rpc_client)
                spdk_ver = spdk_ver["version"]
            except Exception:
                spdk_ver = None
        if not spdk_ver:
            spdk_ver = os.getenv("NVMEOF_SPDK_VERSION")
        gw_info = cli_test(["gw", "info"])
        assert gw_info is not None
        assert gw_info.cli_version == cli_ver
        assert gw_info.version == cli_ver
        assert gw_info.spdk_version == spdk_ver
        assert gw_info.name == gw.gateway_name
        assert gw_info.hostname == gw.host_name
        assert gw_info.max_subsystems == 4
        assert gw_info.max_namespaces == 12
        assert gw_info.max_namespaces_per_subsystem == 11
        assert gw_info.max_hosts == 6
        assert gw_info.max_hosts_per_subsystem == 4
        assert gw_info.status == 0
        assert gw_info.bool_status

    def test_get_gateway_stats(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "gateway", "get_stats"])
        assert '"status": 0' in caplog.text
        assert '"tick_rate": ' in caplog.text
        assert '"poll_groups": [' in caplog.text
        assert '"name": "nvmf_tgt_poll_group_000",' in caplog.text
        assert '"admin_qpairs": 0,' in caplog.text
        assert '"io_qpairs": 0,' in caplog.text
        assert '"current_admin_qpairs": 0,' in caplog.text
        assert '"current_io_qpairs": 0,' in caplog.text
        assert '"pending_bdev_io": "0",' in caplog.text
        assert '"completed_nvme_io": "0"' in caplog.text
        assert '"trtype": "TCP"' in caplog.text


class TestCreate:
    def test_create_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016", "--no-group-append"])
        assert 'NQN "nqn.2016" is too short, minimal length is 11' in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem",
             "nqn.2016-06XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
             "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
             "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
             "XXXXXXXXXXXXXXXXXXX",
             "--no-group-append"])
        assert "is too long, maximal length is 223" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2014-08.org.nvmexpress:uuid:0",
             "--no-group-append"])
        assert "UUID is not the correct length" in caplog.text
        caplog.clear()
        cli(["subsystem", "add",
             "--subsystem", "nqn.2014-08.org.nvmexpress:uuid:9e9134-3cb431-4f3e-91eb-a13cefaabebf",
             "--no-group-append"])
        assert "UUID is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "qqn.2016-06.io.spdk:cnode1", "--no-group-append"])
        assert "doesn't start with" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.016-206.io.spdk:cnode1", "--no-group-append"])
        assert "invalid date code" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2X16-06.io.spdk:cnode1", "--no-group-append"])
        assert "invalid date code" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io.spdk:", "--no-group-append"])
        assert "must contain a user specified name starting with" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io..spdk:cnode1", "--no-group-append"])
        assert "reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add",
             "--subsystem", "nqn.2016-06.io.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                            "XXXXXXXXXXXXXXXXXXXXX.spdk:cnode1", "--no-group-append"])
        assert "reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io.-spdk:cnode1",
             "--no-group-append"])
        assert "reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", f"{subsystem}_X", "--no-group-append"])
        assert "Invalid NQN" in caplog.text
        assert "contains invalid characters" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem,
             "--max-namespaces", "3700", "--no-group-append"])
        assert f"Failure creating subsystem {subsystem}: Max namespaces " \
               f"can't be greater than 2048" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem,
             "--max-namespaces", "2039", "--no-group-append"])
        assert f"The requested max number of namespaces for subsystem {subsystem} (2039) " \
               f"is greater than the global limit on the number of namespaces (12), " \
               f"will continue" in caplog.text
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        cli(["--format", "json", "subsystem", "list"])
        assert f'"serial_number": "{serial}"' not in caplog.text
        assert f'"nqn": "{subsystem}"' in caplog.text
        assert '"max_namespaces": 2039' in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem,
             "--max-namespaces", "2039", "--no-group-append"])
        assert f"Failure creating subsystem {subsystem}: Subsystem already exists" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem2,
             "--serial-number", serial, "--no-group-append"])
        assert f"Adding subsystem {subsystem2}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list"])
        assert f'"serial_number": "{serial}"' in caplog.text
        assert f'"nqn": "{subsystem}"' in caplog.text
        assert f'"nqn": "{subsystem2}"' in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list", "--subsystem", subsystem])
        assert f'"nqn": "{subsystem}"' in caplog.text
        assert f'"nqn": "{subsystem2}"' not in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list", "--serial-number", serial])
        assert f'"nqn": "{subsystem}"' not in caplog.text
        assert f'"nqn": "{subsystem2}"' in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert f'{serial}' in caplog.text
        assert f'{subsystem}' in caplog.text
        assert f'{subsystem2}' in caplog.text
        caplog.clear()
        cli(["--format", "plain", "subsystem", "list"])
        assert f'{serial}' in caplog.text
        assert f'{subsystem}' in caplog.text
        assert f'{subsystem2}' in caplog.text
        caplog.clear()
        cli(["subsystem", "list", "--serial-number", "JUNK"])
        assert "No subsystem with serial number JUNK" in caplog.text
        caplog.clear()
        cli(["subsystem", "list", "--subsystem", "JUNK"])
        assert "Failure listing subsystems: No such device" in caplog.text
        assert '"nqn": "JUNK"' in caplog.text
        caplog.clear()
        subs_list = cli_test(["--format", "text", "subsystem", "list"])
        assert subs_list is not None
        assert subs_list.status == 0
        assert subs_list.subsystems[0].nqn == subsystem
        assert subs_list.subsystems[1].nqn == subsystem2
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem8,
             "--serial-number", serial, "--no-group-append"])
        assert f"Failure creating subsystem {subsystem8}: Serial number {serial} " \
               f"is already used by subsystem {subsystem2}" in caplog.text
        caplog.clear()
        subs_list = cli_test(["subsystem", "list"])
        assert subs_list is not None
        assert subs_list.status == 0
        assert subs_list.subsystems[0].nqn == subsystem
        assert subs_list.subsystems[1].nqn == subsystem2

    def test_create_subsystem_with_discovery_nqn(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["subsystem", "add", "--subsystem", discovery_nqn])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "Can't add a discovery subsystem" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["subsystem", "add", "--subsystem", discovery_nqn, "--no-group-append"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "Can't add a discovery subsystem" in caplog.text
        assert rc == 2

    def test_add_namespace_wrong_balancing_group(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image4, "--size", "16MB", "--rbd-create-image",
             "--load-balancing-group", "100", "--force"])
        assert f"Failure adding namespace to {subsystem}:" in caplog.text
        assert "Load balancing group 100 doesn't exist" in caplog.text

    def test_add_namespace_wrong_size(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", "junkimage", "--size", "0", "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", "junkimage", "--size", "1026KB", "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be aligned to MiBs" in caplog.text
        assert rc == 2

    def test_add_namespace_wrong_size_grpc(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        add_namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                                  rbd_pool_name=pool,
                                                  rbd_image_name="junkimage",
                                                  block_size=512,
                                                  create_image=True,
                                                  size=16 * 1024 * 1024 + 20)
        ret = stub.namespace_add(add_namespace_req)
        assert ret.status != 0
        assert "Failure adding namespace" in caplog.text
        assert "image size must be aligned to MiBs" in caplog.text

    def test_add_namespace_wrong_block_size(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        add_namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                                  rbd_pool_name=pool,
                                                  rbd_image_name="junkimage",
                                                  create_image=True,
                                                  size=16 * 1024 * 1024,
                                                  force=True)
        ret = stub.namespace_add(add_namespace_req)
        assert ret.status != 0
        assert "Failure adding namespace" in caplog.text
        assert "block size can't be zero" in caplog.text

    def test_changing_namespace_with_no_size(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image30, "--size", "16MB",
             "--rbd-create-image", "--force"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
        caplog.clear()
        add_namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem,
                                                  rbd_pool_name=pool,
                                                  rbd_image_name=image30,
                                                  block_size=512,
                                                  create_image=False,
                                                  force=True)
        ret = stub.namespace_add(add_namespace_req)
        assert ret.status == 0
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "1",
             "--auto-visible", "no"])
        assert f'Changing visibility of namespace 1 in {subsystem} to ' \
               f'"visible to selected hosts": Successful' in caplog.text
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "1",
             "--auto-visible", "yes"])
        assert f'Changing visibility of namespace 1 in {subsystem} to ' \
               f'"visible to all hosts": Successful' in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text

    def test_add_namespace_double_uuid(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image2, "--uuid", uuid, "--size", "16MB",
             "--rbd-create-image", "--force"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image3, "--uuid", uuid, "--size", "16MB",
             "--rbd-create-image", "--force"])
        assert f"Failure adding namespace, UUID {uuid} is already in use" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text

    def test_add_namespace_double_nsid(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image2, "--size", "16MB", "--rbd-create-image", "--force"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image3, "--nsid", "1", "--size", "16MB",
             "--rbd-create-image", "--force"])
        assert "Failure adding namespace, ID 1 is already in use" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text

    def test_add_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", "junk",
             "--rbd-image", image2, "--uuid", uuid, "--size", "16MB", "--rbd-create-image",
             "--load-balancing-group", anagrpid])
        assert "RBD pool junk doesn't exist" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image2, "--uuid", uuid, "--size", "16MB", "--rbd-create-image",
             "--load-balancing-group", anagrpid, "--force"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        assert f"Allocated cluster name='cluster_context_{anagrpid}_0'" in caplog.text
        assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
        assert "no_auto_visible: False" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image2, "--size", "36", "--rbd-create-image",
             "--load-balancing-group", anagrpid, "--force"])
        assert f"Image {pool}/{image2} already exists with a size of 16777216 bytes " \
               f"which differs from the requested size of 37748736 bytes" in caplog.text
        assert f"Can't create RBD image {pool}/{image2}" in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", image2, "--block-size", "1024", "--size", "16MB",
                 "--load-balancing-group", anagrpid])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size argument is not allowed for add command when " \
               "RBD image creation is disabled" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", image2, "--block-size", "1024", "--size=-16MB",
                 "--rbd-create-image", "--load-balancing-group", anagrpid])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", image2, "--block-size", "1024", "--size", "1x6MB",
                 "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", image2, "--block-size", "1024", "--size", "16MiB",
                 "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
                 "--rbd-image", image2, "--block-size", "1024", "--size", "16mB",
                 "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image, "--block-size", "1024",
             "--load-balancing-group", anagrpid, "--rbd-create-image", "--size", "16MB", "--force"])
        assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", nsid])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert f'"uuid": "{uuid}"' in caplog.text
        assert '"rw_ios_per_second": "0"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "2"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text
        assert '"block_size": 1024' in caplog.text
        assert f'"uuid": "{uuid}"' not in caplog.text
        assert '"rw_ios_per_second": "0"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--uuid", uuid])
        assert f'"uuid": "{uuid}"' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid.upper()])
        assert f'"uuid": "{uuid}"' in caplog.text
        caplog.clear()
        uuid_no_dashes = uuid.replace("-", "")
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid_no_dashes])
        assert f'"uuid": "{uuid}"' in caplog.text
        caplog.clear()
        uuid_no_dashes = uuid.replace("-", "").upper()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid_no_dashes])
        assert f'"uuid": "{uuid}"' in caplog.text
        caplog.clear()
        cli(["namespace", "change_load_balancing_group", "--subsystem", subsystem,
             "--nsid", nsid, "--load-balancing-group", "10"])
        assert f"Failure changing load balancing group for namespace with ID {nsid} " \
               f"in {subsystem}" in caplog.text
        assert "Load balancing group 10 doesn't exist" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image3, "--size", "4GB", "--rbd-create-image"])
        assert f"Adding namespace 3 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "3"])
        assert '"rbd_image_size": "4294967296"' in caplog.text
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    def test_add_namespace_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "namespace", "add", "--subsystem", subsystem,
             "--rbd-pool", pool, "--rbd-image", image, "--load-balancing-group", anagrpid,
             "--nsid", "4", "--force"])
        assert f"Adding namespace 4 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "4"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text
        cli(["--server-address", server_addr_ipv6, "namespace", "add", "--subsystem", subsystem,
             "--nsid", "5", "--rbd-pool", pool, "--rbd-image", image,
             "--load-balancing-group", anagrpid, "--force"])
        assert f"Adding namespace 5 to {subsystem}: Successful" in caplog.text
        assert 'will continue as the "force" argument was used' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "5"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    def test_add_namespace_same_image(self, caplog, gateway):
        caplog.clear()
        img_name = f"{image}_test"
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", img_name, "--size", "16MB", "--load-balancing-group", anagrpid,
             "--rbd-create-image", "--nsid", "6", "--uuid", uuid2])
        assert f"Adding namespace 6 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", img_name, "--size", "16MB",
             "--load-balancing-group", anagrpid, "--rbd-create-image", "--nsid", "7"])
        assert f"RBD image {pool}/{img_name} is already used by a namespace" in caplog.text
        assert "you can find the offending namespace by using" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", img_name, "--load-balancing-group", anagrpid, "--force", "--nsid", "7"])
        assert f"Adding namespace 7 to {subsystem}: Successful" in caplog.text
        assert f"RBD image {pool}/{img_name} is already used by a namespace" in caplog.text
        assert 'will continue as the "force" argument was used' in caplog.text

    def test_add_namespace_no_auto_visible(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image5, "--size", "16MB", "--rbd-create-image", "--no-auto-visible"])
        assert f"Adding namespace 8 to {subsystem}: Successful" in caplog.text
        assert "no_auto_visible: True" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image6, "--size", "16MB", "--rbd-create-image", "--no-auto-visible"])
        assert f"Adding namespace 9 to {subsystem}: Successful" in caplog.text
        assert "no_auto_visible: True" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image7, "--size", "16MB", "--rbd-create-image", "--no-auto-visible"])
        assert f"Adding namespace 10 to {subsystem}: Successful" in caplog.text
        assert "no_auto_visible: True" in caplog.text

    def test_add_host_to_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host8])
        assert f"Adding host {host8} to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8", "--host-nqn", host8])
        assert f"Adding host {host8} to namespace 8 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "9", "--host-nqn", host8])
        assert f"Adding host {host8} to namespace 9 on {subsystem}: Successful" in caplog.text

    def test_add_too_many_hosts_to_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8",
             "--host-nqn", host9, "--force"])
        assert f"Adding host {host9} to namespace 8 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8",
             "--host-nqn", host10, "--force"])
        assert f"Adding host {host10} to namespace 8 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8",
             "--host-nqn", host11, "--force"])
        assert f"Failure adding host {host11} to namespace 8 on {subsystem}: " \
               f"Maximal host count for namespace (3) has already been reached" in caplog.text

    def test_add_all_hosts_to_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8", "--host-nqn", "*"])
        assert f"Failure adding host * to namespace 8 on {subsystem}: " \
               f"Host NQN can't be \"*\"" in caplog.text

    def test_add_host_to_namespace_no_access(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem12, "--no-group-append"])
        assert f"Adding subsystem {subsystem12}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem12, "--rbd-pool", pool,
             "--rbd-image", image24, "--size", "16MB", "--rbd-create-image", "--no-auto-visible"])
        assert f"Adding namespace 1 to {subsystem12}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem12, "--nsid", "1",
             "--host-nqn", host12])
        assert f"Failure adding host {host12} to namespace 1 on {subsystem12}: " \
               f"Host is not allowed to access the subsystem" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert '"hosts": []' in caplog.text
        assert f'"{host12}"' not in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem12, "--host-nqn", "*"])
        assert f"Subsystem {subsystem12} will be opened to be accessed from any " \
               f"host. This might be a security breach" in caplog.text
        assert f"Allowing open host access to {subsystem12}: Successful" in caplog.text
        assert f"Open host access to subsystem {subsystem12} might be a " \
               f"security breach" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem12, "--nsid", "1",
             "--host-nqn", host12])
        assert f"Adding host {host12} to namespace 1 on {subsystem12}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert f'"{host12}"' in caplog.text
        assert '"hosts": []' not in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem12, "--nsid", "1",
             "--host-nqn", host12])
        assert f"Deleting host {host12} from namespace 1 on {subsystem12}: " \
               f"Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert f'"{host12}"' not in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem12, "--host-nqn", "*"])
        assert f"Disabling open host access to {subsystem12}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem12, "--nsid", "1",
             "--host-nqn", host12])
        assert f"Failure adding host {host12} to namespace 1 on {subsystem12}: " \
               f"Host is not allowed to access the subsystem" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert '"hosts": []' in caplog.text
        assert f'"{host12}"' not in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem12, "--nsid", "1",
             "--host-nqn", host12, "--force"])
        assert f"Adding host {host12} to namespace 1 on {subsystem12}: Successful" in caplog.text
        assert f"Host {host12} is not allowed to access subsystem {subsystem12} but it will " \
               f"be added to namespace 1 as the \"--force\" parameter " \
               f"was used" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem12, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem12}",' in caplog.text
        assert f'"{host12}"' in caplog.text
        assert '"hosts": []' not in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem12, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem12}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem12])
        assert f"Deleting subsystem {subsystem12}: Successful" in caplog.text

    def test_change_namespace_visibility(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
             "--auto-visible", "yes"])
        assert f"Failure changing visibility for namespace 8 in {subsystem}: " \
               f"Asking to change visibility of namespace to be visible to all hosts while " \
               f"there are already hosts added to it." in caplog.text
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
             "--auto-visible", "yes", "--force"])
        assert f'Asking to change visibility of namespace 8 in {subsystem} to be visible to ' \
               f'all hosts while there are already hosts added to it. Will continue as the ' \
               f'"--force" parameter was used but these hosts will be removed ' \
               f'from the namespace.' in caplog.text
        assert f'Changing visibility of namespace 8 in {subsystem} to ' \
               f'"visible to all hosts": Successful' in caplog.text
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
             "--auto-visible", "yes"])
        assert f'Changing visibility of namespace 8 in {subsystem} to ' \
               f'"visible to all hosts": Successful' in caplog.text
        assert f"No change to namespace 8 in {subsystem} visibility, nothing to do" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "8"])
        assert '"nsid": 8,' in caplog.text
        assert '"auto_visible": true' in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8", "--host-nqn", host8])
        assert f"Failure adding host {host8} to namespace 8 on {subsystem}: " \
               f"Namespace is visible to all hosts" in caplog.text
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
             "--auto-visible", "no"])
        assert f'Changing visibility of namespace 8 in {subsystem} to ' \
               f'"visible to selected hosts": Successful' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "8"])
        assert '"nsid": 8,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8", "--host-nqn", host8])
        assert f"Adding host {host8} to namespace 8 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "8"])
        assert '"nsid": 8,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert '"hosts": []' not in caplog.text
        assert f"{host8}" in caplog.text

    def test_change_namespace_visibility_wrong_params(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --auto-visible" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
                 "--auto-visible"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --auto-visible: expected one argument" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "change_visibility", "--subsystem", subsystem, "--nsid", "8",
                 "--auto-visible", "junk"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --auto-visible: invalid choice: 'junk' " \
               "(choose from 'yes', 'no')" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "change_visibility", "--subsystem", subsystem,
                 "--nsid", "-8", "--auto-visible", "yes"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "nsid value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "change_visibility", "--subsystem", subsystem,
                 "--nsid", "X8", "--auto-visible", "yes"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "argument --nsid: invalid int value" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystem,
             "--nsid", "28", "--auto-visible", "yes"])
        assert f"Failure changing visibility for namespace 28 in {subsystem}: " \
               f"Can't find namespace" in caplog.text
        caplog.clear()
        cli(["namespace", "change_visibility", "--subsystem", subsystemX,
             "--nsid", "8", "--auto-visible", "yes"])
        assert f"Failure changing visibility for namespace 8 in {subsystemX}: " \
               f"Can't find subsystem" in caplog.text

    def test_add_namespace_no_such_subsys(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", f"{subsystem3}", "--rbd-pool", pool,
             "--rbd-image", image13, "--size", "16MB", "--rbd-create-image"])
        assert f"Failure adding namespace to {subsystem3}: No such subsystem"

    def test_add_too_many_namespaces_to_a_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image9, "--nsid", "3000", "--size", "16MB", "--rbd-create-image"])
        assert f"Failure adding namespace using ID 3000 to {subsystem}: " \
               f"Requested ID 3000 is bigger than the maximal one (2039)" in caplog.text
        assert "Received request to delete bdev" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem5, "--no-group-append",
             "--max-namespaces", "1"])
        assert f"Adding subsystem {subsystem5}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem5, "--rbd-pool", pool,
             "--rbd-image", image9, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 1 to {subsystem5}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem5, "--rbd-pool", pool,
             "--rbd-image", image10, "--size", "16MB", "--rbd-create-image"])
        assert f"Failure adding namespace to {subsystem5}: Subsystem's maximal number of " \
               f"namespaces (1) has already been reached" in caplog.text
        assert "Received request to delete bdev" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem5, "--force"])
        assert f"Deleting subsystem {subsystem5}: Successful" in caplog.text

    def test_add_discovery_to_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8",
             "--host-nqn", discovery_nqn])
        assert f"Failure adding host {discovery_nqn} to namespace 8 on {subsystem}: " \
               f"Host NQN can't be a discovery NQN" in caplog.text

    def test_add_junk_host_to_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "8",
             "--host-nqn", "junk"])
        assert f"Failure adding host junk to namespace 8 on {subsystem}: " \
               f"Invalid host NQN" in caplog.text

    def test_add_host_to_namespace_subsystem_not_found(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystemX, "--nsid", "8",
             "--host-nqn", hostxx])
        assert f"Failure adding host {hostxx} to namespace 8 on {subsystemX}: " \
               f"Can't find subsystem" in caplog.text

    def test_add_host_to_wrong_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "1",
             "--host-nqn", host10])
        assert f"Failure adding host {host10} to namespace 1 on {subsystem}: " \
               f"Namespace is visible to all hosts" in caplog.text

    def test_add_too_many_namespaces_with_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image8, "--size", "16MB", "--rbd-create-image", "--no-auto-visible"])
        assert f"Failure adding namespace to {subsystem}: Maximal number of namespaces " \
               f"which are only visible to selected hosts (3) " \
               f"has already been reached" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image14, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 11 to {subsystem}: Successful" in caplog.text

    def test_list_namespace_with_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "9"])
        assert '"nsid": 9,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert f'"{host8}"' in caplog.text
        assert '"hosts": []' not in caplog.text

    def test_del_namespace_host(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "-8",
                 "--host-nqn", host8])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "nsid value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "9", "--host-nqn", "*"])
        assert f"Failure deleting host * from namespace 9 on {subsystem}: " \
               f"Host NQN can't be \"*\"" in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "29",
             "--host-nqn", host8])
        assert f"Failure deleting host {host8} from namespace 29 on {subsystem}: " \
               f"Can't find namespace" in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "1", "--host-nqn", host8])
        assert f"Failure deleting host {host8} from namespace 1 on {subsystem}: " \
               f"Namespace is visible to all hosts" in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystemX, "--nsid", "9",
             "--host-nqn", host8])
        assert f"Failure deleting host {host8} from namespace 9 on {subsystemX}: " \
               f"Can't find subsystem" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "9"])
        assert '"nsid": 9,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert f'"{host8}"' in caplog.text
        assert '"hosts": []' not in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "9", "--host-nqn", host8])
        assert f"Deleting host {host8} from namespace 9 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "9"])
        assert '"nsid": 9,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert f'"{host8}"' not in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "9",
             "--host-nqn", hostxx])
        assert f"Failure deleting host {hostxx} from namespace 9 on {subsystem}: " \
               f"Host is not found in namespace's host list" in caplog.text

    def test_add_namespace_multiple_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host8])
        assert f"Failure adding host {host8} to {subsystem}: Host is already added" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host9])
        assert f"Adding host {host9} to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host10])
        assert f"Adding host {host10} to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", subsystem, "--nsid", "9",
             "--host-nqn", host8, host9, host10])
        assert f"Adding host {host8} to namespace 9 on {subsystem}: Successful" in caplog.text
        assert f"Adding host {host9} to namespace 9 on {subsystem}: Successful" in caplog.text
        assert f"Adding host {host10} to namespace 9 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "9"])
        assert '"nsid": 9,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert f'"{host8}"' in caplog.text
        assert f'"{host9}"' in caplog.text
        assert f'"{host10}"' in caplog.text
        assert '"hosts": []' not in caplog.text

    def test_list_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "host", "list", "--subsystem", subsystem])
        assert '"status": 0,' in caplog.text
        assert f'"subsystem_nqn": "{subsystem}",' in caplog.text
        assert f'"nqn": "{host8}",' in caplog.text
        assert f'"nqn": "{host9}",' in caplog.text
        assert f'"nqn": "{host10}",' in caplog.text
        assert '"use_psk": true,' not in caplog.text
        assert '"use_dhchap": true,' not in caplog.text
        assert '"allow_any_host": false' in caplog.text
        caplog.clear()
        hosts = cli_test(["host", "list", "--subsystem", subsystem])
        assert hosts is not None
        assert hosts.status == 0
        assert not hosts.allow_any_host
        assert hosts.subsystem_nqn == subsystem
        assert len(hosts.hosts) == 3
        assert hosts.hosts[0].nqn in [host8, host9, host10]
        assert hosts.hosts[1].nqn in [host8, host9, host10]
        assert hosts.hosts[2].nqn in [host8, host9, host10]
        assert hosts.hosts[0].nqn != hosts.hosts[1].nqn
        assert hosts.hosts[0].nqn != hosts.hosts[2].nqn
        assert hosts.hosts[1].nqn != hosts.hosts[2].nqn
        assert not hosts.hosts[0].use_psk
        assert not hosts.hosts[1].use_psk
        assert not hosts.hosts[2].use_psk
        assert not hosts.hosts[0].use_dhchap
        assert not hosts.hosts[1].use_dhchap
        assert not hosts.hosts[2].use_dhchap
        assert not hosts.hosts[0].disconnected_due_to_keepalive_timeout
        assert not hosts.hosts[1].disconnected_due_to_keepalive_timeout
        assert not hosts.hosts[2].disconnected_due_to_keepalive_timeout

    def test_del_namespace_multiple_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "del_host", "--subsystem", subsystem, "--nsid", "9",
             "--host-nqn", host8, host9, host10])
        assert f"Deleting host {host8} from namespace 9 on {subsystem}: Successful" in caplog.text
        assert f"Deleting host {host9} from namespace 9 on {subsystem}: Successful" in caplog.text
        assert f"Deleting host {host10} from namespace 9 on {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "9"])
        assert '"nsid": 9,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert f'"{host8}"' not in caplog.text
        assert f'"{host9}"' not in caplog.text
        assert f'"{host10}"' not in caplog.text
        assert '"hosts": []' in caplog.text
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", host8])
        assert f"Removing host {host8} access from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", host9])
        assert f"Removing host {host9} access from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", host10])
        assert f"Removing host {host10} access from {subsystem}: Successful" in caplog.text

    def test_list_namespace_with_no_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "10"])
        assert '"nsid": 10,' in caplog.text
        assert '"auto_visible":' not in caplog.text or '"auto_visible": false' in caplog.text
        assert '"hosts": []' in caplog.text

    def test_add_too_many_namespaces(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image11, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 12 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image12, "--size", "16MB", "--rbd-create-image"])
        assert f"Failure adding namespace to {subsystem}: Maximal number of namespaces (12) " \
               f"has already been reached" in caplog.text

    def test_resize_namespace(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "16777216"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", "junk", "--nsid", "6", "--size", "2MB"])
        assert "Failure resizing namespace 6 on junk: Can't find subsystem" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "2MB"])
        assert "new size 2097152 bytes is smaller than current size 16777216 bytes" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "2"])
        assert "new size 2097152 bytes is smaller than current size 16777216 bytes" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "3145728B"])
        assert "new size 3145728 bytes is smaller than current size 16777216 bytes" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "32MB"])
        assert f"Resizing namespace 6 in {subsystem} to 32 MiB: Successful" in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "32mB"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size=-32MB"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "3x2GB"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "33554432"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "resize", "--subsystem", subsystem, "--uuid", uuid2,
                 "--size", "64MB"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --nsid" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6",
                 "--uuid", uuid2, "--size", "64MB"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: unrecognized arguments: --uuid" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "64MB"])
        assert f"Resizing namespace 6 in {subsystem} to 64 MiB: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--uuid", uuid2])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid2.upper()])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        uuid2_no_dashes = uuid2.replace("-", "")
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid2_no_dashes])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        uuid2_no_dashes = uuid2.replace("-", "").upper()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid2_no_dashes])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        uuid2_some_dashes = uuid2.replace("-", "", 2)
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid2_some_dashes])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        uuid2_some_dashes = uuid2.replace("-", "", 2).upper()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem,
             "--uuid", uuid2_some_dashes])
        assert '"nsid": 6,' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        assert '"nsid": 3,' not in caplog.text
        assert '"nsid": 4,' not in caplog.text
        assert '"nsid": 5,' not in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "22", "--size", "128MB"])
        assert f"Failure resizing namespace 22 on {subsystem}: Can't find namespace" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "32MB"])
        assert f"Failure resizing namespace 6 on {subsystem}: new size 33554432 bytes is " \
               f"smaller than current size 67108864 bytes" in caplog.text
        ns = cli_test(["namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert ns is not None
        assert ns.status == 0
        assert len(ns.namespaces) == 1
        assert ns.namespaces[0].rbd_image_size == 67108864
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "4", "--size", "6GB"])
        assert f"Resizing namespace 4 in {subsystem} to 6 GiB: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "4", "--size", "8192"])
        assert f"Resizing namespace 4 in {subsystem} to 8 GiB: Successful" in caplog.text

    def test_set_namespace_qos_limits(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert '"nsid": 6,' in caplog.text
        assert '"rw_ios_per_second": "0"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", "junk", "--nsid", "6",
             "--rw-ios-per-second", "2000"])
        assert "Failure setting QOS limits for namespace 6 on junk: " \
               "Can't find subsystem" in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6",
             "--rw-ios-per-second", "2000"])
        assert f"Setting QOS limits of namespace 6 in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for " \
               f"namespace 6 on {subsystem}" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert '"nsid": 6,' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6",
             "--rw-megabytes-per-second", "30"])
        assert f"Setting QOS limits of namespace 6 in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for " \
               f"namespace 6 on {subsystem}" not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--uuid", uuid2])
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 6,' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "30"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6",
             "--r-megabytes-per-second", "15", "--w-megabytes-per-second", "25"])
        assert f"Setting QOS limits of namespace 6 in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for " \
               f"namespace 6 on {subsystem}" not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert '"nsid": 6,' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "30"' in caplog.text
        assert '"r_mbytes_per_second": "15"' in caplog.text
        assert '"w_mbytes_per_second": "25"' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: At least one QOS limit should be set" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "set_qos", "--subsystem", subsystem,
                 "--nsid", "6", "--w-megabytes-per-second", "JUNK"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --w-megabytes-per-second: invalid int value: 'JUNK'" in caplog.text
        assert rc == 2

    def test_namespace_io_stats(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "get_io_stats", "--subsystem", "junk", "--nsid", "6"])
        assert "Failure getting IO stats for namespace 6, can't find " \
               "subsystem \"junk\"" in caplog.text
        caplog.clear()
        cli(["namespace", "get_io_stats", "--subsystem", subsystem, "--nsid", "6"])
        assert f'IO statistics for namespace 6 in {subsystem}' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "get_io_stats",
             "--subsystem", subsystem, "--nsid", "6"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem}"' in caplog.text
        assert '"nsid": 6,' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"ticks":' in caplog.text
        assert '"bytes_written":' in caplog.text
        assert '"bytes_read":' in caplog.text
        assert '"max_write_latency_ticks":' in caplog.text
        assert '"io_error":' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "get_io_stats", "--subsystem", subsystem,
                 "--uuid", uuid2, "--nsid", "1"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: unrecognized arguments: --uuid" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["--format", "json", "namespace", "get_io_stats", "--subsystem", subsystem])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --nsid" in caplog.text
        assert rc == 2

    def test_host_missing_nqn(self, caplog):
        caplog.clear()
        rc = 0
        try:
            cli(["host", "add", "--subsystem", subsystem])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --host-nqn/-t" in caplog.text
        assert rc == 2

    def test_add_host_subsys_not_found(self, caplog):
        caplog.clear()
        cli(["host", "add", "--subsystem", "junk", "--host-nqn", host1])
        assert f"Failure adding host {host1} to junk: can't find subsystem junk" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", "junk", "--host-nqn", "*"])
        assert "Failure allowing open host access to junk: can't find subsystem junk" in caplog.text

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host])
        if host == "*":
            assert f"Subsystem {subsystem} will be opened to be accessed from any " \
                   f"host. This might be a security breach" in caplog.text
            assert f"Allowing open host access to {subsystem}: Successful" in caplog.text
            assert f"Open host access to subsystem {subsystem} might be a " \
                   f"security breach" in caplog.text
        else:
            assert f"Adding host {host} to {subsystem}: Successful" in caplog.text

    def test_add_host_invalid_nqn(self, caplog):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "nqn.2016"])
        assert 'NQN "nqn.2016" is too short, minimal length is 11' in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "nqn.2X16-06.io.spdk:host1"])
        assert "invalid date code" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", "nqn.2016-06.io.spdk:host1_X"])
        assert "Invalid host NQN" in caplog.text
        assert "contains invalid characters" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", f"{subsystem}_X",
             "--host-nqn", "nqn.2016-06.io.spdk:host2"])
        assert "Invalid subsystem NQN" in caplog.text
        assert "contains invalid characters" in caplog.text

    def test_host_list(self, caplog):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host-nqn", host5, host6, host7])
        assert f"Adding host {host5} to {subsystem}: Successful" in caplog.text
        assert f"A specific host {host5} was added to subsystem {subsystem} " \
               f"in which all hosts are allowed" in caplog.text
        assert f"Adding host {host6} to {subsystem}: Successful" in caplog.text
        assert f"A specific host {host6} was added to subsystem {subsystem} " \
               f"in which all hosts are allowed" in caplog.text
        assert f"Adding host {host7} to {subsystem}: Successful" in caplog.text
        assert f"A specific host {host7} was added to subsystem {subsystem} " \
               f"in which all hosts are allowed" in caplog.text

    def test_create_litener_wrong_subsystem(self, caplog):
        caplog.clear()
        cli(["listener", "add", "--subsystem", "junk", "--host-name", "host",
             "-a", addr, "-s", "5009", "--verify-host-name"])
        assert f"Failure adding junk listener at {addr}:5009: " \
               f"can't find subsystem junk" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name"] + listener)
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: " \
               f"Successful" in caplog.text

    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_create_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "listener", "add",
             "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name"] + listener_ipv6)
        assert "ipv6" in caplog.text.lower()
        assert f"Adding {subsystem} listener at [{listener_ipv6[1]}]:{listener_ipv6[3]}: " \
               f"Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list_no_port)
    def test_create_listener_no_port(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name"] + listener)
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:4420: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_list_listeners(self, caplog, listener, listener_ipv6, gateway):
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"host_name": "{host_name}",' in caplog.text
        assert f'"traddr": "{listener[1]}",' in caplog.text
        assert f'"trsvcid": {listener[3]},' in caplog.text
        assert '"adrfam": "ipv4"' in caplog.text
        assert f'"traddr": "[{listener_ipv6[1]}]",' in caplog.text
        assert f'"trsvcid": {listener_ipv6[3]},' in caplog.text
        assert '"adrfam": "ipv6"' in caplog.text
        assert '"active": true,' in caplog.text
        assert '"active": false,' not in caplog.text

    def test_list_listeners_bad_subsys(self, caplog, gateway):
        caplog.clear()
        cli(["listener", "list", "--subsystem", "junk"])
        assert 'Failure listing listeners: No such subsystem "junk"' in caplog.text

    @pytest.mark.parametrize("listener", listener_list_negative_port)
    def test_create_listener_negative_port(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                 "--verify-host-name"] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: trsvcid value must be positive" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("listener", listener_list_big_port)
    def test_create_listener_port_too_big(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                 "--verify-host-name"] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: trsvcid value must be smaller than 65536" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("listener", listener_list_wrong_host)
    def test_create_listener_wrong_hostname(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--verify-host-name"] + listener)
        assert f"Gateway's host name must match current host ({host_name})" in caplog.text
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem] + listener)
        assert f"Adding {subsystem} listener at {listener[3]}:{listener[5]}: " \
               f"listener will only be active when appropriate gateway is up" in caplog.text
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"host_name": "{listener[1]}",' in caplog.text
        assert f'"traddr": "{listener[3]}",' in caplog.text
        assert f'"trsvcid": {listener[5]},' in caplog.text
        assert f'"adrfam": "{listener[7]}"' in caplog.text
        assert '"active": false,' in caplog.text

    @pytest.mark.parametrize("listener", listener_list_bad_ips)
    def test_create_listener_bad_ips(self, caplog, listener, gateway):
        gw, stub = gateway
        traddr = GatewayUtils.unescape_address(listener[0])
        listener_add_req = pb2.create_listener_req(
            nqn=subsystem,
            host_name=host_name,
            adrfam=listener[2],
            traddr=listener[0],
            trsvcid=listener[1],
            verify_host_name=True)
        caplog.clear()
        stub.create_listener(listener_add_req)
        assert f"Failure adding {subsystem} listener at {listener[0]}:{listener[1]}: " \
               f"Address {traddr} is not available" in caplog.text

    @pytest.mark.parametrize("listener", listener_list_invalid_adrfam)
    def test_create_listener_invalid_adrfam(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
                 "--verify-host-name"] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --adrfam/-f: invalid choice: 'JUNK'" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("listener", listener_list_discovery)
    def test_create_listener_on_discovery(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--host-name", host_name, "--verify-host-name"] + listener)
        assert "Can't create a listener for a discovery subsystem" in caplog.text

    def test_list_namespaces_all_subsystems(self, caplog):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem9, "--no-group-append"])
        assert f"Adding subsystem {subsystem9}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", "junk", "--nsid", "10"])
        assert "Failure deleting namespace 10, can't find subsystem \"junk\"" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "10"])
        assert f"Deleting namespace 10 from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "11"])
        assert f"Deleting namespace 11 from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "12"])
        assert f"Deleting namespace 12 from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem9, "--rbd-pool", pool,
             "--rbd-image", image15, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 1 to {subsystem9}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem9, "--rbd-pool", pool,
             "--rbd-image", image16, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 2 to {subsystem9}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list"])
        assert '"subsystem_nqn": "*"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem9}"' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert '"nsid": 2,' in caplog.text
        assert '"nsid": 6,' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--nsid", "1"])
        assert '"subsystem_nqn": "*"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem9}"' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert '"nsid": 2,' not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--nsid", "6"])
        assert '"subsystem_nqn": "*"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem9}"' not in caplog.text
        assert '"nsid": 6,' in caplog.text
        assert '"nsid": 1,' not in caplog.text
        assert '"nsid": 2,' not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--uuid", uuid])
        assert '"subsystem_nqn": "*"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"ns_subsystem_nqn": "{subsystem9}"' not in caplog.text
        assert f'"uuid": "{uuid}"' in caplog.text
        caplog.clear()
        cli(["namespace", "list", "--nsid", "1"])
        assert "Cluster" not in caplog.text
        assert "(Configured)" not in caplog.text
        caplog.clear()
        cli(["--verbose", "namespace", "list", "--nsid", "1"])
        assert "Cluster" in caplog.text
        assert "(Configured)" in caplog.text
        assert "cluster_context_1_0" in caplog.text
        assert f"{image2}" in caplog.text
        assert f"{image15}" in caplog.text
        assert "1 (1)" in caplog.text

    def test_namespace_count_updated(self, caplog):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem5, "--no-group-append"])
        assert f"Adding subsystem {subsystem5}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem5, "--rbd-pool", pool,
             "--rbd-image", image17, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 1 to {subsystem5}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem5, "--rbd-pool", pool,
             "--rbd-image", image18, "--size", "16MB", "--rbd-create-image"])
        assert f"Failure adding namespace to {subsystem5}: Maximal number of namespaces (12) " \
               f"has already been reached" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem9, "--force"])
        assert f"Deleting subsystem {subsystem9}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem5, "--rbd-pool", pool,
             "--rbd-image", image18, "--size", "16MB", "--rbd-create-image"])
        assert f"Adding namespace 2 to {subsystem5}: Successful" in caplog.text


class TestDelete:
    @pytest.mark.parametrize("host", host_list)
    def test_remove_host(self, caplog, host, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["host", "del", "--subsystem", subsystem])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --host-nqn/-t" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", host])
        if host == "*":
            assert f"Disabling open host access to {subsystem}: Successful" in caplog.text
        else:
            assert f"Removing host {host} access from {subsystem}: Successful" in caplog.text

    def test_remove_not_existing_host(self, caplog, gateway):
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", hostxx])
        assert f"Failure removing host {hostxx} access from {subsystem}: " \
               f"Host is not found" in caplog.text

    def remove_host_list(self, caplog):
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host-nqn", host5, host6, host7])
        assert f"Removing host {host5} access from {subsystem}: Successful" in caplog.text
        assert f"Removing host {host6} access from {subsystem}: Successful" in caplog.text
        assert f"Removing host {host7} access from {subsystem}: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_using_wild_hostname_no_force(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "del", "--subsystem", subsystem, "--host-name", "*"] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: must use --force when setting host name to *" in caplog.text
        assert rc == 2

    def test_delete_non_existing_listener(self, caplog, gateway):
        caplog.clear()
        cli(["listener", "del", "--subsystem", subsystem, "--host-name", host_name,
             "--traddr", "4.4.4.4", "--trsvcid", "1234"])
        assert f"Failed to delete listener 4.4.4.4:1234 from {subsystem}: " \
               f"Listener not found" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "del", "--force", "--subsystem", subsystem,
             "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} " \
               f"for host {host_name}: Successful" in caplog.text

    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_delete_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "listener", "del", "--subsystem", subsystem,
             "--host-name", host_name] + listener_ipv6)
        assert f"Deleting listener [{listener_ipv6[1]}]:{listener_ipv6[3]} from {subsystem} " \
               f"for host {host_name}: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list_no_port)
    def test_delete_listener_no_port(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "del", "--subsystem", subsystem, "--host-name", host_name] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --trsvcid/-s" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["listener", "del", "--trsvcid", "4420", "--subsystem", subsystem,
             "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:4420 from {subsystem} for host {host_name}: " \
               f"Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_using_wild_hostname(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name"] + listener)
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: " \
               f"Successful" in caplog.text
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"host_name": "{host_name}"' in caplog.text
        assert f'"traddr": "{listener[1]}"' in caplog.text
        assert f'"trsvcid": {listener[3]}' in caplog.text
        caplog.clear()
        cli(["listener", "del", "--force", "--subsystem", subsystem, "--host-name", "*"] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} for all hosts: " \
               f"Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"trsvcid": {listener[3]}' not in caplog.text

    def test_remove_namespace(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        ns_list = cli_test(["namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert ns_list is not None
        assert ns_list.status == 0
        assert len(ns_list.namespaces) == 1
        bdev_name = ns_list.namespaces[0].bdev_name
        assert bdev_name
        bdev_found = False
        with gw.rpc_lock:
            bdev_list = rpc_bdev.bdev_get_bdevs(gw.spdk_rpc_client)
        for b in bdev_list:
            try:
                if bdev_name == b["name"]:
                    bdev_found = True
                    break
            except KeyError:
                print(f"Couldn't find field name in: {b}")
        assert bdev_found
        caplog.clear()
        del_ns_req = pb2.namespace_delete_req(subsystem_nqn=subsystem)
        stub.namespace_delete(del_ns_req)
        assert "Failure deleting namespace, missing ID" in caplog.text
        caplog.clear()
        del_ns_req = pb2.namespace_delete_req(nsid=1)
        stub.namespace_delete(del_ns_req)
        assert "Failure deleting namespace 1, missing subsystem NQN" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "6"])
        assert f"Deleting namespace 6 from {subsystem}: Successful" in caplog.text
        bdev_found = False
        with gw.rpc_lock:
            bdev_list = rpc_bdev.bdev_get_bdevs(gw.spdk_rpc_client)
        for b in bdev_list:
            try:
                if bdev_name == b["name"]:
                    bdev_found = True
                    break
            except KeyError:
                print(f"Couldn't find field name in: {b}")
        assert not bdev_found
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "2"])
        assert f"Deleting namespace 2 from {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "4"])
        assert f"Deleting namespace 4 from {subsystem}: Successful" in caplog.text

    def test_delete_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem])
        assert f"Failure deleting subsystem {subsystem}: Namespace 2 is still using the subsystem"
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem, "--force"])
        assert f"Deleting subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem2])
        assert f"Deleting subsystem {subsystem2}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem5, "--force"])
        assert f"Deleting subsystem {subsystem5}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text

    def test_delete_subsystem_with_discovery_nqn(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["subsystem", "del", "--subsystem", discovery_nqn])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "Can't delete a discovery subsystem" in caplog.text
        assert rc == 2


class TestCreateWithAna:
    def test_create_subsystem_ana(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert serial not in caplog.text
        assert subsystem in caplog.text

    def test_add_namespace_ana(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image, "--load-balancing-group", anagrpid, "--force", "--nsid", "10"])
        assert f"Adding namespace 10 to {subsystem}: Successful" in caplog.text
        assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "10"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name"] + listener)
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: " \
               f"Successful" in caplog.text


class TestDeleteAna:
    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "del", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} for " \
               f"host {host_name}: Successful" in caplog.text

    def test_remove_namespace_ana(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "10"])
        assert f"Deleting namespace 10 from {subsystem}: Successful" in caplog.text

    def test_delete_subsystem_ana(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem])
        assert f"Deleting subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text


class TestSubsysWithGroupName:
    def test_create_subsys_group_name(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem3])
        assert f"Adding subsystem {subsystem3}.{group_name}: Successful" in caplog.text
        assert f"Subsystem NQN was changed to {subsystem3}.{group_name}, " \
               f"adding the group name" in caplog.text
        assert f"Adding subsystem {subsystem3}: Successful" not in caplog.text
        cli(["--format", "json", "subsystem", "list"])
        assert f'"nqn": "{subsystem3}.{group_name}"' in caplog.text
        assert f'"nqn": "{subsystem3}"' not in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem4, "--no-group-append"])
        assert f"Adding subsystem {subsystem4}: Successful" in caplog.text
        assert "Subsystem NQN will not be changed" in caplog.text
        assert f"Adding subsystem {subsystem4}.{group_name}: Successful" not in caplog.text
        cli(["--format", "json", "subsystem", "list"])
        assert f'"nqn": "{subsystem4}.{group_name}"' not in caplog.text
        assert f'"nqn": "{subsystem4}"' in caplog.text


class TestTooManySubsystemsAndHosts:
    def test_add_too_many_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem6, "--no-group-append",
             "--max-namespaces", "12"])
        assert f"The requested max number of namespaces for subsystem {subsystem6} (12) " \
               f"is greater than the limit on the number of namespaces per subsystem (11), " \
               f"will continue" in caplog.text
        assert f"Adding subsystem {subsystem6}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem7, "--no-group-append"])
        assert f"Adding subsystem {subsystem7}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem8, "--no-group-append"])
        assert f"Failure creating subsystem {subsystem8}: Maximal number of subsystems (4) has " \
               f"already been reached" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem7])
        assert f"Deleting subsystem {subsystem7}: Successful" in caplog.text

    def test_too_many_hosts(self, caplog, gateway):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem6, "--host-nqn", host1])
        assert f"Adding host {host1} to {subsystem6}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem6, "--host-nqn", host2])
        assert f"Adding host {host2} to {subsystem6}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem6, "--host-nqn", host3])
        assert f"Adding host {host3} to {subsystem6}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem6, "--host-nqn", host4])
        assert f"Adding host {host4} to {subsystem6}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem6, "--host-nqn", host5])
        assert f"Failure adding host {host5} to {subsystem6}: Maximal number of hosts for " \
               f"subsystem (4) has already been reached" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem7, "--no-group-append"])
        assert f"Adding subsystem {subsystem7}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem7, "--host-nqn", host1])
        assert f"Adding host {host1} to {subsystem7}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem7, "--host-nqn", host2])
        assert f"Adding host {host2} to {subsystem7}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem7, "--host-nqn", host3])
        assert f"Failure adding host {host3} to {subsystem7}: Maximal number of hosts " \
               f"(6) has already been reached" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem7])
        assert f"Deleting subsystem {subsystem7}: Successful" in caplog.text


class TestGwLogLevel:
    def test_gw_log_level(self, caplog, gateway):
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "debug"' in caplog.text
        caplog.clear()
        cli(["gw", "set_log_level", "--level", "error"])
        assert 'Set gateway log level to "error": Successful' in caplog.text
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "error"' in caplog.text
        caplog.clear()
        cli(["gw", "set_log_level", "-l", "CRITICAL"])
        assert 'Set gateway log level to "critical": Successful' in caplog.text
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "critical"' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["gw", "set_log_level", "-l", "JUNK"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --level/-l: invalid choice: 'JUNK'" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["--format", "json", "gw", "get_log_level"])
        assert '"log_level": "critical"' in caplog.text
        caplog.clear()
        cli(["--log-level", "critical", "gw", "set_log_level", "--level", "DEBUG"])
        assert 'Set gateway log level to "debug": Successful' not in caplog.text
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "debug"' in caplog.text


class TestSPDKLOg:
    def test_log_flags(self, caplog, gateway):
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK log flag "nvmf" is disabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is disabled' in caplog.text
        assert "virtio" not in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get", "--all-log-flags"])
        assert 'SPDK log flag "nvmf" is disabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is disabled' in caplog.text
        assert 'SPDK log flag "virtio" is disabled' in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set", "--level", "DEBUG"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is DEBUG' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set", "--print", "error"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is DEBUG' in caplog.text
        assert 'SPDK log print level is ERROR' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "disable"])
        assert "Disable SPDK log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK log flag "nvmf" is disabled' in caplog.text
        assert 'SPDK log flag "nvmf_tcp" is disabled' in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["spdk_log_level", "set", "-l", "JUNK"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --level/-l: invalid choice: 'JUNK'" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["spdk_log_level", "set", "--extra-log-flags", "virtio", "vmd"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get", "--all-log-flags"])
        assert 'SPDK log flag "virtio" is enabled' in caplog.text
        assert 'SPDK log flag "vmd" is enabled' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "disable", "--extra-log-flags", "virtio", "vmd"])
        assert "Disable SPDK log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get", "--all-log-flags"])
        assert 'SPDK log flag "virtio" is disabled' in caplog.text
        assert 'SPDK log flag "vmd" is disabled' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["spdk_log_level", "set", "--extra-log-flags"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --extra-log-flags/-e: expected at least one argument" in caplog.text
        assert rc == 2


class TestDeleteRBDImage:
    def test_delete_rbd_image(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem10, "--no-group-append"])
        assert f"Adding subsystem {subsystem10}: Successful" in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
                 "--rbd-image", image19, "--rbd-trash-image-on-delete"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: Can't trash associated RBD image on delete if it wasn't " \
               "created automatically by the gateway" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
             "--rbd-image", image19, "--rbd-create-image", "--size", "16MB",
             "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 1 to {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem10, "--nsid", "1"])
        assert f"Failure deleting namespace 1 from {subsystem10}: Confirmation for trashing " \
               "RBD image is needed"
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem10, "--nsid", "1", "--i-am-sure"])
        assert f"Deleting namespace 1 from {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
             "--rbd-image", image19])
        assert f"Failure adding namespace to {subsystem10}: Failure creating bdev" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
             "--rbd-image", image19, "--rbd-create-image", "--size", "32MB"])
        assert f"Adding namespace 1 to {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem10, "--nsid", "1"])
        assert f"Deleting namespace 1 from {subsystem10}: Successful" in caplog.text


class TestSubsystemWithIdenticalPrefix:
    def test_subsystem_with_identical_prefix(self, caplog, gateway):
        gw, stub = gateway
        # Make sure one NQN is a prefix of the other
        assert subsystem10.startswith(subsystem)
        # Clean old subsystems as we are limited to only 4
        subs = cli_test(["subsystem", "list"])
        for s in subs.subsystems:
            caplog.clear()
            cli(["subsystem", "del", "--subsystem", s.nqn, "--force"])
            assert f"Deleting subsystem {s.nqn}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text
        # OK, all clear, now we can add the subsystems where one NQN is a prefix of the other
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem10, "--no-group-append"])
        assert f"Adding subsystem {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image20, "--rbd-create-image", "--size", "16MB",
             "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
             "--rbd-image", image21, "--rbd-create-image", "--size", "16MB",
             "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 2 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
             "--rbd-image", image22, "--rbd-create-image", "--size", "16MB",
             "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 1 to {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem10, "--rbd-pool", pool,
             "--rbd-image", image23, "--rbd-create-image", "--size", "16MB",
             "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 2 to {subsystem10}: Successful" in caplog.text
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name,
             "--verify-host-name", "-a", addr, "-s", "4440", "-f", "ipv4"])
        assert f"Adding {subsystem} listener at {addr}:{4440}: Successful" in caplog.text
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem10, "--host-name", host_name,
             "--verify-host-name", "-a", addr, "-s", "4450", "-f", "ipv4"])
        assert f"Adding {subsystem10} listener at {addr}:{4450}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert '"trsvcid": 4440,' in caplog.text
        assert '"trsvcid": 4450,' not in caplog.text
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem10])
        assert '"trsvcid": 4440,' not in caplog.text
        assert '"trsvcid": 4450,' in caplog.text
        found = 0
        found10 = 0
        state = gw.gateway_state.omap.get_state()
        for key, val in state.items():
            if not key.startswith(gw.gateway_state.local.NAMESPACE_PREFIX):
                continue
            valstr = val.decode()
            if f'"subsystem_nqn": "{subsystem}",' in valstr:
                found += 1
            elif f'"subsystem_nqn": "{subsystem10}",' in valstr:
                found10 += 1
        assert found == 2
        assert found10 == 2
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem, "--force"])
        assert f"Deleting subsystem {subsystem}: Successful" in caplog.text
        found = 0
        found10 = 0
        state = gw.gateway_state.omap.get_state()
        for key, val in state.items():
            if not key.startswith(gw.gateway_state.local.NAMESPACE_PREFIX):
                continue
            valstr = val.decode()
            if f'"subsystem_nqn": "{subsystem}",' in valstr:
                found += 1
            elif f'"subsystem_nqn": "{subsystem10}",' in valstr:
                found10 += 1
        assert found == 0
        assert found10 == 2


class TestListenerBadIPAddresses:
    def test_listener_bad_ip_adresses(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem11, "--no-group-append"])
        assert f"Adding subsystem {subsystem11}: Successful" in caplog.text
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem11, "--traddr", "3.4",
                 "--trsvcid", "4620", "--host-name", host_name])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: invalid IP address 3.4" in caplog.text
        assert rc == 2
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem11, "--traddr", "192.44.32.43",
                 "--adrfam", "ipv6", "--trsvcid", "4620", "--host-name", host_name])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: IP address 192.44.32.43 is not an IPv6 address" in caplog.text
        assert rc == 2
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem11, "--traddr", "::",
                 "--adrfam", "ipv4", "--trsvcid", "4620", "--host-name", host_name])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: IP address :: is not an IPv4 address" in caplog.text
        assert rc == 2
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem11, "--traddr", "::",
                 "--trsvcid", "4620", "--host-name", host_name])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: IP address :: is not an IPv4 address" in caplog.text
        assert rc == 2


class TestImageResize:
    def test_namespace_no_auto_resize(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem11, "--rbd-pool", pool,
             "--rbd-image", image25, "--size", "10MB",
             "--rbd-create-image", "--disable-auto-resize"])
        assert f"Adding namespace 1 to {subsystem11}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem11, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem11}",' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert f'"rbd_image_name": "{image25}",' in caplog.text
        assert '"disable_auto_resize": true,' in caplog.text
        caplog.clear()
        cli(["namespace", "set_auto_resize", "--subsystem", "junk", "--nsid", "1",
             "--auto-resize-enabled", "yes"])
        assert 'Failure setting auto resize flag for namespace 1, ' \
               'can\'t find subsystem "junk"' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "set_auto_resize", "--subsystem", subsystem11, "--nsid", "1",
                 "--auto-resize-enabled", "junk"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --auto-resize-enabled: invalid choice: 'junk' " \
               "(choose from 'yes', 'no')" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "set_auto_resize", "--subsystem", subsystem11, "--nsid", "1",
             "--auto-resize-enabled", "yes"])
        assert f"Setting auto resize flag for namespace 1 in {subsystem11} to " \
               f"\"auto resize namespace\": Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem11, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem11}",' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert f'"rbd_image_name": "{image25}",' in caplog.text
        assert '"disable_auto_resize": false,' in caplog.text
        caplog.clear()
        cli(["namespace", "set_auto_resize", "--subsystem", subsystem11, "--nsid", "1",
             "--auto-resize-enabled", "no"])
        assert f"Setting auto resize flag for namespace 1 in {subsystem11} to " \
               f"\"do not auto resize namespace\": Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem11, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem11}",' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert f'"rbd_image_name": "{image25}",' in caplog.text
        assert '"disable_auto_resize": true,' in caplog.text


class TestReadOnlyNamespace:
    def test_read_only_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem13, "--no-group-append"])
        assert f"Adding subsystem {subsystem13}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem13, "--rbd-pool", pool,
             "--rbd-image", image26, "--size", "10MB",
             "--rbd-create-image", "--read-only"])
        assert f"Adding namespace 1 to {subsystem13}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem13, "--nsid", "1"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem13}",' in caplog.text
        assert '"nsid": 1,' in caplog.text
        assert f'"rbd_image_name": "{image26}",' in caplog.text
        assert '"read_only": true,' in caplog.text
        assert '"read_only": false,' not in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem13, "--rbd-pool", pool,
             "--rbd-image", image27, "--size", "10MB",
             "--rbd-create-image"])
        assert f"Adding namespace 2 to {subsystem13}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem13, "--nsid", "2"])
        assert '"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem13}",' in caplog.text
        assert '"nsid": 2,' in caplog.text
        assert f'"rbd_image_name": "{image27}",' in caplog.text
        assert '"read_only": false,' in caplog.text
        assert '"read_only": true,' not in caplog.text


class TestTrashImageOnSubsysDel:
    def test_trash_image_on_subsys_del(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem14, "--no-group-append"])
        assert f"Adding subsystem {subsystem14}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem14, "--rbd-pool", pool,
             "--rbd-image", image28, "--size", "10MB",
             "--rbd-create-image", "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 1 to {subsystem14}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem14, "--rbd-pool", pool,
             "--rbd-image", image28])
        assert f"RBD image {pool}/{image28} is already used by a namespace"
        cli(["subsystem", "del", "--subsystem", subsystem14, "--force"])
        assert f"Deleting subsystem {subsystem14}: Successful" in caplog.text
        assert f"Failure deleting namespace 1 from {subsystem14}: Confirmation for " \
               f"trashing RBD image is needed." in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem15, "--no-group-append"])
        assert f"Adding subsystem {subsystem15}: Successful" in caplog.text
        cli(["namespace", "add", "--subsystem", subsystem15, "--rbd-pool", pool,
             "--rbd-image", image29, "--size", "10MB",
             "--rbd-create-image", "--rbd-trash-image-on-delete"])
        assert f"Adding namespace 1 to {subsystem15}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem15, "--force", "--i-am-sure"])
        assert f"Deleting subsystem {subsystem15}: Successful" in caplog.text
        assert f"Failure deleting namespace 1 from {subsystem15}: Confirmation for " \
               f"trashing RBD image is needed." not in caplog.text


class TestSubsystemsCache:
    def test_subsystems_cache(self, caplog, gateway):
        gw, _ = gateway
        subs = cli_test(["subsystem", "list"])
        for s in subs.subsystems:
            cli(["subsystem", "del", "--subsystem", s.nqn,
                 "--force", "--i-am-sure"])
        subs = cli_test(["subsystem", "list"])
        assert len(subs.subsystems) == 0
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem16, "--no-group-append"])
        assert f"Adding subsystem {subsystem16}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem17, "--no-group-append"])
        assert f"Adding subsystem {subsystem17}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem18, "--no-group-append",
             "--serial-number", serial2])
        assert f"Adding subsystem {subsystem18}: Successful" in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' not in caplog.text
        assert f'"nqn": "{subsystem17}",' not in caplog.text
        assert f'"nqn": "{subsystem18}",' not in caplog.text
        assert f'"serial_number": "{serial2}",' not in caplog.text
        # Only after the call to "subsystem list" we should get a fresh cache
        caplog.clear()
        cli(["--format", "json", "subsystem", "list"])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem16, "--host-nqn", host13])
        assert f"Adding host {host13} to {subsystem16}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem17, "--host-nqn", host14])
        assert f"Adding host {host14} to {subsystem17}: Successful" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem18, "--host-nqn", host15])
        assert f"Adding host {host15} to {subsystem18}: Successful" in caplog.text
        host_list_req = pb2.list_hosts_req(subsystem=subsystem16)
        caplog.clear()
        ret = gw.list_hosts(host_list_req)
        assert ret.status == 0
        assert host13 not in caplog.text
        assert "Received request to list hosts" in caplog.text
        host_list_req = pb2.list_hosts_req(subsystem=subsystem17)
        caplog.clear()
        ret = gw.list_hosts(host_list_req)
        assert ret.status == 0
        assert host14 not in caplog.text
        assert "Received request to list hosts" in caplog.text
        host_list_req = pb2.list_hosts_req(subsystem=subsystem18)
        caplog.clear()
        ret = gw.list_hosts(host_list_req)
        assert ret.status == 0
        assert host15 not in caplog.text
        assert "Received request to list hosts" in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        assert host13 not in caplog.text
        assert host14 not in caplog.text
        assert host15 not in caplog.text
        caplog.clear()
        cli(["--format", "json", "host", "list", "--subsystem", subsystem16, "--clear-alerts"])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{host13}",' in caplog.text
        caplog.clear()
        cli(["--format", "json", "host", "list", "--subsystem", subsystem17])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{host14}",' in caplog.text
        caplog.clear()
        cli(["--format", "json", "host", "list", "--subsystem", subsystem18])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{host15}",' in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        assert host13 not in caplog.text
        assert host14 not in caplog.text
        assert host15 not in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list"])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        assert host13 in caplog.text
        assert host14 in caplog.text
        assert host15 in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list", "--serial", serial2])
        assert '"status": 0' in caplog.text
        assert f'"nqn": "{subsystem16}",' not in caplog.text
        assert f'"nqn": "{subsystem17}",' not in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem18}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        assert host13 in caplog.text
        assert host14 in caplog.text
        assert host15 in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list"])
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem16])
        assert f"Deleting subsystem {subsystem16}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem17])
        assert f"Deleting subsystem {subsystem17}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "del", "--subsystem", subsystem18])
        assert f"Deleting subsystem {subsystem18}: Successful" in caplog.text
        # Subsystems should still be in the cache
        caplog.clear()
        cli(["get_subsystems"])
        assert '"subsystems": []' not in caplog.text
        assert f'"nqn": "{subsystem16}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"nqn": "{subsystem17}",' in caplog.text
        assert f'"serial_number": "{serial2}",' in caplog.text
        assert host13 in caplog.text
        assert host14 in caplog.text
        assert host15 in caplog.text
        caplog.clear()
        cli(["--format", "json", "subsystem", "list"])
        assert '"status": 0' in caplog.text
        assert '"subsystems": []' in caplog.text
        assert f'"nqn": "{subsystem16}",' not in caplog.text
        assert f'"nqn": "{subsystem17}",' not in caplog.text
        assert f'"nqn": "{subsystem18}",' not in caplog.text
        caplog.clear()
        cli(["get_subsystems"])
        assert '"subsystems": []' in caplog.text
        assert f'"nqn": "{subsystem16}",' not in caplog.text
        assert f'"nqn": "{subsystem17}",' not in caplog.text
        assert f'"nqn": "{subsystem17}",' not in caplog.text
        assert f'"serial_number": "{serial2}",' not in caplog.text
        assert host13 not in caplog.text
        assert host14 not in caplog.text
        assert host15 not in caplog.text
