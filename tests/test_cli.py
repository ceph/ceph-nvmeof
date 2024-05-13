import pytest
from control.server import GatewayServer
import socket
from control.cli import main as cli
from control.cli import main_test as cli_test
from control.cephutils import CephUtils
import spdk.rpc.bdev as rpc_bdev
import grpc
from control.proto import gateway_pb2 as pb2
from control.proto import gateway_pb2_grpc as pb2_grpc
import os
import copy

image = "mytestdevimage"
image2 = "mytestdevimage2"
image3 = "mytestdevimage3"
image4 = "mytestdevimage4"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
subsystem2 = "nqn.2016-06.io.spdk:cnode2"
discovery_nqn = "nqn.2014-08.org.nvmexpress.discovery"
serial = "Ceph00000000000001"
uuid = "948878ee-c3b2-4d58-a29b-2cff713fc02d"
uuid2 = "948878ee-c3b2-4d58-a29b-2cff713fc02e"
host_list = ["nqn.2016-06.io.spdk:host1", "*"]
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
listener_list_ipv6 = [["-a", addr_ipv6, "-s", "5003", "--adrfam", "ipv6"], ["-a", addr_ipv6, "-s", "5004", "--adrfam", "IPV6"]]
listener_list_discovery = [["-n", discovery_nqn, "-t", host_name, "-a", addr, "-s", "5012"]]
listener_list_negative_port = [["-t", host_name, "-a", addr, "-s", "-2000"]]
listener_list_big_port = [["-t", host_name, "-a", addr, "-s", "70000"]]
listener_list_wrong_host = [["-t", "WRONG", "-a", addr, "-s", "5015", "-f", "ipv4"]]
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
        spdk_ver = os.getenv("NVMEOF_SPDK_VERSION")
        gw_info = cli_test(["gw", "info"])
        assert gw_info != None
        assert gw_info.cli_version == cli_ver
        assert gw_info.version == cli_ver
        assert gw_info.spdk_version == spdk_ver
        assert gw_info.name == gw.gateway_name
        assert gw_info.hostname == gw.host_name
        assert gw_info.status == 0
        assert gw_info.bool_status == True

class TestCreate:
    def test_create_subsystem(self, caplog, gateway):
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016"])
        assert f'NQN "nqn.2016" is too short, minimal length is 11' in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem",
"nqn.2016-06XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"])
        assert f"is too long, maximal length is 223" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2014-08.org.nvmexpress:uuid:0"])
        assert f"UUID is not the correct length" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2014-08.org.nvmexpress:uuid:9e9134-3cb431-4f3e-91eb-a13cefaabebf"])
        assert f"UUID is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "qqn.2016-06.io.spdk:cnode1"])
        assert f"doesn't start with" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.016-206.io.spdk:cnode1"])
        assert f"invalid date code" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2X16-06.io.spdk:cnode1"])
        assert f"invalid date code" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io.spdk:"])
        assert f"must contain a user specified name starting with" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io..spdk:cnode1"])
        assert f"reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.spdk:cnode1"])
        assert f"reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", "nqn.2016-06.io.-spdk:cnode1"])
        assert f"reverse domain is not formatted correctly" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", f"{subsystem}_X"])
        assert f"Invalid NQN" in caplog.text
        assert f"contains invalid characters" in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem, "--max-namespaces", "2049"])
        assert f"create_subsystem {subsystem}: True" in caplog.text
        cli(["--format", "json", "subsystem", "list"])
        assert f'"serial_number": "{serial}"' not in caplog.text
        assert f'"nqn": "{subsystem}"' in caplog.text
        assert f'"max_namespaces": 2049' in caplog.text
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", subsystem2, "--serial-number", serial])
        assert f"create_subsystem {subsystem2}: True" in caplog.text
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
        assert f"No subsystem with serial number JUNK" in caplog.text
        caplog.clear()
        cli(["subsystem", "list", "--subsystem", "JUNK"])
        assert f"Failure listing subsystems: No such device" in caplog.text
        assert f'"nqn": "JUNK"' in caplog.text
        caplog.clear()
        subs_list = cli_test(["--format", "text", "subsystem", "list"])
        assert subs_list != None
        assert subs_list.status == 0
        assert subs_list.subsystems[0].nqn == subsystem
        assert subs_list.subsystems[1].nqn == subsystem2
        caplog.clear()
        subs_list = cli_test(["subsystem", "list"])
        assert subs_list != None
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

    def test_add_namespace_wrong_balancing_group(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image4, "--size", "16MB", "--rbd-create-image", "--load-balancing-group", "100", "--force"])
        assert f"Failure adding namespace to {subsystem}:" in caplog.text
        assert f"Load balancing group 100 doesn't exist" in caplog.text

    def test_add_namespace_wrong_size(self, caplog, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", "junkimage", "--size", "0", "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", "junkimage", "--size", "1026KB", "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be aligned to MiBs" in caplog.text
        assert rc == 2

    def test_add_namespace_wrong_size_grpc(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        add_namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem, rbd_pool_name=pool, rbd_image_name="junkimage",
                                                  block_size=512, create_image=True, size=16*1024*1024+20)
        ret = stub.namespace_add(add_namespace_req)
        assert ret.status != 0
        assert f"Failure adding namespace" in caplog.text
        assert f"image size must be aligned to MiBs" in caplog.text

    def test_add_namespace_wrong_block_size(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        add_namespace_req = pb2.namespace_add_req(subsystem_nqn=subsystem, rbd_pool_name=pool, rbd_image_name="junkimage",
                                                  create_image=True, size=16*1024*1024, force=True)
        ret = stub.namespace_add(add_namespace_req)
        assert ret.status != 0
        assert f"Failure adding namespace" in caplog.text
        assert f"block size can't be zero" in caplog.text

    def test_add_namespace(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", "junk", "--rbd-image", image2, "--uuid", uuid, "--size", "16MB", "--rbd-create-image", "--load-balancing-group", anagrpid])
        assert f"RBD pool junk doesn't exist" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--uuid", uuid, "--size", "16MB", "--rbd-create-image", "--load-balancing-group", anagrpid, "--force"])
        assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
        assert f"Allocated cluster name='cluster_context_{anagrpid}_0'" in caplog.text
        assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--size", "36", "--rbd-create-image", "--load-balancing-group", anagrpid, "--force"])
        assert f"Image {pool}/{image2} already exists with a size of 16777216 bytes which differs from the requested size of 37748736 bytes" in caplog.text
        assert f"Can't create RBD image {pool}/{image2}" in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--block-size", "1024", "--size", "16MB", "--load-balancing-group", anagrpid])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size argument is not allowed for add command when RBD image creation is disabled" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--block-size", "1024", "--size=-16MB", "--rbd-create-image", "--load-balancing-group", anagrpid])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "size value must be positive" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--block-size", "1024", "--size", "1x6MB", "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--block-size", "1024", "--size", "16MiB", "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        rc = 0
        try:
            cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image2, "--block-size", "1024", "--size", "16mB", "--load-balancing-group", anagrpid, "--rbd-create-image"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "must be numeric" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--block-size", "1024", "--load-balancing-group", anagrpid, "--force"])
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
        cli(["namespace", "change_load_balancing_group", "--subsystem", subsystem, "--nsid", nsid, "--load-balancing-group", "10"])
        assert f"Failure changing load balancing group for namespace using NSID {nsid}" in caplog.text
        assert f"Load balancing group 10 doesn't exist" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image3, "--size", "4GB", "--rbd-create-image"])
        assert f"Adding namespace 3 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "3"])
        assert '"rbd_image_size": "4294967296"' in caplog.text
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    def test_add_namespace_ipv6(self, caplog, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image,  "--load-balancing-group", anagrpid, "--nsid", "4", "--force"])
        assert f"Adding namespace 4 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "4"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text
        cli(["--server-address", server_addr_ipv6, "namespace", "add", "--subsystem", subsystem, "--nsid", "5", "--rbd-pool", pool, "--rbd-image", image,  "--load-balancing-group", anagrpid, "--force"])
        assert f"Adding namespace 5 to {subsystem}: Successful" in caplog.text
        assert f'will continue as the "force" argument was used' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "5"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    def test_add_namespace_same_image(self, caplog, gateway):
        caplog.clear()
        img_name = f"{image}_test"
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", img_name, "--size", "16MB", "--load-balancing-group", anagrpid, "--rbd-create-image", "--nsid", "6", "--uuid", uuid2])
        assert f"Adding namespace 6 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", img_name, "--size", "16MB", "--load-balancing-group", anagrpid, "--rbd-create-image", "--nsid", "7"])
        assert f"RBD image {pool}/{img_name} is already used by a namespace" in caplog.text
        assert f"you can find the offending namespace by using" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", img_name, "--load-balancing-group", anagrpid, "--force", "--nsid", "7"])
        assert f"Adding namespace 7 to {subsystem}: Successful" in caplog.text
        assert f"RBD image {pool}/{img_name} is already used by a namespace" in caplog.text
        assert f'will continue as the "force" argument was used' in caplog.text

    def test_resize_namespace(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert f'"nsid": 6' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "16777216"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "2MB"])
        assert f"new size 2097152 bytes is smaller than current size 16777216 bytes" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "2"])
        assert f"new size 2097152 bytes is smaller than current size 16777216 bytes" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "3145728B"])
        assert f"new size 3145728 bytes is smaller than current size 16777216 bytes" in caplog.text
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
        assert f'"nsid": 6' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "33554432"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1' not in caplog.text
        assert '"nsid": 2' not in caplog.text
        assert '"nsid": 3' not in caplog.text
        assert '"nsid": 4' not in caplog.text
        assert '"nsid": 5' not in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--uuid", uuid2, "--size", "64MB"])
        assert f"Resizing namespace with UUID {uuid2} in {subsystem} to 64 MiB: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--uuid", uuid2])
        assert f'"nsid": 6' in caplog.text
        assert '"block_size": 512' in caplog.text
        assert '"rbd_image_size": "67108864"' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"nsid": 1' not in caplog.text
        assert '"nsid": 2' not in caplog.text
        assert '"nsid": 3' not in caplog.text
        assert '"nsid": 4' not in caplog.text
        assert '"nsid": 5' not in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "12", "--uuid", uuid, "--size", "128MB"])
        assert f"Failure resizing namespace using NSID 12 and UUID {uuid} on {subsystem}: Can't find namespace" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "32MB"])
        assert f"Failure resizing namespace using NSID 6 on {subsystem}: new size 33554432 bytes is smaller than current size 67108864 bytes" in caplog.text
        ns = cli_test(["namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert ns != None
        assert ns.status == 0
        assert len(ns.namespaces) == 1
        assert ns.namespaces[0].rbd_image_size == 67108864
        rc = rpc_bdev.bdev_rbd_delete(gw.spdk_rpc_client, name=ns.namespaces[0].bdev_name)
        assert rc
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "6", "--size", "128MB"])
        assert f"Failure resizing namespace using NSID 6 on {subsystem}: Can't find namespace" in caplog.text
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--nsid", "6", "--rbd-pool", pool, "--rbd-image", image, "--uuid", uuid2, "--force",  "--load-balancing-group", anagrpid, "--force"])
        assert f"Adding namespace 6 to {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "4", "--size", "6GB"])
        assert f"Resizing namespace 4 in {subsystem} to 6 GiB: Successful" in caplog.text
        caplog.clear()
        cli(["namespace", "resize", "--subsystem", subsystem, "--nsid", "4", "--size", "8192"])
        assert f"Resizing namespace 4 in {subsystem} to 8 GiB: Successful" in caplog.text

    def test_set_namespace_qos_limits(self, caplog, gateway):
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert f'"nsid": 6' in caplog.text
        assert '"rw_ios_per_second": "0"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6", "--rw-ios-per-second", "2000"])
        assert f"Setting QOS limits of namespace 6 in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace using NSID 6 on {subsystem}" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert f'"nsid": 6' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "0"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--uuid", uuid2, "--rw-megabytes-per-second", "30"])
        assert f"Setting QOS limits of namespace with UUID {uuid2} in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace using NSID 6 on {subsystem}" not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--uuid", uuid2])
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert f'"nsid": 6' in caplog.text
        assert '"rw_ios_per_second": "2000"' in caplog.text
        assert '"rw_mbytes_per_second": "30"' in caplog.text
        assert '"r_mbytes_per_second": "0"' in caplog.text
        assert '"w_mbytes_per_second": "0"' in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6",
             "--r-megabytes-per-second", "15", "--w-megabytes-per-second", "25"])
        assert f"Setting QOS limits of namespace 6 in {subsystem}: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace using NSID 6 on {subsystem}" not in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert f'"nsid": 6' in caplog.text
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
            cli(["namespace", "set_qos", "--subsystem", subsystem, "--nsid", "6", "--w-megabytes-per-second", "JUNK"])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --w-megabytes-per-second: invalid int value: 'JUNK'" in caplog.text
        assert rc == 2

    def test_namespace_io_stats(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "get_io_stats", "--subsystem", subsystem, "--nsid", "6"])
        assert f'IO statistics for namespace 6 in {subsystem}' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "get_io_stats", "--subsystem", subsystem, "--nsid", "6"])
        assert f'"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"nsid": 6' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert f'"ticks":' in caplog.text
        assert f'"bytes_written":' in caplog.text
        assert f'"bytes_read":' in caplog.text
        assert f'"max_write_latency_ticks":' in caplog.text
        assert f'"io_error":' in caplog.text
        caplog.clear()
        cli(["namespace", "get_io_stats", "--subsystem", subsystem, "--uuid", uuid2])
        assert f'IO statistics for namespace with UUID {uuid2} in {subsystem}' in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "get_io_stats", "--subsystem", subsystem, "--uuid", uuid2])
        assert f'"status": 0' in caplog.text
        assert f'"subsystem_nqn": "{subsystem}"' in caplog.text
        assert f'"nsid": 6' in caplog.text
        assert f'"uuid": "{uuid2}"' in caplog.text
        assert f'"ticks":' in caplog.text
        assert f'"bytes_written":' in caplog.text
        assert f'"bytes_read":' in caplog.text
        assert f'"max_write_latency_ticks":' in caplog.text
        assert f'"io_error":' in caplog.text
        caplog.clear()
        rc = 0
        try:
            cli(["--format", "json", "namespace", "get_io_stats", "--subsystem", subsystem])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: At least one of --nsid or --uuid arguments is mandatory for get_io_stats command" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("host", host_list)
    def test_add_host(self, caplog, host):
        caplog.clear()
        rc = 0
        try:
            cli(["host", "add", "--subsystem", subsystem])
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: the following arguments are required: --host/-t" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host", host])
        if host == "*":
            assert f"Allowing open host access to {subsystem}: Successful" in caplog.text
        else:
            assert f"Adding host {host} to {subsystem}: Successful" in caplog.text

    def test_add_host_invalid_nqn(self, caplog):
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host", "nqn.2016"])
        assert f'NQN "nqn.2016" is too short, minimal length is 11' in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host", "nqn.2X16-06.io.spdk:host1"])
        assert f"invalid date code" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", subsystem, "--host", "nqn.2016-06.io.spdk:host1_X"])
        assert f"Invalid host NQN" in caplog.text
        assert f"contains invalid characters" in caplog.text
        caplog.clear()
        cli(["host", "add", "--subsystem", f"{subsystem}_X", "--host", "nqn.2016-06.io.spdk:host2"])
        assert f"Invalid subsystem NQN" in caplog.text
        assert f"contains invalid characters" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert "enable_ha: True" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: Successful" in caplog.text


    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_create_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener_ipv6)
        assert "enable_ha: True" in caplog.text
        assert "ipv6" in caplog.text.lower()
        assert f"Adding {subsystem} listener at [{listener_ipv6[1]}]:{listener_ipv6[3]}: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list_no_port)
    def test_create_listener_no_port(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert "enable_ha: True" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:4420: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_list_listeners(self, caplog, listener, listener_ipv6, gateway):
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"host_name": "{host_name}"' in caplog.text
        assert f'"traddr": "{listener[1]}"' in caplog.text
        assert f'"trsvcid": {listener[3]}' in caplog.text
        assert f'"adrfam": "ipv4"' in caplog.text
        assert f'"traddr": "[{listener_ipv6[1]}]"' in caplog.text
        assert f'"trsvcid": {listener_ipv6[3]}' in caplog.text
        assert f'"adrfam": "ipv6"' in caplog.text

    @pytest.mark.parametrize("listener", listener_list_negative_port)
    def test_create_listener_negative_port(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
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
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: trsvcid value must be smaller than 65536" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("listener", listener_list_wrong_host)
    def test_create_listener_wrong_hostname(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem] + listener)
        assert f"Gateway's host name must match current host ({host_name})" in caplog.text

    @pytest.mark.parametrize("listener", listener_list_invalid_adrfam)
    def test_create_listener_invalid_adrfam(self, caplog, listener, gateway):
        caplog.clear()
        rc = 0
        try:
            cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        except SystemExit as sysex:
            rc = int(str(sysex))
            pass
        assert "error: argument --adrfam/-f: invalid choice: 'JUNK'" in caplog.text
        assert rc == 2

    @pytest.mark.parametrize("listener", listener_list_discovery)
    def test_create_listener_on_discovery(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--host-name", host_name] + listener)
        assert "Can't create a listener for a discovery subsystem" in caplog.text

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
        assert "error: the following arguments are required: --host/-t" in caplog.text
        assert rc == 2
        caplog.clear()
        cli(["host", "del", "--subsystem", subsystem, "--host", host])
        if host == "*":
            assert f"Disabling open host access to {subsystem}: Successful" in caplog.text
        else:
            assert f"Removing host {host} access from {subsystem}: Successful" in caplog.text

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

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "del", "--force", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} for host {host_name}: Successful" in caplog.text

    @pytest.mark.parametrize("listener_ipv6", listener_list_ipv6)
    def test_delete_listener_ipv6(self, caplog, listener_ipv6, gateway):
        caplog.clear()
        cli(["--server-address", server_addr_ipv6, "listener", "del", "--subsystem", subsystem, "--host-name", host_name] + listener_ipv6)
        assert f"Deleting listener [{listener_ipv6[1]}]:{listener_ipv6[3]} from {subsystem} for host {host_name}: Successful" in caplog.text

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
        cli(["listener", "del", "--trsvcid", "4420", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:4420 from {subsystem} for host {host_name}: Successful" in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_using_wild_hostname(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert "enable_ha: True" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: Successful" in caplog.text
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"host_name": "{host_name}"' in caplog.text
        assert f'"traddr": "{listener[1]}"' in caplog.text
        assert f'"trsvcid": {listener[3]}' in caplog.text
        caplog.clear()
        cli(["listener", "del", "--force", "--subsystem", subsystem, "--host-name", "*"] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} for all hosts: Successful" in caplog.text
        caplog.clear()
        cli(["--format", "json", "listener", "list", "--subsystem", subsystem])
        assert f'"trsvcid": {listener[3]}' not in caplog.text

    def test_remove_namespace(self, caplog, gateway):
        gw, stub = gateway
        caplog.clear()
        ns_list = cli_test(["namespace", "list", "--subsystem", subsystem, "--nsid", "6"])
        assert ns_list != None
        assert ns_list.status == 0
        assert len(ns_list.namespaces) == 1
        bdev_name = ns_list.namespaces[0].bdev_name
        assert bdev_name
        bdev_found = False
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
        ret = stub.namespace_delete(del_ns_req)
        assert "At least one of NSID or UUID should be specified for finding a namesapce" in caplog.text
        caplog.clear()
        cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "6"])
        assert f"Deleting namespace 6 from {subsystem}: Successful" in caplog.text
        assert f"Error removing namespace's QOS limits, they might not have been set" not in caplog.text
        bdev_found = False
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
        assert f"Error removing namespace's QOS limits, they might not have been set" in caplog.text
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
        cli(["subsystem", "add", "--subsystem", subsystem])
        assert f"Adding subsystem {subsystem}: Successful" in caplog.text
        caplog.clear()
        cli(["subsystem", "list"])
        assert serial not in caplog.text
        assert subsystem in caplog.text

    def test_add_namespace_ana(self, caplog, gateway):
        caplog.clear()
        cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--load-balancing-group", anagrpid, "--force", "--nsid", "10"])
        assert f"Adding namespace 10 to {subsystem}: Successful" in caplog.text
        assert f"get_cluster cluster_name='cluster_context_{anagrpid}_0'" in caplog.text
        caplog.clear()
        cli(["--format", "json", "namespace", "list", "--subsystem", subsystem, "--nsid", "10"])
        assert f'"load_balancing_group": {anagrpid}' in caplog.text

    @pytest.mark.parametrize("listener", listener_list)
    def test_create_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "add", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert "enable_ha: True" in caplog.text
        assert "ipv4" in caplog.text.lower()
        assert f"Adding {subsystem} listener at {listener[1]}:{listener[3]}: Successful" in caplog.text

class TestDeleteAna:

    @pytest.mark.parametrize("listener", listener_list)
    def test_delete_listener_ana(self, caplog, listener, gateway):
        caplog.clear()
        cli(["listener", "del", "--subsystem", subsystem, "--host-name", host_name] + listener)
        assert f"Deleting listener {listener[1]}:{listener[3]} from {subsystem} for host {host_name}: Successful" in caplog.text

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

class TestGwLogLevel:
    def test_gw_log_level(self, caplog, gateway):
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "debug"' in caplog.text
        caplog.clear()
        cli(["gw", "set_log_level", "--level", "error"])
        assert f'Set gateway log level to "error": Successful' in caplog.text
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "error"' in caplog.text
        caplog.clear()
        cli(["gw", "set_log_level", "-l", "CRITICAL"])
        assert f'Set gateway log level to "critical": Successful' in caplog.text
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
        assert f'"log_level": "critical"' in caplog.text
        caplog.clear()
        cli(["--log-level", "critical", "gw", "set_log_level", "--level", "DEBUG"])
        assert f'Set gateway log level to "debug": Successful' not in caplog.text
        caplog.clear()
        cli(["gw", "get_log_level"])
        assert 'Gateway log level is "debug"' in caplog.text

class TestSPDKLOg:
    def test_log_flags(self, caplog, gateway):
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK nvmf log flag "nvmf" is disabled' in caplog.text
        assert 'SPDK nvmf log flag "nvmf_tcp" is disabled' in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK nvmf log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK nvmf log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is NOTICE' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set", "--level", "DEBUG"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK nvmf log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK nvmf log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is DEBUG' in caplog.text
        assert 'SPDK log print level is INFO' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "set", "--print", "error"])
        assert "Set SPDK log levels and nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK nvmf log flag "nvmf" is enabled' in caplog.text
        assert 'SPDK nvmf log flag "nvmf_tcp" is enabled' in caplog.text
        assert 'SPDK log level is DEBUG' in caplog.text
        assert 'SPDK log print level is ERROR' in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "disable"])
        assert "Disable SPDK nvmf log flags: Successful" in caplog.text
        caplog.clear()
        cli(["spdk_log_level", "get"])
        assert 'SPDK nvmf log flag "nvmf" is disabled' in caplog.text
        assert 'SPDK nvmf log flag "nvmf_tcp" is disabled' in caplog.text
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
