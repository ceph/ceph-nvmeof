import grpc
import pytest
import time
import rbd
import rados

from control.cephutils import CephUtils
from control.cli import main as cli
from control.proto import gateway_pb2_grpc as pb2_grpc
from control.server import GatewayServer

pool = "rbd"
pool_junkapp = "junkapp"
pool_rbdapp = "rbdapp"
pool_rbd_nvme_app = "data_rbd_nvme_app"
data_pool_junkapp = "data_junkapp"
data_pool_rbdapp = "data_rbdapp"
data_pool_rbd_nvme_app = "rbd_nvme_app"
app_junk = "junk"
app_nvme = "NVMe"
group_name = "GROUPNAME"
subsystem1 = "nqn.2016-06.io.spdk:cnode1"
junk_image1 = "junk_image1"
junk_image2 = "junk_image2"
junk_image3 = "junk_image3"
image1 = "mytestdevimage01"
image2 = "mytestdevimage02"
image3 = "mytestdevimage03"
image4 = "mytestdevimage04"
image5 = "mytestdevimage05"
image6 = "mytestdevimage06"
image7 = "mytestdevimage07"
image8 = "mytestdevimage08"
image9 = "mytestdevimage09"
image10 = "mytestdevimage10"
restarted_gw = None


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up a Gateway"""
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = group_name
    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    ceph_utils = CephUtils(config)
    gateway = GatewayServer(config)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gateway.gw_logger_object.set_log_level("debug")
    gateway.serve()
    channel = grpc.insecure_channel(f"{addr}:{port}")
    pb2_grpc.GatewayStub(channel)
    return gateway


def pool_init(pool_name):
    with rados.Rados(conffile="/etc/ceph/ceph.conf", rados_id="") as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            rbd_inst.pool_init(ioctx, True)


def create_image(pool_name, img, size, data_pool=None):
    with rados.Rados(conffile="/etc/ceph/ceph.conf", rados_id="") as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            rbd_inst = rbd.RBD()
            rbd_inst.create(ioctx, img, size, data_pool=data_pool)


def test_setup_environment(caplog, gateway):
    gw = gateway
    gw_config = gw.gateway_rpc.config
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem1, "--no-group-append"])
    assert f"Adding subsystem {subsystem1}: Successful" in caplog.text
    ceph_utils = CephUtils(gw_config)
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{pool_junkapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{pool_junkapp}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{pool_junkapp}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{pool_rbdapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{pool_rbdapp}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{pool_rbdapp}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{pool_rbd_nvme_app}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{pool_rbd_nvme_app}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{pool_rbd_nvme_app}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{data_pool_junkapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{data_pool_junkapp}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{data_pool_junkapp}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{data_pool_rbdapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{data_pool_rbdapp}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{data_pool_rbdapp}\' created")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool create", "pool": "{data_pool_rbd_nvme_app}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool create", "pool": ' \
           f'"{data_pool_rbd_nvme_app}"}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "pool \'{data_pool_rbd_nvme_app}\' created")' in caplog.text
    pool_init(pool_junkapp)
    pool_init(pool_rbdapp)
    pool_init(pool_rbd_nvme_app)
    pool_init(data_pool_junkapp)
    pool_init(data_pool_rbdapp)
    pool_init(data_pool_rbd_nvme_app)
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{pool_junkapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{pool_junkapp}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{pool_rbdapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{pool_rbdapp}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{pool_rbd_nvme_app}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{pool_rbd_nvme_app}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{data_pool_junkapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{data_pool_junkapp}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{data_pool_rbdapp}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{data_pool_rbdapp}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application get", "pool": "{data_pool_rbd_nvme_app}"}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application get", "pool": ' \
           f'"{data_pool_rbd_nvme_app}"}}' in caplog.text
    assert 'Monitor reply: (0, b\'{\\n    "rbd": {}\\n}\\n\', \'\')' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application enable", "pool": "{pool_junkapp}", '
        f'"app": "{app_junk}", "yes_i_really_mean_it": true}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application enable", "pool": ' \
           f'"{pool_junkapp}", "app": "{app_junk}", "yes_i_really_mean_it": true}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "enabled application \'{app_junk}\' on pool ' \
           f'\'{pool_junkapp}\'")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application enable", "pool": "{pool_rbd_nvme_app}", '
        f'"app": "{app_nvme}", "yes_i_really_mean_it": true}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application enable", "pool": ' \
           f'"{pool_rbd_nvme_app}", "app": "{app_nvme}", ' \
           f'"yes_i_really_mean_it": true}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "enabled application \'{app_nvme}\' on pool ' \
           f'\'{pool_rbd_nvme_app}\'")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application enable", "pool": "{data_pool_junkapp}", '
        f'"app": "{app_junk}", "yes_i_really_mean_it": true}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application enable", "pool": ' \
           f'"{data_pool_junkapp}", "app": "{app_junk}", ' \
           f'"yes_i_really_mean_it": true}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "enabled application \'{app_junk}\' on pool ' \
           f'\'{data_pool_junkapp}\'")' in caplog.text
    caplog.clear()
    ceph_utils.execute_ceph_monitor_command(
        f'{{"prefix":"osd pool application enable", "pool": "{data_pool_rbd_nvme_app}", '
        f'"app": "{app_nvme}", "yes_i_really_mean_it": true}}'
    )
    assert f'Execute monitor command: {{"prefix":"osd pool application enable", "pool": ' \
           f'"{data_pool_rbd_nvme_app}", "app": "{app_nvme}", ' \
           f'"yes_i_really_mean_it": true}}' in caplog.text
    assert f'Monitor reply: (0, b\'\', "enabled application \'{app_nvme}\' on pool ' \
           f'\'{data_pool_rbd_nvme_app}\'")' in caplog.text
    create_image(pool_junkapp, junk_image1, 16 * 1024 * 1024)
    create_image(pool, junk_image2, 16 * 1024 * 1024, data_pool_junkapp)
    create_image(pool, junk_image3, 16 * 1024 * 1024, data_pool_junkapp)


def test_create_image_with_junk_app(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_junkapp,
         "--rbd-image", image1, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD pool "{pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t create image ' \
           f'{pool_junkapp}/{image1}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_create_image_with_junk_app_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_junkapp,
         "--rbd-image", image1, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t create image ' \
           f'{pool}/{image1}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_create_image_with_junk_app_data_image_exist(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool,
         "--rbd-image", junk_image3, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool}/{junk_image3}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text
    assert f'Passed data pool name "{pool}" differs from actual ' \
           f'one "{data_pool_junkapp}"' in caplog.text


def test_create_image_with_junk_app_data_image_exist_empty_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-image", junk_image3, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool}/{junk_image3}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text
    assert f'Passed data pool name "{pool}" differs from actual ' \
           f'one "{data_pool_junkapp}"' not in caplog.text


def test_use_existing_image_with_junk_app(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_junkapp,
         "--rbd-image", junk_image1])
    assert f'Failure adding namespace to {subsystem1}: RBD pool "{pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool_junkapp}/{junk_image1}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_use_existing_image_with_junk_app_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_junkapp, "--rbd-image", junk_image2])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool}/{junk_image2}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_use_existing_image_with_junk_app_data_wrong_name(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", pool, "--rbd-image", junk_image3])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool}/{junk_image3}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text
    assert f'Passed data pool name "{pool}" differs from actual ' \
           f'one "{data_pool_junkapp}"' in caplog.text


def test_use_existing_image_with_junk_app_data_empty_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-image", junk_image3])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t use image ' \
           f'{pool}/{junk_image3}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text
    assert f'Passed data pool name "{pool}" differs from actual ' \
           f'one "{data_pool_junkapp}"' not in caplog.text


def test_create_image_rbd_app(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_rbdapp,
         "--rbd-image", image1, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 1 to {subsystem1}: Successful" in caplog.text


def test_create_image_with_rbd_nvme_app(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_rbd_nvme_app,
         "--rbd-image", image2, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD pool "{pool_rbd_nvme_app}" has ' \
           f'application "NVMe" enabled, can\'t create image ' \
           f'{pool_rbd_nvme_app}/{image2}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_create_image_with_rbd_nvme_app_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_rbd_nvme_app,
         "--rbd-image", image7, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool ' \
           f'"{data_pool_rbd_nvme_app}" has ' \
           f'application "NVMe" enabled, can\'t create image ' \
           f'{pool}/{image7}.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_create_image_default_pool(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-image", image3, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 2 to {subsystem1}: Successful" in caplog.text


def test_create_image_with_rbd_nvme_app_with_nvme_allowed(caplog, gateway):
    global restarted_gw
    gw = gateway
    gw_config = gw.gateway_rpc.config
    gw_config.config["gateway"]["allowed_pool_applications"] = "rbd,     NVMe,  stam,,,,"
    gw.server.stop(grace=1)
    gw.__exit__(None, None, None)
    print("Restarting gateway")
    time.sleep(90)
    gw = GatewayServer(gw_config)
    restarted_gw = gw
    ceph_utils = CephUtils(gw_config)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gw.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gw.serve()
    time.sleep(30)
    assert gw.gateway_rpc.up_and_running
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_rbd_nvme_app,
         "--rbd-image", image4, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 3 to {subsystem1}: Successful" in caplog.text


def test_create_image_with_rbd_nvme_app_with_nvme_allowed_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_rbd_nvme_app,
         "--rbd-image", image8, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 4 to {subsystem1}: Successful" in caplog.text


def test_create_image_with_junk_app_when_nvme_allowed(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_junkapp,
         "--rbd-image", image5, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD pool "{pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t create image ' \
           f'{pool_junkapp}/{image5}.' in caplog.text
    assert "Allowed applications are: rbd, NVMe, stam" in caplog.text


def test_create_image_with_junk_app_when_nvme_allowed_data(caplog, gateway):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_junkapp,
         "--rbd-image", image9, "--size", "16MB", "--rbd-create-image"])
    assert f'Failure adding namespace to {subsystem1}: RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, can\'t create image ' \
           f'{pool}/{image9}.' in caplog.text
    assert "Allowed applications are: rbd, NVMe, stam" in caplog.text


def test_create_image_with_no_pool_app_verification(caplog):
    assert restarted_gw, "Don't have the restarted gateway object"
    gw = restarted_gw
    gw_config = gw.gateway_rpc.config
    gw_config.config["gateway"].pop("allowed_pool_applications", None)
    gw_config.config["gateway"]["verify_pool_applications"] = "false"
    gw.server.stop(grace=1)
    gw.__exit__(None, None, None)
    print("Restarting gateway again")
    time.sleep(90)
    gw = GatewayServer(gw_config)
    ceph_utils = CephUtils(gw_config)
    ceph_utils.execute_ceph_monitor_command(
        "{" + f'"prefix":"nvme-gw create", "id": "{gw.name}", "pool": "{pool}", '
        f'"group": "{group_name}"' + "}"
    )
    gw.serve()
    time.sleep(30)
    assert gw.gateway_rpc.up_and_running
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_junkapp,
         "--rbd-image", image6, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 5 to {subsystem1}: Successful" in caplog.text
    assert f'RBD pool "{pool_junkapp}" has ' \
           f'application "junk" enabled, will create image ' \
           f'{pool_junkapp}/{image6} anyway as pool application verification ' \
           f'is disabled.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_create_image_with_no_pool_app_verification_data(caplog):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_junkapp,
         "--rbd-image", image10, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 6 to {subsystem1}: Successful" in caplog.text
    assert f'RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, will create image ' \
           f'{pool}/{image10} anyway as pool application verification ' \
           f'is disabled.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_use_existing_image_with_junk_app_no_pool_app_verification(caplog):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool_junkapp,
         "--rbd-image", junk_image1])
    assert f"Adding namespace 7 to {subsystem1}: Successful" in caplog.text
    assert f'RBD pool "{pool_junkapp}" has ' \
           f'application "junk" enabled, will use image ' \
           f'{pool_junkapp}/{junk_image1} anyway as pool application verification ' \
           f'is disabled.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text


def test_use_existing_image_with_junk_app_no_pool_app_verification_data(caplog):
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem1, "--rbd-pool", pool,
         "--rbd-data-pool", data_pool_junkapp,
         "--rbd-image", junk_image2])
    assert f"Adding namespace 8 to {subsystem1}: Successful" in caplog.text
    assert f'RBD data pool "{data_pool_junkapp}" has ' \
           f'application "junk" enabled, will use image ' \
           f'{pool}/{junk_image2} anyway as pool application verification ' \
           f'is disabled.' in caplog.text
    assert "Allowed application is: rbd" in caplog.text
