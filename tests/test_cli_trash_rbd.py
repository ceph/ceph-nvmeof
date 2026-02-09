import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import copy
import time

image = "mytestdevimage"
image2 = "image2"
image3 = "image3"
image4 = "image4"
pool = "rbd"
subsystem = "nqn.2016-06.io.spdk:cnode1"
config = "ceph-nvmeof.conf"


@pytest.fixture(scope="module")
def two_gateways(config):
    """Sets up and tears down two Gateways"""
    nameA = "GatewayAA"
    nameB = "GatewayBB"
    sockA = f"spdk_{nameA}.sock"
    sockB = f"spdk_{nameB}.sock"
    config.config["gateway-logs"]["log_level"] = "debug"
    config.config["gateway"]["group"] = "group1"
    config.config["gateway"]["state_update_notify"] = "False"
    config.config["gateway"]["state_update_interval_sec"] = "20"
    addr = config.get("gateway", "addr")
    configA = copy.deepcopy(config)
    configB = copy.deepcopy(config)
    configA.config["gateway"]["name"] = nameA
    configA.config["gateway"]["override_hostname"] = nameA
    configA.config["spdk"]["rpc_socket_name"] = sockA
    configA.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (0-1)"
    portA = configA.getint("gateway", "port")
    configB.config["gateway"]["name"] = nameB
    configB.config["gateway"]["override_hostname"] = nameB
    configB.config["spdk"]["rpc_socket_name"] = sockB
    portB = portA + 2
    discPortB = configB.getint("discovery", "port") + 1
    configB.config["gateway"]["port"] = str(portB)
    configB.config["discovery"]["port"] = str(discPortB)
    configB.config["spdk"]["tgt_cmd_extra_args"] = "--lcores (2-3)"

    ceph_utils = CephUtils(config)
    with (GatewayServer(configA) as gatewayA,
          GatewayServer(configB) as gatewayB):
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameA}", "pool": "{pool}",'
            f'"group": "group1"' + "}"
        )
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{nameB}", "pool": "{pool}",'
            f'"group": "group1"' + "}"
        )
        gatewayA.serve()
        gatewayB.serve()

        channelA = grpc.insecure_channel(f"{addr}:{portA}")
        stubA = pb2_grpc.GatewayStub(channelA)
        channelB = grpc.insecure_channel(f"{addr}:{portB}")
        stubB = pb2_grpc.GatewayStub(channelB)

        yield gatewayA, stubA, gatewayB, stubB, ceph_utils
        gatewayA.gateway_rpc.gateway_state.delete_state()
        gatewayB.gateway_rpc.gateway_state.delete_state()
        gatewayA.server.stop(grace=1)
        gatewayB.server.stop(grace=1)


def test_rbd_image_trash(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB, ceph_utils = two_gateways
    caplog.clear()
    cli(["subsystem", "add", "--subsystem", subsystem, "--no-group-append"])
    assert f"create_subsystem {subsystem}: True" in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image2, "--size", "16MB", "--rbd-create-image",
         "--rbd-trash-image-on-delete"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    time.sleep(30)
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image2}"' in caplog.text
    assert '"trash_image": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image2}"' in caplog.text
    assert '"trash_image": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "subsystem", "list"])
    assert f'"nqn": "{subsystem}"' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1", "--i-am-sure"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
    assert ceph_utils.does_image_exist(pool, image2)
    time.sleep(30)    # wait for second gateway to delete namespace and RBD image
    # now the RBD image should be gone
    assert not ceph_utils.does_image_exist(pool, image2)
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert '"namespaces": []' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list"])
    assert '"namespaces": []' in caplog.text
    caplog.clear()
    if not ceph_utils.does_image_exist(pool, image):
        ceph_utils.create_image(pool, None, None, image, 16777216)
    assert ceph_utils.does_image_exist(pool, image)
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image, "--size", "16MB", "--rbd-create-image",
         "--rbd-trash-image-on-delete"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    assert f"Notice that as image {pool}/{image} was created outside the gateway " \
           f"it won't get trashed on namespace deletion" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image}"' in caplog.text
    assert '"trash_image": false' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert '"namespaces": []' in caplog.text
    assert ceph_utils.does_image_exist(pool, image)
    ceph_utils.delete_image(pool, image)
    assert not ceph_utils.does_image_exist(pool, image)


def test_change_rbd_image_trash(caplog, two_gateways):
    gatewayA, stubA, gatewayB, stubB, ceph_utils = two_gateways
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert '"namespaces": []' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image3, "--size", "16MB", "--rbd-create-image",
         "--rbd-trash-image-on-delete"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    time.sleep(30)
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image3}"' in caplog.text
    assert '"trash_image": true' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image3}"' in caplog.text
    assert '"trash_image": true' in caplog.text
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
             "--nsid", "1"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "error: the following arguments are required: " \
           "--rbd-trash-image-on-delete" in caplog.text
    assert rc == 2
    caplog.clear()
    rc = 0
    try:
        cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
             "--nsid", "1", "--rbd-trash-image-on-delete", "junk"])
    except SystemExit as sysex:
        rc = int(str(sysex))
        pass
    assert "error: argument --rbd-trash-image-on-delete: invalid choice: 'junk' (choose from " \
           "'yes', 'no', 'true', 'false', '1', '0')" in caplog.text
    assert rc == 2
    caplog.clear()
    cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
         "--nsid", "1", "--rbd-trash-image-on-delete", "yes"])
    assert f'Setting RBD trash image flag for namespace 1 in {subsystem} to ' \
           f'"trash on namespace deletion": Successful' in caplog.text
    assert f"Namespace 1 in {subsystem} already has the RBD trash image flag set, " \
           f"nothing to do" in caplog.text
    caplog.clear()
    cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
         "--nsid", "2", "--rbd-trash-image-on-delete", "no"])
    assert f"Failure setting RBD trash image flag for namespace 2 in {subsystem}: " \
           f"Can't find namespace" in caplog.text
    caplog.clear()
    cli(["namespace", "set_rbd_trash_image", "--subsystem", subsystem,
         "--nsid", "1", "--rbd-trash-image-on-delete", "no"])
    assert f'Setting RBD trash image flag for namespace 1 in {subsystem} to ' \
           f'"do not trash on namespace deletion": Successful' in caplog.text
    time.sleep(30)
    assert f"Received request to remove namespace 1 from {subsystem}" not in caplog.text
    assert f"Received request to add namespace 1 to {subsystem}" not in caplog.text
    assert f'Received request to set the RBD trash image flag of namespace 1 in ' \
           f'{subsystem} to "do not trash on namespace deletion", context: ' \
           f'<grpc._server' in caplog.text
    assert f'Received request to set the RBD trash image flag of namespace 1 in ' \
           f'{subsystem} to "do not trash on namespace deletion", context: None' in caplog.text
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image3}"' in caplog.text
    assert '"trash_image": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image3}"' in caplog.text
    assert '"trash_image": false' in caplog.text
    caplog.clear()
    cli(["namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
    time.sleep(30)
    assert ceph_utils.does_image_exist(pool, image3)
    ceph_utils.delete_image(pool, image3)
    assert not ceph_utils.does_image_exist(pool, image3)
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert '"namespaces": []' in caplog.text
    caplog.clear()
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool,
         "--rbd-image", image4, "--size", "16MB", "--rbd-create-image"])
    assert f"Adding namespace 1 to {subsystem}: Successful" in caplog.text
    time.sleep(30)
    caplog.clear()
    cli(["--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image4}"' in caplog.text
    assert '"trash_image": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "--format", "json", "namespace", "list"])
    assert f'"ns_subsystem_nqn": "{subsystem}"' in caplog.text
    assert '"nsid": 1' in caplog.text
    assert f'"rbd_image_name": "{image4}"' in caplog.text
    assert '"trash_image": false' in caplog.text
    caplog.clear()
    cli(["--server-port", "5502", "namespace", "del", "--subsystem", subsystem, "--nsid", "1"])
    assert f"Deleting namespace 1 from {subsystem}: Successful" in caplog.text
    time.sleep(30)
    assert ceph_utils.does_image_exist(pool, image4)
    ceph_utils.delete_image(pool, image4)
    assert not ceph_utils.does_image_exist(pool, image4)
