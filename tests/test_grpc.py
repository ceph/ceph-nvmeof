import pytest
import time
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import logging
import warnings

image = "mytestdevimage"
pool = "rbd"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
host_prefix = "nqn.2016-06.io.spdk:host"
created_resource_count = 20
subsys_list_count = 5

def wait_for_string(caplog, str, timeout):
    for i in range(timeout):
        if str in caplog.text:
            return
        time.sleep(1)

    assert False, f"Couldn't find string \"{str}\" in {timeout} seconds"

def create_resource_by_index(i):
    subsystem = f"{subsystem_prefix}{i}"
    cli(["subsystem", "add", "--subsystem", subsystem])
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--size", "16MB", "--rbd-create-image","--load-balancing-group", "1", "--force", "--no-auto-visible"])

def check_resource_by_index(i, caplog):
    subsystem = f"{subsystem_prefix}{i}"
    caplog.clear()
    cli(["--format", "plain", "subsystem", "list", "--subsystem", subsystem])
    assert f"{subsystem}" in caplog.text
    caplog.clear()
    cli(["--format", "plain", "namespace", "list", "--subsystem", subsystem, "--nsid", "1"])
    assert f"No namespace" not in caplog.text
    assert f"Failure listing namespaces:" not in caplog.text

# We want to fail in case we got an exception about invalid data in pb2 functions but this is just a warning
# for pytest. In order for the test to fail in such a case we need to ask pytest to regard this as an error
@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
def test_create_get_subsys(caplog, config):
    config.config["gateway"]["group"] = ""
    ceph_utils = CephUtils(config)
    with GatewayServer(config) as gateway:
        ceph_utils.execute_ceph_monitor_command("{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", "group": ""' + "}")
        gateway.serve()

        for i in range(created_resource_count):
            create_resource_by_index(i)
            assert "failed" not in caplog.text.lower().replace("failed to notify", "")
            assert "Failure" not in caplog.text

        assert f"{subsystem_prefix}0 with ANA group id 1" in caplog.text

        caplog.clear()
        # add a listener
        cli(["listener", "add", "--subsystem", f"{subsystem_prefix}0", "--host-name",
             gateway.gateway_rpc.host_name, "--traddr", "127.0.0.1", "--trsvcid", "5001"])
        assert f"Adding {subsystem_prefix}0 listener at 127.0.0.1:5001: Successful" in caplog.text

        # Set QOS for the first namespace
        cli(["namespace", "set_qos", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1",
             "--rw-ios-per-second", "2000"])
        assert f"Setting QOS limits of namespace 1 in {subsystem_prefix}0: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace 1 on {subsystem_prefix}0" in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1",
             "--r-megabytes-per-second", "5"])
        assert f"Setting QOS limits of namespace 1 in {subsystem_prefix}0: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace 1 on {subsystem_prefix}0" not in caplog.text
 
        # add host to the first namespace
        caplog.clear()
        cli(["namespace", "add_host", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1", "--host-nqn", f"{host_prefix}0"])
        assert f"Failure adding host" not in caplog.text

    caplog.clear()

    # restart the gateway here
    with GatewayServer(config) as gateway:
        ceph_utils.execute_ceph_monitor_command("{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", "group": ""' + "}")
        gateway.serve()

        # wait until we see at least one subsystem created
        wait_for_string(caplog, f"Received request to create subsystem {subsystem_prefix}", 60)

        for i in range(subsys_list_count):
            cli(["--format", "plain", "subsystem", "list"])
            assert "Exception" not in caplog.text
            assert "No subsystems" not in caplog.text
            time.sleep(0.1)

        time.sleep(20)     # Make sure update() is over
        assert f"{subsystem_prefix}0 with ANA group id 1" in caplog.text
        assert f"Received request to set QOS limits for namespace 1 on {subsystem_prefix}0, R/W IOs per second: 2000 Read megabytes per second: 5" in caplog.text
        assert f"Received request to set QOS limits for namespace 1 on {subsystem_prefix}0, R/W IOs per second: 2000 Read megabytes per second: 5" in caplog.text
        assert f"Received request to add host {host_prefix}0 to namespace 1 on {subsystem_prefix}0, context: None" in caplog.text
        caplog.clear()
        cli(["--format", "plain", "subsystem", "list"])
        assert "Exception" not in caplog.text
        assert "No subsystems" not in caplog.text
        for i in range(created_resource_count):
            check_resource_by_index(i, caplog)
