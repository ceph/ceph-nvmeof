import pytest
import time
from control.server import GatewayServer
from control.cli import main as cli
import logging
import warnings

image = "mytestdevimage"
pool = "rbd"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
created_resource_count = 20
subsys_list_count = 5

def create_resource_by_index(i):
    subsystem = f"{subsystem_prefix}{i}"
    cli(["subsystem", "add", "--subsystem", subsystem, "--enable-ha" ])
    cli(["namespace", "add", "--subsystem", subsystem, "--rbd-pool", pool, "--rbd-image", image, "--size", "16MiB", "--rbd-create-image", "--force"])

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
    with GatewayServer(config) as gateway:
        gateway.serve()

        for i in range(created_resource_count):
            create_resource_by_index(i)
            assert "failed" not in caplog.text.lower()
            assert "Failure" not in caplog.text

        assert f"{subsystem_prefix}0 with ANA group id 1" in caplog.text

        caplog.clear()
        # add a listener
        cli(["listener", "add", "--subsystem", f"{subsystem_prefix}0", "--gateway-name",
             gateway.name, "--traddr", "127.0.0.1", "--trsvcid", "5001"])
        assert f"Adding {subsystem_prefix}0 listener at 127.0.0.1:5001: Successful" in caplog.text

        # Change ANA group id for the first namesapce
        cli(["namespace", "change_load_balancing_group", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1",
             "--load-balancing-group", "4"])
        assert f"Changing load balancing group of namespace 1 in {subsystem_prefix}0 to 4: Successful" in caplog.text

        # Set QOS for the first namespace
        cli(["namespace", "set_qos", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1",
             "--rw-ios-per-second", "2000"])
        assert f"Setting QOS limits of namespace 1 in {subsystem_prefix}0: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace using NSID 1 on {subsystem_prefix}0" in caplog.text
        caplog.clear()
        cli(["namespace", "set_qos", "--subsystem", f"{subsystem_prefix}0", "--nsid", "1",
             "--r-megabytes-per-second", "5"])
        assert f"Setting QOS limits of namespace 1 in {subsystem_prefix}0: Successful" in caplog.text
        assert f"No previous QOS limits found, this is the first time the limits are set for namespace using NSID 1 on {subsystem_prefix}0" not in caplog.text

    caplog.clear()

    # restart the gateway here
    with GatewayServer(config) as gateway:
        gateway.serve()

        for i in range(subsys_list_count):
            cli(["--format", "plain", "subsystem", "list"])
            assert "Exception" not in caplog.text
            assert "No subsystems" not in caplog.text
            time.sleep(0.1)

        time.sleep(20)     # Make sure update() is over
        assert f"{subsystem_prefix}0 with ANA group id 4" in caplog.text
        assert f"Received request to set QOS limits for namespace using NSID 1 on {subsystem_prefix}0, R/W IOs per second: 2000 Read megabytes per second: 5" in caplog.text
        caplog.clear()
        cli(["--format", "plain", "subsystem", "list"])
        assert "Exception" not in caplog.text
        assert "No subsystems" not in caplog.text
        for i in range(created_resource_count):
            check_resource_by_index(i, caplog)
