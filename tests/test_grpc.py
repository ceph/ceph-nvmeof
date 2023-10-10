import pytest
import time
from control.server import GatewayServer
from control.cli import main as cli
import logging
import warnings

# Set up a logger
logger = logging.getLogger(__name__)
image = "mytestdevimage"
pool = "rbd"
bdev_prefix = "Ceph0"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
created_resource_count = 150
get_subsys_count = 100

def create_resource_by_index(i):
    bdev = f"{bdev_prefix}_{i}"
    cli(["create_bdev", "-i", image, "-p", pool, "-b", bdev])
    subsystem = f"{subsystem_prefix}{i}"
    cli(["create_subsystem", "-n", subsystem ])
    cli(["add_namespace", "-n", subsystem, "-b", bdev])

def check_resource_by_index(i, caplog):
    bdev = f"{bdev_prefix}_{i}"
    # notice that this also verifies the namespace as the bdev name is in the namespaces section
    assert f"{bdev}" in caplog.text
    subsystem = f"{subsystem_prefix}{i}"
    assert f"{subsystem}" in caplog.text

# We want to fail in case we got an exception about invalid data in pb2 functions but this is just a warning
# for pytest. In order for the test to fail in such a case we need to ask pytest to regard this as an error
@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
def test_create_get_subsys(caplog, config):
    with GatewayServer(config) as gateway:
        gateway.serve()

        for i in range(created_resource_count):
            create_resource_by_index(i)
            assert "Failed" not in caplog.text

    caplog.clear()

    # restart the gateway here
    with GatewayServer(config) as gateway:
        gateway.serve()

        for i in range(get_subsys_count):
            cli(["get_subsystems"])
            assert "Exception" not in caplog.text
            time.sleep(0.1)

        time.sleep(20)     # Make sure update() is over
        caplog.clear()
        cli(["get_subsystems"])
        assert "Exception" not in caplog.text
        assert "get_subsystems: []" not in caplog.text
        for i in range(created_resource_count):
            check_resource_by_index(i, caplog)
