import pytest
import rados
from control.config import GatewayConfig
from control.state import OmapGatewayState


def pytest_addoption(parser):
    """Sets command line options for testing."""
    # Specify base config file for tests
    parser.addoption("--config",
                     action="store",
                     help="Path to config file",
                     default="ceph-nvmeof.conf")
    parser.addoption("--image",
                     action="store",
                     help="RBD image name",
                     default="mytestdevimage")


@pytest.fixture(scope="session")
def conffile(request):
    """Returns the command line input for the config file."""
    return request.config.getoption("--config")


@pytest.fixture(scope="session")
def config(conffile):
    """Returns config file settings."""
    return GatewayConfig(conffile)


@pytest.fixture(scope="session")
def image(request):
    """Returns the command line input for the test rbd image name."""
    return request.config.getoption("--image")
