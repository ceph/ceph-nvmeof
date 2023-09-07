import pytest
import time
import rados
from control.state import OmapGatewayState, GatewayStateHandler
from control.config import GatewayConfig


@pytest.fixture(scope="module")
def ioctx(config: GatewayConfig):
    """Opens IO context to ceph pool."""
    ceph_pool = config.get("ceph", "pool")
    ceph_conf = config.get("ceph", "config_file")
    conn = rados.Rados(conffile=ceph_conf)
    conn.connect()
    ioctx = conn.open_ioctx(ceph_pool)
    yield ioctx
    ioctx.close()



@pytest.fixture
def omap_state(config: GatewayConfig):
    """Sets up and tears down OMAP state object."""
    omap = OmapGatewayState(config)
    omap.state.delete()
    yield omap
    omap.state.delete()


def add_key(ioctx: rados.Ioctx, key: str, value: str, version: int, omap_name: str, omap_version_key: str):
    """Adds key to the specified OMAP and sets version number."""
    with rados.WriteOpCtx() as write_op:
        ioctx.set_omap(write_op, (key,), (value,))
        ioctx.set_omap(write_op, (omap_version_key,), (str(version),))
        ioctx.operate_write_op(write_op, omap_name)


def remove_key(ioctx: rados.Ioctx, key: str, version: int, omap_name: str, omap_version_key: str):
    """Removes key from the specified OMAP."""
    with rados.WriteOpCtx() as write_op:
        ioctx.remove_omap_keys(write_op, (key,))
        ioctx.set_omap(write_op, (omap_version_key,), (str(version),))
        ioctx.operate_write_op(write_op, omap_name)


def test_state_polling_update(config: GatewayConfig, ioctx: rados.Ioctx, omap_state: OmapGatewayState):
    """Confirms periodic polling of the OMAP for updates."""

    update_counter = 0

    def _state_polling_update(update, is_add_req):
        nonlocal update_counter
        update_counter += 1
        for k, v in update.items():
            # Check for addition
            if update_counter == 1:
                assert is_add_req is True
                assert k == key
                assert v.decode("utf-8") == "add"
            # Check for two-step change
            if update_counter == 2:
                assert is_add_req is False
                assert k == key
                assert v.decode("utf-8") == "changed"
            if update_counter == 3:
                assert is_add_req is True
                assert k == key
                assert v.decode("utf-8") == "changed"
            # Check for removal
            if update_counter == 4:
                assert is_add_req is False
                assert k == key
            assert update_counter < 5

    version = 1
    update_interval_sec = 1
    state_handler = GatewayStateHandler(config, omap_state,
                                _state_polling_update)
    state_handler.update_interval = update_interval_sec
    state_handler.use_notify = False
    key = "bdev_test"
    state_handler.start_update()
    omap_obj = omap_state.state

    # Add bdev key to OMAP and update version number
    version += 1
    add_key(ioctx, key, "add", version, omap_obj.name,
            omap_obj.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    # Change bdev key and update version number
    version += 1
    add_key(ioctx, key, "changed", version, omap_obj.name,
            omap_obj.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    # Remove bdev key and update version number
    version += 1
    remove_key(ioctx, key, version, omap_obj.name,
               omap_obj.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    assert update_counter == 4


def test_state_notify_update(config: GatewayConfig, ioctx: rados.Ioctx, omap_state: OmapGatewayState):
    """Confirms use of OMAP watch/notify for updates."""

    update_counter = 0

    def _state_notify_update(update, is_add_req):
        nonlocal update_counter
        update_counter += 1
        elapsed = time.time() - start
        assert elapsed < update_interval_sec
        for k, v in update.items():
            # Check for addition
            if update_counter == 1:
                assert is_add_req is True
                assert k == key
                assert v.decode("utf-8") == "add"
            # Check for two-step change
            if update_counter == 2:
                assert is_add_req is False
                assert k == key
                assert v.decode("utf-8") == "changed"
            if update_counter == 3:
                assert is_add_req is True
                assert k == key
                assert v.decode("utf-8") == "changed"
            # Check for removal
            if update_counter == 4:
                assert is_add_req is False
                assert k == key
            assert update_counter < 5

    version = 1
    update_interval_sec = 10
    state_handler = GatewayStateHandler(config, omap_state,
                                _state_notify_update)
    key = "bdev_test"
    state_handler.update_interval = update_interval_sec
    state_handler.use_notify = True
    start = time.time()
    state_handler.start_update()
    omap_obj = omap_state.state

    # Add bdev key to OMAP and update version number
    version += 1
    add_key(ioctx, key, "add", version, omap_obj.name,
            omap_obj.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_obj.name))  # Send notify signal

    # Change bdev key and update version number
    version += 1
    add_key(ioctx, key, "changed", version, omap_obj.name,
            omap_obj.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_obj.name))  # Send notify signal

    # Remove bdev key and update version number
    version += 1
    remove_key(ioctx, key, version, omap_obj.name,
               omap_obj.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_obj.name))  # Send notify signal

    # any wait interval smaller than update_interval_sec = 10 should be good
    # to test notify capability
    elapsed = time.time() - start
    wait_interval = update_interval_sec - elapsed - 0.5
    assert(wait_interval > 0)
    assert(wait_interval < update_interval_sec)
    time.sleep(wait_interval)

    # expect 4 updates: addition, two-step change and removal
    # registered before update_interval_sec
    assert update_counter == 4
    elapsed = time.time() - start
    assert elapsed < update_interval_sec
