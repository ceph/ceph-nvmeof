import pytest
import time
import rados
from control.state import LocalGatewayState, OmapGatewayState, GatewayStateHandler


@pytest.fixture
def ioctx(omap_state, config):
    """Opens IO context to ceph pool."""
    ioctx = omap_state.open_rados_connection(config)
    yield ioctx
    ioctx.close()


@pytest.fixture
def local_state():
    """Returns local state object."""
    return LocalGatewayState()


@pytest.fixture
def omap_state(config):
    """Sets up and tears down OMAP state object."""
    omap = OmapGatewayState(config)
    omap.delete_state()
    yield omap
    omap.delete_state()


def add_key(ioctx, key, value, version, omap_name, omap_version_key):
    """Adds key to the specified OMAP and sets version number."""
    with rados.WriteOpCtx() as write_op:
        ioctx.set_omap(write_op, (key,), (value,))
        ioctx.set_omap(write_op, (omap_version_key,), (str(version),))
        ioctx.operate_write_op(write_op, omap_name)


def remove_key(ioctx, key, version, omap_name, omap_version_key):
    """Removes key from the specified OMAP."""
    with rados.WriteOpCtx() as write_op:
        ioctx.remove_omap_keys(write_op, (key,))
        ioctx.set_omap(write_op, (omap_version_key,), (str(version),))
        ioctx.operate_write_op(write_op, omap_name)


def test_state_polling_update(config, ioctx, local_state, omap_state):
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
    state = GatewayStateHandler(config, local_state, omap_state,
                                _state_polling_update)
    state.update_interval = update_interval_sec
    state.use_notify = False
    key = "namespace_test"
    state.start_update()

    # Add namespace key to OMAP and update version number
    version += 1
    add_key(ioctx, key, "add", version, omap_state.omap_name,
            omap_state.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    # Change namespace key and update version number
    version += 1
    add_key(ioctx, key, "changed", version, omap_state.omap_name,
            omap_state.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    # Remove namespace key and update version number
    version += 1
    remove_key(ioctx, key, version, omap_state.omap_name,
               omap_state.OMAP_VERSION_KEY)
    time.sleep(update_interval_sec + 1)  # Allow time for polling

    assert update_counter == 4


def test_state_notify_update(config, ioctx, local_state, omap_state):
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
    state = GatewayStateHandler(config, local_state, omap_state,
                                _state_notify_update)
    key = "namespace_test"
    state.update_interval = update_interval_sec
    state.use_notify = True
    start = time.time()
    state.start_update()

    # Add namespace key to OMAP and update version number
    version += 1
    add_key(ioctx, key, "add", version, omap_state.omap_name,
            omap_state.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_state.omap_name))  # Send notify signal

    # Change namespace key and update version number
    version += 1
    add_key(ioctx, key, "changed", version, omap_state.omap_name,
            omap_state.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_state.omap_name))  # Send notify signal

    # Remove namespace key and update version number
    version += 1
    remove_key(ioctx, key, version, omap_state.omap_name,
               omap_state.OMAP_VERSION_KEY)
    assert (ioctx.notify(omap_state.omap_name))  # Send notify signal

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
