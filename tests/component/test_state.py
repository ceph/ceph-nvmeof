import pytest
import time
from typing import Dict, List, NamedTuple
from unittest import mock
with mock.patch.dict("sys.modules", rados=mock.Mock()):
    from control.state import (
        GatewayState,
        LocalGatewayState,
        OmapGatewayState,
        GatewayStateHandler,
    )


class UpdateCall(NamedTuple):
    """Holds a single component update."""

    component_update: Dict[str, str]
    is_add_req: bool


class StateUpdateTest(NamedTuple):
    """Holds inputs and expected results of a state update."""

    local: Dict[str, str]
    omap: Dict[str, str]
    calls: List[UpdateCall]


class TestGatewayStateHandler:
    """Tests for GatewayStateHandler."""

    @pytest.fixture(autouse=True)
    def state_init_with_mocks(self) -> None:
        """Initializes GatewayStateHandler with mocked arguments."""
        mock_config = mock.Mock()
        mock_config.getint.return_value = 1
        self.state = GatewayStateHandler(
            mock_config,
            mock.Mock(spec=LocalGatewayState),
            mock.Mock(spec=OmapGatewayState),
            mock.Mock(),
        )

    @pytest.mark.parametrize(
        "input",
        [
            # Tests no change.
            #
            # Expects 0 calls to gateway_rpc_caller.
            StateUpdateTest(local={},
                            omap={OmapGatewayState.OMAP_VERSION_KEY: "2"},
                            calls=[]),
            # Tests key addition of an invalid prefix.
            #
            # Expects 0 calls to gateway_rpc_caller.
            StateUpdateTest(
                local={},
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    "foo": "bar",
                },
                calls=[],
            ),
            # Tests key additions of a single valid prefix.
            #
            # Expects 1 call to gateway_rpc_caller containing a dictionary
            # with the added keys and a flag (True) to indicate addition.
            StateUpdateTest(
                local={},
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.BDEV_PREFIX}_baz": "qux",
                },
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                            f"{GatewayState.BDEV_PREFIX}_baz": "qux",
                        },
                        is_add_req=True,
                    )
                ],
            ),
            # Tests key additions of every valid prefix.
            #
            # Expects 5 ordered calls to gateway_rpc_caller, each containing
            # a dictionary with an added key and a flag (True) to indicate
            # addition.
            StateUpdateTest(
                local={},
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    f"{GatewayState.HOST_PREFIX}_foo": "bar",
                    f"{GatewayState.NAMESPACE_PREFIX}_foo": "bar",
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "bar",
                    f"{GatewayState.LISTENER_PREFIX}_foo": "bar",
                },
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "bar"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "bar"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.NAMESPACE_PREFIX}_foo": "bar"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.HOST_PREFIX}_foo": "bar"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.LISTENER_PREFIX}_foo": "bar"
                        },
                        is_add_req=True,
                    ),
                ],
            ),
            # Tests key removal of an invalid prefix.
            #
            # Expects 0 calls to gateway_rpc_caller.
            StateUpdateTest(
                local={"foo": "bar"},
                omap={OmapGatewayState.OMAP_VERSION_KEY: "2"},
                calls=[],
            ),
            # Tests key removals of a single valid prefix.
            #
            # Expects 1 call to gateway_rpc_caller containing a dictionary
            # with the removed keys and a flag (False) to indicate removal.
            StateUpdateTest(
                local={
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.BDEV_PREFIX}_baz": "qux",
                },
                omap={OmapGatewayState.OMAP_VERSION_KEY: "2"},
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                            f"{GatewayState.BDEV_PREFIX}_baz": "qux",
                        },
                        is_add_req=False,
                    )
                ],
            ),
            # Tests key removals of every valid prefix.
            #
            # Expects 5 ordered calls to gateway_rpc_caller, each containing
            # a dictionary with a removed key and a flag (False) to indicate
            # removal.
            StateUpdateTest(
                local={
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.NAMESPACE_PREFIX}_foo": "bar",
                    f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "bar",
                    f"{GatewayState.HOST_PREFIX}_foo": "bar",
                    f"{GatewayState.LISTENER_PREFIX}_foo": "bar",
                },
                omap={OmapGatewayState.OMAP_VERSION_KEY: "2"},
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.LISTENER_PREFIX}_foo": "bar"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.HOST_PREFIX}_foo": "bar"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.NAMESPACE_PREFIX}_foo": "bar"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "bar"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "bar"
                        },
                        is_add_req=False,
                    ),
                ],
            ),
            # Tests value change on key of an invalid prefix.
            #
            # Expects 0 calls to gateway_rpc_caller.
            StateUpdateTest(
                local={"foo": "bar"},
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    "foo": "quux",
                },
                calls=[],
            ),
            # Tests value changes on keys of a single valid prefix.
            #
            # Expects 2 ordered calls to gateway_rpc_caller. The first call
            # contains a dictionary with the changed keys and a flag (False)
            # to indicate removal. The second call contains a flag (True) to
            # indicate addition with new values.
            StateUpdateTest(
                local={
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.BDEV_PREFIX}_baz": "qux",
                },
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    f"{GatewayState.BDEV_PREFIX}_foo": "quux",
                    f"{GatewayState.BDEV_PREFIX}_baz": "quuz",
                },
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "quux",
                            f"{GatewayState.BDEV_PREFIX}_baz": "quuz",
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "quux",
                            f"{GatewayState.BDEV_PREFIX}_baz": "quuz",
                        },
                        is_add_req=True,
                    ),
                ],
            ),
            # Tests value changes on keys of every valid prefix.
            #
            # Expects 10 ordered calls to gateway_rpc_caller, 2 per prefix.
            # The first call for each prefix contains a dictionary with the
            # changed keys and a flag (False) to indicate removal. The
            # second call contains a flag (True) to indicate addition with
            # new values. Calls for removal are ordered by prefix and occur
            # in reverse order of calls for addition.
            StateUpdateTest(
                local={
                    f"{GatewayState.BDEV_PREFIX}_foo": "bar",
                    f"{GatewayState.NAMESPACE_PREFIX}_foo": "bar",
                    f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "bar",
                    f"{GatewayState.HOST_PREFIX}_foo": "bar",
                    f"{GatewayState.LISTENER_PREFIX}_foo": "bar",
                },
                omap={
                    OmapGatewayState.OMAP_VERSION_KEY: "2",
                    f"{GatewayState.BDEV_PREFIX}_foo": "quux",
                    f"{GatewayState.NAMESPACE_PREFIX}_foo": "quux",
                    f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "quux",
                    f"{GatewayState.HOST_PREFIX}_foo": "quux",
                    f"{GatewayState.LISTENER_PREFIX}_foo": "quux",
                },
                calls=[
                    UpdateCall(
                        component_update={
                            f"{GatewayState.LISTENER_PREFIX}_foo": "quux"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.HOST_PREFIX}_foo": "quux"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.NAMESPACE_PREFIX}_foo": "quux"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "quux"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "quux"
                        },
                        is_add_req=False,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.BDEV_PREFIX}_foo": "quux"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.SUBSYSTEM_PREFIX}_foo": "quux"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.NAMESPACE_PREFIX}_foo": "quux"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.HOST_PREFIX}_foo": "quux"
                        },
                        is_add_req=True,
                    ),
                    UpdateCall(
                        component_update={
                            f"{GatewayState.LISTENER_PREFIX}_foo": "quux"
                        },
                        is_add_req=True,
                    ),
                ],
            ),
        ],
    )
    def test_update_from_omap(self, input: StateUpdateTest):
        """Confirms call order for local update to reflect changes in OMAP."""
        self.state.local.get_state.return_value = input.local
        self.state.omap.get_local_version.return_value = 1
        self.state.omap.OMAP_VERSION_KEY = OmapGatewayState.OMAP_VERSION_KEY
        self.state.omap.get_state.return_value = input.omap
        self.state.update()
        assert self.state.gateway_rpc_caller.call_args_list == [
            mock.call(i.component_update, i.is_add_req) for i in input.calls
        ]

    def test_update_reset_local_state(self):
        """Confirms reset of local state after update."""
        self.state.local.get_state.return_value = {}
        self.state.omap.get_local_version.return_value = 1
        self.state.omap.OMAP_VERSION_KEY = OmapGatewayState.OMAP_VERSION_KEY
        omap_dict = {
            OmapGatewayState.OMAP_VERSION_KEY: "2",
            f"{GatewayState.BDEV_PREFIX}_foo": "bar"
        }
        self.state.omap.get_state.return_value = omap_dict
        self.state.update()
        self.state.local.reset.assert_called_once_with(omap_dict)

    def test_update_reset_local_version(self):
        """Confirms reset of local version after update."""
        self.state.local.get_state.return_value = {}
        self.state.omap.get_local_version.return_value = 1
        self.state.omap.OMAP_VERSION_KEY = OmapGatewayState.OMAP_VERSION_KEY
        omap_version = 2
        self.state.omap.get_state.return_value = {
            OmapGatewayState.OMAP_VERSION_KEY: str(omap_version),
            f"{GatewayState.BDEV_PREFIX}_foo": "bar"
        }
        self.state.update()
        self.state.omap.set_local_version.assert_called_once_with(omap_version)

    @pytest.mark.parametrize("omap", [{
        OmapGatewayState.OMAP_VERSION_KEY: "0"
    }, {
        OmapGatewayState.OMAP_VERSION_KEY: "1"
    }])
    def test_update_not_needed(self, omap: Dict[str, str]):
        """Confirms lack of update when the local version >= OMAP version."""
        self.state.omap.get_local_version.return_value = 1
        self.state.omap.OMAP_VERSION_KEY = OmapGatewayState.OMAP_VERSION_KEY
        self.state.omap.get_state.return_value = omap
        self.state.update()
        self.state.omap.set_local_version.assert_not_called()

    def test_update_caller_periodic(self):
        """Confirms periodic call for update."""
        with mock.patch.object(self.state, "update") as mock_update:
            self.state.update_interval = 1
            start = time.time()
            self.state.start_update()
            time.sleep(3)
            self.state.stop_event.set()
            self.state.update_timer.join()
            expected_count = int(time.time() - start)
            assert len(mock_update.call_args_list) == expected_count

    def test_update_caller_notify(self):
        """Confirms notify signal interrupts periodic call for update."""
        with mock.patch.object(self.state, "update") as mock_update:
            self.state.update_interval = 1
            start = time.time()
            self.state.start_update()
            self.state.notify_event.set()
            time.sleep(3)
            self.state.stop_event.set()
            self.state.update_timer.join()
            expected_count = int(time.time() - start) + 1
            assert len(mock_update.call_args_list) == expected_count
