#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import time
import threading
import rados
import logging
from collections import defaultdict
from typing import DefaultDict, Dict, List, Callable
from .omap import OmapObject
from .config import GatewayConfig


# Declare a callback function called when the gateway state changes
StateUpdate = Callable[[Dict[str, str], bool], None]

class GatewayState:
    """
        X_PREFIX: Key prefix for key of type "X"
    """
    BDEV_PREFIX = "bdev_"
    NAMESPACE_PREFIX = "namespace_"
    SUBSYSTEM_PREFIX = "subsystem_"
    HOST_PREFIX = "host_"
    LISTENER_PREFIX = "listener_"
    RANGE_PREFIX = "range_"

def bdev_key(bdev_name: str) -> str:
    return f"{GatewayState.BDEV_PREFIX}{bdev_name}"

def namespace_key(subsystem_nqn: str, nsid: str) -> str:
    return f"{GatewayState.NAMESPACE_PREFIX}{subsystem_nqn}_{nsid}"

def subsystem_key(subsystem_nqn: str) -> str:
    return f"{GatewayState.SUBSYSTEM_PREFIX}{subsystem_nqn}"

def host_key(subsystem_nqn: str, host_nqn: str) -> str:
    return f"{GatewayState.HOST_PREFIX}{subsystem_nqn}_{host_nqn}"

def listener_key(subsystem_nqn: str, gateway: str, trtype: str,
                    traddr: str, trsvcid: str) -> str:
    return f"{GatewayState.LISTENER_PREFIX}{subsystem_nqn}_{gateway}_{trtype}_{traddr}_{trsvcid}"

class OmapSpdkState:
    """Persists gateway NVMeoF target SPDK configuration"""

    def __init__(self, spdk: OmapObject) -> None:
        self.obj = spdk

    def add_bdev(self, bdev_name: str, val: str) -> None:
        """Adds a bdev to the state data store."""
        self.obj.add_key(bdev_key(bdev_name), val)

    def remove_bdev(self, bdev_name: str) -> None:
        """Removes a bdev from the state data store."""
        self.obj.remove_key(bdev_key(bdev_name))

    def add_namespace(self, subsystem_nqn: str, nsid: str, val: str) -> None:
        """Adds a namespace to the state data store."""
        self.obj.add_key(namespace_key(subsystem_nqn, nsid), val)

    def remove_namespace(self, subsystem_nqn: str, nsid: str) -> None:
        """Removes a namespace from the state data store."""
        self.obj.remove_key(namespace_key(subsystem_nqn, nsid))

    def add_subsystem(self, subsystem_nqn: str, val: str) -> None:
        """Adds a subsystem to the state data store."""
        self.obj.add_key(subsystem_key(subsystem_nqn), val)

    def remove_subsystem(self, subsystem_nqn: str) -> None:
        """Removes a subsystem from the state data store."""
        self.obj.remove_key(subsystem_key(subsystem_nqn))

        # Delete all keys related to subsystem
        state = self.obj.get()
        for key in state.keys():
            if (key.startswith(GatewayState.NAMESPACE_PREFIX + subsystem_nqn) or
                    key.startswith(GatewayState.HOST_PREFIX + subsystem_nqn) or
                    key.startswith(GatewayState.LISTENER_PREFIX + subsystem_nqn)):
                self.obj.remove_key(key)

    def add_host(self, subsystem_nqn: str, host_nqn: str, val: str) -> None:
        """Adds a host to the state data store."""
        self.obj.add_key(host_key(subsystem_nqn, host_nqn), val)

    def remove_host(self, subsystem_nqn: str, host_nqn: str) -> None:
        """Removes a host from the state data store."""
        self.obj.remove_key(host_key(subsystem_nqn, host_nqn))

    def add_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                     traddr: str, trsvcid: str, val: str) -> None:
        """Adds a listener to the state data store."""
        self.obj.add_key(listener_key(subsystem_nqn, gateway, trtype, traddr, trsvcid), val)

    def remove_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                        traddr: str, trsvcid: str) -> None:
        """Removes a listener from the state data store."""
        self.obj.remove_key(listener_key(subsystem_nqn, gateway, trtype, traddr, trsvcid))

class OmapControllerIdRanges:
    """Persists gateway NVMeoF target controller id ranges to an OMAP object.

    Instance attributes:
        config: Basic gateway parameters
        ranges: OMAP object with controller id ranges
    """
    def __init__(self, ranges: OmapObject, config: GatewayConfig) -> None:
        self.config = config
        self.obj = ranges
        self.logger = logging.getLogger(__name__)

    def controllerid_range(self, gateway_name: str) -> (int, int):
        range_length = self.config.getint_with_default("spdk",
                                    "controllerid_range_length", 4096)

        gateway_range = {key: value for key, value in self.obj.get().items() if value == gateway_name}
        if len(gateway_range) > 1:
            raise Exception(f"Too many gateway ranges for {gateway_name=}"
                            f" ({gateway_range})")
        elif len(gateway_range) == 1:
            start, end = gateway_range[0]
            return (int(start), int(end))

        keys = [key for key in self.obj.cached.keys() if key != self.obj.OMAP_VERSION_KEY]
        ranges = [self._parse_range(range) for range in keys]
        start, end = self._calculate_non_overlapping_range(ranges, range_length)
        self.obj.add_key(f"{GatewayState.RANGE_PREFIX}{start}_{end}", gateway_name)
        return ( start, end )

    def _parse_range(self, range: str) -> (int, int):
        start, end = range.removeprefix(GatewayState.RANGE_PREFIX).split("_")
        return (int(start), int(end))

    def _calculate_non_overlapping_range(self, existing_ranges, range_length):
        # Sort the existing ranges by their minimum values
        existing_ranges.sort(key=lambda x: x[0])

        # Initialize variables for the absolute minimum and maximum values
        absolute_min = 1
        absolute_max = 65519

        # Initialize the new range's start value to the absolute minimum
        new_range_start = absolute_min

        # Iterate through existing ranges to find a gap
        for min_range, max_range in existing_ranges:
            if new_range_start + range_length <= min_range:
                # Found a gap before the current existing range
                new_range_end = new_range_start + range_length
                return (new_range_start, new_range_end)
            else:
                # Update new range's start to be just after the current existing range
                new_range_start = max_range + 1

        # If no gap is found within existing ranges, use the end of the last range
        if new_range_start + range_length <= absolute_max:
            new_range_end = new_range_start + range_length
        else:
            # No non-overlapping range can be found within the absolute maximum
            raise Exception(f"Can not find range: {range_length=}, {existing_ranges=}")

        return (new_range_start, new_range_end)

class OmapGatewayState:
    """Persists gateway NVMeoF target state to an OMAP object.

    Handles reads/writes of persistent NVMeoF target state data in key/value
    format within an OMAP object.

    Instance attributes:
        config: Basic gateway parameters
        version: Local gateway NVMeoF target state version
        logger: Logger instance to track OMAP access events
        state: OMAP object representing SPDK state
        ranges: OMAP object with controller id ranges
        ioctx: I/O context which allows OMAP access
    """

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)
        gateway_group = self.config.get("gateway", "group")

        ceph_pool = self.config.get("ceph", "pool")
        ceph_conf = self.config.get("ceph", "config_file")
        rados_id = self.config.get_with_default("ceph", "id", "")

        try:
            conn = rados.Rados(conffile=ceph_conf, rados_id=rados_id)
            conn.connect()
            self.ioctx = conn.open_ioctx(ceph_pool)
            omap_name_prefix = f"nvmeof.{gateway_group}" if gateway_group else "nvmeof"
            self.spdk = OmapSpdkState(OmapObject(f"{omap_name_prefix}.state", self.ioctx))
            self.ranges = OmapControllerIdRanges(OmapObject(f"{omap_name_prefix}.ranges", self.ioctx), config)
        except Exception:
            self.logger.exception(f"Unable to create omap:")
            raise

class GatewayStateHandler:
    """Maintains consistency in NVMeoF target state store instances.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track events
        gateway_rpc_caller: StateUpdate callback, implemented by GatewayServer
        state: OMAP GatewayState instance
        update_interval: Interval to periodically poll for updates
        update_timer: Timer to check for gateway state updates
        use_notify: Flag to indicate use of OMAP watch/notify
    """

    def __init__(self, config: GatewayConfig, state: OmapGatewayState,
                 gateway_rpc_caller: StateUpdate) -> None:
        self.config = config
        self.state = state
        self.gateway_rpc_caller = gateway_rpc_caller
        self.update_timer = None
        self.logger = logging.getLogger(__name__)
        self.update_interval = self.config.getint("gateway",
                                                  "state_update_interval_sec")
        if self.update_interval < 1:
            self.logger.info("Invalid state_update_interval_sec. Setting to 1.")
            self.update_interval = 1
        self.use_notify = self.config.getboolean("gateway",
                                                 "state_update_notify")
        # set this instance of GatewayStateHandler as updater for the spdk object
        self.state.spdk.obj._update = self.update

    def start_update(self) -> None:
        """Initiates periodic polling and watch/notify for updates."""
        notify_event = threading.Event()
        if self.use_notify:
            # Register a watch on omap state
            self.state.spdk.obj.register_watch(notify_event)

        # Start polling for state updates
        if self.update_timer is None:
            self.update_timer = threading.Thread(target=self._update_caller,
                                                 daemon=True,
                                                 args=(notify_event,))
            self.update_timer.start()
        else:
            self.logger.info("Update timer already set.")

    def _update_caller(self, notify_event: threading.Event) -> None:
        """Periodically calls for update."""
        while True:
            update_time = time.time() + self.update_interval
            self.update()
            notify_event.wait(max(update_time - time.time(), 0))
            notify_event.clear()

    def update(self) -> None:
        """Checks for updated omap state and initiates local update."""
        prefix_list = [
            GatewayState.BDEV_PREFIX, GatewayState.SUBSYSTEM_PREFIX,
            GatewayState.NAMESPACE_PREFIX, GatewayState.HOST_PREFIX,
            GatewayState.LISTENER_PREFIX
        ]

        # Get version and state from OMAP
        omap_state_dict = self.state.spdk.obj.get()
        omap_version = int(omap_state_dict[OmapObject.OMAP_VERSION_KEY])

        if self.state.spdk.obj.version < omap_version:
            local_state_dict = self.state.spdk.obj.cached
            local_state_keys = local_state_dict.keys()
            omap_state_keys = omap_state_dict.keys()

            # Find OMAP additions
            added_keys = omap_state_keys - local_state_keys
            added = {key: omap_state_dict[key] for key in added_keys}
            grouped_added = self._group_by_prefix(added, prefix_list)
            # Find OMAP changes
            same_keys = omap_state_keys & local_state_keys
            changed = {
                key: omap_state_dict[key]
                for key in same_keys
                if omap_state_dict[key] != local_state_dict[key]
            }
            grouped_changed = self._group_by_prefix(changed, prefix_list)
            # Find OMAP removals
            removed_keys = local_state_keys - omap_state_keys
            removed = {key: local_state_dict[key] for key in removed_keys}
            grouped_removed = self._group_by_prefix(removed, prefix_list)

            # Handle OMAP removals and remove outdated changed components
            grouped_removed.update(grouped_changed)
            if grouped_removed:
                self._update_call_rpc(grouped_removed, False, prefix_list)
            # Handle OMAP additions and add updated changed components
            grouped_added.update(grouped_changed)
            if grouped_added:
                self._update_call_rpc(grouped_added, True, prefix_list)

            # Update local state and version
            self.state.spdk.obj.cached = omap_state_dict.copy()
            self.state.spdk.obj.version = omap_version
            self.logger.debug("Update complete.")

    def _group_by_prefix(self, state_update: Dict[str, str], prefix_list: List[str]) -> DefaultDict[str, Dict[str, str]] :
        """Groups state update by key prefixes."""
        grouped_state_update = defaultdict(dict)
        for key, val in state_update.items():
            for prefix in prefix_list:
                if key.startswith(prefix):
                    grouped_state_update[prefix][key] = val
        return grouped_state_update

    def _update_call_rpc(self, grouped_state_update: DefaultDict[str, Dict[str, str]],
                         is_add_req: bool, prefix_list: List[str]) -> None:
        """Calls to initiate gateway RPCs in necessary component order."""
        if is_add_req:
            for prefix in prefix_list:
                component_update = grouped_state_update.get(prefix, {})
                if component_update:
                    self.gateway_rpc_caller(component_update, True)
        else:
            for prefix in list(reversed(prefix_list)):
                component_update = grouped_state_update.get(prefix, {})
                if component_update:
                    self.gateway_rpc_caller(component_update, False)
