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
import errno
from typing import Dict
from collections import defaultdict
from abc import ABC, abstractmethod
from .utils import GatewayLogger

class GatewayState(ABC):
    """Persists gateway NVMeoF target state.

    Class attributes:
        X_PREFIX: Key prefix for key of type "X"
    """

    OMAP_KEY_DELIMITER = "_"
    NAMESPACE_PREFIX = "namespace" + OMAP_KEY_DELIMITER
    SUBSYSTEM_PREFIX = "subsystem" + OMAP_KEY_DELIMITER
    HOST_PREFIX = "host" + OMAP_KEY_DELIMITER
    LISTENER_PREFIX = "listener" + OMAP_KEY_DELIMITER
    NAMESPACE_QOS_PREFIX = "qos" + OMAP_KEY_DELIMITER

    def build_namespace_key(subsystem_nqn: str, nsid) -> str:
        key = GatewayState.NAMESPACE_PREFIX + subsystem_nqn
        if nsid is not None:
            key = key + GatewayState.OMAP_KEY_DELIMITER + str(nsid)
        return key

    def build_namespace_qos_key(subsystem_nqn: str, nsid) -> str:
        key = GatewayState.NAMESPACE_QOS_PREFIX + subsystem_nqn
        if nsid is not None:
            key = key + GatewayState.OMAP_KEY_DELIMITER + str(nsid)
        return key

    def build_subsystem_key(subsystem_nqn: str) -> str:
        return GatewayState.SUBSYSTEM_PREFIX + subsystem_nqn

    def build_host_key(subsystem_nqn: str, host_nqn) -> str:
        key = GatewayState.HOST_PREFIX + subsystem_nqn
        if host_nqn is not None:
            key = key + GatewayState.OMAP_KEY_DELIMITER + host_nqn
        return key

    def build_partial_listener_key(subsystem_nqn: str) -> str:
        return GatewayState.LISTENER_PREFIX + subsystem_nqn

    def build_listener_key_suffix(gateway: str, trtype: str, traddr: str, trsvcid: int) -> str:
        if gateway:
            return GatewayState.OMAP_KEY_DELIMITER + gateway + GatewayState.OMAP_KEY_DELIMITER + trtype + GatewayState.OMAP_KEY_DELIMITER + traddr + GatewayState.OMAP_KEY_DELIMITER + str(trsvcid)
        if trtype:
            return GatewayState.OMAP_KEY_DELIMITER + trtype + GatewayState.OMAP_KEY_DELIMITER + traddr + GatewayState.OMAP_KEY_DELIMITER + str(trsvcid)
        return GatewayState.OMAP_KEY_DELIMITER + traddr + GatewayState.OMAP_KEY_DELIMITER + str(trsvcid)
        
    def build_listener_key(subsystem_nqn: str, gateway: str, trtype: str, traddr: str, trsvcid: int) -> str:
        return GatewayState.build_partial_listener_key(subsystem_nqn) + GatewayState.build_listener_key_suffix(gateway, trtype, traddr, str(trsvcid))

    @abstractmethod
    def get_state(self) -> Dict[str, str]:
        """Returns the state dictionary."""
        pass

    @abstractmethod
    def _add_key(self, key: str, val: str):
        """Adds key to state data store."""
        pass

    @abstractmethod
    def _remove_key(self, key: str):
        """Removes key from state data store."""
        pass

    def add_namespace(self, subsystem_nqn: str, nsid: str, val: str):
        """Adds a namespace to the state data store."""
        key = GatewayState.build_namespace_key(subsystem_nqn, nsid)
        self._add_key(key, val)

    def remove_namespace(self, subsystem_nqn: str, nsid: str):
        """Removes a namespace from the state data store."""
        key = GatewayState.build_namespace_key(subsystem_nqn, nsid)
        self._remove_key(key)

    def add_namespace_qos(self, subsystem_nqn: str, nsid: str, val: str):
        """Adds namespace's QOS settings to the state data store."""
        key = GatewayState.build_namespace_qos_key(subsystem_nqn, nsid)
        self._add_key(key, val)

    def remove_namespace_qos(self, subsystem_nqn: str, nsid: str):
        """Removes namespace's QOS settings from the state data store."""
        key = GatewayState.build_namespace_qos_key(subsystem_nqn, nsid)
        self._remove_key(key)

    def add_subsystem(self, subsystem_nqn: str, val: str):
        """Adds a subsystem to the state data store."""
        key = GatewayState.build_subsystem_key(subsystem_nqn)
        self._add_key(key, val)

    def remove_subsystem(self, subsystem_nqn: str):
        """Removes a subsystem from the state data store."""
        key = GatewayState.build_subsystem_key(subsystem_nqn)
        self._remove_key(key)

        # Delete all keys related to subsystem
        state = self.get_state()
        for key in state.keys():
            if (key.startswith(GatewayState.build_namespace_key(subsystem_nqn, None)) or
                    key.startswith(GatewayState.build_host_key(subsystem_nqn, None)) or
                    key.startswith(GatewayState.build_partial_listener_key(subsystem_nqn))):
                self._remove_key(key)

    def add_host(self, subsystem_nqn: str, host_nqn: str, val: str):
        """Adds a host to the state data store."""
        key = GatewayState.build_host_key(subsystem_nqn, host_nqn)
        self._add_key(key, val)

    def remove_host(self, subsystem_nqn: str, host_nqn: str):
        """Removes a host from the state data store."""
        key = GatewayState.build_host_key(subsystem_nqn, host_nqn)
        self._remove_key(key)

    def add_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                     traddr: str, trsvcid: int, val: str):
        """Adds a listener to the state data store."""
        key = GatewayState.build_listener_key(subsystem_nqn, gateway, trtype, traddr, trsvcid)
        self._add_key(key, val)

    def remove_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                        traddr: str, trsvcid: int):
        """Removes a listener from the state data store."""
        key = GatewayState.build_listener_key(subsystem_nqn, gateway, trtype, traddr, trsvcid)
        self._remove_key(key)

    @abstractmethod
    def delete_state(self):
        """Deletes state data store."""
        pass


class LocalGatewayState(GatewayState):
    """Records gateway NVMeoF target state in a dictionary.

    Instance attributes:
        state: Local gateway NVMeoF target state
    """

    def __init__(self):
        self.state = {}

    def get_state(self) -> Dict[str, str]:
        """Returns local state dictionary."""
        return self.state.copy()

    def _add_key(self, key: str, val: str):
        """Adds key and value to the local state dictionary."""
        self.state[key] = val

    def _remove_key(self, key: str):
        """Removes key from the local state dictionary."""
        self.state.pop(key)

    def delete_state(self):
        """Deletes contents of local state dictionary."""
        self.state.clear()

    def reset(self, omap_state):
        """Resets dictionary with OMAP state."""
        self.state = omap_state


class OmapLock:
    OMAP_FILE_LOCK_NAME = "omap_file_lock"
    OMAP_FILE_LOCK_COOKIE = "omap_file_cookie"

    def __init__(self, omap_state, gateway_state) -> None:
        self.logger = omap_state.logger
        self.omap_state = omap_state
        self.gateway_state = gateway_state
        self.is_locked = False
        self.omap_file_lock_duration = self.omap_state.config.getint_with_default("gateway", "omap_file_lock_duration", 60)
        self.omap_file_update_reloads = self.omap_state.config.getint_with_default("gateway", "omap_file_update_reloads", 10)
        self.omap_file_lock_retries = self.omap_state.config.getint_with_default("gateway", "omap_file_lock_retries", 15)
        self.omap_file_lock_retry_sleep_interval = self.omap_state.config.getint_with_default("gateway",
                                                                                    "omap_file_lock_retry_sleep_interval", 5)
        # This is used for testing purposes only. To allow us testing locking from two gateways at the same time
        self.omap_file_disable_unlock = self.omap_state.config.getboolean_with_default("gateway", "omap_file_disable_unlock", False)
        if self.omap_file_disable_unlock:
            self.logger.warning(f"Will not unlock OMAP file for testing purposes")
        self.enter_args = {}

    def __call__(self, **kwargs):
        self.enter_args.clear()
        self.enter_args.update(kwargs)
        return self

    #
    # We pass the context from the different functions here. It should point to a real object in case we come from a real
    # resource changing function, resulting from a CLI command. It will be None in case we come from an automatic update
    # which is done because the local state is out of date. In case context is None, that is we're in the middle of an update
    # we should not try to lock the OMAP file as the code will not try to make changes there, only the local spdk calls
    # are done in such a case.
    #
    def __enter__(self):
        context = self.enter_args.get("context")
        if context and self.omap_file_lock_duration > 0:
            self.lock_omap()
        return self

    def __exit__(self, typ, value, traceback):
        context = self.enter_args.get("context")
        self.enter_args.clear()
        if context and self.omap_file_lock_duration > 0:
            self.unlock_omap()

    #
    # This function accepts a function in which there is Omap locking. It will execute this function
    # and in case the Omap is not current, will reload it and try again
    #
    def execute_omap_locking_function(self, grpc_func, omap_locking_func, request, context):
        for i in range(1, self.omap_file_update_reloads):
            need_to_update = False
            try:
                return grpc_func(omap_locking_func, request, context)
            except OSError as err:
                if err.errno == errno.EAGAIN:
                    need_to_update = True
                else:
                    raise

            assert need_to_update
            for j in range(10):
                if self.gateway_state.update():
                    # update was succesful, we can stop trying
                    break
                time.sleep(1)

        if need_to_update:
            raise Exception(f"Unable to lock OMAP file after reloading {self.omap_file_update_reloads} times, exiting")

    def lock_omap(self):
        got_lock = False

        for i in range(1, self.omap_file_lock_retries):
            try:
                self.omap_state.ioctx.lock_exclusive(self.omap_state.omap_name, self.OMAP_FILE_LOCK_NAME,
                                         self.OMAP_FILE_LOCK_COOKIE, "OMAP file changes lock", self.omap_file_lock_duration, 0)
                got_lock = True
                if i > 1:
                    self.logger.info(f"Succeeded to lock OMAP file after {i} tries")
                break
            except rados.ObjectExists as ex:
                self.logger.info(f"We already locked the OMAP file")
                got_lock = True
                break
            except rados.ObjectBusy as ex:
                self.logger.warning(
                       f"The OMAP file is locked, will try again in {self.omap_file_lock_retry_sleep_interval} seconds")
                time.sleep(self.omap_file_lock_retry_sleep_interval)
            except Exception as ex:
                self.logger.error(f"Unable to lock OMAP file, exiting: {ex}")
                raise

        if not got_lock:
            self.logger.error(f"Unable to lock OMAP file after {self.omap_file_lock_retries} tries. Exiting!")
            raise Exception("Unable to lock OMAP file")

        self.is_locked = True
        omap_version = self.omap_state.get_omap_version()
        local_version = self.omap_state.get_local_version()

        if omap_version > local_version:
            self.logger.warning(
                       f"Local version {local_version} differs from OMAP file version {omap_version}."
                       f" The file is not current, will reload it and try again")
            self.unlock_omap()
            raise OSError(errno.EAGAIN, "Unable to lock OMAP file, file not current", self.omap_state.omap_name)

    def unlock_omap(self):
        if self.omap_file_disable_unlock:
            self.logger.warning(f"OMAP file unlock was disabled, will not unlock file")
            return

        try:
            self.omap_state.ioctx.unlock(self.omap_state.omap_name, self.OMAP_FILE_LOCK_NAME, self.OMAP_FILE_LOCK_COOKIE)
            self.is_locked = False
        except rados.ObjectNotFound as ex:
            self.logger.warning(f"No such lock, the lock duration might have passed")
        except Exception as ex:
            self.logger.error(f"Unable to unlock OMAP file: {ex}")
            pass

    def locked(self):
        return self.is_locked

class OmapGatewayState(GatewayState):
    """Persists gateway NVMeoF target state to an OMAP object.

    Handles reads/writes of persistent NVMeoF target state data in key/value
    format within an OMAP object.

    Class attributes:
        X_KEY: Key name for "X"

    Instance attributes:
        config: Basic gateway parameters
        version: Local gateway NVMeoF target state version
        logger: Logger instance to track OMAP access events
        omap_name: OMAP object name
        ioctx: I/O context which allows OMAP access
        watch: Watcher for the OMAP object
    """

    OMAP_VERSION_KEY = "omap_version"

    def __init__(self, config):
        self.config = config
        self.version = 1
        self.logger = GatewayLogger(self.config).logger
        self.watch = None
        gateway_group = self.config.get("gateway", "group")
        self.omap_name = f"nvmeof.{gateway_group}.state" if gateway_group else "nvmeof.state"
        self.conn = None

        try:
            self.ioctx = self.open_rados_connection(self.config)
            # Create a new gateway persistence OMAP object
            with rados.WriteOpCtx() as write_op:
                # Set exclusive parameter to fail write_op if object exists
                write_op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(self.version),))
                self.ioctx.operate_write_op(write_op, self.omap_name)
                self.logger.info(
                    f"First gateway: created object {self.omap_name}")
        except rados.ObjectExists:
            self.logger.info(f"{self.omap_name} omap object already exists.")
        except Exception as ex:
            self.logger.error(f"Unable to create omap: {ex}. Exiting!")
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self.watch is not None:
            self.watch.close()
        self.ioctx.close()

    def check_for_old_format_omap_files(self):
        omap_dict = self.get_state()
        for omap_item_key in omap_dict.keys():
           if omap_item_key.startswith("bdev"):
               raise Exception("Old OMAP file format, still contains bdevs, please remove file and try again")

    def open_rados_connection(self, config):
        ceph_pool = config.get("ceph", "pool")
        ceph_conf = config.get("ceph", "config_file")
        rados_id = config.get_with_default("ceph", "id", "")
        conn = rados.Rados(conffile=ceph_conf, rados_id=rados_id)
        conn.connect()
        self.conn = conn
        ioctx = conn.open_ioctx(ceph_pool)
        return ioctx

    def get_local_version(self) -> int:
        """Returns local version."""
        return self.version

    def set_local_version(self, version_update: int):
        """Sets local version."""
        self.version = version_update

    def get_omap_version(self) -> int:
        """Returns OMAP version."""
        with rados.ReadOpCtx() as read_op:
            i, _ = self.ioctx.get_omap_vals_by_keys(read_op,
                                                    (self.OMAP_VERSION_KEY,))
            self.ioctx.operate_read_op(read_op, self.omap_name)
        value_list = list(dict(i).values())
        if len(value_list) == 1:
            val = int(value_list[0])
            return val
        else:
            self.logger.error(
                f"Read of OMAP version key ({self.OMAP_VERSION_KEY}) returns"
                f" invalid number of values ({value_list}).")
            raise

    def get_state(self) -> Dict[str, str]:
        """Returns dict of all OMAP keys and values."""
        omap_list = [("", 0)]   # Dummy, non empty, list value. Just so we would enter the while
        omap_dict = {}
        # The number of items returned is limited by Ceph, so we need to read in a loop until no more items are returned
        while len(omap_list) > 0:
            last_key_read = omap_list[-1][0]
            with rados.ReadOpCtx() as read_op:
                i, _ = self.ioctx.get_omap_vals(read_op, last_key_read, "", -1)
                self.ioctx.operate_read_op(read_op, self.omap_name)
                omap_list = list(i)
                omap_dict.update(dict(omap_list))
        return omap_dict

    def _add_key(self, key: str, val: str):
        """Adds key and value to the OMAP."""
        try:
            version_update = self.version + 1
            with rados.WriteOpCtx() as write_op:
                # Compare operation failure will cause write failure
                write_op.omap_cmp(self.OMAP_VERSION_KEY, str(self.version),
                                  rados.LIBRADOS_CMPXATTR_OP_EQ)
                self.ioctx.set_omap(write_op, (key,), (val,))
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(version_update),))
                self.ioctx.operate_write_op(write_op, self.omap_name)
            self.version = version_update
            self.logger.debug(f"omap_key generated: {key}")
        except Exception as ex:
            self.logger.error(f"Unable to add key to omap: {ex}. Exiting!")
            raise

        # Notify other gateways within the group of change
        try:
            self.ioctx.notify(self.omap_name)
        except Exception as ex:
            self.logger.info(f"Failed to notify.")

    def _remove_key(self, key: str):
        """Removes key from the OMAP."""
        try:
            version_update = self.version + 1
            with rados.WriteOpCtx() as write_op:
                # Compare operation failure will cause remove failure
                write_op.omap_cmp(self.OMAP_VERSION_KEY, str(self.version),
                                  rados.LIBRADOS_CMPXATTR_OP_EQ)
                self.ioctx.remove_omap_keys(write_op, (key,))
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(version_update),))
                self.ioctx.operate_write_op(write_op, self.omap_name)
            self.version = version_update
            self.logger.debug(f"omap_key removed: {key}")
        except Exception as ex:
            self.logger.error(f"Unable to remove key from omap: {ex}. Exiting!")
            raise

        # Notify other gateways within the group of change
        try:
            self.ioctx.notify(self.omap_name)
        except Exception as ex:
            self.logger.info(f"Failed to notify.")

    def delete_state(self):
        """Deletes OMAP object contents."""
        try:
            with rados.WriteOpCtx() as write_op:
                self.ioctx.clear_omap(write_op)
                self.ioctx.operate_write_op(write_op, self.omap_name)
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(1),))
                self.ioctx.operate_write_op(write_op, self.omap_name)
                self.logger.info(f"Deleted OMAP contents.")
        except Exception as ex:
            self.logger.error(f"Error deleting OMAP contents: {ex}. Exiting!")
            raise

    def register_watch(self, notify_event):
        """Sets a watch on the OMAP object for changes."""

        def _watcher_callback(notify_id, notifier_id, watch_id, data):
            notify_event.set()

        if self.watch is None:
            try:
                self.watch = self.ioctx.watch(self.omap_name, _watcher_callback)
            except Exception as ex:
                self.logger.error(f"Unable to initiate watch: {ex}")
        else:
            self.logger.info(f"Watch already exists.")


class GatewayStateHandler:
    """Maintains consistency in NVMeoF target state store instances.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track events
        local: Local GatewayState instance
        gateway_rpc_caller: Callback to GatewayServer.gateway_rpc_caller
        omap: OMAP GatewayState instance
        update_interval: Interval to periodically poll for updates
        update_timer: Timer to check for gateway state updates
        use_notify: Flag to indicate use of OMAP watch/notify
    """

    def __init__(self, config, local, omap, gateway_rpc_caller):
        self.config = config
        self.local = local
        self.omap = omap
        self.gateway_rpc_caller = gateway_rpc_caller
        self.update_timer = None
        self.logger = GatewayLogger(self.config).logger
        self.update_interval = self.config.getint("gateway",
                                                  "state_update_interval_sec")
        if self.update_interval < 1:
            self.logger.info("Invalid state_update_interval_sec. Setting to 1.")
            self.update_interval = 1
        self.use_notify = self.config.getboolean("gateway",
                                                 "state_update_notify")
        self.update_is_active_lock = threading.Lock()

    def add_namespace(self, subsystem_nqn: str, nsid: str, val: str):
        """Adds a namespace to the state data store."""
        self.omap.add_namespace(subsystem_nqn, nsid, val)
        self.local.add_namespace(subsystem_nqn, nsid, val)

    def remove_namespace(self, subsystem_nqn: str, nsid: str):
        """Removes a namespace from the state data store."""
        self.omap.remove_namespace(subsystem_nqn, nsid)
        self.local.remove_namespace(subsystem_nqn, nsid)

    def add_namespace_qos(self, subsystem_nqn: str, nsid: str, val: str):
        """Adds namespace's QOS settings to the state data store."""
        self.omap.add_namespace_qos(subsystem_nqn, nsid, val)
        self.local.add_namespace_qos(subsystem_nqn, nsid, val)

    def remove_namespace_qos(self, subsystem_nqn: str, nsid: str):
        """Removes namespace's QOS settings from the state data store."""
        self.omap.remove_namespace_qos(subsystem_nqn, nsid)
        self.local.remove_namespace_qos(subsystem_nqn, nsid)

    def add_subsystem(self, subsystem_nqn: str, val: str):
        """Adds a subsystem to the state data store."""
        self.omap.add_subsystem(subsystem_nqn, val)
        self.local.add_subsystem(subsystem_nqn, val)

    def remove_subsystem(self, subsystem_nqn: str):
        """Removes a subsystem from the state data store."""
        self.omap.remove_subsystem(subsystem_nqn)
        self.local.remove_subsystem(subsystem_nqn)

    def add_host(self, subsystem_nqn: str, host_nqn: str, val: str):
        """Adds a host to the state data store."""
        self.omap.add_host(subsystem_nqn, host_nqn, val)
        self.local.add_host(subsystem_nqn, host_nqn, val)

    def remove_host(self, subsystem_nqn: str, host_nqn: str):
        """Removes a host from the state data store."""
        self.omap.remove_host(subsystem_nqn, host_nqn)
        self.local.remove_host(subsystem_nqn, host_nqn)

    def add_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                     traddr: str, trsvcid: str, val: str):
        """Adds a listener to the state data store."""
        self.omap.add_listener(subsystem_nqn, gateway, trtype, traddr, trsvcid,
                               val)
        self.local.add_listener(subsystem_nqn, gateway, trtype, traddr, trsvcid,
                                val)

    def remove_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                        traddr: str, trsvcid: str):
        """Removes a listener from the state data store."""
        self.omap.remove_listener(subsystem_nqn, gateway, trtype, traddr,
                                  trsvcid)
        self.local.remove_listener(subsystem_nqn, gateway, trtype, traddr,
                                   trsvcid)

    def delete_state(self):
        """Deletes state data stores."""
        self.omap.delete_state()
        self.local.delete_state()

    def start_update(self):
        """Initiates periodic polling and watch/notify for updates."""
        notify_event = threading.Event()
        if self.use_notify:
            # Register a watch on omap state
            self.omap.register_watch(notify_event)

        # Start polling for state updates
        if self.update_timer is None:
            self.update_timer = threading.Thread(target=self._update_caller,
                                                 daemon=True,
                                                 args=(notify_event,))
            self.update_timer.start()
        else:
            self.logger.info("Update timer already set.")

    def _update_caller(self, notify_event):
        """Periodically calls for update."""
        while True:
            update_time = time.time() + self.update_interval
            self.update()
            notify_event.wait(max(update_time - time.time(), 0))
            notify_event.clear()

    def compare_state_values(self, val1, val2) -> bool:
        # We sometimes get one value as type bytes and the other as type str, so convert them both to str for the comparison
        val1_str = val1.decode() if type(val1) == type(b'') else val1
        val2_str = val2.decode() if type(val2) == type(b'') else val2
        return val1_str == val2_str

    def update(self) -> bool:
        """Checks for updated omap state and initiates local update."""

        if self.update_is_active_lock.locked():
            self.logger.warning(f"An update is already running, ignore")
            return False

        with self.update_is_active_lock:
            prefix_list = [
                GatewayState.SUBSYSTEM_PREFIX,
                GatewayState.NAMESPACE_PREFIX, GatewayState.HOST_PREFIX,
                GatewayState.NAMESPACE_QOS_PREFIX,
                GatewayState.LISTENER_PREFIX
            ]

            # Get version and state from OMAP
            omap_state_dict = self.omap.get_state()
            omap_version = int(omap_state_dict[self.omap.OMAP_VERSION_KEY])

            if self.omap.get_local_version() < omap_version:
                local_state_dict = self.local.get_state()
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
                    if not self.compare_state_values(local_state_dict[key], omap_state_dict[key])
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
                self.local.reset(omap_state_dict)
                self.omap.set_local_version(omap_version)
                self.logger.debug("Update complete.")

        return True

    def _group_by_prefix(self, state_update, prefix_list):
        """Groups state update by key prefixes."""
        grouped_state_update = defaultdict(dict)
        for key, val in state_update.items():
            for prefix in prefix_list:
                if key.startswith(prefix):
                    grouped_state_update[prefix][key] = val
        return grouped_state_update

    def _update_call_rpc(self, grouped_state_update, is_add_req, prefix_list):
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
