#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import rados
from typing import Dict, Optional
from abc import ABC, abstractmethod
import nvme_gw_pb2 as pb2
from google.protobuf import json_format


class PersistentConfig(ABC):
    """Persists gateway NVMeoF target configuration."""

    @abstractmethod
    def add_bdev(self, bdev_name: str, val: str):
        pass

    @abstractmethod
    def delete_bdev(self, bdev_name: str):
        pass

    @abstractmethod
    def add_namespace(self, subsystem_nqn: str, bdev_name: str, val: str):
        pass

    @abstractmethod
    def delete_namespace(self, subsystem_nqn: str, bdev_name: str):
        pass

    @abstractmethod
    def add_subsystem(self, subsystem_nqn: str, val: str):
        pass

    @abstractmethod
    def delete_subsystem(self, subsystem_nqn: str):
        pass

    @abstractmethod
    def add_host(self, subsystem_nqn: str, host_nqn: str, val: str):
        pass

    @abstractmethod
    def delete_host(self, subsystem_nqn: str, host_nqn: str):
        pass

    @abstractmethod
    def add_listener(self, subsystem_nqn: str, traddr: str, trsvcid: str,
                     val: str):
        pass

    @abstractmethod
    def delete_listener(self, subsystem_nqn: str, traddr: str, trsvcid: str):
        pass

    @abstractmethod
    def delete_config(self):
        pass

    @abstractmethod
    def restore(self, callbacks):
        pass


class OmapPersistentConfig(PersistentConfig):
    """Persists NVMeoF target configuration to an OMAP object.

    Handles reads/writes of persistent NVMeoF target configuration data in 
    key/value format within an OMAP object.

    Class attributes:
        X_KEY: OMAP key name for "X"
        X_PREFIX: OMAP key prefix for key of type "X"

    Instance attributes:
        version: Local gateway NVMeoF target configuration version
        nvme_config: Basic gateway parameters
        logger: Logger instance to track OMAP access events
        spdk_rpc: Module methods for SPDK
        spdk_rpc_client: Client of SPDK RPC server
        omap_name: OMAP object name
        ioctx: I/O context which allows OMAP access
    """

    OMAP_VERSION_KEY = "omap_version"
    BDEV_PREFIX = "bdev_"
    NAMESPACE_PREFIX = "namespace_"
    SUBSYSTEM_PREFIX = "subsystem_"
    HOST_PREFIX = "host_"
    LISTENER_PREFIX = "listener_"

    def __init__(self, nvme_config):
        self.version = 1
        self.nvme_config = nvme_config
        self.logger = nvme_config.logger

        gateway_group = self.nvme_config.get("config", "gateway_group")
        self.omap_name = f"nvme.{gateway_group}.config" if gateway_group else "nvme.config"

        ceph_pool = self.nvme_config.get("ceph", "pool")
        ceph_conf = self.nvme_config.get("ceph", "config_file")
        conn = rados.Rados(conffile=ceph_conf)
        conn.connect()
        self.ioctx = conn.open_ioctx(ceph_pool)

        try:
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
            self.logger.error(f"Unable to write to omap: {ex}. Exiting!")
            raise

    def _write_key(self, key: str, val: str):
        """Writes key and value to the persistent config."""

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
            self.logger.error(f"Unable to write to omap: {ex}. Exiting!")
            raise

    def _delete_key(self, key: str):
        """Deletes key from omap persistent config."""

        try:
            version_update = self.version + 1
            with rados.WriteOpCtx() as write_op:
                # Compare operation failure will cause delete failure
                write_op.omap_cmp(self.OMAP_VERSION_KEY, str(self.version),
                                rados.LIBRADOS_CMPXATTR_OP_EQ)
                self.ioctx.remove_omap_keys(write_op, (key,))
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(version_update),))
                self.ioctx.operate_write_op(write_op, self.omap_name)
            self.version = version_update
            self.logger.debug(f"omap_key deleted: {key}")
        except Exception as ex:
            self.logger.error(f"Unable to delete from omap: {ex}. Exiting!")
            raise

    def add_bdev(self, bdev_name: str, val: str):
        """Adds a bdev to the persistent config."""
        key = self.BDEV_PREFIX + bdev_name
        self._write_key(key, val)

    def delete_bdev(self, bdev_name: str):
        """Deletes a bdev from the persistent config."""
        key = self.BDEV_PREFIX + bdev_name
        self._delete_key(key)

    def _restore_bdevs(self, omap_dict, callback):
        """Restores a bdev from the persistent config."""

        for (key, val) in omap_dict.items():
            if key.startswith(self.BDEV_PREFIX):
                req = json_format.Parse(val, pb2.bdev_create_req())
                callback(req)

    def add_namespace(self, subsystem_nqn: str, nsid: str, val: str):
        """Adds a namespace to the persistent config."""
        key = self.NAMESPACE_PREFIX + subsystem_nqn + "_" + nsid
        self._write_key(key, val)

    def delete_namespace(self, subsystem_nqn: str, nsid: str):
        """Deletes a namespace from the persistent config."""
        key = self.NAMESPACE_PREFIX + subsystem_nqn + "_" + nsid
        self._delete_key(key)

    def _restore_namespaces(self, omap_dict, callback):
        """Restores a namespace from the persistent config."""

        for (key, val) in omap_dict.items():
            if key.startswith(self.NAMESPACE_PREFIX):
                # Get NSID from end of key
                nsid = key.rsplit("_", 1)[1]
                req = json_format.Parse(val, pb2.subsystem_add_ns_req())
                req.nsid = int(nsid)
                callback(req)

    def add_subsystem(self, subsystem_nqn: str, val: str):
        """Adds a subsystem to the persistent config."""
        key = self.SUBSYSTEM_PREFIX + subsystem_nqn
        self._write_key(key, val)

    def delete_subsystem(self, subsystem_nqn: str):
        """Deletes a subsystem from the persistent config."""
        key = self.SUBSYSTEM_PREFIX + subsystem_nqn
        self._delete_key(key)

        # Delete all keys related to subsystem
        omap_dict = self._read_all()
        for key in omap_dict.keys():
            if (key.startswith(self.NAMESPACE_PREFIX + subsystem_nqn) or
                    key.startswith(self.HOST_PREFIX + subsystem_nqn) or
                    key.startswith(self.LISTENER_PREFIX + subsystem_nqn)):
                self._delete_key(key)

    def _restore_subsystems(self, omap_dict, callback):
        """Restores subsystems from the persistent config."""

        for (key, val) in omap_dict.items():
            if key.startswith(self.SUBSYSTEM_PREFIX):
                req = json_format.Parse(val, pb2.subsystem_create_req())
                callback(req)

    def add_host(self, subsystem_nqn: str, host_nqn: str, val: str):
        """Adds a host to the persistent config."""
        key = "{}{}_{}".format(self.HOST_PREFIX, subsystem_nqn, host_nqn)
        self._write_key(key, val)

    def delete_host(self, subsystem_nqn: str, host_nqn: str):
        """Deletes a host from the persistent config."""
        key = "{}{}_{}".format(self.HOST_PREFIX, subsystem_nqn, host_nqn)
        self._delete_key(key)

    def _restore_hosts(self, omap_dict, callback):
        """Restore hosts from the persistent config."""

        for (key, val) in omap_dict.items():
            if key.startswith(self.HOST_PREFIX):
                req = json_format.Parse(val, pb2.subsystem_add_host_req())
                callback(req)

    def add_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                     traddr: str, trsvcid: str, val: str):
        """Adds a listener to the persistent config."""
        key = "{}{}_{}_{}_{}_{}".format(self.LISTENER_PREFIX, gateway,
                                        subsystem_nqn, trtype, traddr, trsvcid)
        self._write_key(key, val)

    def delete_listener(self, subsystem_nqn: str, gateway: str, trtype: str,
                        traddr: str, trsvcid: str):
        """Deletes a listener from the persistent config."""
        key = "{}{}_{}_{}_{}_{}".format(self.LISTENER_PREFIX, gateway,
                                        subsystem_nqn, trtype, traddr, trsvcid)
        self._delete_key(key)

    def _restore_listeners(self, omap_dict, callback):
        """Restores listeners from the persistent config."""

        for (key, val) in omap_dict.items():
            if key.startswith(self.LISTENER_PREFIX):
                req = json_format.Parse(val, pb2.subsystem_add_listener_req())
                callback(req)

    def _read_key(self, key) -> Optional[str]:
        """Reads single key from persistent config and returns its value."""

        with rados.ReadOpCtx() as read_op:
            iter, _ = self.ioctx.get_omap_vals_by_keys(read_op, (key,))
            self.ioctx.operate_read_op(read_op, self.omap_name)
            value_list = list(dict(iter).values())
            if len(value_list) == 1:
                val = str(value_list[0], "utf-8")
                self.logger.debug(f"Read key: {key} -> {val}")
                return val
        return None

    def _read_all(self) -> Dict[str, str]:
        """Reads persistent config and returns dict of all keys and values."""

        with rados.ReadOpCtx() as read_op:
            iter, _ = self.ioctx.get_omap_vals(read_op, "", "", -1)
            self.ioctx.operate_read_op(read_op, self.omap_name)
            omap_dict = dict(iter)
        return omap_dict

    def delete_config(self):
        """Deletes OMAP object."""

        try:
            self.ioctx.remove_object(self.omap_name)
            self.logger.info(f"Object {self.omap_name} deleted.")
        except rados.ObjectNotFound:
            self.logger.info(f"Object {self.omap_name} not found.")

    def restore(self, callbacks):
        """Restores gateway config to persistent config specifications."""

        omap_version = self._read_key(self.OMAP_VERSION_KEY)
        if omap_version == "1":
            self.logger.info("This omap was just created. Nothing to restore")
        else:
            omap_dict = self._read_all()
            self._restore_bdevs(omap_dict, callbacks[self.BDEV_PREFIX])
            self._restore_subsystems(omap_dict,
                                     callbacks[self.SUBSYSTEM_PREFIX])
            self._restore_namespaces(omap_dict,
                                     callbacks[self.NAMESPACE_PREFIX])
            self._restore_hosts(omap_dict, callbacks[self.HOST_PREFIX])
            self._restore_listeners(omap_dict, callbacks[self.LISTENER_PREFIX])
            self.version = int(omap_dict[self.OMAP_VERSION_KEY])
            self.logger.info("Restore complete.")
