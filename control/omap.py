#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#
import logging
import rados
from typing import Dict
from collections import defaultdict


class OmapObject:
    """Class representing versioned omap object"""
    OMAP_VERSION_KEY = "omap_version"

    def __init__(self, name, ioctx) -> None:
        self.version = 1
        self.watch = None
        self.cached_object = defaultdict(dict)
        self.name = name
        self.logger = logging.getLogger(__name__)
        self.ioctx = ioctx
        self.create()

    def create(self) -> None:
        """Create OMAP object if does not exist already"""
        try:
            # Create a new persistence OMAP object
            with rados.WriteOpCtx() as write_op:
                # Set exclusive parameter to fail write_op if object exists
                write_op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(self.version),))
                self.ioctx.operate_write_op(write_op, self.name)
                self.logger.info(
                    f"First gateway: created object {self.name}")
        except rados.ObjectExists:
            self.logger.info(f"{self.name} omap object already exists.")
        except Exception:
            self.logger.exception(f"Unable to create omap {self.name}:")
            raise

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Context destructor"""
        if self.watch is not None:
            self.watch.close()
        self.ioctx.close()

    def get(self) -> Dict[str, str]:
        """Returns dict of all OMAP keys and values."""
        with rados.ReadOpCtx() as read_op:
            i, _ = self.ioctx.get_omap_vals(read_op, "", "", -1)
            self.ioctx.operate_read_op(read_op, self.name)
            omap_dict = dict(i)
        return omap_dict

    def _notify(self) -> None:
        """ Notify other gateways within the group of change """
        try:
            self.ioctx.notify(self.name)
        except Exception as ex:
            self.logger.info(f"Failed to notify.")

    def add_key(self, key: str, val: str) -> None:
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
                self.ioctx.operate_write_op(write_op, self.name)
            self.version = version_update
            self.logger.debug(f"omap_key generated: {key}")
        except Exception as ex:
            self.logger.error(f"Unable to add key to omap: {ex}. Exiting!")
            raise

        self._notify()

    def remove_key(self, key: str) -> None:
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
                self.ioctx.operate_write_op(write_op, self.name)
            self.version = version_update
            self.logger.debug(f"omap_key removed: {key}")
        except Exception:
            self.logger.exception(f"Unable to remove key from omap:")
            raise

        self._notify()

    def delete(self) -> None:
        """Deletes OMAP object contents."""
        try:
            with rados.WriteOpCtx() as write_op:
                self.ioctx.clear_omap(write_op)
                self.ioctx.operate_write_op(write_op, self.name)
                self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,),
                                    (str(1),))
                self.ioctx.operate_write_op(write_op, self.name)
                self.logger.info(f"Deleted OMAP {self.name} contents.")
        except Exception:
            self.logger.exception(f"Error deleting OMAP {self.name} contents:")
            raise

    def register_watch(self, notify_event) -> None:
        """Sets a watch on the OMAP object for changes."""

        def _watcher_callback(notify_id, notifier_id, watch_id, data):
            notify_event.set()

        if self.watch is None:
            try:
                self.watch = self.ioctx.watch(self.name, _watcher_callback)
            except Exception:
                self.logger.exception(f"Unable to initiate watch {self.name}:")
                raise
        else:
            self.logger.info(f"Watch {self.name} already exists.")
