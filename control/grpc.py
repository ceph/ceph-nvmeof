#  ############################
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import socket
import grpc
import json
import uuid
import random
import os
import errno
import threading
import hashlib
import tempfile
import time
from ipaddress import ip_address
from pathlib import Path
from typing import Iterator, Callable, Optional
from collections import defaultdict
from copy import deepcopy
import logging
import shutil

from spdk.rpc.client import JSONRPCException
from google.protobuf import json_format
from google.protobuf.empty_pb2 import Empty
from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc
from .proto import monitor_pb2
from .proto import monitor_pb2_grpc
from .config import GatewayConfig
from .utils import GatewayEnumUtils
from .utils import GatewayUtils
from .utils import GatewayUtilsCrypto
from .utils import GatewayKeyUtils
from .utils import GatewayLogger
from .utils import NICS
from .state import GatewayState, GatewayStateHandler, OmapLock
from .cephutils import CephUtils
from .rebalance import Rebalance
from .cluster import get_cluster_allocator
from .kmip_client import NVMeoFKMIPClient

# Assuming max of 32 gateways and protocol min 1 max 65519
CNTLID_RANGE_SIZE = 2040
DEFAULT_MODEL_NUMBER = "Ceph bdev Controller"

MONITOR_POLLING_RATE_SEC = 2     # monitor polls gw each 2 seconds


class SubsystemsCache:
    def __init__(self):
        self.cache_lock = threading.Lock()
        with self.cache_lock:
            self.subsystems_info = pb2.subsystems_info(subsystems=[])

    def get_subsystems(self) -> pb2.subsystems_info:
        with self.cache_lock:
            return self.subsystems_info

    def get_one_subsystem(self, subsys: str) -> list[pb2.subsystem]:
        if not subsys:
            return []

        with self.cache_lock:
            for s in self.subsystems_info.subsystems:
                if s.nqn == subsys:
                    return [s]
        return []

    def set_subsystems(self, subsystems: pb2.subsystems_info):
        with self.cache_lock:
            self.subsystems_info = subsystems


class BdevStatus:
    def __init__(self, status, error_message, bdev_name="",
                 rbd_pool=None, rbd_image_name=None, rados_namespace_name=None, trash_image=False):
        self.status = status
        self.error_message = error_message
        self.bdev_name = bdev_name
        self.rbd_pool = rbd_pool
        self.rbd_image_name = rbd_image_name
        self.rados_namespace_name = rados_namespace_name
        self.trash_image = trash_image


class MonitorGroupService(monitor_pb2_grpc.MonitorGroupServicer):
    def __init__(self, set_group_id: Callable[[int], None]) -> None:
        self.set_group_id = set_group_id

    def group_id(self, request: monitor_pb2.group_id_req, context=None) -> Empty:
        self.set_group_id(request.id)
        return Empty()


class SubsystemHostAuth:
    def __init__(self):
        self.subsys_allow_any_hosts = defaultdict(dict)
        self.subsys_created_without_key = defaultdict(set)
        self.subsys_dhchap_key = defaultdict(dict)
        self.host_dhchap_key = defaultdict(dict)
        self.host_dhchap_ctrlr_key = defaultdict(dict)
        self.host_psk_key = defaultdict(dict)
        self.host_nqn = defaultdict(set)
        self.host_ka_timeout = defaultdict(set)
        self.host_ka_timeout_lock = threading.Lock()

    def clean_subsystem(self, subsys):
        self.host_psk_key.pop(subsys, None)
        self.host_dhchap_key.pop(subsys, None)
        self.host_dhchap_ctrlr_key.pop(subsys, None)
        self.subsys_allow_any_hosts.pop(subsys, None)
        self.subsys_dhchap_key.pop(subsys, None)
        self.host_nqn.pop(subsys, None)
        self.host_ka_timeout.pop(subsys, None)

    def add_psk_host(self, subsys, host, key):
        if key:
            self.host_psk_key[subsys][host] = key

    def remove_psk_host(self, subsys, host):
        if subsys in self.host_psk_key:
            self.host_psk_key[subsys].pop(host, None)
            if len(self.host_psk_key[subsys]) == 0:
                self.host_psk_key.pop(subsys, None)    # last host was removed from subsystem

    def is_psk_host(self, subsys, host) -> bool:
        key = self.get_host_psk_key(subsys, host)
        return True if key else False

    def get_host_psk_key(self, subsys, host) -> str:
        key = None
        if subsys in self.host_psk_key and host in self.host_psk_key[subsys]:
            key = self.host_psk_key[subsys][host]
        return key

    def add_dhchap_host(self, subsys, host, key):
        if key:
            self.host_dhchap_key[subsys][host] = key

    def remove_dhchap_host(self, subsys, host):
        if subsys in self.host_dhchap_key:
            self.host_dhchap_key[subsys].pop(host, None)
            if len(self.host_dhchap_key[subsys]) == 0:
                self.host_dhchap_key.pop(subsys, None)    # last host was removed from subsystem

    def is_dhchap_host(self, subsys, host) -> bool:
        key = self.get_host_dhchap_key(subsys, host)
        return True if key else False

    def get_host_dhchap_key(self, subsys, host) -> str:
        key = None
        if subsys in self.host_dhchap_key and host in self.host_dhchap_key[subsys]:
            key = self.host_dhchap_key[subsys][host]
        return key

    def get_hosts_with_dhchap_key(self, subsys):
        if subsys in self.host_dhchap_key:
            return self.host_dhchap_key[subsys]
        return {}

    def add_dhchap_ctrlr_host(self, subsys, host, key):
        if key:
            self.host_dhchap_ctrlr_key[subsys][host] = key

    def remove_dhchap_ctrlr_host(self, subsys, host):
        if subsys in self.host_dhchap_ctrlr_key:
            self.host_dhchap_ctrlr_key[subsys].pop(host, None)
            if len(self.host_dhchap_ctrlr_key[subsys]) == 0:
                self.host_dhchap_ctrlr_key.pop(subsys, None)    # last host removed from subsystem

    def is_dhchap_ctrlr_host(self, subsys, host) -> bool:
        key = self.get_host_dhchap_ctrlr_key(subsys, host)
        return True if key else False

    def get_host_dhchap_ctrlr_key(self, subsys, host) -> str:
        key = None
        if subsys in self.host_dhchap_ctrlr_key and host in self.host_dhchap_ctrlr_key[subsys]:
            key = self.host_dhchap_ctrlr_key[subsys][host]
        return key

    def get_hosts_with_dhchap_ctrlr_key(self, subsys):
        if subsys in self.host_dhchap_key:
            return self.host_dhchap_ctrlr_key[subsys]
        return {}

    def get_hosts_with_any_dhchap_key(self, subsys):
        return self.get_hosts_with_dhchap_key(subsys) | self.get_hosts_with_dhchap_ctrlr_key(
            subsys)

    def add_host_nqn(self, subsys, hostnqn):
        self.host_nqn[subsys].add(hostnqn)

    def remove_host_nqn(self, subsys, hostnqn):
        if subsys not in self.host_nqn:
            return
        self.host_nqn[subsys].discard(hostnqn)

    def does_host_exist(self, subsys, hostnqn) -> bool:
        if subsys not in self.host_nqn:
            return False
        if hostnqn not in self.host_nqn[subsys]:
            return False
        return True

    def get_host_count(self, subsys):
        if subsys is None:
            subsys_list = self.host_nqn
        else:
            if subsys not in self.host_nqn:
                return 0
            subsys_list = [subsys]

        cnt = 0
        for s in subsys_list:
            cnt += len(self.host_nqn[s])
        return cnt

    def set_host_keepalive_timeout_disconnection(self, subsys, hostnqn):
        with self.host_ka_timeout_lock:
            self.host_ka_timeout[subsys].add(hostnqn)

    def reset_host_keepalive_timeout_disconnection(self, subsys, hostnqn=None):
        with self.host_ka_timeout_lock:
            if subsys not in self.host_ka_timeout:
                return
            if hostnqn is None:
                # No host NQN, clear the entire subsystem
                self.host_ka_timeout.pop(subsys, None)
                return
            self.host_ka_timeout[subsys].discard(hostnqn)
            if not self.host_ka_timeout[subsys]:
                # We removed the last host of this subsystem, delete it
                self.host_ka_timeout.pop(subsys, None)

    def get_keepalive_timeout_disconnections(self, subsys):
        disconnected_list = []
        with self.host_ka_timeout_lock:
            if subsys in self.host_ka_timeout:
                for h in self.host_ka_timeout[subsys]:
                    disconnected_list.append(h)
        return disconnected_list

    def was_host_disconnected_due_to_keepalive_timeout(self, subsys, hostnqn) -> bool:
        with self.host_ka_timeout_lock:
            if subsys not in self.host_ka_timeout:
                return False
            return hostnqn in self.host_ka_timeout[subsys]

    def allow_any_host(self, subsys):
        self.subsys_allow_any_hosts[subsys] = True

    def disallow_any_host(self, subsys):
        self.subsys_allow_any_hosts.pop(subsys, None)

    def is_any_host_allowed(self, subsys) -> bool:
        return subsys in self.subsys_allow_any_hosts

    def set_subsystem_created_without_key(self, subsys):
        self.subsys_created_without_key[subsys]

    def reset_subsystem_created_without_key(self, subsys):
        self.subsys_created_without_key.pop(subsys, None)

    def was_subsystem_created_without_key(self, subsys):
        return subsys in self.subsys_created_without_key

    def add_dhchap_key_to_subsystem(self, subsys, key):
        if key:
            self.subsys_dhchap_key[subsys] = key

    def remove_dhchap_key_from_subsystem(self, subsys):
        self.subsys_dhchap_key.pop(subsys, None)

    def does_subsystem_have_dhchap_key(self, subsys) -> bool:
        key = self.get_subsystem_dhchap_key(subsys)
        return True if key else False

    def get_subsystem_dhchap_key(self, subsys) -> str:
        key = None
        if subsys in self.subsys_dhchap_key:
            key = self.subsys_dhchap_key[subsys]
        return key


class NamespaceInfo:
    def __init__(self, subsys, nsid, bdev, uuid, anagrpid, auto_visible, pool, data_pool, image,
                 rados_namespace_name, trash_image, read_only, location, auto_resize,
                 encryption_entries, encryption_algorithm):
        self.subsys = subsys
        self.nsid = nsid
        self.bdev = bdev
        self.uuid = uuid
        self.auto_visible = auto_visible
        self.anagrpid = anagrpid
        self.host_list = []
        self.pool = pool
        self.data_pool = data_pool
        self.image = image
        self.rados_namespace_name = rados_namespace_name
        self.trash_image = trash_image
        self.read_only = read_only
        self.location = location
        self.auto_resize = auto_resize
        self.image_was_shrunk = False
        self.encryption_entries = encryption_entries
        self.encryption_algorithm = encryption_algorithm

    def __str__(self):
        return f"subsys: {self.subsys}, nsid: {self.nsid}, " \
               f"bdev: {self.bdev}, uuid: {self.uuid}, " \
               f"auto_visible: {self.auto_visible}, anagrpid: {self.anagrpid}, " \
               f"pool: {self.pool}, data_pool: {self.data_pool}, image: {self.image}, " \
               f"rados_namespace: {self.rados_namespace_name}, " \
               f"trash_image: {self.trash_image}, " \
               f"read_only: {self.read_only}, image_shrunk: {self.image_was_shrunk}, " \
               f"location: {self.location}, " \
               f"auto_resize: {self.auto_resize}, " \
               f"encryption_entries: {self.encryption_entries}, " \
               f"encryption_algorithm: {self.encryption_algorithm}, " \
               f"hosts: {self.host_list}"

    def empty(self) -> bool:
        if self.bdev or self.uuid:
            return False
        return True

    def add_host(self, host_nqn):
        if host_nqn not in self.host_list:
            self.host_list.append(host_nqn)

    def remove_host(self, host_nqn):
        try:
            self.host_list.remove(host_nqn)
        except ValueError:
            pass
        if len(self.host_list) == 1 and self.is_host_in_namespace("*"):
            self.remove_all_hosts()

    def remove_all_hosts(self):
        self.host_list = []

    def set_visibility(self, auto_visible: bool):
        self.auto_visible = auto_visible

    def set_location(self, location: str):
        self.location = location

    def is_host_in_namespace(self, host_nqn):
        return host_nqn in self.host_list

    def host_count(self):
        return len(self.host_list)

    def set_ana_group_id(self, anagrpid):
        self.anagrpid = anagrpid

    def set_image_was_shrunk(self, was_shrunk: bool):
        self.image_was_shrunk = was_shrunk

    def was_image_shrunk(self):
        return self.image_was_shrunk

    @staticmethod
    def are_uuids_equal(uuid1: str, uuid2: str) -> bool:
        assert uuid1 and uuid2, "UUID can't be empty"
        try:
            if uuid.UUID(uuid1) == uuid.UUID(uuid2):
                return True
        except Exception:
            pass
        return False


class NamespacesLocalList:
    EMPTY_NAMESPACE = NamespaceInfo(None, None, None, None, 0, False, None, None,
                                    None, None, False, False, None, False, None, None)

    def __init__(self):
        self.namespace_list = defaultdict(dict)
        self.namespace_list_lock = threading.Lock()

    def remove_namespace(self, nqn, nsid=None):
        with self.namespace_list_lock:
            if nqn in self.namespace_list:
                if nsid:
                    if nsid in self.namespace_list[nqn]:
                        self.namespace_list[nqn].pop(nsid, None)
                        if len(self.namespace_list[nqn]) == 0:
                            self.namespace_list.pop(nqn, None)   # last ns of subsystem was removed
                else:
                    self.namespace_list.pop(nqn, None)

    def add_namespace(
            self,
            nqn,
            nsid,
            bdev,
            uuid,
            anagrpid,
            auto_visible,
            pool,
            data_pool,
            image,
            rados_namespace_name,
            trash_image,
            read_only,
            location,
            auto_resize,
            encryption_entries,
            encryption_algorithm):
        if not bdev:
            bdev = GatewayService.find_unique_bdev_name(uuid)
        with self.namespace_list_lock:
            self.namespace_list[nqn][nsid] = NamespaceInfo(nqn, nsid, bdev, uuid, anagrpid,
                                                           auto_visible, pool, data_pool,
                                                           image, rados_namespace_name,
                                                           trash_image, read_only,
                                                           location, auto_resize,
                                                           encryption_entries,
                                                           encryption_algorithm)

    def find_namespace(self, nqn, nsid, uuid=None, bdev=None) -> NamespaceInfo:
        with self.namespace_list_lock:
            if nqn is not None and nqn not in self.namespace_list:
                return NamespacesLocalList.EMPTY_NAMESPACE

            if nqn is None:
                nqn_list = self.namespace_list
            else:
                nqn_list = [nqn]

            for one_nqn in nqn_list:
                # if we have nsid, use it as the key
                if nsid:
                    if nsid in self.namespace_list[one_nqn]:
                        return self.namespace_list[one_nqn][nsid]
                elif uuid:
                    for ns in self.namespace_list[one_nqn]:
                        if NamespaceInfo.are_uuids_equal(uuid,
                                                         self.namespace_list[one_nqn][ns].uuid):
                            return self.namespace_list[one_nqn][ns]
                elif bdev:
                    for ns in self.namespace_list[one_nqn]:
                        if bdev == self.namespace_list[one_nqn][ns].bdev:
                            return self.namespace_list[one_nqn][ns]

        return NamespacesLocalList.EMPTY_NAMESPACE

    def get_namespace_count(self, nqn, auto_visible=None, min_hosts=0) -> int:
        with self.namespace_list_lock:
            if nqn and nqn not in self.namespace_list:
                return 0

            if nqn:
                subsystems = [nqn]
            else:
                subsystems = self.namespace_list.keys()

            ns_count = 0
            for one_subsys in subsystems:
                for nsid in self.namespace_list[one_subsys]:
                    ns = self.namespace_list[one_subsys][nsid]
                    if ns.empty():
                        continue
                    if auto_visible is not None:
                        if ns.auto_visible == auto_visible and ns.host_count() >= min_hosts:
                            ns_count += 1
                    else:
                        if ns.host_count() >= min_hosts:
                            ns_count += 1

        return ns_count

    def get_namespace_infos_for_anagrpid(self, nqn: str, anagrpid: int) -> Iterator[NamespaceInfo]:
        """Yield NamespaceInfo instances for a given nqn and anagrpid."""

        with self.namespace_list_lock:
            if nqn in self.namespace_list:
                for ns_info in self.namespace_list[nqn].values():
                    if ns_info.anagrpid == anagrpid:
                        yield ns_info

    def get_all_namespaces_by_ana_group_id(self, anagrpid):
        ns_list = []
        # Loop through all nqn values in the namespace list
        with self.namespace_list_lock:
            for nqn in self.namespace_list:
                for nsid in self.namespace_list[nqn]:
                    ns = self.namespace_list[nqn][nsid]
                    if ns.empty():
                        continue
                    if ns.anagrpid == anagrpid:
                        ns_list.append((nsid, nqn))           # list of tupples
        return ns_list

    def get_ana_group_id_by_nsid_subsys(self, nqn, nsid):
        with self.namespace_list_lock:
            if nqn not in self.namespace_list:
                return 0
            if nsid not in self.namespace_list[nqn]:
                return 0
            ns = self.namespace_list[nqn][nsid]
            if ns.empty():
                return 0
            return ns.anagrpid

    def get_subsys_namespaces_by_ana_group_id(self, nqn, anagrpid):
        ns_list = []
        with self.namespace_list_lock:
            if nqn not in self.namespace_list:
                return ns_list

            for nsid in self.namespace_list[nqn]:
                ns = self.namespace_list[nqn][nsid]
                if ns.empty():
                    continue
                if ns.anagrpid == anagrpid:
                    ns_list.append(ns)

        return ns_list

    def get_all_namespaces_with_location(self, location: str, nqn=None) -> list:
        with self.namespace_list_lock:
            if nqn and nqn not in self.namespace_list:
                return []

            if nqn:
                subsystems = [nqn]
            else:
                subsystems = self.namespace_list.keys()

            ns_list = []
            for nqn in subsystems:
                for nsid in self.namespace_list[nqn]:
                    ns = self.namespace_list[nqn][nsid]
                    if ns.empty():
                        continue
                    if not location and not ns.location:
                        ns_list.append((nsid, nqn))
                    elif ns.location == location:
                        ns_list.append((nsid, nqn))
        return ns_list

    def get_all_namespaces_with_host(self, host: str, nqn=None) -> list:
        ns_list = []
        if not host:
            return []
        with self.namespace_list_lock:
            if nqn and nqn not in self.namespace_list:
                return []

            if nqn:
                subsystems = [nqn]
            else:
                subsystems = self.namespace_list.keys()

            for nqn in subsystems:
                for nsid in self.namespace_list[nqn]:
                    ns = self.namespace_list[nqn][nsid]
                    if ns.empty():
                        continue
                    if ns.is_host_in_namespace(host):
                        ns_list.append((nsid, nqn))
        return ns_list


class ImageIdentification:
    FIELD_DELIMITER = GatewayState.OMAP_KEY_DELIMITER
    ID_DELIMITER = f"{GatewayState.OMAP_KEY_DELIMITER}" \
                   f"{GatewayState.OMAP_KEY_DELIMITER}" \
                   f"{GatewayState.OMAP_KEY_DELIMITER}"

    def __init__(self, group_name, subsys, uuid, fsid, rbd_id=None):
        self.group_name = group_name if not group_name else \
            group_name.replace(ImageIdentification.FIELD_DELIMITER, "-")
        assert not self.group_name or ImageIdentification.ID_DELIMITER not in self.group_name
        self.subsys = subsys if not subsys else \
            subsys.replace(ImageIdentification.FIELD_DELIMITER, "-")
        assert not self.subsys or ImageIdentification.ID_DELIMITER not in self.subsys
        self.uuid = uuid if not uuid else \
            uuid.replace(ImageIdentification.FIELD_DELIMITER, "-")
        assert not self.uuid or ImageIdentification.ID_DELIMITER not in self.uuid
        self.fsid = fsid if not fsid else \
            fsid.replace(ImageIdentification.FIELD_DELIMITER, "-")
        assert not self.fsid or ImageIdentification.ID_DELIMITER not in self.fsid
        self.rbd_id = rbd_id if not rbd_id else \
            rbd_id.replace(ImageIdentification.FIELD_DELIMITER, "-")
        assert not self.rbd_id or ImageIdentification.ID_DELIMITER not in self.rbd_id

    def __str__(self):
        return f"{self.group_name}{ImageIdentification.FIELD_DELIMITER}{self.subsys}" \
               f"{ImageIdentification.FIELD_DELIMITER}{self.uuid}" \
               f"{ImageIdentification.FIELD_DELIMITER}{self.fsid}" \
               f"{ImageIdentification.FIELD_DELIMITER}{self.rbd_id}"

    def empty(self) -> bool:
        if self.group_name:
            return False
        if self.subsys:
            return False
        if self.uuid:
            return False
        if self.fsid:
            return False
        if self.rbd_id:
            return False
        return True

    def does_fsid_match(self, fsid) -> bool:
        if not fsid:
            return not self.fsid
        return self.fsid == fsid.replace(ImageIdentification.FIELD_DELIMITER, "-")

    def does_rbd_id_match(self, rbd_id) -> bool:
        if not rbd_id:
            return not self.rbd_id
        return self.rbd_id == rbd_id.replace(ImageIdentification.FIELD_DELIMITER, "-")

    def is_same_group(self, group_name: str) -> bool:
        if not group_name:
            return not self.group_name
        return self.group_name == group_name.replace(ImageIdentification.FIELD_DELIMITER, "-")

    def is_same_uuid(self, uuid: str) -> bool:
        if not uuid:
            return not self.uuid
        return self.uuid == uuid.replace(ImageIdentification.FIELD_DELIMITER, "-")

    def is_same_image_id(self, img_id) -> bool:
        if self.fsid and img_id.fsid and not self.does_fsid_match(img_id.fsid):
            return False
        if self.rbd_id and img_id.rbd_id and not self.does_rbd_id_match(img_id.rbd_id):
            return False
        if not self.is_same_group(img_id.group_name):
            return False
        if self.subsys != img_id.subsys:
            return False
        if not self.is_same_uuid(img_id.uuid):
            return False
        return True

    @classmethod
    def parse(cls, img_ids: str) -> list:
        ids_list = []
        ids = img_ids.split(ImageIdentification.ID_DELIMITER)
        for one_id in ids:
            parts = one_id.split(ImageIdentification.FIELD_DELIMITER)
            if len(parts) < 4:
                parts.append(None)
            if len(parts) < 5:
                parts.append(None)
            assert len(parts) == 5, f"Invalid image id {one_id}"
            group_name, subsys, uuid, fsid, rbd_id = parts
            ids_list.append(ImageIdentification(group_name, subsys, uuid, fsid, rbd_id))

        return ids_list

    @classmethod
    def list2string(cls, img_ids: list) -> str:
        if not img_ids:
            return ""

        result = ""
        for one_img in img_ids:
            result += f"{one_img}, "

        result = "[" + result.removesuffix(", ") + "]"
        return result


class KMIPServerEndpoint:
    def __init__(self, address, port):
        self.address = address
        self.port = port


class KMIPServerEndpointList():
    KMIP_DEFAULT_PORT = 5696

    def __init__(self):
        self.kmip_server_endpoints_lock = threading.Lock()
        # the server endpoint list consists of elements of a server name
        # and a set of endpoints address and port
        self.kmip_server_endpoints = {}

    def add_server_endpoint(self, nqn: str, name: str, endpoint: KMIPServerEndpoint) -> None:
        with self.kmip_server_endpoints_lock:
            if nqn not in self.kmip_server_endpoints:
                self.kmip_server_endpoints[nqn] = {"name": name, "endpoints": set()}
            self.kmip_server_endpoints[nqn]["endpoints"].add((endpoint.address, endpoint.port))

    def remove_server_endpoint(self, nqn: str, name: str, endpoint: KMIPServerEndpoint) -> None:
        with self.kmip_server_endpoints_lock:
            entry = self.kmip_server_endpoints.get(nqn)
            if entry is None or name != entry["name"]:
                return
            entry["endpoints"].discard((endpoint.address, endpoint.port))
            if len(entry["endpoints"]) == 0:
                del self.kmip_server_endpoints[nqn]

    def get_subsystem_server_name(self, nqn: str) -> Optional[str]:
        if not nqn:
            return None
        with self.kmip_server_endpoints_lock:
            entry = self.kmip_server_endpoints.get(nqn)
            if entry is None:
                return None
            return entry["name"]

    def subsystem_has_server_endpoints(self, nqn: str) -> bool:
        if not nqn:
            return False
        with self.kmip_server_endpoints_lock:
            entry = self.kmip_server_endpoints.get(nqn)
            return entry is not None and len(entry["endpoints"]) > 0

    def remove_subsystem_server_endpoints(self, nqn: str) -> None:
        if not nqn:
            return
        with self.kmip_server_endpoints_lock:
            self.kmip_server_endpoints.pop(nqn, None)

    def get_server_endpoint_list(self, nqn=None, name=None) -> list:
        endpoint_list = []
        with self.kmip_server_endpoints_lock:
            if nqn is not None:
                nqn_list = [nqn]
            else:
                nqn_list = list(self.kmip_server_endpoints.keys())

            for subsys in nqn_list:
                entry = self.kmip_server_endpoints.get(subsys)
                if entry is None:
                    continue
                if name is not None and name != entry["name"]:
                    continue
                for (addr, port) in entry["endpoints"]:
                    endpoint_list.append((subsys, entry["name"],
                                          KMIPServerEndpoint(addr, port)))
        return endpoint_list

    def does_kmip_server_endpoint_exist(self, nqn: str, name: str,
                                        endpoint: KMIPServerEndpoint) -> bool:
        if not nqn or not name:
            return False
        with self.kmip_server_endpoints_lock:
            entry = self.kmip_server_endpoints.get(nqn)
            if entry is None or name != entry["name"]:
                return False
            return (endpoint.address, endpoint.port) in entry["endpoints"]


class KMIPClientList():
    def __init__(self, config, cert, key, ca):
        self.kmip_clients_lock = threading.Lock()
        self.config = config
        self.cert = cert
        self.key = key
        self.ca = ca
        # for each subsystem we keep a KMIP client object which will handle the keys needed
        # by encrypted namespaces in that subsystem
        self.kmip_clients = dict()

    def add_client(self, cert_dir, nqn) -> Optional[NVMeoFKMIPClient]:
        if not cert_dir or not self.cert or not self.key or not self.ca:
            return None
        with self.kmip_clients_lock:
            client = self.kmip_clients.get(nqn)
            if client is not None:
                return client
            client = NVMeoFKMIPClient(logger_config=self.config,
                                      cert_path=os.path.join(cert_dir, self.cert),
                                      key_path=os.path.join(cert_dir, self.key),
                                      ca_path=os.path.join(cert_dir, self.ca))
            self.kmip_clients[nqn] = client
        return client

    def remove_client(self, nqn):
        with self.kmip_clients_lock:
            client = self.kmip_clients.get(nqn)
            if client is None:
                return
            client.disconnect_all()
            self.kmip_clients.pop(nqn, None)


class GatewayService(pb2_grpc.GatewayServicer):
    """Implements gateway service interface.

    Handles configuration of the SPDK NVMEoF target according to client requests.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_name: Gateway identifier
        gateway_state: Methods for target state persistence
        spdk_rpc_client: Client of SPDK RPC server
        spdk_rpc_subsystems_client: Client of SPDK RPC server for get_subsystems
        spdk_rpc_subsystems_lock: Mutex to hold while using get subsystems SPDK client
        shared_state_lock: guard mutex for bdev_cluster and cluster_nonce
        subsystem_nsid_bdev_and_uuid: map of nsid to bdev
        cluster_nonce: cluster context nonce map
    """

    PSK_PREFIX = "psk"
    DHCHAP_PREFIX = "dhchap"
    DHCHAP_CONTROLLER_PREFIX = "dhchap_ctrlr"
    KEYS_DIR = "/var/tmp"
    MAX_SUBSYSTEMS_DEFAULT = 128
    MAX_NAMESPACES_DEFAULT = 4096
    MAX_NAMESPACES_PER_SUBSYSTEM_DEFAULT = 512
    LISTENER_PORT_DEFAULT = 4420
    SECURE_LISTENER_PORT_DEFAULT = 4421
    # The actual highest value seems to be 3647, so pick a lower value
    MAX_VALUE_FOR_MAX_NAMESPACES_PER_SUBSYSTEM = 2048
    MAX_HOSTS_PER_SUBSYS_DEFAULT = 128
    MAX_HOSTS_DEFAULT = 2048
    # notification name should be the same as in spdk/lib/nvmf/ctrlr.c
    SPDK_HOST_KEEPALIVE_TIMEOUT_NOTIFICATION = "host_keepalive_timeout"
    # notification name should be the same as in spdk/lib/bdev/bdev.c
    SPDK_RBD_IMAGE_SHRINK_NOTIFICATION = "bdev_shrink"

    def __init__(self, config: GatewayConfig, gateway_state: GatewayStateHandler,
                 rpc_lock, omap_lock: OmapLock, group_id: int, spdk_rpc_client,
                 spdk_rpc_subsystems_client, ceph_utils: CephUtils) -> None:
        """Constructor"""
        self.gw_logger_object = GatewayLogger(config)
        self.logger = self.gw_logger_object.logger
        # notice that this was already called from main, the extra call is for the
        # tests environment where we skip main
        config.display_environment_info(self.logger)
        self.ceph_utils = ceph_utils
        self.ceph_utils.fetch_and_display_ceph_version()
        self.config = config
        config.dump_config_file(self.logger)
        self.rpc_lock = rpc_lock
        self.gateway_state = gateway_state
        self.omap_lock = omap_lock
        self.group_id = group_id
        self.spdk_rpc_client = spdk_rpc_client
        self.spdk_rpc_subsystems_client = spdk_rpc_subsystems_client
        self.spdk_rpc_subsystems_lock = threading.Lock()
        self.shared_state_lock = threading.Lock()
        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()
        override_hostname = self.config.get_with_default("gateway", "override_hostname", "")
        if override_hostname:
            self.host_name = override_hostname
            self.logger.info(f"Gateway's host name was overridden to {override_hostname}")
        else:
            self.host_name = socket.gethostname()
        if not GatewayUtils.is_valid_host_name(self.host_name):
            self.logger.warning(f"Gateway's host name {self.host_name} is invalid")
        self.verify_nqns = self.config.getboolean_with_default("gateway", "verify_nqns", True)
        self.verify_keys = self.config.getboolean_with_default("gateway", "verify_keys", True)
        self.verify_listener_ip = self.config.getboolean_with_default("gateway",
                                                                      "verify_listener_ip",
                                                                      True)
        self.gateway_group = self.config.get_with_default("gateway", "group", "")
        self.max_hosts_per_namespace = self.config.getint_with_default(
            "gateway",
            "max_hosts_per_namespace",
            16)
        self.max_namespaces_with_netmask = self.config.getint_with_default(
            "gateway",
            "max_namespaces_with_netmask",
            1000)
        self.max_subsystems = self.config.getint_with_default(
            "gateway",
            "max_subsystems",
            GatewayService.MAX_SUBSYSTEMS_DEFAULT)
        self.max_namespaces = self.config.getint_with_default(
            "gateway",
            "max_namespaces",
            GatewayService.MAX_NAMESPACES_DEFAULT)
        self.max_namespaces_per_subsystem = self.config.getint_with_default(
            "gateway",
            "max_namespaces_per_subsystem",
            GatewayService.MAX_NAMESPACES_PER_SUBSYSTEM_DEFAULT)
        biggest_max_ns_per_subsys = GatewayService.MAX_VALUE_FOR_MAX_NAMESPACES_PER_SUBSYSTEM
        if self.max_namespaces_per_subsystem > biggest_max_ns_per_subsys:
            self.logger.error(f"Max namespaces per subsystem can't be greater than "
                              f"{biggest_max_ns_per_subsys}, will use "
                              f"this value instead")
            self.max_namespaces_per_subsystem = biggest_max_ns_per_subsys
        self.max_hosts_per_subsystem = self.config.getint_with_default(
            "gateway",
            "max_hosts_per_subsystem",
            GatewayService.MAX_HOSTS_PER_SUBSYS_DEFAULT)
        self.max_hosts = self.config.getint_with_default(
            "gateway",
            "max_hosts",
            GatewayService.MAX_HOSTS_DEFAULT)
        self.gateway_pool = self.config.get_with_default("ceph", "pool", "")
        self.enable_key_encryption = self.config.getboolean_with_default(
            "gateway",
            "enable_key_encryption",
            True)
        self.ana_map = defaultdict(dict)
        self.ana_grp_state = {}
        self.ana_grp_ns_load = {}
        self.ana_grp_subs_load = defaultdict(dict)
        self.max_ana_grps = self.config.getint_with_default("gateway", "max_gws_in_grp", 16)
        if self.max_ana_grps > self.max_namespaces:
            self.logger.warning(f"Maximal number of load balancing groups can't be greather "
                                f"than the maximal number of namespaces, will truncate "
                                f"to {self.max_namespaces}")
            self.max_ana_grps = self.max_namespaces

        if self.max_namespaces_per_subsystem > self.max_namespaces:
            self.logger.warning(f"Maximal number of namespace per subsystem can't be greater "
                                f"than the global maximal number of namespaces, will truncate "
                                f"to {self.max_namespaces}")
            self.max_namespaces_per_subsystem = self.max_namespaces

        self.kmip_cert_dir = self.config.get_with_default("kmip",
                                                          "cert_dir",
                                                          "./certs/kmip/{server_name}")
        if not self.kmip_cert_dir:
            self.kmip_cert_dir = "."
        self.kmip_client_cert = self.config.get_with_default("kmip", "client_cert", None)
        self.kmip_client_key = self.config.get_with_default("kmip", "client_key", None)
        self.kmip_ca_cert = self.config.get_with_default("kmip", "ca_cert", None)
        self.kmip_server_endpoints = KMIPServerEndpointList()
        self.kmip_clients = KMIPClientList(self.config, self.kmip_client_cert,
                                           self.kmip_client_key, self.kmip_ca_cert)

        for i in range(self.max_ana_grps + 1):
            self.ana_grp_ns_load[i] = 0
            self.ana_grp_state[i] = pb2.ana_state.INACCESSIBLE
        self.cluster_nonce = {}
        self.bdev_cluster = {}
        self.bdev_params = {}
        self.subsystem_nsid_bdev_and_uuid = NamespacesLocalList()
        self.subsystem_listeners = defaultdict(set)
        self.cluster_allocator = get_cluster_allocator(config, self)
        self.subsys_max_ns = {}
        self.subsys_serial = {}
        self.subsys_network = {}
        self.subsystems_cache = SubsystemsCache()
        self.host_info = SubsystemHostAuth()
        self.up_and_running = True
        self.rebalance = Rebalance(self)
        self.spdk_version = None
        self.spdk_qos_timeslice = self.config.getint_with_default("spdk",
                                                                  "qos_timeslice_in_usecs", None)
        self.force_tls = self.config.getboolean_with_default("gateway", "force_tls", False)
        self.io_stats_enabled = self.config.getboolean_with_default("gateway",
                                                                    "io_stats_enabled", True)
        if self.io_stats_enabled:
            io_stats_req = pb2.set_gateway_io_stats_mode_req(enabled=True)
            self.logger.info("Will enable gateway's IO statistics")
            rc = self.set_gateway_io_stats_mode(io_stats_req, "context")
            self.logger.debug(f"set_gateway_io_stats_mode: {rc.error_message}")
            if rc.status != 0:
                self.logger.error(f"Failure enabling gateway's IO statistics: {rc.error_message}")
                self.io_stats_enabled = False
        else:
            self.logger.info("Gateway's IO statistics is disabled")

        self.fsid = None
        spdk_notifications_interval = self.config.getint_with_default("spdk",
                                                                      "notifications_interval",
                                                                      60)
        self.spdk_notifications_thread = None
        if spdk_notifications_interval > 0:
            self.spdk_notifications_thread = threading.Thread(target=self.read_spdk_notifications,
                                                              name="SPDK Notifications",
                                                              daemon=True,
                                                              args=(spdk_notifications_interval,))
            self.spdk_notifications_thread.start()

    def read_spdk_notifications(self, read_interval):
        if read_interval <= 0:
            return

        spdk_notification_last_id_read = -1

        while self.up_and_running:
            with self.rpc_lock:
                notifications = self.spdk_rpc_client.notify_get_notifications(
                    id=spdk_notification_last_id_read + 1)
            if notifications:
                self.logger.debug(f"spdk_notifications: {notifications}")
                for n in notifications:
                    try:
                        spdk_notification_last_id_read = n["id"]
                        if n["type"] == GatewayService.SPDK_HOST_KEEPALIVE_TIMEOUT_NOTIFICATION:
                            n_ctx = n["ctx"]
                            (hostnqn,
                             subsysnqn,
                             timeout) = n_ctx.split(GatewayState.OMAP_KEY_DELIMITER)
                            self.logger.warning(f"Host {hostnqn} was disconnected from "
                                                f"subsystem {subsysnqn} due to keep alive "
                                                f"timeout after {timeout} milliseconds")
                            self.host_info.set_host_keepalive_timeout_disconnection(subsysnqn,
                                                                                    hostnqn)
                        elif n["type"] == GatewayService.SPDK_RBD_IMAGE_SHRINK_NOTIFICATION:
                            n_ctx = n["ctx"]
                            (bdev, size, new_size) = n_ctx.split(",")
                            self.logger.debug(f"Bdev {bdev} size was shrunk from "
                                              f"{size} to {new_size} bytes")
                            if bdev:
                                with self.rpc_lock:
                                    ns = self.subsystem_nsid_bdev_and_uuid.find_namespace(None,
                                                                                          None,
                                                                                          None,
                                                                                          bdev)
                                    if not ns.empty():
                                        ns.set_image_was_shrunk(True)
                                        self.logger.warning(f"Namespace {ns.nsid} on {ns.subsys} "
                                                            f"size was shrunk from "
                                                            f"{size} to {new_size} bytes")
                    except Exception:
                        self.logger.exception(f"Invalid notification: {n}")
            time.sleep(read_interval)

    def get_directories_for_key_file(self, key_type: str,
                                     subsysnqn: str, create_dir: bool = False) -> []:
        tmp_dirs = []
        dir_prefix = f"{key_type}_{subsysnqn}_"

        try:
            for f in Path(self.KEYS_DIR).iterdir():
                if f.is_dir() and f.match(dir_prefix + "*"):
                    tmp_dirs.insert(0, str(f))
        except Exception:
            self.logger.exception(f"Error listing files in {self.KEYS_DIR}")
            return None

        if tmp_dirs:
            return tmp_dirs

        if not create_dir:
            return None

        tmp_dir_name = None
        try:
            tmp_dir_name = tempfile.mkdtemp(prefix=dir_prefix, dir=self.KEYS_DIR)
        except Exception:
            self.logger.exception("Error creating directory for key file")
            return None
        return [tmp_dir_name]

    def create_host_key_file(self, key_type: str,
                             subsysnqn: str, hostnqn: str, key_value: str) -> str:
        assert subsysnqn, "Subsystem NQN can't be empty"
        assert hostnqn, "Host NQN can't be empty"
        assert key_type, "Key type can't be empty"
        assert key_value, "Key value can't be empty"

        tmp_dir_names = self.get_directories_for_key_file(key_type, subsysnqn, create_dir=True)
        if not tmp_dir_names:
            return None

        filepath = None
        keyfile_prefix = f"{hostnqn}_"
        try:
            (file_fd, filepath) = tempfile.mkstemp(prefix=keyfile_prefix,
                                                   dir=tmp_dir_names[0], text=True)
        except Exception:
            self.logger.exception("Error creating key file")
            return None
        if not filepath:
            self.loger.error("Error creating key file")
            return None
        try:
            with open(file_fd, "wt") as f:
                f.write(key_value)
        except Exception:
            self.logger.exception("Error creating file")
            try:
                os.remove(filepath)
            except Exception:
                pass
            return None
        return filepath

    def create_host_psk_file(self, subsysnqn: str, hostnqn: str, key_value: str) -> str:
        return self.create_host_key_file(self.PSK_PREFIX, subsysnqn, hostnqn, key_value)

    def create_host_dhchap_file(self, subsysnqn: str, hostnqn: str, key_value: str) -> str:
        return self.create_host_key_file(self.DHCHAP_PREFIX, subsysnqn, hostnqn, key_value)

    def remove_host_key_file(self, key_type: str, subsysnqn: str, hostnqn: str) -> None:
        assert key_type, "Key type can't be empty"
        assert subsysnqn, "Subsystem NQN can't be empty"

        tmp_dir_names = self.get_directories_for_key_file(key_type, subsysnqn, create_dir=False)
        if not tmp_dir_names:
            return

        # If there is no host NQN remove all hosts in this subsystem
        if not hostnqn:
            for one_tmp_dir in tmp_dir_names:
                try:
                    shutil.rmtree(one_tmp_dir, ignore_errors=True)
                except Exception:
                    pass
            return

        # We have a host NQN so only remove its files
        for one_tmp_dir in tmp_dir_names:
            for f in Path(one_tmp_dir).iterdir():
                if f.is_file() and f.match(f"{hostnqn}_*"):
                    try:
                        f.unlink()
                    except Exception:
                        self.logger.exception(f"Error deleting file {f.name}")
                        pass

    def remove_host_psk_file(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_host_key_file(self.PSK_PREFIX, subsysnqn, hostnqn)

    def remove_host_dhchap_file(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_host_key_file(self.DHCHAP_PREFIX, subsysnqn, hostnqn)

    def remove_all_host_key_files(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_host_psk_file(subsysnqn, hostnqn)
        self.remove_host_dhchap_file(subsysnqn, hostnqn)

    def remove_all_subsystem_key_files(self, subsysnqn: str) -> None:
        self.remove_all_host_key_files(subsysnqn, None)

    @staticmethod
    def construct_key_name_for_keyring(subsysnqn: str, hostnqn: str, prefix: str = None) -> str:
        key_name = hashlib.sha256(subsysnqn.encode()).hexdigest() + "_"
        key_name += hashlib.sha256(hostnqn.encode()).hexdigest()
        if prefix:
            key_name = prefix + "_" + key_name
        return key_name

    def remove_key_from_keyring(self, key_type: str, subsysnqn: str, hostnqn: str) -> None:
        key_name = GatewayService.construct_key_name_for_keyring(subsysnqn, hostnqn, key_type)
        assert self.rpc_lock.locked(), "RPC is unlocked when calling keyring_file_remove_key()"
        key_list = []
        try:
            key_list = self.spdk_rpc_client.keyring_get_keys()
        except Exception:
            self.logger.exception("Can't list keyring keys")
            key_list = []

        key_exists = False
        for one_key in key_list:
            try:
                if one_key["name"] == key_name:
                    key_exists = True
                    break
            except Exception:
                pass

        if key_exists:
            try:
                self.spdk_rpc_client.keyring_file_remove_key(name=key_name)
            except Exception:
                self.logger.exception(f"Can't remove key {key_name}")
                pass

    def remove_psk_key_from_keyring(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_key_from_keyring(self.PSK_PREFIX, subsysnqn, hostnqn)

    def remove_dhchap_key_from_keyring(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_key_from_keyring(self.DHCHAP_PREFIX, subsysnqn, hostnqn)

    def remove_dhchap_controller_key_from_keyring(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_key_from_keyring(self.DHCHAP_CONTROLLER_PREFIX, subsysnqn, hostnqn)

    def remove_all_host_keys_from_keyring(self, subsysnqn: str, hostnqn: str) -> None:
        self.remove_psk_key_from_keyring(subsysnqn, hostnqn)
        self.remove_dhchap_key_from_keyring(subsysnqn, hostnqn)
        self.remove_dhchap_controller_key_from_keyring(subsysnqn, hostnqn)

    def remove_all_subsystem_keys_from_keyring(self, subsysnqn: str) -> None:
        assert self.rpc_lock.locked(), "RPC is unlocked when calling " \
                                       "remove_all_subsystem_keys_from_keyring()"
        try:
            key_list = self.spdk_rpc_client.keyring_get_keys()
        except Exception:
            self.logger.exception("Can't list keyring keys")
            return
        for one_key in key_list:
            key_path = None
            key_name = None
            try:
                key_path = one_key["path"]
                key_name = one_key["name"]
            except Exception:
                self.logger.exception(f"Can't get details for key {one_key}")
                continue
            if not key_name or not key_path:
                continue

            should_remove = False
            if key_path.startswith(f"{self.KEYS_DIR}/{self.PSK_PREFIX}_{subsysnqn}_"):
                should_remove = True
            elif key_path.startswith(f"{self.KEYS_DIR}/{self.DHCHAP_PREFIX}_{subsysnqn}_"):
                should_remove = True

            if should_remove:
                try:
                    self.spdk_rpc_client.keyring_file_remove_key(name=key_name)
                except Exception:
                    self.logger.exception(f"Can't remove key {key_name}")
                    pass

    @staticmethod
    def is_valid_host_nqn(nqn):
        if nqn == "*":
            return pb2.req_status(status=0, error_message=os.strerror(0))
        rc = GatewayUtils.is_valid_nqn(nqn)
        return pb2.req_status(status=rc[0], error_message=rc[1])

    def parse_json_exeption(self, ex):
        if not isinstance(ex, JSONRPCException):
            return None

        json_error_text = "Got JSON-RPC error response"
        resp = None
        try:
            resp_index = ex.message.find(json_error_text)
            if resp_index >= 0:
                resp_str = ex.message[resp_index + len(json_error_text):]
                resp_index = resp_str.find("response:")
                if resp_index >= 0:
                    resp_str = resp_str[resp_index + len("response:"):]
                    resp = json.loads(resp_str)
        except Exception:
            self.logger.exception("Got exception parsing JSON exception")
            pass
        if resp:
            if resp["code"] < 0:
                resp["code"] = -resp["code"]
        else:
            resp = {}
            if "timeout" in ex.message.lower():
                resp["code"] = errno.ETIMEDOUT
            else:
                resp["code"] = errno.EINVAL
            resp["message"] = ex.message

        return resp

    def set_cluster_nonce(self, name: str, nonce: str) -> None:
        with self.shared_state_lock:
            self.logger.info(f"Allocated cluster {name=} {nonce=}")
            self.cluster_nonce[name] = nonce

    def _grpc_function_with_lock(self, func, request, context):
        with self.rpc_lock:
            rc = func(request, context)
            if not self.omap_lock.omap_file_disable_unlock:
                assert not self.omap_lock.write_locked_by_me(), \
                    f"OMAP is still locked when exiting function {func.__name__}()\n" \
                    f"locked by: {self.omap_lock.locked_by}, " \
                    f"with cookie: {self.omap_lock.lock_cookie}" \
                    f"current thread id: {threading.get_native_id()} " \
                    f"locked: {self.omap_lock.is_exclusively_locked}"
            return rc

    def execute_grpc_function(self, func, request, context, err_prefix=""):
        """This functions handles RPC lock by wrapping 'func' with
           self._grpc_function_with_lock, and assumes (?!) the function 'func'
           called might take OMAP lock internally, however does NOT ensure
           taking OMAP lock in any way.
        """

        if not self.up_and_running:
            self.logger.debug(f"Gateway {self.gateway_name} is going down "
                              f"while executing {func.__name__}()")
            errmsg = f"Gateway {self.gateway_name} is going down"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ESHUTDOWN, error_message=errmsg)

        try:
            rc = self.omap_lock.execute_omap_locking_function(
                self._grpc_function_with_lock, func, request, context)
        except Exception:
            self.logger.exception(f"Failure while executing {func.__name__}()")
            return pb2.req_status(status=errno.EBUSY,
                                  error_message=f"{err_prefix}Couldn't lock the OMAP file")

        return rc

    def get_image_identification(self, rbd_pool: str, rbd_image: str,
                                 rados_namespace_name: str) -> list[ImageIdentification]:
        image_id_metadata = None
        image_path = f"{rbd_pool}/{rbd_image}" if not rados_namespace_name \
            else f"{rbd_pool}/{rados_namespace_name}/{rbd_image}"
        if not self.ceph_utils.does_image_exist(rbd_pool, rbd_image, rados_namespace_name):
            self.logger.debug(f"Image {image_path} not found")
            return []
        try:
            image_id_metadata = self.ceph_utils.get_image_metadata(
                rbd_pool, rbd_image, rados_namespace_name, CephUtils.METADATA_KEY_IMAGE_ID)
        except KeyError:
            pass
        except Exception:
            self.logger.exception(f"Error getting image identification for image "
                                  f"{image_path}")
        if not image_id_metadata:
            return []

        try:
            img_ids_list = ImageIdentification.parse(image_id_metadata)
        except Exception:
            self.logger.exception(f"Error parsing {image_id_metadata}")
            return []

        self.logger.debug(f"Found image ids {ImageIdentification.list2string(img_ids_list)} "
                          f"for {image_path}")

        return img_ids_list

    def set_image_identification(self, rbd_pool: str, rbd_image: str,
                                 rados_namespace_name: str, img_id: ImageIdentification):
        assert img_id, "Can't set an empty image id"
        image_path = f"{rbd_pool}/{rbd_image}" if not rados_namespace_name \
            else f"{rbd_pool}/{rados_namespace_name}/{rbd_image}"
        if self.fsid is None:
            self.fsid = self.ceph_utils.fetch_ceph_fsid()
            self.logger.debug(f"Cluster's FSID is {self.fsid}")
        if not self.fsid:
            self.logger.error("Can't read cluster's FSID, this might affect the prevention "
                              "of RBD image re-use in namespaces")
        if img_id.fsid is None:
            self.logger.debug(f"No FSID set for image id {img_id}, "
                              f"will use cluster's FSID {self.fsid}")
            img_id.fsid = self.fsid
        if img_id.rbd_id is None:
            img_id.rbd_id = self.ceph_utils.get_image_id(rbd_pool, rbd_image, rados_namespace_name)
        if not img_id.rbd_id:
            img_path = f"{rbd_pool}/{rados_namespace_name}/{rbd_image}" if rados_namespace_name \
                else f"{rbd_pool}/{rbd_image}"
            self.logger.error(f"Can't read the ID of image {img_path}, this might affect "
                              f"the prevention of RBD image re-use in namespaces")
        img_id_value = ""
        img_ids_list = self.get_image_identification(rbd_pool, rbd_image, rados_namespace_name)
        for one_id in img_ids_list:
            if one_id.is_same_image_id(img_id):
                self.logger.debug(f"Image id {img_id} already included in "
                                  f"{ImageIdentification.list2string(img_ids_list)}")
                return
            img_id_value += f"{one_id}{ImageIdentification.ID_DELIMITER}"

        img_id_value += f"{img_id}"

        try:
            self.ceph_utils.set_image_metadata(rbd_pool, rbd_image,
                                               rados_namespace_name,
                                               CephUtils.METADATA_KEY_IMAGE_ID,
                                               img_id_value)
            self.logger.debug(f"set image id {img_id_value} for {image_path}")
        except Exception:
            self.logger.exception(f"Error setting image identification {img_id_value} for "
                                  f"{image_path}")

    def delete_image_identification(self, rbd_pool: str,
                                    rbd_image: str, rados_namespace_name: str,
                                    img_id: ImageIdentification):
        assert img_id, "Can't delete an empty image id"

        image_path = f"{rbd_pool}/{rbd_image}" if not rados_namespace_name \
            else f"{rbd_pool}/{rados_namespace_name}/{rbd_image}"
        if self.fsid is None:
            self.fsid = self.ceph_utils.fetch_ceph_fsid()
            self.logger.debug(f"Cluster FSID is {self.fsid}")
        if img_id.fsid is None:
            self.logger.debug(f"No FSID set for image id {img_id}, "
                              f"will use current FSID {self.fsid}")
            img_id.fsid = self.fsid
        img_id_value = ""
        img_ids_list = self.get_image_identification(rbd_pool, rbd_image, rados_namespace_name)
        for one_id in img_ids_list:
            if one_id.is_same_image_id(img_id):
                self.logger.debug(f"Image id {img_id} was found in "
                                  f"{ImageIdentification.list2string(img_ids_list)}")
                continue
            img_id_value += f"{one_id}{ImageIdentification.ID_DELIMITER}"

        img_id_value = img_id_value.removesuffix(ImageIdentification.ID_DELIMITER)

        if not img_id_value:
            try:
                self.ceph_utils.remove_image_metadata(rbd_pool, rbd_image,
                                                      rados_namespace_name,
                                                      CephUtils.METADATA_KEY_IMAGE_ID)
                self.logger.debug(f"remove all image ids for {image_path}")
            except Exception:
                self.logger.exception(f"Error removing image identifications for "
                                      f"{image_path}")
        else:
            try:
                self.ceph_utils.set_image_metadata(rbd_pool, rbd_image,
                                                   rados_namespace_name,
                                                   CephUtils.METADATA_KEY_IMAGE_ID,
                                                   img_id_value)
                self.logger.debug(f"set image id {img_id_value} for {image_path}")
            except Exception:
                self.logger.exception(f"Error setting image identification {img_id_value} for "
                                      f"{image_path}")

    def create_bdev(self, anagrp: int, name, uuid, rbd_pool_name, rbd_data_pool_name,
                    rbd_image_name,
                    block_size, create_image, trash_image, rbd_image_size, disable_auto_resize,
                    read_only, rados_namespace_name,
                    encryption_entries, encryption_algorithm,
                    context, peer_msg=""):
        """Creates a bdev from an RBD image."""

        if create_image:
            cr_img_msg = "will create image if doesn't exist"
        else:
            cr_img_msg = "will not create image if doesn't exist"

        trsh_msg = ""
        if trash_image:
            trsh_msg = "will trash the image on namespace delete, "

        if read_only:
            ro_msg = "read only"
        else:
            ro_msg = "read write"

        image_path = f"{rbd_pool_name}/{rados_namespace_name}/{rbd_image_name}" \
            if rados_namespace_name else f"{rbd_pool_name}/{rbd_image_name}"

        if rbd_data_pool_name:
            data_pool_msg = f", using data pool {rbd_data_pool_name}, "
        else:
            data_pool_msg = ""

        enc_formats_str = ""
        enc_algo_str = None
        if encryption_entries is None:
            encryption_entries = []
        if encryption_algorithm is None:
            encryption_algorithm = pb2.EncryptionAlgorithm.no_algorithm

        for ent in encryption_entries:
            if ent.format is not None and ent.format != pb2.EncryptionFormat.none:
                format_str = GatewayEnumUtils.get_key_from_value(pb2.EncryptionFormat,
                                                                 ent.format)
                if format_str is None:
                    return BdevStatus(status=errno.EINVAL,
                                      error_message=f"Invalid encryption format {ent.format}")
                enc_formats_str += f"{format_str}, "

        enc_formats_str = enc_formats_str.removesuffix(", ")

        if create_image and len(encryption_entries) > 1:
            return BdevStatus(status=errno.EINVAL,
                              error_message="At most one encryption format can be specified when "
                                            "creating a new image")

        if encryption_algorithm != pb2.EncryptionAlgorithm.no_algorithm:
            if not create_image:
                return BdevStatus(status=errno.EINVAL,
                                  error_message="Encryption algorithm is only allowed when "
                                                "creating a new image")
            enc_algo_str = GatewayEnumUtils.get_key_from_value(pb2.EncryptionAlgorithm,
                                                               encryption_algorithm)
            if enc_algo_str is None:
                return BdevStatus(status=errno.EINVAL,
                                  error_message=f"Invalid encryption algorithm "
                                                f"{encryption_algorithm}")

        enc_format_msg = ""
        if enc_formats_str:
            enc_format_msg = f"using encryption format(s) {enc_formats_str}"
            if enc_algo_str:
                enc_format_msg += f" and encryption algorithm {enc_algo_str}"
            enc_format_msg += ", "

        self.logger.info(f"Received request to create {ro_msg} bdev {name} from"
                         f" {image_path} {data_pool_msg}"
                         f"(size {rbd_image_size} bytes)"
                         f" with block size {block_size}, {cr_img_msg}, {trsh_msg}"
                         f"{enc_format_msg}"
                         f"context={context}{peer_msg}")

        if block_size == 0:
            return BdevStatus(status=errno.EINVAL,
                              error_message="Block size can't be zero")

        created_rbd_pool = None
        created_rbd_image_name = None
        if create_image:
            if not rbd_pool_name:
                return BdevStatus(status=errno.ENODEV,
                                  error_message="Empty RBD pool name")
            if not rbd_image_name:
                return BdevStatus(status=errno.ENODEV,
                                  error_message="Empty RBD image name")
            if rbd_image_size <= 0:
                return BdevStatus(status=errno.EINVAL,
                                  error_message="Image size must be positive")
            if rbd_image_size % (1024 * 1024):
                return BdevStatus(status=errno.EINVAL,
                                  error_message="Image size must be aligned to MiBs")
            rc = self.ceph_utils.pool_exists(rbd_pool_name)
            if not rc:
                return BdevStatus(status=errno.ENODEV,
                                  error_message=f"RBD pool {rbd_pool_name} doesn't exist")

            pool_type = self.ceph_utils.get_pool_type(rbd_pool_name)
            if pool_type != CephUtils.CephPoolType.REPLICATED:
                self.logger.error(f"RBD pool {rbd_pool_name} has type {pool_type.name}")
                return BdevStatus(status=errno.EINVAL,
                                  error_message=f"RBD pool "
                                                f"{rbd_pool_name} is not a replicated pool")

            if rbd_data_pool_name:
                rc = self.ceph_utils.pool_exists(rbd_data_pool_name)
                if not rc:
                    return BdevStatus(status=errno.ENODEV,
                                      error_message=f"RBD data pool "
                                                    f"{rbd_data_pool_name} doesn't exist")
                pool_type = self.ceph_utils.get_pool_type(rbd_data_pool_name)
                if pool_type == CephUtils.CephPoolType.ERASURE:
                    overwrites = self.ceph_utils.allow_ec_overwrites_is_set(rbd_data_pool_name)
                    if not overwrites:
                        self.logger.error(f"RBD data pool {rbd_data_pool_name} doesn't have "
                                          f"\"allow_ec_overwrites\" set")
                        return BdevStatus(status=errno.EINVAL,
                                          error_message=f"RBD data pool {rbd_data_pool_name} "
                                                        f"doesn't have \"allow_ec_overwrites\" "
                                                        f"set\nIn order to set it please run "
                                                        f"'cpeh osd pool set {rbd_data_pool_name}"
                                                        f" ec_pool_overwrites true'")
                elif pool_type != CephUtils.CephPoolType.REPLICATED:
                    self.logger.error(f"RBD data pool {rbd_data_pool_name} has "
                                      f"type {pool_type.name}")
                    return BdevStatus(status=errno.EINVAL,
                                      error_message=f"RBD data pool "
                                                    f"{rbd_data_pool_name} is not a replicated "
                                                    f"or erasure coded pool")

            try:
                enc_format = None
                passphrase = None
                if len(encryption_entries) > 0:
                    enc_format = encryption_entries[0].format
                    passphrase = encryption_entries[0].key_id
                rc = self.ceph_utils.create_image(rbd_pool_name, rbd_data_pool_name,
                                                  rados_namespace_name,
                                                  rbd_image_name, rbd_image_size,
                                                  enc_format, encryption_algorithm,
                                                  passphrase)
                if rc:
                    data_pool_msg = ""
                    if rbd_data_pool_name:
                        data_pool_msg = f", data pool is {rbd_data_pool_name}"
                    self.logger.info(f"Image {image_path} created, size "
                                     f"is {rbd_image_size} bytes{data_pool_msg}")
                    created_rbd_pool = rbd_pool_name
                    created_rbd_image_name = rbd_image_name
                    created_rados_namespace_name = rados_namespace_name
                else:
                    self.logger.info(f"Image {image_path} already exists "
                                     f"with size {rbd_image_size} bytes")
                    if trash_image:
                        self.logger.warning(f"Notice that as image "
                                            f"{image_path} was created "
                                            f"outside the gateway it won't get trashed on "
                                            f"namespace deletion")
                        trash_image = False
            except Exception as ex:
                errcode = 0
                msg = ""
                ex_details = self.ceph_utils.get_rbd_exception_details(ex)
                if ex_details is not None:
                    errcode = ex_details[0]
                    msg = ex_details[1]
                if not errcode:
                    errcode = errno.ENODEV
                if not msg:
                    msg = str(ex)
                errmsg = f"Can't create RBD image {image_path}: {msg}"
                self.logger.exception(errmsg)
                return BdevStatus(status=errcode, error_message=errmsg)
        else:
            if not self.ceph_utils.does_image_exist(rbd_pool_name, rbd_image_name,
                                                    rados_namespace_name):
                self.logger.error(f"RBD image {image_path} "
                                  f"does not exist and '--rbd-create-image' "
                                  f"was not specified")
                return BdevStatus(status=errno.EEXIST,
                                  error_message=f"RBD image {image_path} "
                                                f"does not exist and '--rbd-create-image' "
                                                f"was not specified")

        if disable_auto_resize:
            try:
                self._set_image_auto_resize(rbd_pool_name, rbd_image_name,
                                            rados_namespace_name, False)
            except Exception as ex:
                self.logger.warning(f"Error setting auto resize flag for image "
                                    f"{image_path}, namespace "
                                    f"will get resized in case of an image resize:\n{ex}")

        cluster_name = None
        assert self.rpc_lock.locked(), "RPC is unlocked when calling bdev_rbd_create()"
        enc_format_list = []
        passphrase_list = []
        for ent in encryption_entries:
            rbd_format = CephUtils.gateway_encryption_format_to_rbd(ent.format)
            assert rbd_format is not None and rbd_format >= 0, \
                f"Invalid encryption format {ent.format}"
            enc_format_list.append(rbd_format)
            passphrase_list.append(ent.key_id)
        try:
            cluster_name = self.cluster_allocator.get_cluster(anagrp)
            bdev_name = self.spdk_rpc_client.bdev_rbd_create(
                name=name,
                cluster_name=cluster_name,
                namespace_name=rados_namespace_name,
                pool_name=rbd_pool_name,
                rbd_name=rbd_image_name,
                block_size=block_size,
                uuid=uuid,
                read_only=read_only,
                encryption_format=enc_format_list,
                passphrase=passphrase_list,
            )
            with self.shared_state_lock:
                self.bdev_cluster[name] = cluster_name
            self.bdev_params[name] = {'uuid': uuid, 'pool_name': rbd_pool_name,
                                      'image_name': rbd_image_name,
                                      'image_size': rbd_image_size, 'block_size': block_size}

            self.logger.debug(f"bdev_rbd_create: {bdev_name}, cluster_name {cluster_name}")
        except Exception as ex:
            if cluster_name is not None:
                self.cluster_allocator.put_cluster(cluster_name)
            errmsg = f"bdev_rbd_create {name} failed"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg} with:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.ENODEV
            if resp:
                status = resp["code"]
                errmsg = resp['message']
            if trash_image:
                self.delete_rbd_image(created_rbd_pool, created_rbd_image_name,
                                      created_rados_namespace_name)
            return BdevStatus(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not bdev_name:
            errmsg = f"Can't create bdev {name}"
            self.logger.error(errmsg)
            if trash_image:
                self.delete_rbd_image(created_rbd_pool, created_rbd_image_name,
                                      created_rados_namespace_name)
            return BdevStatus(status=errno.ENODEV, error_message=errmsg)

        assert name == bdev_name, f"Created bdev name {bdev_name} differs " \
                                  f"from requested name {name}"

        return BdevStatus(status=0, error_message=os.strerror(0), bdev_name=name,
                          rbd_pool=rbd_pool_name,
                          rbd_image_name=rbd_image_name,
                          rados_namespace_name=rados_namespace_name, trash_image=trash_image)

    def resize_bdev(self, bdev_name, new_size, peer_msg=""):
        """Resizes a bdev."""

        self.logger.info(f"Received request to resize bdev {bdev_name} to {new_size} MiB{peer_msg}")
        assert self.rpc_lock.locked(), "RPC is unlocked when calling resize_bdev()"
        rbd_pool_name = None
        rbd_image_name = None
        rados_namespace_name = None
        bdev_info = self.get_bdev_info(bdev_name)
        if bdev_info is not None:
            try:
                drv_specific_info = bdev_info["driver_specific"]
                rbd_info = drv_specific_info["rbd"]
                rbd_pool_name = rbd_info["pool_name"]
                rbd_image_name = rbd_info["rbd_name"]
                rados_namespace_name = rbd_info["namespace_name"]
            except KeyError as err:
                self.logger.warning(f"Key {err} is not found, will not check size for shrinkage")
                pass
        else:
            self.logger.warning(f"Can't get information for associated block device "
                                f"{bdev_name}, won't check size for shrinkage")

        if rbd_pool_name and rbd_image_name:
            try:
                current_size = self.ceph_utils.get_image_size(rbd_pool_name,
                                                              rbd_image_name, rados_namespace_name)
                # a new size of 0 is a special case to instruct SPDK to not change the size
                # and only send a notification to update its internal data
                if new_size > 0 and current_size > new_size * 1024 * 1024:
                    return pb2.req_status(status=errno.EINVAL,
                                          error_message=f"new size {new_size * 1024 * 1024} bytes "
                                                        f"is smaller than current size "
                                                        f"{current_size} bytes")
            except Exception as ex:
                image_path = f"{rbd_pool_name}/{rados_namespace_name}/{rbd_image_name}" \
                    if rados_namespace_name else f"{rbd_pool_name}/{rbd_image_name}"
                self.logger.warning(f"Error trying to get the size of image "
                                    f"{image_path}, won't check "
                                    f"size for shrinkage:\n{ex}")
                pass

        try:
            ret = self.spdk_rpc_client.bdev_rbd_resize(
                name=bdev_name,
                new_size=new_size,
            )
            self.logger.debug(f"resize_bdev {bdev_name}: {ret}")
        except Exception as ex:
            errmsg = f"Failure resizing bdev {bdev_name}"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure resizing bdev {bdev_name}: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        if not ret:
            errmsg = f"Failure resizing bdev {bdev_name}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def delete_bdev(self, bdev_name, recycling_mode=False, peer_msg=""):
        """Deletes a bdev."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling delete_bdev()"

        self.logger.info(f"Received request to delete bdev {bdev_name}{peer_msg}")
        try:
            ret = self.spdk_rpc_client.bdev_rbd_delete(name=bdev_name)
            if not recycling_mode:
                del self.bdev_params[bdev_name]
            with self.shared_state_lock:
                cluster = self.bdev_cluster[bdev_name]
            self.logger.debug(f"to delete_bdev {bdev_name} cluster {cluster} ")
            self.cluster_allocator.put_cluster(cluster)
            self.logger.debug(f"delete_bdev {bdev_name}: {ret}")
        except Exception as ex:
            errmsg = f"Failure deleting bdev {bdev_name}"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure deleting bdev {bdev_name}: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not ret:
            errmsg = f"Failure deleting bdev {bdev_name}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def get_peer_message(self, context) -> str:
        if not context:
            return ""

        if not hasattr(context, 'peer'):
            return ""

        try:
            peer = context.peer().split(":", 1)
            addr_fam = peer[0].lower()
            addr = peer[1]
            if addr_fam == "ipv6":
                addr_fam = "IPv6"
                addr = addr.replace("%5B", "[", 1)
                addr = addr.replace("%5D", "]", 1)
            elif addr_fam == "ipv4":
                addr_fam = "IPv4"
            else:
                addr_fam = "<Unknown>"
            return f", client address: {addr_fam} {addr}"
        except Exception:
            self.logger.exception("Got exception trying to get peer's address")

        return ""

    def create_subsystem_safe(self, request, context):
        """Creates a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling create_subsystem_safe()"
        create_subsystem_error_prefix = f"Failure creating subsystem {request.subsystem_nqn}"
        peer_msg = self.get_peer_message(context)

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}, enable_ha: "
            f"{request.enable_ha}, max_namespaces: {request.max_namespaces}, no group "
            f"append: {request.no_group_append}, network mask: {request.network_mask}, "
            f"port: {request.port}, "
            f"secure listeners: {request.secure_listeners}, context: {context}{peer_msg}")

        if not request.enable_ha:
            errmsg = f"{create_subsystem_error_prefix}: HA must be enabled for subsystems"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.EINVAL,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if not request.subsystem_nqn:
            errmsg = "Failure creating subsystem, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.EINVAL,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if request.max_namespaces:
            if request.max_namespaces > GatewayService.MAX_VALUE_FOR_MAX_NAMESPACES_PER_SUBSYSTEM:
                errmsg = f"{create_subsystem_error_prefix}: Max namespaces can't be greater " \
                         f"than {GatewayService.MAX_VALUE_FOR_MAX_NAMESPACES_PER_SUBSYSTEM}"
                self.logger.error(errmsg)
                return pb2.subsys_status(status=errno.EINVAL,
                                         error_message=errmsg,
                                         nqn=request.subsystem_nqn)
            if request.max_namespaces > self.max_namespaces:
                self.logger.warning(f"The requested max number of namespaces for subsystem "
                                    f"{request.subsystem_nqn} ({request.max_namespaces}) is "
                                    f"greater than the global limit on the number of namespaces "
                                    f"({self.max_namespaces}), will continue")
            elif request.max_namespaces > self.max_namespaces_per_subsystem:
                self.logger.warning(f"The requested max number of namespaces for subsystem "
                                    f"{request.subsystem_nqn} ({request.max_namespaces}) is "
                                    f"greater than the limit on the number of namespaces per "
                                    f"subsystem ({self.max_namespaces_per_subsystem}), "
                                    f"will continue")

        if request.port > 0xffff:
            errmsg = f"{create_subsystem_error_prefix}: Port value must be smaller than 65536"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.EINVAL,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if request.network_mask:
            for netmask in list(request.network_mask):
                if not NICS.is_valid_subnet(netmask):
                    errmsg = f"{create_subsystem_error_prefix}: Invalid subnet for " \
                             f"network_mask \"{netmask}\""
                    self.logger.error(errmsg)
                    return pb2.subsys_status(status=errno.EADDRNOTAVAIL, error_message=errmsg,
                                             nqn=request.subsystem_nqn)

        errmsg = ""
        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{create_subsystem_error_prefix}: Invalid NQN " \
                     f"\"{request.subsystem_nqn}\", contains invalid characters"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.EINVAL,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{create_subsystem_error_prefix}: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.subsys_status(status=rc[0],
                                         error_message=errmsg,
                                         nqn=request.subsystem_nqn)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{create_subsystem_error_prefix}: Can't create a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.EINVAL,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if len(self.subsys_max_ns) >= self.max_subsystems:
            errmsg = f"{create_subsystem_error_prefix}: Maximal number of subsystems " \
                     f"({self.max_subsystems}) has already been reached"
            self.logger.error(errmsg)
            return pb2.subsys_status(status=errno.E2BIG,
                                     error_message=errmsg,
                                     nqn=request.subsystem_nqn)

        if context and self.verify_keys:
            if request.dhchap_key:
                rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_key)
                if rc[0] != 0:
                    errmsg = f"{create_subsystem_error_prefix}: {rc[1]}"
                    self.logger.error(errmsg)
                    return pb2.subsys_status(status=rc[0],
                                             error_message=errmsg,
                                             nqn=request.subsystem_nqn)

        # Set client ID range according to group id assigned by the monitor
        offset = self.group_id * CNTLID_RANGE_SIZE
        min_cntlid = offset + 1
        max_cntlid = offset + CNTLID_RANGE_SIZE

        ret = False
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if not request.max_namespaces:
                request.max_namespaces = self.max_namespaces_per_subsystem

            if request.max_namespaces >= Rebalance.INVALID_LOAD_BALANCING_GROUP:
                errmsg = f"{create_subsystem_error_prefix}: Maximal number of namespaces " \
                         f"({request.max_namespaces}) is too big"
                self.logger.error(errmsg)
                return pb2.subsys_status(status=errno.E2BIG,
                                         error_message=errmsg,
                                         nqn=request.subsystem_nqn)

            if not request.serial_number:
                random.seed()
                randser = random.randint(2, 99999999999999)
                request.serial_number = f"Ceph{randser}"
                self.logger.info(f"No serial number specified for {request.subsystem_nqn}, will "
                                 f"use {request.serial_number}")

            if context:

                if request.no_group_append or not self.gateway_group:
                    self.logger.info("Subsystem NQN will not be changed")
                else:
                    group_name_to_use = self.gateway_group.replace(
                        GatewayState.OMAP_KEY_DELIMITER, "-")
                    request.subsystem_nqn += f".{group_name_to_use}"
                    request.no_group_append = True
                    self.logger.info(f"Subsystem NQN was changed to {request.subsystem_nqn}, "
                                     f"adding the group name")
            errmsg = ""
            try:
                subsys_using_serial = None
                if request.subsystem_nqn in self.subsys_serial:
                    subsys_already_exists = True
                else:
                    subsys_already_exists = False
                if subsys_already_exists:
                    errmsg = "Subsystem already exists"
                else:
                    subsys_using_serial = None
                    for subsys, sn in self.subsys_serial.items():
                        if sn == request.serial_number:
                            subsys_using_serial = subsys
                            errmsg = f"Serial number {request.serial_number} is already used " \
                                     f"by subsystem {subsys}"
                            break
                if subsys_already_exists or subsys_using_serial:
                    errmsg = f"{create_subsystem_error_prefix}: {errmsg}"
                    self.logger.error(errmsg)
                    return pb2.subsys_status(status=errno.EEXIST,
                                             error_message=errmsg,
                                             nqn=request.subsystem_nqn)
                ret = self.spdk_rpc_client.nvmf_create_subsystem(
                    nqn=request.subsystem_nqn,
                    serial_number=request.serial_number,
                    model_number=DEFAULT_MODEL_NUMBER,
                    max_namespaces=request.max_namespaces,
                    min_cntlid=min_cntlid,
                    max_cntlid=max_cntlid,
                    ana_reporting=True,
                )
                self.logger.debug(f"create_subsystem {request.subsystem_nqn}: {ret}")
                self.subsys_max_ns[request.subsystem_nqn] = request.max_namespaces
                self.subsys_serial[request.subsystem_nqn] = request.serial_number
                self.subsys_network[request.subsystem_nqn] = list(request.network_mask)

                dhchap_key_for_omap = request.dhchap_key
                key_encrypted_for_omap = False
                self.host_info.reset_subsystem_created_without_key(request.subsystem_nqn)
                if context and self.enable_key_encryption and request.dhchap_key:
                    if self.gateway_state.crypto:
                        dhchap_key_for_omap = self.gateway_state.crypto.encrypt_text(
                            request.dhchap_key)
                        key_encrypted_for_omap = True
                    else:
                        self.logger.warning(f"No encryption key or the wrong key was found but "
                                            f"we need to encrypt subsystem "
                                            f"{request.subsystem_nqn} DH-HMAC-CHAP key. "
                                            f"Any attempt to add host access using a "
                                            f"DH-HMAC-CHAP key to the subsystem "
                                            f"would fail")
                        dhchap_key_for_omap = GatewayUtilsCrypto.INVALID_KEY_VALUE
                        key_encrypted_for_omap = False
                        self.host_info.set_subsystem_created_without_key(request.subsystem_nqn)

                if request.dhchap_key:
                    self.host_info.add_dhchap_key_to_subsystem(request.subsystem_nqn,
                                                               request.dhchap_key)
            except Exception as ex:
                self.logger.exception(create_subsystem_error_prefix)
                errmsg = f"{create_subsystem_error_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{create_subsystem_error_prefix}: {resp['message']}"
                return pb2.subsys_status(status=status,
                                         error_message=errmsg, nqn=request.subsystem_nqn)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(create_subsystem_error_prefix)
                return pb2.subsys_status(status=errno.EINVAL,
                                         error_message=create_subsystem_error_prefix,
                                         nqn=request.subsystem_nqn)

            if context:
                # Update gateway state
                try:
                    assert not request.key_encrypted, "Encrypted keys can only come from update()"
                    if self.enable_key_encryption and dhchap_key_for_omap:
                        request.dhchap_key = dhchap_key_for_omap
                        request.key_encrypted = key_encrypted_for_omap
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_subsystem(request.subsystem_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting subsystem {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.subsys_status(status=errno.EINVAL,
                                             error_message=errmsg, nqn=request.subsystem_nqn)

        status = 0
        error_message = os.strerror(0)
        if request.network_mask and context:
            try:
                rt = self._create_auto_listeners_safe(request)
                if rt.status != 0:
                    status = errno.EAGAIN
                    error_message = f"Subsystem {request.subsystem_nqn} created successfully; " \
                                    f"Failed to create one or more NVMeoF listeners " \
                                    f"(network mask). You can try adding these listeners manually."
            except Exception:
                status = errno.EAGAIN
                error_message = f"Created subsystem {request.subsystem_nqn}. " \
                                f"An error occurred when adding network mask. Try " \
                                f"adding the listeners manually."
                self.logger.exception(error_message)
        return pb2.subsys_status(status=status, error_message=error_message,
                                 nqn=request.subsystem_nqn)

    def create_subsystem(self, request, context=None):
        err_prefix = f"Failure creating subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.create_subsystem_safe, request, context, err_prefix)

    def add_listeners(self, subsystem_nqn, ip_list, is_secure, port):
        req_status = 0
        for ip in ip_list:
            hostname = self.host_name
            if not port:
                if is_secure:
                    port = GatewayService.SECURE_LISTENER_PORT_DEFAULT
                else:
                    port = GatewayService.LISTENER_PORT_DEFAULT
            adrfam = f'ipv{ip_address(ip).version}'
            secure = is_secure
            lstnr_req = pb2.create_listener_req(
                nqn=subsystem_nqn,
                host_name=hostname,
                adrfam=adrfam,
                traddr=ip,
                trsvcid=port,
                secure=secure,
                verify_host_name=False)
            rt = self.create_listener_safe(lstnr_req, None)
            status = rt.status
            if status != 0:
                errmsg = f"Failure creating auto-listeners for {subsystem_nqn} " \
                         f"subsystem: {rt.error_message}"
                self.logger.error(errmsg)
                if status != errno.EEXIST:
                    req_status = status
            else:
                ip_ = GatewayUtils.escape_address_if_ipv6(ip)
                self.logger.info(f'Automatically created listener at {ip_}:{port} for '
                                 f'{subsystem_nqn}')
        return req_status

    def del_listeners(self, subsystem_nqn, ip_list, is_secure, port):
        req_status = 0
        if not port:
            if is_secure:
                port = GatewayService.SECURE_LISTENER_PORT_DEFAULT
            else:
                port = GatewayService.LISTENER_PORT_DEFAULT
        for ip in ip_list:
            hostname = self.host_name
            adrfam = f'ipv{ip_address(ip).version}'
            lstnr_req = pb2.delete_listener_req(
                nqn=subsystem_nqn,
                host_name=hostname,
                traddr=ip,
                adrfam=adrfam,
                trsvcid=int(port),
                force=True)
            rt = self.delete_listener_safe(lstnr_req, None)
            status = rt.status
            if status != 0:
                errmsg = f"Failure deleting auto-listeners for {subsystem_nqn} " \
                         f"subsystem: {rt.error_message}"
                self.logger.error(errmsg)
                if status != errno.ENOENT:
                    req_status = status
            else:
                ip_ = GatewayUtils.escape_address_if_ipv6(ip)
                self.logger.info(f'Automatically deleted listener at {ip_}:{port} for '
                                 f'{subsystem_nqn}')
        return req_status

    def _create_auto_listeners_safe(self, request):
        """
        Internal method - Automatically create listeners for IPs within subnet of 'network_mask'
        request: create_subsystem_req type
        """

        req_status = 0
        network_mask_subnets = request.network_mask
        for subnet in set(network_mask_subnets):
            found_host_ips = NICS(self.logger, True).get_ips_in_subnet(subnet)
            req_status = self.add_listeners(request.subsystem_nqn, found_host_ips,
                                            request.secure_listeners, request.port)
        if req_status != 0:
            err_msg = f"Failed to create auto-listeners for subsystem {request.subsystem_nqn}"
            return pb2.req_status(status=req_status, error_message=err_msg)
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def create_auto_listeners(self, request):
        """
        Create auto-listeners for request.network_mask (Internal method)
        request: create_subsystem_req type
        """
        with self.rpc_lock:
            return self._create_auto_listeners_safe(request)

    def add_subsystem_network_safe(self, request, context):

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling add_subsystem_network_safe()"

        self.logger.info(
            f"Received request to add network to subsystem {request.subsystem_nqn}, "
            f"network mask: {request.network_mask}, context: {context}")

        if not request.subsystem_nqn:
            errmsg = "Failure adding network_mask, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.network_mask:
            errmsg = f"Failure adding network_mask for subsystem " \
                     f"{request.subsystem_nqn}: Missing network_mask"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not NICS.is_valid_subnet(request.network_mask):
            errmsg = f"Failure adding network_mask for subsystem " \
                     f"{request.subsystem_nqn}: Invalid subnet \"{request.network_mask}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EADDRNOTAVAIL, error_message=errmsg)

        req_status = 0
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            subsys_entry = None
            state = self.gateway_state.local.get_state()
            subsys_key = GatewayState.build_subsystem_key(request.subsystem_nqn)
            try:
                state_subsys = state[subsys_key]
                subsys_entry = json_format.Parse(state_subsys, pb2.create_subsystem_req(),
                                                 ignore_unknown_fields=True)
            except Exception:
                errmsg = f"Can't find entry for subsystem {request.subsystem_nqn}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            assert subsys_entry, f"Can't find entry for subsystem {request.subsystem_nqn}"
            try:
                network_to_add = request.network_mask
                existing_network_masks = set(subsys_entry.network_mask)
                if network_to_add in existing_network_masks:
                    self.logger.warning(f"Network mask already exists for "
                                        f"subsystem {request.subsystem_nqn}")
                    return pb2.req_status(status=0, error_message=os.strerror(0))

                found_ips = NICS(self.logger, True).get_ips_in_subnet(network_to_add)
                req_status = self.add_listeners(request.subsystem_nqn, found_ips,
                                                subsys_entry.secure_listeners, subsys_entry.port)
                if req_status != 0:
                    self.logger.error(f'Failed to add all listeners in network mask '
                                      f'{request.network_mask} (all IPs: {found_ips}) '
                                      f'for subsystem {request.subsystem_nqn}.')
                else:
                    existing_network_masks.add(network_to_add)
                    new_network_mask = list(existing_network_masks)
                    self.subsys_network[request.subsystem_nqn] = new_network_mask
                    if context:
                        # remove listener from subsystem's OMAP
                        subsys_entry.network_mask[:] = new_network_mask
                        json_req = json_format.MessageToJson(
                            subsys_entry, preserving_proto_field_name=True,
                            including_default_value_fields=True)
                        self.gateway_state.add_subsystem(request.subsystem_nqn, json_req)
                    self.logger.info(f"Added network {request.network_mask} for subsystem "
                                     f"{request.subsystem_nqn}")
            except Exception as ex:
                errmsg = f"Failure occurred:\n{ex}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        err_msg = os.strerror(0)
        if req_status != 0:
            err_msg = f"Failed to add network for subsystem {request.subsystem_nqn}"
        return pb2.req_status(status=req_status, error_message=err_msg)

    def add_subsystem_network(self, request, context=None):
        """Add a network_mask on subsystem"""
        err_prefix = f"Failure adding network {request.network_mask} for " \
                     f"subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.add_subsystem_network_safe, request,
                                          context, err_prefix)

    def del_subsystem_network_safe(self, request, context):
        """Delete a network mask on subsystem"""
        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling del_subsystem_network_safe()"

        self.logger.info(
            f"Received request to delete network to subsystem {request.subsystem_nqn}, "
            f"network mask: {request.network_mask}, context: {context}")

        if not request.subsystem_nqn:
            errmsg = "Failure deleting network_mask, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.network_mask:
            errmsg = f"Failure deleting network_mask for subsystem " \
                     f"{request.subsystem_nqn}: Missing network_mask"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not NICS.is_valid_subnet(request.network_mask):
            errmsg = f"Failure deleting network_mask for subsystem " \
                     f"{request.subsystem_nqn}: Invalid subnet \"{request.network_mask}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EADDRNOTAVAIL, error_message=errmsg)

        req_status = 0
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            subsys_entry = None
            state = self.gateway_state.local.get_state()
            subsys_key = GatewayState.build_subsystem_key(request.subsystem_nqn)
            try:
                state_subsys = state[subsys_key]
                subsys_entry = json_format.Parse(state_subsys, pb2.create_subsystem_req(),
                                                 ignore_unknown_fields=True)
            except Exception:
                errmsg = f"Can't find entry for subsystem {request.subsystem_nqn}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            assert subsys_entry, f"Can't find entry for subsystem {request.subsystem_nqn}"
            try:
                network_to_delete = request.network_mask
                if not subsys_entry.network_mask:
                    errmsg = f"No existing network mask found for " \
                             f"subsystem {request.subsystem_nqn}"
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
                existing_network_mask = set(subsys_entry.network_mask)
                if network_to_delete not in existing_network_mask:
                    self.logger.warning(f"Network mask {request.network_mask} not "
                                        f"found for subsystem {request.subsystem_nqn}")
                    return pb2.req_status(status=0, error_message=os.strerror(0))

                found_ips = NICS(self.logger, True).get_ips_in_subnet(network_to_delete)
                req_status = self.del_listeners(request.subsystem_nqn, found_ips,
                                                subsys_entry.secure_listeners, subsys_entry.port)
                if req_status != 0:
                    self.logger.error(f'Failed to delete all listeners under network mask '
                                      f'{request.network_mask} (all IPs: {found_ips}) '
                                      f'for subsystem {request.subsystem_nqn}.')
                else:
                    existing_network_mask.remove(network_to_delete)
                    new_network_mask = list(existing_network_mask)
                    self.subsys_network[request.subsystem_nqn] = new_network_mask
                    if context:
                        # remove listener from subsystem's OMAP
                        subsys_entry.network_mask[:] = new_network_mask
                        json_req = json_format.MessageToJson(
                            subsys_entry, preserving_proto_field_name=True,
                            including_default_value_fields=True)
                        self.gateway_state.add_subsystem(request.subsystem_nqn, json_req)
                    self.logger.info(f"Deleted network {network_to_delete} for subsystem "
                                     f"{request.subsystem_nqn}")
            except Exception as ex:
                errmsg = f"Failure occurred:\n{ex}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        err_msg = os.strerror(0)
        if req_status != 0:
            err_msg = f"Failed to delete network for subsystem {request.subsystem_nqn}"
        return pb2.req_status(status=req_status, error_message=err_msg)

    def del_subsystem_network(self, request, context=None):
        """Delete a network mask on subsystem"""
        err_prefix = f"Failure deleting network {request.network_mask} for " \
                     f"subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.del_subsystem_network_safe, request,
                                          context, err_prefix)

    def _add_one_kmip_server_endpoint(self, subsys, server, endpoint, context):
        """Add a KMIP server endpoint to the subsystem"""

        if not endpoint.HasField("port") or endpoint.port is None:
            endpoint.port = KMIPServerEndpointList.KMIP_DEFAULT_PORT
            self.logger.info(f"KMIP server {server} endpoint's port wasn't specified, will use "
                             f"default port {KMIPServerEndpointList.KMIP_DEFAULT_PORT}")

        error_prefix = f"Failure adding an endpoint, with address " \
                       f"{endpoint.address}:{endpoint.port}, to " \
                       f"KMIP server \"{server}\" on subsystem {subsys}"

        if endpoint.port <= 0 or endpoint.port > 0xffff:
            errmsg = f"{error_prefix}: Server endpoint's port must be between 1 and 65535"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not server.strip():
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\", " \
                     f"name can't be empty"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(server) or os.path.sep in server:
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        normsrvr = os.path.normpath(server)
        if normsrvr in (".", ".."):
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(endpoint.address):
            errmsg = f"{error_prefix}: Invalid KMIP server endpoint address " \
                     f"\"{endpoint.address}\", contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(subsys):
            errmsg = f"{error_prefix}: Invalid subsystem NQN \"{subsys}\"," \
                     f" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if subsys not in self.subsys_serial:
            errmsg = f"{error_prefix}: Can't find subsystem {subsys}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if not GatewayUtils.is_valid_host_name(endpoint.address):
            errmsg = f"{error_prefix}: Invalid KMIP server endpoint " \
                     f"address \"{endpoint.address}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        try:
            cert_dir = self.kmip_cert_dir.format(server_name=server)
        except Exception:
            self.logger.exception(f"Error formatting {self.kmip_cert_dir}")
            errmsg = f"{error_prefix}: Invalid KMIP certificate directory " \
                     f"configuration \"{self.kmip_cert_dir}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not self.kmip_client_cert:
            errmsg = f"{error_prefix}: Client certificate name is undefined"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
        client_cert = os.path.join(cert_dir, self.kmip_client_cert)
        if not Path(client_cert).is_file():
            errmsg = f"{error_prefix}: Missing client certificate {client_cert}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if not self.kmip_client_key:
            errmsg = f"{error_prefix}: Client key name is undefined"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
        client_key = os.path.join(cert_dir, self.kmip_client_key)
        if not Path(client_key).is_file():
            errmsg = f"{error_prefix}: Missing client key {client_key}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if not self.kmip_ca_cert:
            errmsg = f"{error_prefix}: CA certificate is undefined"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
        ca_cert = os.path.join(cert_dir, self.kmip_ca_cert)
        if not Path(ca_cert).is_file():
            errmsg = f"{error_prefix}: Missing CA certificate {ca_cert}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        # if we have a server endpoint with the exact attributes, just issue a warning
        ep = KMIPServerEndpoint(endpoint.address, endpoint.port)
        if self.kmip_server_endpoints.does_kmip_server_endpoint_exist(subsys, server, ep):
            errmsg = f"{error_prefix}: Server endpoint already exists"
            self.logger.warning(errmsg)
            return pb2.req_status(status=errno.EEXIST, error_message=errmsg)

        subsys_server_name = self.kmip_server_endpoints.get_subsystem_server_name(subsys)
        if subsys_server_name and subsys_server_name != server:
            errmsg = f"{error_prefix}: Subsystem already uses KMIP server " \
                     f"\"{subsys_server_name}\", no other server is allowed"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EADDRINUSE, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if context:
                # Update gateway state
                try:
                    request = pb2.add_kmip_server_endpoints_req(subsystem_nqn=subsys,
                                                                server_name=server,
                                                                endpoints=[endpoint])
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_kmip_server_endpoint(subsys,
                                                                server,
                                                                endpoint.address,
                                                                endpoint.port,
                                                                json_req)
                except Exception as ex:
                    errmsg = f"Error persisting addition of " \
                             f"endpoint with address {endpoint.address}:{endpoint.port} " \
                             f"for KMIP server {server} on subsystem {subsys}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        self.kmip_server_endpoints.add_server_endpoint(subsys,
                                                       server,
                                                       KMIPServerEndpoint(endpoint.address,
                                                                          endpoint.port))
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def add_kmip_server_endpoints_safe(self, request, context):
        """Add KMIP server endpoints to the subsystem"""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling add_kmip_server_endpoints_safe()"

        peer_msg = self.get_peer_message(context)
        self.logger.info(
            f"Received request to add KMIP server endpoints to subsystem {request.subsystem_nqn}, "
            f"server name: {request.server_name}, endpoints: {request.endpoints}, "
            f"context: {context}{peer_msg}")

        if not request.endpoints:
            errmsg = f"Failure adding endpoints to KMIP server \"{request.server_name}\" on " \
                     f"subsystem {request.subsystem_nqn}: No endpoints were specified"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        had_one_ok_status = False
        final_ret = None
        for endpoint in request.endpoints:
            ret = self._add_one_kmip_server_endpoint(request.subsystem_nqn,
                                                     request.server_name,
                                                     endpoint,
                                                     context)
            if ret.status == 0:
                had_one_ok_status = True
            if not final_ret:
                final_ret = ret
            if final_ret.status == 0 and ret.status != 0:
                final_ret = ret
            if final_ret.status == errno.EEXIST and ret.status != 0:
                final_ret = ret

        if final_ret:
            if final_ret.status == errno.EEXIST and had_one_ok_status:
                final_ret.status = 0
                final_ret.error_message = os.strerror(0)
        else:
            final_ret = pb2.req_status(status=0, error_message=os.strerror(0))

        return pb2.req_status(status=final_ret.status, error_message=final_ret.error_message)

    def add_kmip_server_endpoints(self, request, context=None):
        """Add KMIP server endpoints to the subsystem"""
        err_prefix = f"Failure adding KMIP server endpoints to " \
                     f"subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.add_kmip_server_endpoints_safe, request,
                                          context, err_prefix)

    def _del_one_kmip_server_endpoint(self, subsys, server, endpoint, context):
        """Delete a KMIP server endpoint from the subsystem"""

        if not endpoint.HasField("port") or endpoint.port is None:
            endpoint.port = KMIPServerEndpointList.KMIP_DEFAULT_PORT
            self.logger.info(f"KMIP server {server} endpoint's port wasn't specified, will use "
                             f"default port {KMIPServerEndpointList.KMIP_DEFAULT_PORT}")

        error_prefix = f"Failure deleting endpoint, with address " \
                       f"{endpoint.address}:{endpoint.port}, from " \
                       f"KMIP server \"{server}\" on subsystem {subsys}"

        if endpoint.port <= 0 or endpoint.port > 0xffff:
            errmsg = f"{error_prefix}: Server endpoint's port must be between 1 and 65535"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not server.strip():
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\", " \
                     f"name can't be empty"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(server) or os.path.sep in server:
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        normsrvr = os.path.normpath(server)
        if normsrvr in (".", ".."):
            errmsg = f"{error_prefix}: Invalid KMIP server name \"{server}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(endpoint.address):
            errmsg = f"{error_prefix}: Invalid KMIP server endpoint address " \
                     f"\"{endpoint.address}\", contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(subsys):
            errmsg = f"{error_prefix}: Invalid subsystem NQN \"{subsys}\"," \
                     f" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if subsys not in self.subsys_serial:
            errmsg = f"{error_prefix}: Can't find subsystem {subsys}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if not GatewayUtils.is_valid_host_name(endpoint.address):
            errmsg = f"{error_prefix}: Invalid KMIP server endpoint " \
                     f"address \"{endpoint.address}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        ep = KMIPServerEndpoint(endpoint.address, endpoint.port)
        if not self.kmip_server_endpoints.does_kmip_server_endpoint_exist(subsys, server, ep):
            errmsg = f"{error_prefix}: server endpoint not found"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_kmip_server_endpoint(subsys, server,
                                                                   endpoint.address, endpoint.port)
                except Exception as ex:
                    errmsg = f"Error persisting removal of KMIP server endpoint " \
                             f"{endpoint.address}:{endpoint.port}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        self.kmip_server_endpoints.remove_server_endpoint(subsys,
                                                          server,
                                                          KMIPServerEndpoint(endpoint.address,
                                                                             endpoint.port))
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def del_kmip_server_endpoints_safe(self, request, context):
        """Delete KMIP server endpoints from the subsystem"""
        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling del_kmip_server_endpoints_safe()"

        peer_msg = self.get_peer_message(context)
        self.logger.info(
            f"Received request to delete KMIP server endpoints from subsystem "
            f"{request.subsystem_nqn}, server name: {request.server_name}, "
            f"endpoints: {request.endpoints}, context: {context}{peer_msg}")

        if not request.endpoints:
            errmsg = f"Failure deleting endpoints from KMIP server \"{request.server_name}\" on " \
                     f"subsystem {request.subsystem_nqn}: No endpoints were specified"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        had_eps = self.kmip_server_endpoints.subsystem_has_server_endpoints(request.subsystem_nqn)
        final_ret = None
        for endpoint in request.endpoints:
            ret = self._del_one_kmip_server_endpoint(request.subsystem_nqn,
                                                     request.server_name,
                                                     endpoint,
                                                     context)
            if not final_ret:
                final_ret = ret
            if final_ret.status == 0 and ret.status != 0:
                final_ret = ret

        if not final_ret:
            final_ret = pb2.req_status(status=0, error_message=os.strerror(0))

        if had_eps:
            has_eps = self.kmip_server_endpoints.subsystem_has_server_endpoints(
                request.subsystem_nqn)
            if not has_eps:
                self.logger.info(f"Last server endpoint for subsystem "
                                 f"{request.subsystem_nqn} was deleted")
                self.kmip_clients.remove_client(request.subsystem_nqn)

        return pb2.req_status(status=final_ret.status, error_message=final_ret.error_message)

    def del_kmip_server_endpoints(self, request, context=None):
        """Delete KMIP server endpoints from the subsystem"""
        err_prefix = f"Failure deleting KMIP server endpoints from " \
                     f"subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.del_kmip_server_endpoints_safe, request,
                                          context, err_prefix)

    def list_kmip_server_endpoints(self, request, context=None):
        """List KMIP server endpoints for one or all subsystems"""

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to list the KMIP server endpoints for "
                         f"{request.subsystem_nqn}, server: {request.server_name}, "
                         f"context: {context}{peer_msg}")
        if not request.subsystem_nqn or request.subsystem_nqn == GatewayUtils.ALL_SUBSYSTEMS:
            subsystem_nqn = None
        else:
            subsystem_nqn = request.subsystem_nqn

        server_name = request.server_name if request.server_name else None

        endpoints = self.kmip_server_endpoints.get_server_endpoint_list(subsystem_nqn, server_name)
        endpoints_out = []
        for s in endpoints:
            endpoints_out.append(pb2.kmip_server_endpoint_cli(subsystem_nqn=s[0],
                                                              server_name=s[1],
                                                              address=s[2].address,
                                                              port=s[2].port))
        return pb2.kmip_server_endpoints_info(status=0,
                                              error_message=os.strerror(0),
                                              endpoints=endpoints_out)

    def get_subsystem_namespaces(self, nqn) -> list:
        ns_list = []
        local_state_dict = self.gateway_state.local.get_state()
        for key, val in local_state_dict.items():
            if not key.startswith(self.gateway_state.local.NAMESPACE_PREFIX):
                continue
            try:
                ns = json_format.Parse(val, pb2.namespace_add_req(),
                                       ignore_unknown_fields=True)
                if ns.subsystem_nqn == nqn:
                    nsid = ns.nsid
                    ns_list.append(nsid)
            except Exception:
                self.logger.exception(f"Got exception trying to get subsystem {nqn} namespaces")
                pass

        return ns_list

    def subsystem_has_listeners(self, nqn) -> bool:
        local_state_dict = self.gateway_state.local.get_state()
        for key, val in local_state_dict.items():
            if not key.startswith(self.gateway_state.local.LISTENER_PREFIX):
                continue
            try:
                lsnr = json_format.Parse(val, pb2.create_listener_req(),
                                         ignore_unknown_fields=True)
                if lsnr.nqn == nqn:
                    return True
            except Exception:
                self.logger.exception(f"Got exception trying to get subsystem {nqn} listener")
                pass

        return False

    def remove_subsystem_from_state(self, nqn, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        # Update gateway state
        try:
            self.gateway_state.remove_subsystem(nqn)
        except Exception as ex:
            errmsg = f"Error persisting deletion of subsystem {nqn}"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def delete_subsystem_safe(self, request, context):
        """Deletes a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling delete_subsystem_safe()"
        delete_subsystem_error_prefix = f"Failure deleting subsystem {request.subsystem_nqn}"

        ret = False
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                ret = self.spdk_rpc_client.nvmf_delete_subsystem(nqn=request.subsystem_nqn)
                self.subsys_max_ns.pop(request.subsystem_nqn)
                self.subsys_serial.pop(request.subsystem_nqn)
                self.subsys_network.pop(request.subsystem_nqn)
                self.kmip_server_endpoints.remove_subsystem_server_endpoints(request.subsystem_nqn)
                self.kmip_clients.remove_client(request.subsystem_nqn)
                if request.subsystem_nqn in self.subsystem_listeners:
                    self.subsystem_listeners.pop(request.subsystem_nqn, None)
                self.host_info.clean_subsystem(request.subsystem_nqn)
                self.subsystem_nsid_bdev_and_uuid.remove_namespace(request.subsystem_nqn)
                self.remove_all_subsystem_key_files(request.subsystem_nqn)
                self.remove_all_subsystem_keys_from_keyring(request.subsystem_nqn)
                self.logger.debug(f"delete_subsystem {request.subsystem_nqn}: {ret}")
            except Exception as ex:
                self.logger.exception(delete_subsystem_error_prefix)
                errmsg = f"{delete_subsystem_error_prefix}:\n{ex}"
                self.remove_subsystem_from_state(request.subsystem_nqn, context)
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{delete_subsystem_error_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(delete_subsystem_error_prefix)
                self.remove_subsystem_from_state(request.subsystem_nqn, context)
                return pb2.req_status(status=errno.EINVAL,
                                      error_message=delete_subsystem_error_prefix)

            return self.remove_subsystem_from_state(request.subsystem_nqn, context)

    def delete_subsystem(self, request, context=None):
        """Deletes a subsystem."""

        peer_msg = self.get_peer_message(context)
        delete_subsystem_error_prefix = f"Failure deleting subsystem {request.subsystem_nqn}"
        self.logger.info(f"Received request to delete subsystem {request.subsystem_nqn}, "
                         f"force: {request.force}, i_am_sure: {request.i_am_sure}, "
                         f"context: {context}{peer_msg}")

        if not request.subsystem_nqn:
            errmsg = "Failure deleting subsystem, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{delete_subsystem_error_prefix}: No such subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{delete_subsystem_error_prefix}: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{delete_subsystem_error_prefix}: Can't delete a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        ns_list = []
        if context:
            if self.subsystem_has_listeners(request.subsystem_nqn):
                self.logger.warning(f"About to delete subsystem {request.subsystem_nqn} "
                                    f"which has a listener defined")
            ns_list = self.get_subsystem_namespaces(request.subsystem_nqn)

        # We found a namespace still using this subsystem and --force wasn't used fail with EBUSY
        if not request.force and len(ns_list) > 0:
            errmsg = f"{delete_subsystem_error_prefix}: Namespace {ns_list[0]} is still using " \
                     f"the subsystem. Either remove it or use the \"--force\" command line option"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EBUSY, error_message=errmsg)

        for nsid in ns_list:
            # We found a namespace still using this subsystem and --force was used so
            # we will try to remove the namespace
            self.logger.warning(f"Will remove namespace {nsid} from {request.subsystem_nqn}")
            del_req = pb2.namespace_delete_req(subsystem_nqn=request.subsystem_nqn,
                                               nsid=nsid, i_am_sure=request.i_am_sure)
            ret = self.namespace_delete(del_req, context)
            if ret.status == 0:
                self.logger.info(f"Automatically removed namespace {nsid} from "
                                 f"{request.subsystem_nqn}")
            else:
                self.logger.error(f"Failure removing namespace {nsid} from "
                                  f"{request.subsystem_nqn}:\n{ret.error_message}")
                self.logger.warning(f"Will continue deleting {request.subsystem_nqn} anyway")
        return self.execute_grpc_function(self.delete_subsystem_safe, request, context,
                                          f"{delete_subsystem_error_prefix}: ")

    def check_if_image_used(self, pool_name, image_name, rados_namespace_name, uuid):
        """Check if image is used by any other namespace."""

        errmsg = ""
        nqn = None
        pool_name = GatewayStateHandler._normalize_json_string(pool_name)
        image_name = GatewayStateHandler._normalize_json_string(image_name)
        rados_namespace_name = GatewayStateHandler._normalize_json_string(rados_namespace_name)
        image_path = f"{pool_name}/{rados_namespace_name}/{image_name}" \
            if rados_namespace_name else f"{pool_name}/{image_name}"
        rbd_id = self.ceph_utils.get_image_id(pool_name, image_name, rados_namespace_name)
        img_ids_list = self.get_image_identification(pool_name, image_name, rados_namespace_name)
        for img_id in img_ids_list:
            if img_id.empty():
                continue
            if not img_id.does_fsid_match(self.fsid):
                continue
            if rbd_id and not img_id.does_rbd_id_match(rbd_id):
                continue
            if not img_id.is_same_group(self.gateway_group):
                grp_txt = f", group {img_id.group_name}" if img_id.group_name else ""
                errmsg = f"RBD image {image_path} is already used by a namespace " \
                         f"in subsystem {img_id.subsys}{grp_txt}"
                return errmsg, img_id.subsys
            if not img_id.is_same_uuid(uuid):
                uuid_txt = f"with UUID {img_id.uuid} " if img_id.uuid else ""
                grp_txt = f", group {img_id.group_name}" if img_id.group_name else ""
                errmsg = f"RBD image {image_path} is already used by a namespace " \
                         f"{uuid_txt}in subsystem {img_id.subsys}{grp_txt}"
                return errmsg, img_id.subsys

        state = self.gateway_state.local.get_state()
        for key, val in state.items():
            if not key.startswith(self.gateway_state.local.NAMESPACE_PREFIX):
                continue
            try:
                ns = json_format.Parse(val, pb2.namespace_add_req(),
                                       ignore_unknown_fields=True)
                ns_pool = ns.rbd_pool_name
                ns_pool = GatewayStateHandler._normalize_json_string(ns_pool)
                ns_image = ns.rbd_image_name
                ns_image = GatewayStateHandler._normalize_json_string(ns_image)
                ns_rados_namespace = ns.rados_namespace_name
                ns_rados_namespace = GatewayStateHandler._normalize_json_string(ns_rados_namespace)
                # Notice that the normalized values can't be None. None will be changed into ""
                if pool_name != ns_pool:
                    continue
                if image_name != ns_image:
                    continue
                if rados_namespace_name != ns_rados_namespace:
                    continue
                nqn = ns.subsystem_nqn
                path = f"{ns_pool}/{ns_rados_namespace}/{ns_image}" \
                    if ns_rados_namespace else f"{ns_pool}/{ns_image}"
                errmsg = f"RBD image {path} is already used by a namespace " \
                         f"in subsystem {nqn}"
                break
            except Exception:
                self.logger.exception(f"Got exception while parsing {val}, will continue")
                continue
        return errmsg, nqn

    def create_namespace(self, subsystem_nqn, bdev_name, nsid, anagrpid, uuid,
                         auto_visible, rbd_pool, rbd_data_pool, rbd_image_name,
                         rados_namespace_name, trash_image, read_only, location,
                         auto_resize, encryption_entries, encryption_algorithm, context):
        """Adds a namespace to a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling create_namespace()"
        assert context is None or self.omap_lock.write_locked_by_me(), \
            f"OMAP is unlocked when calling create_namespace()\n" \
            f"in thread: {threading.get_native_id()}. Locked by: " \
            f"{self.omap_lock.locked_by}, with cookie: {self.omap_lock.lock_cookie}, " \
            f"locked: {self.omap_lock.is_exclusively_locked}"

        assert (rbd_pool and rbd_image_name) or ((not rbd_pool) and (not rbd_image_name)), \
            "RBD pool and image name should either be both set or both empty"

        nsid_msg = ""
        if nsid:
            nsid_msg = f" using ID {nsid}"

        if not subsystem_nqn:
            errmsg = "Failure adding namespace, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        add_namespace_error_prefix = f"Failure adding namespace{nsid_msg} to {subsystem_nqn}"

        peer_msg = self.get_peer_message(context)
        rbd_msg = ""
        if rbd_pool and rbd_image_name:
            rbd_msg = f"RBD image {rbd_pool}/{rbd_image_name}, " if not rados_namespace_name \
                      else f"RBD image {rbd_pool}/{rados_namespace_name}/{rbd_image_name}, "
        self.logger.info(f"Received request to add {bdev_name} to {subsystem_nqn} with load "
                         f"balancing group id {anagrpid}{nsid_msg}, auto_visible: {auto_visible}, "
                         f"auto_resize: {auto_resize}, "
                         f"{rbd_msg}context: {context}{peer_msg}")

        if subsystem_nqn not in self.subsys_serial:
            errmsg = f"{add_namespace_error_prefix}: No such subsystem"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.ENOENT, error_message=errmsg)

        subsys_max_ns = 0
        if subsystem_nqn in self.subsys_max_ns:
            subsys_max_ns = self.subsys_max_ns[subsystem_nqn]

        if anagrpid > subsys_max_ns:
            errmsg = f"{add_namespace_error_prefix}: Group ID {anagrpid} is bigger than " \
                     f"configured maximum {subsys_max_ns}"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(subsystem_nqn):
            errmsg = f"{add_namespace_error_prefix}: Can't add namespaces to a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        if not auto_visible:
            ns_count = self.subsystem_nsid_bdev_and_uuid.get_namespace_count(subsystem_nqn,
                                                                             False, 0)
            if ns_count >= self.max_namespaces_with_netmask:
                errmsg = f"{add_namespace_error_prefix}: Maximal number of namespaces which are " \
                         f"only visible to selected hosts ({self.max_namespaces_with_netmask}) " \
                         f"has already been reached"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        if nsid and nsid > subsys_max_ns:
            errmsg = f"{add_namespace_error_prefix}: Requested ID {nsid} is bigger than " \
                     f"the maximal one ({subsys_max_ns})"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        ns_count = self.subsystem_nsid_bdev_and_uuid.get_namespace_count(subsystem_nqn, None, 0)
        if ns_count >= subsys_max_ns:
            errmsg = f"{add_namespace_error_prefix}: Subsystem's maximal number of " \
                     f"namespaces ({subsys_max_ns}) has already been reached"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        ns_count = self.subsystem_nsid_bdev_and_uuid.get_namespace_count(None, None, 0)
        if ns_count >= self.max_namespaces:
            errmsg = f"{add_namespace_error_prefix}: Maximal number of namespaces " \
                     f"({self.max_namespaces}) has already been reached"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        try:
            nsid = self.spdk_rpc_client.nvmf_subsystem_add_ns(
                nqn=subsystem_nqn,
                namespace={
                    "bdev_name": bdev_name,
                    "nsid": nsid,
                    "anagrpid": anagrpid,
                    "uuid": uuid,
                    "no_auto_visible": not auto_visible,
                    "ptpl_file": "PTPL"
                },
            )
            self.subsystem_nsid_bdev_and_uuid.add_namespace(subsystem_nqn, nsid,
                                                            bdev_name, uuid,
                                                            anagrpid, auto_visible,
                                                            rbd_pool, rbd_data_pool,
                                                            rbd_image_name,
                                                            rados_namespace_name,
                                                            trash_image, read_only,
                                                            location, auto_resize,
                                                            encryption_entries,
                                                            encryption_algorithm)
            self.logger.debug(f"subsystem_add_ns: {nsid}")
            self.ana_grp_ns_load[anagrpid] += 1
            if anagrpid in self.ana_grp_subs_load:
                if subsystem_nqn in self.ana_grp_subs_load[anagrpid]:
                    self.ana_grp_subs_load[anagrpid][subsystem_nqn] += 1
                else:
                    self.ana_grp_subs_load[anagrpid][subsystem_nqn] = 1
            else:
                self.ana_grp_subs_load[anagrpid][subsystem_nqn] = 1
        except Exception as ex:
            self.logger.exception(add_namespace_error_prefix)
            errmsg = f"{add_namespace_error_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{add_namespace_error_prefix}: {resp['message']}"
            self.subsystem_nsid_bdev_and_uuid.remove_namespace(subsystem_nqn, nsid)
            return pb2.nsid_status(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not nsid:
            self.logger.error(add_namespace_error_prefix)
            return pb2.nsid_status(status=errno.EINVAL, error_message=add_namespace_error_prefix)

        return pb2.nsid_status(nsid=nsid, status=0, error_message=os.strerror(0))

    def find_unique_bdev_name(uuid) -> str:
        assert uuid, "Got an empty UUID"
        return f"bdev_{uuid}"

    def set_ana_state(self, request, context=None):
        return self.execute_grpc_function(self.set_ana_state_safe, request, context)

    def set_ana_state_safe(self, ana_info: pb2.ana_info, context=None):
        peer_msg = self.get_peer_message(context)
        """Sets ana state for this gateway."""
        self.logger.info(f"Received request to set ana states {ana_info.states}, {peer_msg}")

        assert self.rpc_lock.locked(), "RPC is unlocked when calling set_ana_state_safe()"
        inaccessible_ana_groups = {}
        awaited_cluster_contexts = set()
        # Iterate over nqn_ana_states in ana_info
        for nas in ana_info.states:

            # fill the static gateway dictionary per nqn and grp_id
            nqn = nas.nqn
            for gs in nas.states:
                self.ana_map[nqn][gs.grp_id] = gs.state
                self.ana_grp_state[gs.grp_id] = gs.state

            # If this is not set the subsystem was not created yet
            if nqn not in self.subsys_serial:
                continue

            self.logger.debug(f"Iterate over {nqn=} {self.subsystem_listeners[nqn]=}")
            for listener in self.subsystem_listeners[nqn]:
                self.logger.debug(f"{listener=}")

                # Iterate over ana_group_state in nqn_ana_states
                for gs in nas.states:
                    # Access grp_id and state
                    grp_id = gs.grp_id
                    # The gateway's interface gRPC ana_state into SPDK JSON RPC values,
                    # see nvmf_subsystem_listener_set_ana_state
                    # method https://spdk.io/doc/jsonrpc.html
                    if gs.state == pb2.ana_state.OPTIMIZED:
                        ana_state = "optimized"
                    else:
                        ana_state = "inaccessible"
                    try:
                        # Need to wait for the latest OSD map, for each RADOS
                        # cluster context before becoming optimized,
                        # part of blocklist logic
                        if gs.state == pb2.ana_state.OPTIMIZED:
                            # Go over the namespaces belonging to the ana group
                            ns = self.subsystem_nsid_bdev_and_uuid.get_namespace_infos_for_anagrpid(
                                nqn, grp_id)
                            for ns_info in ns:
                                # get the cluster name for this namespace
                                with self.shared_state_lock:
                                    cluster = self.bdev_cluster[ns_info.bdev]
                                if not cluster:
                                    raise Exception(f"can not find cluster context name for "
                                                    f"bdev {ns_info.bdev}")

                                if cluster in awaited_cluster_contexts:
                                    # this cluster context was already awaited
                                    continue
                                if not self.spdk_rpc_client.bdev_rbd_wait_for_latest_osdmap(
                                        name=cluster):
                                    raise Exception(f"bdev_rbd_wait_for_latest_osdmap({cluster=})"
                                                    f" error")
                                self.logger.debug(f"set_ana_state "
                                                  f"bdev_rbd_wait_for_latest_osdmap {cluster=}")
                                awaited_cluster_contexts.add(cluster)

                        self.logger.debug(f"set_ana_state nvmf_subsystem_listener_set_ana_state "
                                          f"{nqn=} {listener=} {ana_state=} {grp_id=}")
                        (adrfam, traddr, trsvcid, secure, active) = listener
                        if not active:
                            continue
                        ret = self.spdk_rpc_client.nvmf_subsystem_listener_set_ana_state(
                            nqn=nqn,
                            listen_address={"trtype": "TCP",
                                            "traddr": traddr,
                                            "trsvcid": str(trsvcid),
                                            "adrfam": adrfam},
                            ana_state=ana_state,
                            anagrpid=grp_id)
                        if ana_state == "inaccessible":
                            inaccessible_ana_groups[grp_id] = True
                        self.logger.debug(f"set_ana_state nvmf_subsystem_listener_set_ana_state "
                                          f"response {ret=}")
                        if not ret:
                            raise Exception(f"nvmf_subsystem_listener_set_ana_state({nqn=}, "
                                            f"{listener=}, {ana_state=}, {grp_id=}) error")
                    except Exception as ex:
                        self.logger.exception("nvmf_subsystem_listener_set_ana_state()")
                        if context:
                            context.set_code(grpc.StatusCode.INTERNAL)
                            context.set_details(f"{ex}")
                        return pb2.req_status()
        return pb2.req_status(status=True)

    def namespace_add_safe(self, request, context):
        """Adds a namespace to a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling namespace_add_safe()"
        if not request.subsystem_nqn:
            errmsg = "Failure adding namespace, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        loc_grps_list = []
        anagrp = 0
        peer_msg = self.get_peer_message(context)
        nsid_msg = f"{request.nsid} " if request.nsid else ""
        loc_msg = f'"{request.location}"' if request.location else '""'
        rados_namespace_msg = ""
        if request.rados_namespace_name:
            rados_namespace_msg = f"rados_namespace_name: {request.rados_namespace_name}, "
        assert request.encryption_entries is not None, "Shouldn't get None from protobuf"
        enc_entries_msg = ""
        for enc in request.encryption_entries:
            try:
                enc_format = GatewayEnumUtils.get_key_from_value(pb2.EncryptionFormat,
                                                                 enc.format)
                if enc_format is None:
                    enc_format = enc.format
            except Exception:
                enc_format = enc.format
            enc_entries_msg += f"(format: {enc_format}, key id: {enc.key_id}), "
        enc_entries_msg = "[" + enc_entries_msg.removesuffix(", ") + "]"
        enc_algorithm_msg = ""
        try:
            enc_algorithm_msg = GatewayEnumUtils.get_key_from_value(pb2.EncryptionAlgorithm,
                                                                    request.encryption_algorithm)
        except Exception:
            pass
        if not enc_algorithm_msg:
            enc_algorithm_msg = request.encryption_algorithm
        self.logger.info(f"Received request to add namespace {nsid_msg}to "
                         f"{request.subsystem_nqn}, ana group {request.anagrpid}, "
                         f"no_auto_visible: {request.no_auto_visible}, "
                         f"disable_auto_resize: {request.disable_auto_resize}, "
                         f"read_only: {request.read_only}, location: {loc_msg}, "
                         f"{rados_namespace_msg}"
                         f"encryption_entries: {enc_entries_msg}, "
                         f"encryption_algorithm: {enc_algorithm_msg}, "
                         f"context: {context}{peer_msg}")

        if not request.uuid:
            request.uuid = str(uuid.uuid4())

        if request.trash_image and not request.create_image:
            self.logger.warning("Can't trash the RBD image on delete if it "
                                "wasn't created by the gateway, will reset the flag")
            request.trash_image = False

        has_a_none_format = False
        has_non_none_format = False
        decrypted_enc_entries = deepcopy(request.encryption_entries)
        current_server_endpoints = []
        server_name = None
        kmip_client = None
        for ent in decrypted_enc_entries:
            if ent.format == pb2.EncryptionFormat.none:
                has_a_none_format = True
                if ent.key_id:
                    errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                             f"Mustn't have a key ID when encryption format is not set"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)
            else:
                has_non_none_format = True
                if not ent.key_id:
                    errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                             f"Must have a key ID when encryption format is set"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.ENOKEY, error_message=errmsg)

                if not current_server_endpoints or not server_name:
                    current_server_endpoints = []
                    server_name = None
                    endpoints = self.kmip_server_endpoints.get_server_endpoint_list(
                        request.subsystem_nqn)
                    if not endpoints:
                        errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}" \
                                 f": No KMIP server endpoints were added to the subsystem but " \
                                 f"encryption was requested"
                        self.logger.error(errmsg)
                        return pb2.nsid_status(status=errno.ENOKEY, error_message=errmsg)
                    for s in endpoints:
                        current_server_endpoints.append((s[2].address, s[2].port))

                    server_name = endpoints[0][1]

                if kmip_client is None:
                    assert server_name, "KMIP server name is missing"
                    try:
                        cert_dir = self.kmip_cert_dir.format(server_name=server_name)
                    except Exception:
                        self.logger.exception(f"Error formatting {self.kmip_cert_dir}")
                        errmsg = f"Failure adding namespace {nsid_msg}to " \
                                 f"{request.subsystem_nqn}: Invalid KMIP certificate directory " \
                                 f"configuration \"{self.kmip_cert_dir}\""
                        self.logger.error(errmsg)
                        return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)
                    kmip_client = self.kmip_clients.add_client(cert_dir, request.subsystem_nqn)
                    if kmip_client is None:
                        errmsg = f"Failure adding namespace {nsid_msg}to " \
                                 f"{request.subsystem_nqn}: " \
                                 f"Can't add a KMIP client for fetching keys"
                        self.logger.error(errmsg)
                        return pb2.nsid_status(status=errno.ENOKEY, error_message=errmsg)

                key_content = None
                try:
                    key_content = kmip_client.get_key_for_rbd_image(ent.key_id,
                                                                    current_server_endpoints)
                except Exception:
                    self.logger.exception(f"Can't fetch passphrase for id {ent.key_id}, "
                                          f"endpoints: {current_server_endpoints}")
                if not key_content:
                    errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                             f"Can't fetch passphrase for id {ent.key_id}"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.ENOKEY, error_message=errmsg)

                try:
                    ent.key_id = key_content.decode()
                except Exception:
                    self.logger.exception("Error decoding key to string")
                    errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                             f"Can't decode passphrase to string"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.ENOKEY, error_message=errmsg)

        if has_a_none_format and has_non_none_format:
            errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                     f"Mismatch in encryption formats, either all should be set or none"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        if not decrypted_enc_entries or has_a_none_format:
            if request.encryption_algorithm != pb2.EncryptionAlgorithm.no_algorithm:
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                         f"Can\'t have an encryption algorithm without an encryption format"
                self.logger.error(errmsg)
                return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        if context:
            try:
                self.ceph_utils.remove_image_metadata(request.rbd_pool_name,
                                                      request.rbd_image_name,
                                                      request.rados_namespace_name,
                                                      "reservation_key")
            except Exception:
                self.logger.warning(f"Failed to delete reservation_key "
                                    f"from image {request.rbd_pool_name}/{request.rbd_image_name}")

            self.ceph_utils.get_number_created_gateways(self.gateway_pool,
                                                        self.gateway_group, True)
            loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(request.location)
            if len(loc_grps_list) == 0:
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                         f"Invalid location {request.location}"
                self.logger.error(errmsg)
                return pb2.nsid_status(status=errno.ENODEV, error_message=errmsg)
            if request.anagrpid == 0:
                _, anagrp = \
                    self.rebalance.find_min_loaded_group_in_subsys(request.subsystem_nqn,
                                                                   loc_grps_list)
                request.anagrpid = anagrp
            else:
                # If an explicit load balancing group was passed, make sure it exists
                if request.anagrpid not in loc_grps_list:
                    self.logger.debug(f"Load balancing groups: {loc_grps_list}")
                    errmsg = f"Failure adding namespace {nsid_msg}to " \
                             f"{request.subsystem_nqn}: Load balancing group " \
                             f"{request.anagrpid} doesn't exist"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.ENODEV, error_message=errmsg)

            if request.nsid:
                ns = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                      request.nsid)
                if not ns.empty():
                    errmsg = f"Failure adding namespace, ID {request.nsid} is already in use"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=errno.EEXIST, error_message=errmsg)

            ns = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                  None, request.uuid)
            if not ns.empty():
                errmsg = f"Failure adding namespace, UUID {request.uuid} is already in use"
                self.logger.error(errmsg)
                return pb2.nsid_status(status=errno.EEXIST, error_message=errmsg)

        anagrp = request.anagrpid
        assert anagrp != 0, "Chosen load balancing group is 0"
        bdev_name = GatewayService.find_unique_bdev_name(request.uuid)
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if context:
                errmsg, ns_nqn = self.check_if_image_used(request.rbd_pool_name,
                                                          request.rbd_image_name,
                                                          request.rados_namespace_name,
                                                          request.uuid)
                if errmsg and ns_nqn:
                    if request.force:
                        self.logger.warning(f"{errmsg}, will continue as the \"force\" "
                                            f"argument was used")
                    else:
                        errmsg = f"{errmsg}, either delete the namespace or use the \"force\" " \
                                 f"argument,\nyou can find the offending namespace by using " \
                                 f"the \"namespace list\" CLI command on subsystem {ns_nqn}"
                        self.logger.error(errmsg)
                        return pb2.nsid_status(status=errno.EEXIST, error_message=errmsg)

            create_image = request.create_image
            if not context:
                create_image = False

            ret_bdev = self.create_bdev(anagrp, bdev_name, request.uuid, request.rbd_pool_name,
                                        request.rbd_data_pool_name,
                                        request.rbd_image_name, request.block_size, create_image,
                                        request.trash_image, request.size,
                                        request.disable_auto_resize, request.read_only,
                                        request.rados_namespace_name,
                                        decrypted_enc_entries, request.encryption_algorithm,
                                        context, peer_msg)
            if ret_bdev.status != 0:
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                         f"{ret_bdev.error_message}"
                self.logger.error(errmsg)
                # Delete the bdev unless there was one already there, just to be on the safe side
                if ret_bdev.status != errno.EEXIST:
                    ns_bdev = self.get_bdev_info(bdev_name)
                    if ns_bdev is not None:
                        try:
                            ret_del = self.delete_bdev(bdev_name, peer_msg=peer_msg)
                            self.logger.debug(f"delete_bdev({bdev_name}): {ret_del.status}")
                        except AssertionError:
                            self.logger.exception(
                                f"Got an assert while trying to delete bdev {bdev_name}")
                            raise
                        except Exception:
                            self.logger.exception(
                                f"Got exception while trying to delete bdev {bdev_name}")
                return pb2.nsid_status(status=ret_bdev.status, error_message=errmsg)

            # If we got here we asserted that ret_bdev.bdev_name == bdev_name

            ret_ns = self.create_namespace(request.subsystem_nqn, bdev_name,
                                           request.nsid, anagrp, request.uuid,
                                           not request.no_auto_visible,
                                           ret_bdev.rbd_pool, request.rbd_data_pool_name,
                                           ret_bdev.rbd_image_name,
                                           ret_bdev.rados_namespace_name,
                                           ret_bdev.trash_image, request.read_only,
                                           request.location, not request.disable_auto_resize,
                                           request.encryption_entries,
                                           request.encryption_algorithm,
                                           context)
            if ret_ns.status == 0 and request.nsid and ret_ns.nsid != request.nsid:
                errmsg = f"Returned ID {ret_ns.nsid} differs from requested one {request.nsid}"
                self.logger.error(errmsg)
                ret_ns.status = errno.ENODEV
                ret_ns.error_message = errmsg

            if ret_ns.status != 0:
                try:
                    ret_del = self.delete_bdev(bdev_name, peer_msg=peer_msg)
                    if ret_del.status != 0:
                        self.logger.warning(f"Failure {ret_del.status} deleting bdev "
                                            f"{bdev_name}: {ret_del.error_message}")
                except AssertionError:
                    self.logger.exception(f"Got an assert while trying to delete bdev {bdev_name}")
                    raise
                except Exception:
                    self.logger.exception(f"Got exception while trying to delete bdev {bdev_name}")
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: " \
                         f"{ret_ns.error_message}"
                self.logger.error(errmsg)
                if ret_bdev.trash_image:
                    self.delete_rbd_image(ret_bdev.rbd_pool, ret_bdev.rbd_image_name,
                                          ret_bdev.rados_namespace_name)
                return pb2.nsid_status(status=ret_ns.status, error_message=errmsg)

            if context:
                # Update gateway state
                request.nsid = ret_ns.nsid
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, ret_ns.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting namespace {nsid_msg}on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    try:
                        ret_del = self.delete_bdev(bdev_name, peer_msg=peer_msg)
                    except Exception:
                        pass
                    if ret_bdev.trash_image:
                        self.delete_rbd_image(ret_bdev.rbd_pool, ret_bdev.rbd_image_name,
                                              ret_bdev.rados_namespace_name)
                    return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

            img_id = ImageIdentification(self.gateway_group,
                                         request.subsystem_nqn,
                                         request.uuid,
                                         self.fsid)
            self.set_image_identification(request.rbd_pool_name,
                                          request.rbd_image_name,
                                          request.rados_namespace_name,
                                          img_id)

        return pb2.nsid_status(status=0, error_message=os.strerror(0), nsid=ret_ns.nsid)

    def namespace_add(self, request, context=None):
        """Adds a namespace to a subsystem."""
        err_prefix = f"Failure adding namespace to {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_add_safe, request, context, err_prefix)

    def namespace_change_load_balancing_group_safe(self, request, context):
        """Changes a namespace load balancing group."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling namespace_change_load_balancing_group_safe()"
        grps_list = []
        peer_msg = self.get_peer_message(context)
        change_lb_group_failure_prefix = f"Failure changing load balancing group for namespace " \
                                         f"with ID {request.nsid} in {request.subsystem_nqn}"
        auto_lb_msg = "auto" if request.auto_lb_logic else "manual"
        self.logger.info(f"Received {auto_lb_msg} request to change load balancing group for "
                         f"namespace with ID {request.nsid} in {request.subsystem_nqn} to "
                         f"{request.anagrpid}, context: {context}{peer_msg}")

        if not request.subsystem_nqn:
            errmsg = "Failure changing load balancing group for namespace, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.nsid:
            errmsg = f"Failure changing load balancing group for namespace in " \
                     f"{request.subsystem_nqn}: No namespace ID was given"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)

        if find_ret.empty():
            errmsg = f"{change_lb_group_failure_prefix}: Namespace not found"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        anagrpid = find_ret.anagrpid

        # below checks are legal only if command is initiated by local cli or is sent from
        # the local rebalance logic.
        if context:
            grps_list = self.ceph_utils.get_number_created_gateways(
                self.gateway_pool, self.gateway_group, False)
            if request.anagrpid not in grps_list:
                self.logger.debug(f"Load balancing groups: {grps_list}")
                errmsg = f"{change_lb_group_failure_prefix}: Load balancing group " \
                         f"{request.anagrpid} doesn't exist"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ns_entry = None
            ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, request.nsid)
            if context:
                # notice that the local state might not be up to date in case we're in the
                # middle of update() but as the context is not None, we are not in an update(),
                # the omap lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                except Exception:
                    errmsg = f"{change_lb_group_failure_prefix}: Can't find entry for " \
                             f"namespace {request.nsid} in {request.subsystem_nqn}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
                assert ns_entry, "Namespace entry is empty"
                if not request.auto_lb_logic:
                    anagrp = ns_entry.anagrpid
                    gw_id = self.ceph_utils.get_gw_id_owner_ana_group(
                        self.gateway_pool, self.gateway_group, anagrp)
                    self.logger.debug(f"Load balancing group of ns#{request.nsid} - {anagrp} is "
                                      f"owned by gateway {gw_id}, self.name is {self.gateway_name}")
                    if gw_id is not None and self.gateway_name != gw_id:
                        errmsg = f"Load balancing group of ns#{request.nsid} - {anagrp} is " \
                                 f"owned by gateway {gw_id}, try running the command from " \
                                 f"there.\nThis gateway name is {self.gateway_name}"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=errno.EEXIST, error_message=errmsg)
            elif not anagrpid:
                # we shouldn't get a zero group id
                self.logger.error("We read a load balancing group id of 0 from the local list. "
                                  "Will try to get it from OMAP")
                # we are in the middle of an update, so we can't rely on the local state
                state = self.gateway_state.omap.get_state()
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                except Exception:
                    self.logger.exception(f"Can't find entry for "
                                          f"namespace {request.nsid} in "
                                          f"{request.subsystem_nqn}")
                    errmsg = f"{change_lb_group_failure_prefix}: Can't find " \
                             f"namespace entry in OMAP file"
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

            # either we're in an update or not, we need to deal with a 0 group id
            if not anagrpid:
                assert ns_entry and ns_entry.anagrpid != 0, "Couldn't get load " \
                                                            "balancing group id"
                anagrpid = ns_entry.anagrpid
                self.logger.debug(f"Read a load balancing group of {anagrpid} from the OMAP file")

            try:
                ret = self.spdk_rpc_client.nvmf_subsystem_set_ns_ana_group(
                    nqn=request.subsystem_nqn,
                    nsid=request.nsid,
                    anagrpid=request.anagrpid,
                )
                self.logger.debug(f"nvmf_subsystem_set_ns_ana_group: {ret}")
            except Exception as ex:
                errmsg = f"{change_lb_group_failure_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{change_lb_group_failure_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(change_lb_group_failure_prefix)
                return pb2.req_status(status=errno.EINVAL,
                                      error_message=change_lb_group_failure_prefix)

            # change LB success - need to update the data structures
            self.ana_grp_ns_load[anagrpid] -= 1   # decrease loading of previous "old" ana group
            try:
                self.ana_grp_subs_load[anagrpid][request.subsystem_nqn] -= 1
            except Exception as ex:
                self.logger.error(f"entry does not exist in ana_grp_subs_load array: ANA grp:"
                                  f" {anagrpid} nqn: {request.subsystem_nqn} {ex} ")
                assert False, "ana_grp_subs_load dictionary should be initialized"

            self.logger.debug(f"updated load in grp {anagrpid} = {self.ana_grp_ns_load[anagrpid]} ")
            self.ana_grp_ns_load[request.anagrpid] += 1
            if request.anagrpid in self.ana_grp_subs_load:
                if request.subsystem_nqn in self.ana_grp_subs_load[request.anagrpid]:
                    self.ana_grp_subs_load[request.anagrpid][request.subsystem_nqn] += 1
                else:
                    self.ana_grp_subs_load[request.anagrpid][request.subsystem_nqn] = 1
            else:
                self.ana_grp_subs_load[request.anagrpid][request.subsystem_nqn] = 1
            self.logger.debug(f"updated load in grp {request.anagrpid} = "
                              f"{self.ana_grp_ns_load[request.anagrpid]} ")
            # here update find_ret.set_ana_group_id(request.anagrpid)
            if not find_ret.empty():
                find_ret.set_ana_group_id(request.anagrpid)

            if context:
                assert ns_entry, "Namespace entry is None for non-update call"
                # Update gateway state
                try:
                    ns_entry.anagrpid = request.anagrpid
                    json_req = json_format.MessageToJson(
                        ns_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn,
                                                     request.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting namespace load balancing group for namespace " \
                             f"with ID {request.nsid} in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_change_load_balancing_group(self, request, context=None):
        """Changes a namespace load balancing group."""
        err_prefix = f"Failure changing load balancing group for namespace " \
                     f"{request.nsid} in {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_change_load_balancing_group_safe,
                                          request, context, err_prefix)

    def subsystem_has_connections(self, subsys: str) -> bool:
        assert subsys, "Subsystem NQN is empty"
        assert self.rpc_lock.locked(), "RPC is unlocked when calling subsystem_has_connections()"
        try:
            ctrl_ret = self.spdk_rpc_client.nvmf_subsystem_get_controllers(nqn=subsys)
        except Exception:
            return False
        if not ctrl_ret:
            return False
        return True

    def namespace_change_visibility_safe(self, request, context):
        """Changes namespace visibility."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling namespace_change_visibility_safe()"
        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure changing visibility for namespace {request.nsid} " \
                         f"in {request.subsystem_nqn}"
        vis_txt = "\"visible to all hosts\"" if request.auto_visible else "\"visible " \
                  "to selected hosts\""
        self.logger.info(f"Received request to change the visibility of namespace {request.nsid} "
                         f"in {request.subsystem_nqn} to {vis_txt}, force: {request.force}, "
                         f"context: {context}{peer_msg}")
        ns_host_to_remove_from_omap = []

        if not request.subsystem_nqn:
            errmsg = "Failure changing visibility for namespace, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.nsid:
            errmsg = f"Failure changing visibility for namespace in {request.subsystem_nqn}: " \
                     f"No namespace ID was given"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if find_ret.host_count() > 0 and request.auto_visible:
            if request.force:
                self.logger.warning(f"Asking to change visibility of namespace {request.nsid} "
                                    f"in {request.subsystem_nqn} to be visible to all hosts "
                                    f"while there are already hosts added to it. Will continue "
                                    f"as the \"force\" parameter was used but these hosts "
                                    f"will be removed from the namespace.")
            else:
                errmsg = f"{failure_prefix}: Asking to change visibility of namespace to be " \
                         f"visible to all hosts while there are already hosts added to it. " \
                         f"Either remove these hosts or use the \"force\" parameter"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EBUSY, error_message=errmsg)

        if self.subsystem_has_connections(request.subsystem_nqn):
            if request.force:
                self.logger.warning(f"Asking to change visibility of namespace {request.nsid} "
                                    f"in {request.subsystem_nqn} while there are active "
                                    f"connections on the subsystem, will continue as the "
                                    f"\"force\" parameter was used.")
            else:
                errmsg = f"{failure_prefix}: Asking to change visibility of namespace while " \
                         f"there are active connections on the subsystem, please disconnect " \
                         f"them or use the \"force\" parameter."
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EBUSY, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ns_entry = None
            if context:
                # notice that the local state might not be up to date in case we're in the middle
                # of update() but as the context is not None, we are not in an update(), the OMAP
                # lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, request.nsid)
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                    if ns_entry.no_auto_visible == (not request.auto_visible):
                        self.logger.warning(f"No change to namespace {request.nsid} in "
                                            f"{request.subsystem_nqn} visibility, nothing to do")
                        return pb2.req_status(status=0, error_message=os.strerror(0))
                except Exception:
                    errmsg = f"{failure_prefix}: Can't find entry for namespace"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            try:
                ret = self.spdk_rpc_client.nvmf_subsystem_set_ns_visibility(
                    nqn=request.subsystem_nqn,
                    nsid=request.nsid,
                    auto_visible=request.auto_visible,
                )
                self.logger.debug(f"nvmf_subsystem_set_ns_visible: {ret}")
                if request.force and find_ret.host_count() > 0 and request.auto_visible:
                    ns_host_to_remove_from_omap = find_ret.host_list.copy()
                    self.logger.warning(f"Removing all hosts added to namespace {request.nsid} in "
                                        f"{request.subsystem_nqn} as it was set to be "
                                        f"visible to all hosts")
                    find_ret.remove_all_hosts()
                find_ret.set_visibility(request.auto_visible)
            except Exception as ex:
                errmsg = f"{failure_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{failure_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(failure_prefix)
                return pb2.req_status(status=errno.EINVAL, error_message=failure_prefix)

            if context:
                assert ns_entry, "Namespace entry is None for non-update call"
                # Update gateway state
                try:
                    ns_entry.no_auto_visible = not request.auto_visible
                    json_req = json_format.MessageToJson(
                        ns_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, request.nsid, json_req)

                    # If we set the namespace to be visible, we need to remote its hosts
                    if len(ns_host_to_remove_from_omap) > 0:
                        assert request.auto_visible, "We only remove hosts for auto visible"
                        assert request.force, "Must use \"force\" to set a namespace " \
                                              "with hosts visible"
                    for host in ns_host_to_remove_from_omap:
                        try:
                            self.gateway_state.remove_namespace_host(request.subsystem_nqn,
                                                                     request.nsid, host)
                        except KeyError:
                            pass
                except Exception as ex:
                    errmsg = f"Error persisting visibility change for namespace " \
                             f"{request.nsid} in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_change_visibility(self, request, context=None):
        """Changes a namespace visibility."""
        err_prefix = f"Failure changing visibility for namespace {request.nsid} " \
                     f"in {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_change_visibility_safe,
                                          request, context, err_prefix)

    def namespace_change_location_safe(self, request, context):
        """Changes namespace location."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling namespace_change_location()"
        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure changing location for namespace {request.nsid} " \
                         f"in {request.subsystem_nqn}"
        self.logger.info(f"Received request to change the location of namespace {request.nsid} "
                         f"in {request.subsystem_nqn} to \"{request.location}\", "
                         f"context: {context}{peer_msg}")

        if not request.subsystem_nqn:
            errmsg = "Failure changing location for namespace, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.nsid:
            errmsg = f"Failure changing location for namespace in {request.subsystem_nqn}: " \
                     f"No namespace ID was given"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        self.ceph_utils.get_number_created_gateways(self.gateway_pool,
                                                    self.gateway_group, False)
        loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(request.location)
        if len(loc_grps_list) == 0:
            errmsg = (f"Failure change namespace location, ID {request.nsid}"
                      f" invalid location {request.location}")
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.ENODEV, error_message=errmsg)
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ns_entry = None
            if context:
                # notice that the local state might not be up to date in case we're in the middle
                # of update() but as the context is not None, we are not in an update(), the OMAP
                # lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, request.nsid)
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                    if ns_entry.location == request.location:
                        self.logger.warning(f"No change to namespace {request.nsid} in "
                                            f"{request.subsystem_nqn} location, nothing to do")
                        return pb2.req_status(status=0, error_message=os.strerror(0))
                except Exception:
                    errmsg = f"{failure_prefix}: Can't find entry for namespace"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

                assert ns_entry, "Namespace entry is None for non-update call"
                # Update gateway state
                try:
                    ns_entry.location = request.location
                    json_req = json_format.MessageToJson(
                        ns_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, request.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting location change for namespace " \
                             f"{request.nsid} in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # this should be done also on update
        find_ret.set_location(request.location)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_change_location(self, request, context=None):
        """Changes a namespace location."""
        err_prefix = f"Failure changing location for namespace {request.nsid} " \
                     f"in {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_change_location_safe,
                                          request, context, err_prefix)

    def namespace_set_rbd_trash_image_safe(self, request, context=None):
        """Changes RBD trash image flag for a namespace."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling namespace_set_rbd_trash_image_safe()"
        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure setting RBD trash image flag for namespace {request.nsid} " \
                         f"in {request.subsystem_nqn}"
        trash_txt = "trash on namespace deletion\""
        if not request.trash_image:
            trash_txt = "do not " + trash_txt
        trash_txt = "\"" + trash_txt
        self.logger.info(f"Received request to set the RBD trash image flag of namespace "
                         f"{request.nsid} in {request.subsystem_nqn} to {trash_txt}, "
                         f"context: {context}{peer_msg}")

        if not request.nsid:
            errmsg = "Failure setting RBD trash image flag for namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure setting RBD trash image flag for namespace {request.nsid}, " \
                     f"missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"Failure setting RBD trash image flag for namespace {request.nsid}, " \
                     f"can't find subsystem \"{request.subsystem_nqn}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if request.trash_image:
            if find_ret.trash_image:
                self.logger.warning(f"Namespace {request.nsid} in {request.subsystem_nqn} already"
                                    f" has the RBD trash image flag set, nothing to do")
                return pb2.req_status(status=0, error_message=os.strerror(0))
        else:
            if not find_ret.trash_image:
                self.logger.warning(f"Namespace {request.nsid} in {request.subsystem_nqn} already"
                                    f" has the RBD trash image flag reset, nothing to do")
                return pb2.req_status(status=0, error_message=os.strerror(0))

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ns_entry = None
            if context:
                # notice that the local state might not be up to date in case we're in the middle
                # of update() but as the context is not None, we are not in an update(), the OMAP
                # lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, request.nsid)
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                    if ns_entry.trash_image == request.trash_image:
                        self.logger.warning(f"Namespace {request.nsid} in {request.subsystem_nqn} "
                                            f"already has the RBD trash image flag set to the "
                                            f"requested value, nothing to do")
                        # We should have caught this earlier, the local flag is not up to date
                        find_ret.trash_image = request.trash_image
                        return pb2.req_status(status=0, error_message=os.strerror(0))
                except Exception:
                    errmsg = f"{failure_prefix}: Can't find entry for namespace"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

                assert ns_entry, "Namespace entry is None"
                # Update gateway state
                try:
                    ns_entry.trash_image = request.trash_image
                    json_req = json_format.MessageToJson(
                        ns_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, request.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting change for RBD trash image flag of namespace " \
                             f"{request.nsid} in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # this should be done also on update
        find_ret.trash_image = request.trash_image

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_set_rbd_trash_image(self, request, context=None):
        """Changes RBD trash image flag for a namespace."""
        err_prefix = f"Failure setting RBD trash image flag for namespace {request.nsid} " \
                     f"in {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_set_rbd_trash_image_safe,
                                          request, context, err_prefix)

    def _set_image_auto_resize(self, rbd_pool: str, rbd_image: str,
                               rados_namespace_name: str, value: bool) -> None:
        if value:
            self.ceph_utils.remove_image_metadata(rbd_pool, rbd_image,
                                                  rados_namespace_name,
                                                  CephUtils.METADATA_KEY_AUTO_RESIZE)
        else:
            self.ceph_utils.set_image_metadata(rbd_pool, rbd_image,
                                               rados_namespace_name,
                                               CephUtils.METADATA_KEY_AUTO_RESIZE,
                                               CephUtils.METADATA_VALUE_NO_AUTO_RESIZE)

    def _is_auto_resize_disabled_for_image(self, rbd_pool: str, rbd_image: str,
                                           rados_namespace_name: str) -> bool:
        try:
            auto_resize_metadata = self.ceph_utils.get_image_metadata(
                rbd_pool, rbd_image, rados_namespace_name, CephUtils.METADATA_KEY_AUTO_RESIZE)
            if auto_resize_metadata is None:
                return False
            return auto_resize_metadata.lower() == CephUtils.METADATA_VALUE_NO_AUTO_RESIZE.lower()
        except KeyError:
            pass
        except Exception:
            image_path = f"{rbd_pool}/{rbd_image}" if not rados_namespace_name else \
                f"{rbd_pool}/{rados_namespace_name}/{rbd_image}"
            self.logger.exception(f"Error getting auto resize flag for image "
                                  f"{image_path}")
        return False

    def namespace_set_auto_resize_safe(self, request, context=None):
        """Sets auto resie flag for a namespace."""

        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure setting auto resize flag for namespace {request.nsid} " \
                         f"in {request.subsystem_nqn}"
        auto_resize_txt = "auto resize namespace\""
        if not request.auto_resize:
            auto_resize_txt = "do not " + auto_resize_txt
        auto_resize_txt = "\"" + auto_resize_txt
        self.logger.info(f"Received request to set the auto resize flag of namespace "
                         f"{request.nsid} in {request.subsystem_nqn} to {auto_resize_txt}, "
                         f"context: {context}{peer_msg}")

        if not request.nsid:
            errmsg = "Failure setting auto resize flag for namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure setting auto resize flag for namespace {request.nsid}, " \
                     f"missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"Failure setting auto resize flag for namespace {request.nsid}, " \
                     f"can't find subsystem \"{request.subsystem_nqn}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if not find_ret.pool:
            errmsg = f"{failure_prefix}: Can't find namespace RBD pool"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if not find_ret.image:
            errmsg = f"{failure_prefix}: Can't find namespace RBD image"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if context:
            # As all gateways use the same RBD image, do it only once
            try:
                self._set_image_auto_resize(find_ret.pool, find_ret.image,
                                            find_ret.rados_namespace_name, request.auto_resize)
            except Exception:
                errmsg = f"Error setting auto resize flag for image " \
                         f"{find_ret.pool}/{find_ret.image}"
                self.logger.exception(f"{errmsg}")
                return pb2.req_status(status=errno.EIO, error_message=errmsg)

            ns_entry = None
            ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, request.nsid)
            omap_lock = self.omap_lock.get_omap_lock_to_use(context)
            with omap_lock:

                if request.auto_resize:
                    # If auto resize is enabled, we no need to send explicit refresh size requests
                    try:
                        self.gateway_state.remove_namespace_refresh_size(request.subsystem_nqn,
                                                                         str(request.nsid))
                    except Exception:
                        pass

                # notice that the local state might not be up to date in case we're in the middle
                # of update() but as the context is not None, we are not in an update(), the OMAP
                # lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                try:
                    state_ns = state[ns_key]
                    ns_entry = json_format.Parse(state_ns, pb2.namespace_add_req(),
                                                 ignore_unknown_fields=True)
                except Exception:
                    errmsg = f"{failure_prefix}: Can't find entry for namespace"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

                assert ns_entry, "Namespace entry is None for non-update call"
                # Update gateway state
                try:
                    ns_entry.disable_auto_resize = not request.auto_resize
                    json_req = json_format.MessageToJson(
                        ns_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, request.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting auto resize flag change for namespace " \
                             f"{request.nsid} in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        find_ret.auto_resize = request.auto_resize

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_set_auto_resize(self, request, context=None):
        """Sets auto resie flag for a namespace."""
        err_prefix = f"Failure setting auto resize flag for namespace {request.nsid} " \
                     f"in {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_set_auto_resize_safe,
                                          request, context, err_prefix)

    def remove_namespace_from_state(self, nqn, nsid, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        assert context is None or self.omap_lock.write_locked_by_me(), \
            f"OMAP is unlocked when calling remove_namespace_from_state()\n" \
            f"in thread: {threading.get_native_id()}. Locked by: " \
            f"{self.omap_lock.locked_by}, with cookie: {self.omap_lock.lock_cookie}, " \
            f"locked: {self.omap_lock.is_exclusively_locked}"

        # Update gateway state
        try:
            self.gateway_state.remove_namespace_qos(nqn, str(nsid))
        except Exception:
            pass
        try:
            self.gateway_state.remove_namespace_refresh_size(nqn, str(nsid))
        except Exception:
            pass
        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(nqn, nsid)
        for hst in find_ret.host_list:
            try:
                self.gateway_state.remove_namespace_host(nqn, str(nsid), hst)
            except Exception:
                pass
        try:
            self.gateway_state.remove_namespace_lb_group(nqn, str(nsid))
        except Exception:
            pass
        try:
            self.gateway_state.remove_namespace(nqn, str(nsid))
        except Exception as ex:
            errmsg = f"Error persisting removing of namespace {nsid} from {nqn}"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def remove_namespace(self, subsystem_nqn, nsid, context):
        """Removes a namespace from a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling remove_namespace()"
        assert context is None or self.omap_lock.write_locked_by_me(), \
            f"OMAP is unlocked when calling remove_namespace()\n" \
            f"in thread: {threading.get_native_id()}. Locked by: " \
            f"{self.omap_lock.locked_by}, with cookie: {self.omap_lock.lock_cookie}, " \
            f"locked: {self.omap_lock.is_exclusively_locked}"

        peer_msg = self.get_peer_message(context)
        namespace_failure_prefix = f"Failure removing namespace {nsid} from {subsystem_nqn}"
        self.logger.info(f"Received request to remove namespace {nsid} from "
                         f"{subsystem_nqn}{peer_msg}")

        if GatewayUtils.is_discovery_nqn(subsystem_nqn):
            errmsg = f"{namespace_failure_prefix}: Can't remove a namespace from " \
                     f"a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        try:
            ret = self.spdk_rpc_client.nvmf_subsystem_remove_ns(
                nqn=subsystem_nqn,
                nsid=nsid,
            )
            self.logger.debug(f"remove_namespace {nsid}: {ret}")
            anagrpid = self.subsystem_nsid_bdev_and_uuid.get_ana_group_id_by_nsid_subsys(
                subsystem_nqn, nsid)
            self.ana_grp_ns_load[anagrpid] -= 1
            self.ana_grp_subs_load[anagrpid][subsystem_nqn] -= 1
        except Exception as ex:
            self.logger.exception(namespace_failure_prefix)
            errmsg = f"{namespace_failure_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{namespace_failure_prefix}: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not ret:
            self.logger.error(namespace_failure_prefix)
            return pb2.req_status(status=errno.EINVAL, error_message=namespace_failure_prefix)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def get_bdev_info(self, bdev_name):
        """Get bdev info"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling get_bdev_info()"
        ret_bdev = None
        try:
            bdevs = self.spdk_rpc_client.bdev_get_bdevs(name=bdev_name)
            self.logger.debug(f"bdev_get_bdevs: {bdevs}")
            if (len(bdevs) > 1):
                self.logger.warning(f"Got {len(bdevs)} bdevs for bdev name {bdev_name}, "
                                    f"will use the first one")
            ret_bdev = bdevs[0]
        except Exception:
            self.logger.exception(f"Got exception while getting bdev {bdev_name} info")

        return ret_bdev

    def list_namespaces(self, request, context=None):
        """List namespaces."""

        peer_msg = self.get_peer_message(context)
        if not request.nsid:
            if request.uuid:
                nsid_msg = f"namespace with UUID {request.uuid}"
            else:
                nsid_msg = "all namespaces"
        else:
            if request.uuid:
                nsid_msg = f"namespace with ID {request.nsid} and UUID {request.uuid}"
            else:
                nsid_msg = f"namespace with ID {request.nsid}"
        self.logger.info(f"Received request to list {nsid_msg} for {request.subsystem}, "
                         f"context: {context}{peer_msg}")

        if not request.subsystem:
            request.subsystem = GatewayUtils.ALL_SUBSYSTEMS

        with self.rpc_lock:
            try:
                if request.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                    ret = self.spdk_rpc_client.nvmf_get_subsystems()
                else:
                    ret = self.spdk_rpc_client.nvmf_get_subsystems(nqn=request.subsystem)
                self.logger.debug(f"list_namespaces: {ret}")
            except Exception as ex:
                errmsg = "Failure listing namespaces"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure listing namespaces: {resp['message']}"
                return pb2.namespaces_info(status=status, error_message=errmsg,
                                           subsystem_nqn=request.subsystem, namespaces=[])

        if not ret:
            ret = []
        namespaces = []
        for s in ret:
            try:
                subsys_nqn = s["nqn"]
                if request.subsystem != GatewayUtils.ALL_SUBSYSTEMS:
                    if subsys_nqn != request.subsystem:
                        self.logger.warning(f'Got subsystem {subsys_nqn} instead of '
                                            f'{request.subsystem}, ignore')
                        continue
                try:
                    ns_list = s["namespaces"]
                except Exception:
                    ns_list = []
                    pass
                if not ns_list:
                    self.subsystem_nsid_bdev_and_uuid.remove_namespace(subsys_nqn)
                for n in ns_list:
                    nsid = n["nsid"]
                    bdev_name = n["bdev_name"]
                    if request.nsid and request.nsid != n["nsid"]:
                        self.logger.debug(f'Filter out namespace {n["nsid"]} which is '
                                          f'different than requested nsid {request.nsid}')
                        continue
                    if request.uuid:
                        if not NamespaceInfo.are_uuids_equal(request.uuid, n["uuid"]):
                            self.logger.debug(f'Filter out namespace with UUID {n["uuid"]} which '
                                              f'is different than requested UUID {request.uuid}')
                            continue
                    lb_group = 0
                    try:
                        lb_group = n["anagrpid"]
                    except KeyError:
                        pass
                    find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(subsys_nqn,
                                                                                nsid)
                    lb_group_configured = 0
                    cluster_name = None
                    was_image_shrunk = False
                    if find_ret.empty():
                        self.logger.warning(f"Can't find info of namesapce {nsid} in "
                                            f"{subsys_nqn}. Some fields value "
                                            f"will be inaccurate")
                    else:
                        was_image_shrunk = find_ret.was_image_shrunk()
                        lb_group_configured = find_ret.anagrpid
                        try:
                            cluster_name = self.bdev_cluster[find_ret.bdev]
                        except KeyError:
                            cluster_name = None

                    one_ns = pb2.namespace_cli(nsid=nsid,
                                               bdev_name=bdev_name,
                                               uuid=n["uuid"],
                                               load_balancing_group=lb_group,
                                               auto_visible=find_ret.auto_visible,
                                               hosts=find_ret.host_list,
                                               ns_subsystem_nqn=subsys_nqn,
                                               trash_image=find_ret.trash_image,
                                               read_only=find_ret.read_only,
                                               configured_load_balancing_group=lb_group_configured,
                                               cluster_name=cluster_name,
                                               image_was_shrunk=was_image_shrunk,
                                               rbd_data_pool_name=find_ret.data_pool,
                                               location=find_ret.location,
                                               encryption_entries=find_ret.encryption_entries,
                                               encryption_algorithm=find_ret.encryption_algorithm)
                    with self.rpc_lock:
                        ns_bdev = self.get_bdev_info(bdev_name)
                    if ns_bdev is None:
                        self.logger.warning(f"Can't find namespace's bdev {bdev_name}, "
                                            f"will not list bdev's information")
                    else:
                        image_size = None
                        try:
                            drv_specific_info = ns_bdev["driver_specific"]
                            rbd_info = drv_specific_info["rbd"]
                            one_ns.rbd_image_name = rbd_info["rbd_name"]
                            one_ns.rbd_pool_name = rbd_info["pool_name"]
                            one_ns.rados_namespace_name = rbd_info["namespace_name"]
                            one_ns.block_size = ns_bdev["block_size"]
                            image_size = ns_bdev["block_size"] * ns_bdev["num_blocks"]
                            assigned_limits = ns_bdev["assigned_rate_limits"]
                            one_ns.rw_ios_per_second = assigned_limits["rw_ios_per_sec"]
                            one_ns.rw_mbytes_per_second = assigned_limits["rw_mbytes_per_sec"]
                            one_ns.r_mbytes_per_second = assigned_limits["r_mbytes_per_sec"]
                            one_ns.w_mbytes_per_second = assigned_limits["w_mbytes_per_sec"]
                        except KeyError as err:
                            self.logger.warning(f"Key {err} is not found, will not list "
                                                f"bdev's information")
                            pass
                        except Exception:
                            self.logger.exception(f"{ns_bdev=} parse error")
                            pass
                        if was_image_shrunk and one_ns.rbd_pool_name and one_ns.rbd_image_name:
                            shrunk_image_size = None
                            try:
                                shrunk_image_size = self.ceph_utils.get_image_size(
                                    one_ns.rbd_pool_name, one_ns.rbd_image_name,
                                    one_ns.rados_namespace_name)
                            except Exception:
                                self.logger.exception(f"error getting size of "
                                                      f"{one_ns.rbd_pool_name}/"
                                                      f"{one_ns.rbd_image_name}")
                                pass
                            if shrunk_image_size is not None:
                                image_size = shrunk_image_size
                        if image_size is not None:
                            one_ns.rbd_image_size = image_size
                        one_ns.disable_auto_resize = self._is_auto_resize_disabled_for_image(
                            one_ns.rbd_pool_name, one_ns.rbd_image_name,
                            one_ns.rados_namespace_name)
                    namespaces.append(one_ns)
                if request.subsystem != GatewayUtils.ALL_SUBSYSTEMS:
                    break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return pb2.namespaces_info(status=0,
                                   error_message=os.strerror(0),
                                   subsystem_nqn=request.subsystem,
                                   namespaces=namespaces)

    def list_namespaces_io_stats(self, request, context=None):
        """Get namespaces IO stats."""
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to list IO stats for namespaces with "
                         f"nsid: {request.nsid}, subsystem: {request.subsystem_nqn}, "
                         f"context: {context}{peer_msg}")
        failure_prefix = "Failure listing IO stats for namespaces"
        if (request.nsid):
            failure_prefix += f" with ID {request.nsid}"
        if (request.subsystem_nqn):
            failure_prefix += f" on subsystem {request.subsystem_nqn}"

        if (request.subsystem_nqn and not request.nsid):
            errmsg = "Failure getting IO stats for namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.list_namespaces_io_stats_info(status=errno.EINVAL, error_message=errmsg)

        if (request.nsid and not request.subsystem_nqn):
            errmsg = f"Failure getting IO stats for namespace {request.nsid}, " \
                     "missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.list_namespaces_io_stats_info(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn and (request.subsystem_nqn not in self.subsys_serial):
            errmsg = f"Failure getting IO stats for namespace {request.nsid}, can't find " \
                     f"subsystem \"{request.subsystem_nqn}\""
            self.logger.error(errmsg)
            return pb2.list_namespaces_io_stats_info(status=errno.ENOENT, error_message=errmsg)

        target_bdev_name = None
        with self.rpc_lock:
            if request.nsid:
                find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
                    request.subsystem_nqn, request.nsid)
                if find_ret.empty():
                    errmsg = f"{failure_prefix}: Can't find namespace"
                    self.logger.error(errmsg)
                    return pb2.list_namespaces_io_stats_info(status=errno.ENODEV,
                                                             error_message=errmsg)
                target_bdev_name = find_ret.bdev
                if not target_bdev_name:
                    errmsg = f"{failure_prefix}: Can't find associated block device"
                    self.logger.error(errmsg)
                    return pb2.list_namespaces_io_stats_info(status=errno.ENODEV,
                                                             error_message=errmsg)

            try:
                ret = self.spdk_rpc_client.bdev_get_iostat(name=target_bdev_name)
                self.logger.debug(f"get_bdev_iostat: {ret}")
            except Exception as ex:
                self.logger.exception(failure_prefix)
                errmsg = f"{failure_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{failure_prefix}: {resp['message']}"
                return pb2.list_namespaces_io_stats_info(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not ret:
            self.logger.error(failure_prefix)
            return pb2.list_namespaces_io_stats_info(status=errno.EINVAL,
                                                     error_message=failure_prefix)

        exmsg = ""
        try:
            bdevs = ret["bdevs"]
            if not bdevs:
                return pb2.list_namespaces_io_stats_info(
                    status=errno.ENODEV,
                    error_message=f"{failure_prefix}: No associated block device found")

            if target_bdev_name and len(bdevs) > 1:
                self.logger.warning("More than one associated block device found for namespace")

            bdev_iostats = []
            for bdev in bdevs:
                io_errs = []
                try:
                    io_error = bdev["io_error"]
                    for err_name in io_error.keys():
                        one_error = pb2.namespace_io_error(name=err_name, value=io_error[err_name])
                        io_errs.append(one_error)
                except Exception:
                    self.logger.exception("failure getting io errors")
                io_stats = pb2.bdev_io_stats_info(
                    bdev_name=bdev["name"],
                    bytes_read=bdev["bytes_read"],
                    num_read_ops=bdev["num_read_ops"],
                    bytes_written=bdev["bytes_written"],
                    num_write_ops=bdev["num_write_ops"],
                    bytes_unmapped=bdev["bytes_unmapped"],
                    num_unmap_ops=bdev["num_unmap_ops"],
                    bytes_copied=bdev["bytes_copied"],
                    num_copy_ops=bdev["num_copy_ops"],
                    read_latency_ticks=bdev["read_latency_ticks"],
                    max_read_latency_ticks=bdev["max_read_latency_ticks"],
                    min_read_latency_ticks=bdev["min_read_latency_ticks"],
                    write_latency_ticks=bdev["write_latency_ticks"],
                    max_write_latency_ticks=bdev["max_write_latency_ticks"],
                    min_write_latency_ticks=bdev["min_write_latency_ticks"],
                    unmap_latency_ticks=bdev["unmap_latency_ticks"],
                    max_unmap_latency_ticks=bdev["max_unmap_latency_ticks"],
                    min_unmap_latency_ticks=bdev["min_unmap_latency_ticks"],
                    copy_latency_ticks=bdev["copy_latency_ticks"],
                    max_copy_latency_ticks=bdev["max_copy_latency_ticks"],
                    min_copy_latency_ticks=bdev["min_copy_latency_ticks"],
                    io_error=io_errs)
                bdev_iostats.append(io_stats)
            return pb2.list_namespaces_io_stats_info(
                status=0,
                error_message=os.strerror(0),
                tick_rate=ret["tick_rate"],
                ticks=ret["ticks"],
                namespaces=bdev_iostats
            )
        except Exception as ex:
            self.logger.exception("parse error")
            exmsg = str(ex)
            pass
        return pb2.list_namespaces_io_stats_info(status=errno.EINVAL,
                                                 error_message=f"{failure_prefix}: Error "
                                                 f"parsing returned stats:\n{exmsg}")

    def namespace_get_io_stats(self, request, context=None):
        """Get namespace's IO stats."""

        failure_prefix = f"Failure getting IO stats for namespace {request.nsid} " \
                         f"on {request.subsystem_nqn}"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get IO stats for namespace {request.nsid} on "
                         f"{request.subsystem_nqn}, context: {context}{peer_msg}")

        list_req = pb2.list_namespaces_io_stats_req(
            subsystem_nqn=request.subsystem_nqn,
            nsid=request.nsid
        )
        list_ret = self.list_namespaces_io_stats(list_req, context)

        if list_ret.status != 0:
            return pb2.namespace_io_stats_info(
                status=list_ret.status,
                error_message=list_ret.error_message
            )

        if not list_ret.namespaces:
            return pb2.namespace_io_stats_info(
                status=errno.ENODEV,
                error_message=f"{failure_prefix}: No namespace found"
            )

        if len(list_ret.namespaces) > 1:
            self.logger.warning("Found multiple devices for same ID, will use first one")
        bdev_stats = list_ret.namespaces[0]

        # get uuid
        with self.rpc_lock:
            find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
                request.subsystem_nqn, request.nsid)
            if find_ret.empty():
                return pb2.namespace_io_stats_info(
                    status=errno.ENODEV,
                    error_message=f"{failure_prefix}: Can't find namespace"
                )
            uuid = find_ret.uuid

        return pb2.namespace_io_stats_info(
            status=0,
            error_message=os.strerror(0),
            subsystem_nqn=request.subsystem_nqn,
            nsid=request.nsid,
            uuid=uuid,
            bdev_name=bdev_stats.bdev_name,
            tick_rate=list_ret.tick_rate,
            ticks=list_ret.ticks,
            bytes_read=bdev_stats.bytes_read,
            num_read_ops=bdev_stats.num_read_ops,
            bytes_written=bdev_stats.bytes_written,
            num_write_ops=bdev_stats.num_write_ops,
            bytes_unmapped=bdev_stats.bytes_unmapped,
            num_unmap_ops=bdev_stats.num_unmap_ops,
            read_latency_ticks=bdev_stats.read_latency_ticks,
            max_read_latency_ticks=bdev_stats.max_read_latency_ticks,
            min_read_latency_ticks=bdev_stats.min_read_latency_ticks,
            write_latency_ticks=bdev_stats.write_latency_ticks,
            max_write_latency_ticks=bdev_stats.max_write_latency_ticks,
            min_write_latency_ticks=bdev_stats.min_write_latency_ticks,
            unmap_latency_ticks=bdev_stats.unmap_latency_ticks,
            max_unmap_latency_ticks=bdev_stats.max_unmap_latency_ticks,
            min_unmap_latency_ticks=bdev_stats.min_unmap_latency_ticks,
            copy_latency_ticks=bdev_stats.copy_latency_ticks,
            max_copy_latency_ticks=bdev_stats.max_copy_latency_ticks,
            min_copy_latency_ticks=bdev_stats.min_copy_latency_ticks,
            io_error=bdev_stats.io_error
        )

    @staticmethod
    def is_optional_field_in_message(request, fld):
        try:
            assert request.DESCRIPTOR.fields_by_name[fld].has_presence, \
                f"Field {fld} is not optional"
            if request.HasField(fld):
                return True
        except AssertionError:
            raise
        except Exception:
            pass
        return False

    def get_qos_limits_string(self, request):
        limits_to_set = ""
        if GatewayService.is_optional_field_in_message(request, "rw_ios_per_second"):
            limits_to_set += f" R/W IOs per second: {request.rw_ios_per_second}"
        if GatewayService.is_optional_field_in_message(request, "rw_mbytes_per_second"):
            limits_to_set += f" R/W megabytes per second: {request.rw_mbytes_per_second}"
        if GatewayService.is_optional_field_in_message(request, "r_mbytes_per_second"):
            limits_to_set += f" Read megabytes per second: {request.r_mbytes_per_second}"
        if GatewayService.is_optional_field_in_message(request, "w_mbytes_per_second"):
            limits_to_set += f" Write megabytes per second: {request.w_mbytes_per_second}"

        return limits_to_set

    def namespace_set_qos_limits_safe(self, request, context):
        """Set namespace's qos limits."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling namespace_set_qos_limits_safe()"
        max_mb_per_second = int(0xffffffffffffffff / (1024 * 1024))

        failure_prefix = f"Failure setting QOS limits for namespace {request.nsid} " \
                         f"on {request.subsystem_nqn}"
        peer_msg = self.get_peer_message(context)
        limits_to_set = self.get_qos_limits_string(request)
        self.logger.info(f"Received request to set QOS limits for namespace {request.nsid} "
                         f"on {request.subsystem_nqn},{limits_to_set}, force: {request.force}, "
                         f"context: {context}{peer_msg}")

        if not request.nsid:
            errmsg = "Failure setting QOS limits for namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure setting QOS limits for namespace {request.nsid}, " \
                     f"missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
            request.subsystem_nqn, request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        bdev_name = find_ret.bdev
        if not bdev_name:
            errmsg = f"{failure_prefix}: Can't find associated block device"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        try:
            if self.ceph_utils.were_image_qos_limits_changed(find_ret.pool, find_ret.image,
                                                             find_ret.rados_namespace_name):
                if request.force:
                    self.logger.warning(f"The QOS limits for image "
                                        f"{find_ret.pool}/{find_ret.image} were changed, will "
                                        f"continue as the \"--force\" parameter was used")
                else:
                    errmsg = f"{failure_prefix}: QOS limits were changed for RBD image " \
                             f"{find_ret.pool}/{find_ret.image}, use the \"--force\" parameter " \
                             f"to set limits anyway"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.EEXIST, error_message=errmsg)
        except Exception:
            self.logger.warning(f"Error trying to get the config attributes of image "
                                f"{find_ret.pool}/{find_ret.image}, can't check "
                                f"for QOS changes")
            pass

        if GatewayService.is_optional_field_in_message(request, "rw_ios_per_second"):
            if request.rw_ios_per_second % 1000 != 0:
                rounded_rate = int((request.rw_ios_per_second + 1000) / 1000) * 1000
                self.logger.warning(f"IOs per second {request.rw_ios_per_second} will be "
                                    f"rounded up to {rounded_rate}")

        if GatewayService.is_optional_field_in_message(request, "rw_mbytes_per_second"):
            if request.rw_mbytes_per_second > max_mb_per_second:
                self.logger.warning(f"Read/Write megabytes per second "
                                    f"{request.rw_mbytes_per_second} is too big, "
                                    f"it will be truncated to {max_mb_per_second}")

        if GatewayService.is_optional_field_in_message(request, "r_mbytes_per_second"):
            if request.r_mbytes_per_second > max_mb_per_second:
                self.logger.warning(f"Read megabytes per second "
                                    f"{request.r_mbytes_per_second} is too big, "
                                    f"it will be truncated to {max_mb_per_second}")

        if GatewayService.is_optional_field_in_message(request, "w_mbytes_per_second"):
            if request.w_mbytes_per_second > max_mb_per_second:
                self.logger.warning(f"Write megabytes per second "
                                    f"{request.w_mbytes_per_second} is too big, "
                                    f"it will be truncated to {max_mb_per_second}")

        set_qos_limits_args = {}
        set_qos_limits_args["name"] = bdev_name
        if GatewayService.is_optional_field_in_message(request, "rw_ios_per_second"):
            set_qos_limits_args["rw_ios_per_sec"] = request.rw_ios_per_second
        if GatewayService.is_optional_field_in_message(request, "rw_mbytes_per_second"):
            set_qos_limits_args["rw_mbytes_per_sec"] = request.rw_mbytes_per_second
        if GatewayService.is_optional_field_in_message(request, "r_mbytes_per_second"):
            set_qos_limits_args["r_mbytes_per_sec"] = request.r_mbytes_per_second
        if GatewayService.is_optional_field_in_message(request, "w_mbytes_per_second"):
            set_qos_limits_args["w_mbytes_per_sec"] = request.w_mbytes_per_second
        if self.spdk_qos_timeslice:
            set_qos_limits_args["timeslice_in_usecs"] = self.spdk_qos_timeslice

        ns_qos_entry = None
        ns_qos_key = GatewayState.build_namespace_qos_key(request.subsystem_nqn, request.nsid)
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if context:
                state = self.gateway_state.local.get_state()
                try:
                    state_ns_qos = state[ns_qos_key]
                    ns_qos_entry = json_format.Parse(state_ns_qos, pb2.namespace_set_qos_req(),
                                                     ignore_unknown_fields=True)
                except Exception:
                    self.logger.info(f"No previous QOS limits found, this is the first time the "
                                     f"limits are set for namespace {request.nsid} on "
                                     f"{request.subsystem_nqn}")

            # Merge current limits with previous ones, if exist
            if ns_qos_entry:
                assert context, "Shouldn't get here on an update"
                if not GatewayService.is_optional_field_in_message(request, "rw_ios_per_second"):
                    if GatewayService.is_optional_field_in_message(ns_qos_entry,
                                                                   "rw_ios_per_second"):
                        request.rw_ios_per_second = ns_qos_entry.rw_ios_per_second
                if not GatewayService.is_optional_field_in_message(request, "rw_mbytes_per_second"):
                    if GatewayService.is_optional_field_in_message(ns_qos_entry,
                                                                   "rw_mbytes_per_second"):
                        request.rw_mbytes_per_second = ns_qos_entry.rw_mbytes_per_second
                if not GatewayService.is_optional_field_in_message(request, "r_mbytes_per_second"):
                    if GatewayService.is_optional_field_in_message(ns_qos_entry,
                                                                   "r_mbytes_per_second"):
                        request.r_mbytes_per_second = ns_qos_entry.r_mbytes_per_second
                if not GatewayService.is_optional_field_in_message(request, "w_mbytes_per_second"):
                    if GatewayService.is_optional_field_in_message(ns_qos_entry,
                                                                   "w_mbytes_per_second"):
                        request.w_mbytes_per_second = ns_qos_entry.w_mbytes_per_second

            limits_to_set = self.get_qos_limits_string(request)
            self.logger.debug(f"After merging current QOS limits with previous ones for "
                              f"namespace {request.nsid} on {request.subsystem_nqn},"
                              f"{limits_to_set}")
            try:
                ret = self.spdk_rpc_client.bdev_set_qos_limit(**set_qos_limits_args)
                self.logger.debug(f"bdev_set_qos_limit {bdev_name}: {ret}")
            except Exception as ex:
                errmsg = f"Failure setting QOS limits for namespace {request.nsid} " \
                         f"on {request.subsystem_nqn}"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure setting namespace's QOS limits: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                errmsg = f"Failure setting QOS limits for namespace {request.nsid} " \
                         f"on {request.subsystem_nqn}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace_qos(request.subsystem_nqn,
                                                         request.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting namespace QOS settings {request.nsid} " \
                             f"on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_set_qos_limits(self, request, context=None):
        """Set namespace's qos limits."""
        err_prefix = f"Failure setting QOS limits for namespace {request.nsid} " \
                     f"on {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_set_qos_limits_safe,
                                          request, context, err_prefix)

    def namespace_resize_safe(self, request, context=None):
        """Resize a namespace."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling namespace_resize_safe()"
        failure_prefix = f"Failure resizing namespace {request.nsid} on {request.subsystem_nqn}"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to resize namespace {request.nsid} on "
                         f"{request.subsystem_nqn} to {request.new_size} MiB, context: "
                         f"{context}{peer_msg}")

        if not request.nsid:
            errmsg = "Failure resizing namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure resizing namespace {request.nsid}, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if request.new_size < 0:
            errmsg = f"{failure_prefix}: New size must be positive"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                    request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        bdev_name = find_ret.bdev
        if not bdev_name:
            errmsg = f"{failure_prefix}: Can't find associated block device"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        ret = self.resize_bdev(bdev_name, request.new_size, peer_msg)

        if ret.status != 0:
            errmsg = f"Failure resizing namespace {request.nsid} on " \
                     f"{request.subsystem_nqn}: {ret.error_message}"
            self.logger.error(errmsg)
            return pb2.req_status(status=ret.status, error_message=errmsg)

        if request.new_size > 0:
            find_ret.set_image_was_shrunk(False)

        # If auto resize is disabled, we need to trigger a size refresh for other gateways
        if context and not find_ret.auto_resize:
            omap_lock = self.omap_lock.get_omap_lock_to_use(context)
            with omap_lock:
                try:
                    self.gateway_state.add_namespace_refresh_size(request.subsystem_nqn,
                                                                  request.nsid)
                except Exception as ex:
                    errmsg = f"Error persisting refresh size for namespace {request.nsid} " \
                             f"on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_resize(self, request, context=None):
        """Resize a namespace."""
        err_prefix = f"Failure resizing namespace {request.nsid} on {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_resize_safe, request, context, err_prefix)

    def delete_rbd_image(self, pool, image, rados_namespace_name):
        if (not pool) and (not image):
            return

        if (not pool) or (not image):
            self.logger.warning("RBD pool and image name should be both set or unset, "
                                "will not delete RBD image")
            return

        path = f"{pool}/{rados_namespace_name}/{image}" \
            if rados_namespace_name else f"{pool}/{image}"
        if self.ceph_utils.delete_image(pool, image, rados_namespace_name):
            self.logger.info(f"Deleted RBD image {path}")
        else:
            self.logger.warning(f"Failed to delete RBD image {path}")

    def namespace_delete_safe(self, request, context):
        """Delete a namespace."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling namespace_delete_safe()"
        if not request.nsid:
            errmsg = "Failure deleting namespace, missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure deleting namespace {request.nsid}, missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"Failure deleting namespace {request.nsid}, can't find subsystem " \
                     f"\"{request.subsystem_nqn}\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        peer_msg = self.get_peer_message(context)
        i_am_sure_msg = "I am sure, " if request.i_am_sure else ""
        self.logger.info(f"Received request to delete namespace {request.nsid} from "
                         f"{request.subsystem_nqn}, {i_am_sure_msg}"
                         f"context: {context}{peer_msg}")

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                    request.nsid)
        if find_ret.empty():
            errmsg = f"Failure deleting namespace {request.nsid}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if find_ret.trash_image and not request.i_am_sure:
            errmsg = f"Failure deleting namespace {request.nsid} from " \
                     f"{request.subsystem_nqn}: Confirmation for trashing " \
                     f"RBD image is needed.\nIn order to delete the namespace " \
                     f"either repeat the command using the \"--i-am-sure\" " \
                     f"parameter,\nor reset the RBD trash image flag using " \
                     f"the \"namespace set_rbd_trash_image \" CLI command " \
                     f"on subsystem {request.subsystem_nqn} " \
                     f"and NSID {request.nsid}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        bdev_name = find_ret.bdev
        if not bdev_name:
            self.logger.warning("Can't find namespace's bdev name, will try to "
                                "delete namespace anyway")

        if find_ret.trash_image:
            rbd_pool = find_ret.pool
            rbd_image_name = find_ret.image
            rados_namespace_name = find_ret.rados_namespace_name
        else:
            rbd_pool = None
            rbd_image_name = None
            rados_namespace_name = None

        if (rbd_pool and (not rbd_image_name)) or ((not rbd_pool) and rbd_image_name):
            self.logger.warning("RBD pool and image name should be both set or unset, "
                                "will not delete RBD image")
            rbd_pool = None
            rbd_image_name = None

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ret = self.remove_namespace(request.subsystem_nqn, request.nsid, context)
            if ret.status != 0:
                return ret

            self.remove_namespace_from_state(request.subsystem_nqn, request.nsid, context)

        self.delete_image_identification(find_ret.pool, find_ret.image,
                                         find_ret.rados_namespace_name,
                                         ImageIdentification(self.gateway_group,
                                                             request.subsystem_nqn,
                                                             find_ret.uuid,
                                                             self.fsid))
        self.subsystem_nsid_bdev_and_uuid.remove_namespace(request.subsystem_nqn, request.nsid)
        if bdev_name:
            ret_del = self.delete_bdev(bdev_name, peer_msg=peer_msg)
            if ret_del.status != 0:
                errmsg = f"Failure deleting namespace {request.nsid} from " \
                         f"{request.subsystem_nqn}: {ret_del.error_message}"
                self.logger.error(errmsg)
                if find_ret.trash_image:
                    self.delete_rbd_image(rbd_pool, rbd_image_name, rados_namespace_name)
                return pb2.nsid_status(status=ret_del.status, error_message=errmsg)

        if find_ret.trash_image:
            self.delete_rbd_image(rbd_pool, rbd_image_name, rados_namespace_name)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_delete(self, request, context=None):
        """Delete a namespace."""
        err_prefix = f"Failure deleting namespace {request.nsid} from " \
                     f"{request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_delete_safe, request, context, err_prefix)

    def namespace_add_host_safe(self, request, context):
        """Add a host to a namespace."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling namespace_add_host_safe()"
        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure adding host {request.host_nqn} to namespace " \
                         f"{request.nsid} on {request.subsystem_nqn}"
        self.logger.info(f"Received request to add host {request.host_nqn} to namespace "
                         f"{request.nsid} on {request.subsystem_nqn}, force: {request.force}, "
                         f"context: {context}{peer_msg}")

        assert context or request.force, "Force must be set on update"
        if not request.nsid:
            errmsg = f"Failure adding host {request.host_nqn} to namespace on " \
                     f"{request.subsystem_nqn}: Missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure adding host to namespace {request.nsid}: Missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.host_nqn:
            errmsg = f"Failure adding host to namespace {request.nsid} on " \
                     f"{request.subsystem_nqn}: Missing host NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if request.host_nqn == "*":
            errmsg = f"{failure_prefix}: Host NQN can't be \"*\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: Invalid subsystem NQN: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)
            rc = GatewayUtils.is_valid_nqn(request.host_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: Invalid host NQN: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Subsystem NQN can't be a discovery NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            errmsg = f"{failure_prefix}: Host NQN can't be a discovery NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                    request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if find_ret.auto_visible:
            errmsg = f"{failure_prefix}: Namespace is visible to all hosts"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if find_ret.host_count() >= self.max_hosts_per_namespace:
            errmsg = f"{failure_prefix}: Maximal host count for namespace " \
                     f"({self.max_hosts_per_namespace}) has already been reached"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        specific_host_allowed = self.host_info.does_host_exist(request.subsystem_nqn,
                                                               request.host_nqn)
        host_allowed = specific_host_allowed
        if not specific_host_allowed:
            host_allowed = self.host_info.is_any_host_allowed(request.subsystem_nqn)

        if not host_allowed:
            if request.force:
                self.logger.info(f"Host {request.host_nqn} is not allowed to access "
                                 f"subsystem {request.subsystem_nqn} but it will be added "
                                 f"to namespace {request.nsid} as the \"force\" parameter "
                                 f"was used")
            else:
                errmsg = f"{failure_prefix}: Host is not allowed to access the subsystem, " \
                         f"use the \"force\" parameter to add the host anyway"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ret = self.spdk_rpc_client.nvmf_ns_add_host(
                nqn=request.subsystem_nqn,
                nsid=request.nsid,
                host=request.host_nqn
            )
            self.logger.debug(f"ns_visible {request.host_nqn}: {ret}")
            find_ret.add_host(request.host_nqn)
            if not specific_host_allowed and host_allowed:
                find_ret.add_host("*")

            # Just in case SPDK failed with no exception
            if not ret:
                errmsg = failure_prefix
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    request.force = True
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_namespace_host(request.subsystem_nqn,
                                                          request.nsid, request.host_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting host {request.host_nqn} for namespace " \
                             f"{request.nsid} on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_add_host(self, request, context=None):
        """Add a host to a namespace."""
        err_prefix = f"Failure adding host {request.host_nqn} to namespace " \
                     f"{request.nsid} on {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_add_host_safe, request,
                                          context, err_prefix)

    def namespace_delete_host_safe(self, request, context):
        """Delete a host from a namespace."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling namespace_delete_host_safe()"
        peer_msg = self.get_peer_message(context)
        failure_prefix = f"Failure deleting host {request.host_nqn} from namespace " \
                         f"{request.nsid} on {request.subsystem_nqn}"
        self.logger.info(f"Received request to delete host {request.host_nqn} from namespace "
                         f"{request.nsid} on {request.subsystem_nqn}, "
                         f"context: {context}{peer_msg}")

        if not request.nsid:
            errmsg = f"Failure deleting host {request.host_nqn} from namespace: Missing ID"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.subsystem_nqn:
            errmsg = f"Failure deleting host {request.host_nqn} from namespace " \
                     f"{request.nsid}: Missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.host_nqn:
            errmsg = f"Failure deleting host from namespace {request.nsid} on " \
                     f"{request.subsystem_nqn}: Missing host NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: Can't find subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if request.host_nqn == "*":
            errmsg = f"{failure_prefix}: Host NQN can't be \"*\""
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: Invalid subsystem NQN: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)
            rc = GatewayUtils.is_valid_nqn(request.host_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: Invalid host NQN: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Subsystem NQN can't be a discovery NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            errmsg = f"{failure_prefix}: Host NQN can't be a discovery NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(request.subsystem_nqn,
                                                                    request.nsid)
        if find_ret.empty():
            errmsg = f"{failure_prefix}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        if find_ret.auto_visible:
            errmsg = f"{failure_prefix}: Namespace is visible to all hosts"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not find_ret.is_host_in_namespace(request.host_nqn):
            errmsg = f"{failure_prefix}: Host is not in namespace's host list"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            ret = self.spdk_rpc_client.nvmf_ns_remove_host(
                nqn=request.subsystem_nqn,
                nsid=request.nsid,
                host=request.host_nqn
            )
            self.logger.debug(f"ns_visible {request.host_nqn}: {ret}")
            if not find_ret.empty():
                find_ret.remove_host(request.host_nqn)

            # Just in case SPDK failed with no exception
            if not ret:
                errmsg = failure_prefix
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_namespace_host(request.subsystem_nqn,
                                                             request.nsid, request.host_nqn)
                except KeyError:
                    pass
                except Exception as ex:
                    errmsg = f"Error persisting deletion of host {request.host_nqn} for " \
                             f"namespace {request.nsid} on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_delete_host(self, request, context=None):
        """Delete a host from a namespace."""
        err_prefix = f"Failure deleting host {request.host_nqn} from namespace " \
                     f"{request.nsid} on {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.namespace_delete_host_safe, request,
                                          context, err_prefix)

    def matching_host_exists(self, context, subsys_nqn, host_nqn) -> bool:
        if not context:
            return False
        state = self.gateway_state.local.get_state()
        host_key = GatewayState.build_host_key(subsys_nqn, host_nqn)
        if state.get(host_key):
            return True
        return False

    def get_subsystem_hosts(self, subsys_nqn):
        hosts = []
        state = self.gateway_state.local.get_state()
        host_key_prefix = GatewayState.build_host_key(subsys_nqn, None)
        for key, val in state.items():
            if key.startswith(host_key_prefix):
                try:
                    host = json_format.Parse(val, pb2.add_host_req(), ignore_unknown_fields=True)
                    host_nqn = host.host_nqn
                    hosts.append(host_nqn)
                except Exception:
                    self.logger.exception(f"Error parsing {val}")
                    pass
        return hosts

    def _create_dhchap_key_files(self, subsystem_nqn, host_nqn, dhchap_key,
                                 dhchap_ctrlr_key, err_prefix):
        assert dhchap_key, "DH-HMAC-CHAP key value can't be empty"
        dhchap_file = None
        dhchap_key_name = None
        if dhchap_key:
            dhchap_file = self.create_host_dhchap_file(subsystem_nqn, host_nqn, dhchap_key)
            if not dhchap_file:
                errmsg = f"{err_prefix}: Can't write DH-HMAC-CHAP file"
                self.logger.error(errmsg)
                return (errno.ENOENT, errmsg, None, None, None, None)
            dhchap_key_name = GatewayService.construct_key_name_for_keyring(
                subsystem_nqn,
                host_nqn, GatewayService.DHCHAP_PREFIX)
        dhchap_ctrlr_file = None
        dhchap_ctrlr_key_name = None
        if dhchap_ctrlr_key:
            dhchap_ctrlr_file = self.create_host_dhchap_file(subsystem_nqn,
                                                             host_nqn, dhchap_ctrlr_key)
            if not dhchap_ctrlr_file:
                errmsg = f"{err_prefix}: Can't write DH-HMAC-CHAP controller file"
                self.logger.error(errmsg)
                if dhchap_file:
                    self.remove_host_dhchap_file(subsystem_nqn, host_nqn)
                return (errno.ENOENT, errmsg, None, None, None, None)
            dhchap_ctrlr_key_name = GatewayService.construct_key_name_for_keyring(
                subsystem_nqn,
                host_nqn,
                GatewayService.DHCHAP_CONTROLLER_PREFIX)

        return (0, "", dhchap_file, dhchap_key_name, dhchap_ctrlr_file, dhchap_ctrlr_key_name)

    def _add_key_to_keyring(self, keytype, filename, keyname):
        if not keyname or not filename:
            return
        assert self.rpc_lock.locked(), "RPC is unlocked when calling _add_key_to_keyring()"
        keys = []
        try:
            keys = self.spdk_rpc_client.keyring_get_keys()
        except Exception:
            self.logger.exception("Can't list keyring keys")
            keys = []
        old_filename = None
        for one_key in keys:
            try:
                if one_key["name"] == keyname:
                    old_filename = one_key["path"]
                    break
            except Exception:
                pass

        if old_filename:
            try:
                os.remove(old_filename)
            except Exception:
                self.logger.exception(f"Can't remove file {old_filename}")
                pass
            try:
                self.spdk_rpc_client.keyring_file_remove_key(name=keyname)
            except Exception:
                self.logger.exception(f"Can't remove {keytype} key {keyname}")
                pass

        try:
            ret = self.spdk_rpc_client.keyring_file_add_key(name=keyname, path=filename)
            self.logger.debug(f"keyring_file_add_key {keyname} and file {filename}: {ret}")
            self.logger.info(f"Added {keytype} key {keyname} to keyring")
        except Exception:
            self.logger.exception(f"Can't add {keytype} key {keyname} to keyring")
            pass

    def add_host_safe(self, request, context):
        """Adds a host to a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling add_host_safe()"
        peer_msg = self.get_peer_message(context)
        if request.host_nqn == "*":
            self.logger.info(f"Received request to allow any host access for "
                             f"{request.subsystem_nqn}, context: {context}{peer_msg}")
        else:
            self.logger.info(
                f"Received request to add host {request.host_nqn} to {request.subsystem_nqn}, "
                f"context: {context}{peer_msg}")

        all_host_failure_prefix = f"Failure allowing open host access to {request.subsystem_nqn}"
        host_failure_prefix = f"Failure adding host {request.host_nqn} to {request.subsystem_nqn}"

        if not GatewayState.is_key_element_valid(request.host_nqn):
            errmsg = f"{host_failure_prefix}: Invalid host NQN \"{request.host_nqn}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{host_failure_prefix}: Invalid subsystem NQN \"{request.subsystem_nqn}\"," \
                     f" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.subsystem_nqn not in self.subsys_serial:
            pref = all_host_failure_prefix if request.host_nqn == "*" else host_failure_prefix
            errmsg = f"{pref}: can't find subsystem {request.subsystem_nqn}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if request.host_nqn == "*":
            if self.host_info.does_subsystem_have_dhchap_key(request.subsystem_nqn):
                errmsg = f"{all_host_failure_prefix}: Can't allow any host access " \
                         f"on a subsystem having a DH-HMAC-CHAP key"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)
            dhchap_host_list = self.host_info.get_hosts_with_any_dhchap_key(request.subsystem_nqn)
            if dhchap_host_list:
                errmsg = f"{all_host_failure_prefix}: Can't allow any host access " \
                         f"on a subsystem having a host with a DH-HMAC-CHAP key. " \
                         f"All such hosts need to be removed from the subsystem first " \
                         f"or their DH-HMAC-CHAP key should be cleared"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        if request.host_nqn != "*" and self.host_info.is_any_host_allowed(request.subsystem_nqn):
            self.logger.warning(f"A specific host {request.host_nqn} was added to subsystem "
                                f"{request.subsystem_nqn} in which all hosts are allowed")

        if request.host_nqn == "*":
            self.logger.warning(f"Subsystem {request.subsystem_nqn} will be opened to be "
                                f"accessed from any host. This might be a security breach")

        if self.verify_nqns:
            rc = GatewayService.is_valid_host_nqn(request.host_nqn)
            if rc.status != 0:
                errmsg = f"{host_failure_prefix}: {rc.error_message}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc.status, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: Can't allow host access " \
                         f"to a discovery subsystem"
            else:
                errmsg = f"{host_failure_prefix}: Can't add host to a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            errmsg = f"{host_failure_prefix}: Can't use a discovery NQN as host's"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.psk and request.host_nqn == "*":
            errmsg = f"{all_host_failure_prefix}: PSK is only allowed for specific hosts"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.dhchap_key or request.dhchap_ctrlr_key:
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: DH-HMAC-CHAP key is " \
                         f"only allowed for specific hosts"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)
            elif self.host_info.is_any_host_allowed(request.subsystem_nqn):
                errmsg = f"{host_failure_prefix}: DH-HMAC-CHAP key is " \
                         f"not allowed for hosts on subsystems which are open for all hosts. " \
                         f"You need to remove the open access in order to add the host"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        if self.force_tls and request.host_nqn != "*" and not request.psk:
            errmsg = f"{host_failure_prefix}: host must have a PSK key"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if not context:
            if request.dhchap_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{host_failure_prefix}: No valid DH-HMAC-CHAP key was found for host"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

            if request.dhchap_ctrlr_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{host_failure_prefix}: No valid DH-HMAC-CHAP key was " \
                         f"found for controller"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

            if request.psk == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{host_failure_prefix}: No valid PSK key was found for host"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

        if context and self.verify_keys:
            if request.psk:
                rc = GatewayKeyUtils.is_valid_psk(request.psk)
                if rc[0] != 0:
                    errmsg = f"{host_failure_prefix}: {rc[1]}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=rc[0], error_message=errmsg)

            if request.dhchap_key:
                rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_key)
                if rc[0] != 0:
                    errmsg = f"{host_failure_prefix}: {rc[1]}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=rc[0], error_message=errmsg)

            if request.dhchap_ctrlr_key:
                rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_ctrlr_key, True)
                if rc[0] != 0:
                    errmsg = f"{host_failure_prefix}: {rc[1]}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=rc[0], error_message=errmsg)

        if request.host_nqn == "*":
            secure = False
            try:
                for listener in self.subsystem_listeners[request.subsystem_nqn]:
                    (_, _, _, secure, _) = listener
                    if secure:
                        errmsg = f"{all_host_failure_prefix}: Can't allow open host access " \
                                 f"on a subsystem with secure listeners"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
            except Exception:
                pass

        host_already_exist = self.matching_host_exists(context,
                                                       request.subsystem_nqn, request.host_nqn)
        if host_already_exist:
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: Open host access is already allowed"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EEXIST, error_message=errmsg)
            else:
                errmsg = f"{host_failure_prefix}: Host is already added"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EEXIST, error_message=errmsg)

        if request.host_nqn != "*":
            if self.host_info.get_host_count(request.subsystem_nqn) >= self.max_hosts_per_subsystem:
                errmsg = f"{host_failure_prefix}: Maximal number of hosts for subsystem " \
                         f"({self.max_hosts_per_subsystem}) has already been reached"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.E2BIG, error_message=errmsg)
            if self.host_info.get_host_count(None) >= self.max_hosts:
                errmsg = f"{host_failure_prefix}: Maximal number of hosts " \
                         f"({self.max_hosts}) has already been reached"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        dhchap_key_for_omap = request.dhchap_key
        dhchap_ctrlr_key_for_omap = request.dhchap_ctrlr_key
        key_encrypted_for_omap = request.key_encrypted
        ctrlr_key_encrypted_for_omap = request.ctrlr_key_encrypted
        psk_for_omap = request.psk
        psk_encrypted_for_omap = request.psk_encrypted
        if context and self.enable_key_encryption:
            if request.dhchap_key:
                if self.gateway_state.crypto:
                    dhchap_key_for_omap = self.gateway_state.crypto.encrypt_text(request.dhchap_key)
                    key_encrypted_for_omap = True
                else:
                    errmsg = f"{host_failure_prefix}: No encryption key or the wrong key was " \
                             f"found but we need to encrypt host {request.host_nqn} " \
                             f"DH-HMAC-CHAP key"
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
            if request.dhchap_ctrlr_key:
                if self.gateway_state.crypto:
                    dhchap_ctrlr_key_for_omap = self.gateway_state.crypto.encrypt_text(
                        request.dhchap_ctrlr_key)
                    ctrlr_key_encrypted_for_omap = True
                else:
                    errmsg = f"{host_failure_prefix}: No encryption key or the wrong key was " \
                             f"found but we need to encrypt host {request.host_nqn} " \
                             f"DH-HMAC-CHAP controller key"
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
            if request.psk:
                if self.gateway_state.crypto:
                    psk_for_omap = self.gateway_state.crypto.encrypt_text(request.psk)
                    psk_encrypted_for_omap = True
                else:
                    errmsg = f"{host_failure_prefix}: No encryption key or the wrong key was " \
                             f"found but we need to encrypt host {request.host_nqn} PSK key"
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        dhchap_ctrlr_key = self.host_info.get_subsystem_dhchap_key(request.subsystem_nqn)
        if dhchap_ctrlr_key and request.dhchap_ctrlr_key:
            errmsg = f"{host_failure_prefix}: Host DH-HMAC-CHAP controller keys and subsystem " \
                     f"DH-HMAC-CHAP keys are mutually exclusive"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

        if dhchap_ctrlr_key:
            self.logger.info(f"Got DH-HMAC-CHAP key for subsystem {request.subsystem_nqn}")
        else:
            dhchap_ctrlr_key = request.dhchap_ctrlr_key

        if not dhchap_ctrlr_key and request.dhchap_key:
            self.logger.warning(f"Host {request.host_nqn} has a DH-HMAC-CHAP key but no "
                                f"controller key, and subsystem {request.subsystem_nqn} "
                                f"has no key, a unidirectional authentication will be used")

        if dhchap_ctrlr_key and not request.dhchap_key:
            errmsg = f"{host_failure_prefix}: Host must have a DH-HMAC-CHAP " \
                     f"key if the controller or subsystem has one"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if not context:
            if dhchap_ctrlr_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{host_failure_prefix}: No valid DH-HMAC-CHAP key was found for subsystem"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

        psk_file = None
        psk_key_name = None
        if request.psk:
            psk_file = self.create_host_psk_file(request.subsystem_nqn,
                                                 request.host_nqn, request.psk)
            if not psk_file:
                errmsg = f"{host_failure_prefix}: Can't write PSK file"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            psk_key_name = GatewayService.construct_key_name_for_keyring(request.subsystem_nqn,
                                                                         request.host_nqn,
                                                                         GatewayService.PSK_PREFIX)
            if len(psk_key_name) >= GatewayKeyUtils.MAX_PSK_KEY_NAME_LENGTH:
                errmsg = f"{host_failure_prefix}: PSK key name {psk_key_name} is too long, " \
                         f"max length is {GatewayKeyUtils.MAX_PSK_KEY_NAME_LENGTH}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.E2BIG, error_message=errmsg)

        dhchap_file = None
        dhchap_key_name = None
        dhchap_ctrlr_file = None
        dhchap_ctrlr_key_name = None
        if request.dhchap_key:
            (key_files_status,
             key_file_errmsg,
             dhchap_file,
             dhchap_key_name,
             dhchap_ctrlr_file,
             dhchap_ctrlr_key_name) = self._create_dhchap_key_files(
                 request.subsystem_nqn, request.host_nqn,
                 request.dhchap_key, dhchap_ctrlr_key, host_failure_prefix)
            if key_files_status != 0:
                if psk_file:
                    self.remove_host_psk_file(request.subsystem_nqn, request.host_nqn)
                return pb2.req_status(status=key_files_status, error_message=key_file_errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                if request.host_nqn == "*":  # Allow any host access to subsystem
                    ret = self.spdk_rpc_client.nvmf_subsystem_allow_any_host(
                        nqn=request.subsystem_nqn,
                        allow_any_host=True,
                    )
                    self.logger.debug(f"add_host *: {ret}")
                    self.host_info.allow_any_host(request.subsystem_nqn)
                else:  # Allow single host access to subsystem
                    self._add_key_to_keyring("PSK", psk_file, psk_key_name)
                    self._add_key_to_keyring("DH-HMAC-CHAP", dhchap_file, dhchap_key_name)
                    self._add_key_to_keyring("DH-HMAC-CHAP controller",
                                             dhchap_ctrlr_file, dhchap_ctrlr_key_name)
                    ret = self.spdk_rpc_client.nvmf_subsystem_add_host(
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                        psk=psk_key_name,
                        dhchap_key=dhchap_key_name,
                        dhchap_ctrlr_key=dhchap_ctrlr_key_name,
                    )
                    self.logger.debug(f"add_host {request.host_nqn}: {ret}")
                    if psk_file:
                        self.host_info.add_psk_host(request.subsystem_nqn,
                                                    request.host_nqn, request.psk)
                        self.remove_host_psk_file(request.subsystem_nqn, request.host_nqn)
                        self.remove_psk_key_from_keyring(request.subsystem_nqn, request.host_nqn)
                    if dhchap_file:
                        self.host_info.add_dhchap_host(request.subsystem_nqn,
                                                       request.host_nqn, request.dhchap_key)
                    if dhchap_ctrlr_file and request.dhchap_ctrlr_key:
                        self.host_info.add_dhchap_ctrlr_host(request.subsystem_nqn,
                                                             request.host_nqn,
                                                             request.dhchap_ctrlr_key)
                    self.host_info.add_host_nqn(request.subsystem_nqn, request.host_nqn)
            except Exception as ex:
                if request.host_nqn == "*":
                    self.logger.exception(all_host_failure_prefix)
                    errmsg = f"{all_host_failure_prefix}:\n{ex}"
                else:
                    self.remove_all_host_key_files(request.subsystem_nqn, request.host_nqn)
                    self.remove_all_host_keys_from_keyring(request.subsystem_nqn, request.host_nqn)
                    self.logger.exception(host_failure_prefix)
                    errmsg = f"{host_failure_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    if request.host_nqn == "*":
                        errmsg = f"{all_host_failure_prefix}: {resp['message']}"
                    else:
                        errmsg = f"{host_failure_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                if request.host_nqn == "*":
                    errmsg = all_host_failure_prefix
                else:
                    errmsg = host_failure_prefix
                    self.remove_all_host_key_files(request.subsystem_nqn, request.host_nqn)
                    self.remove_all_host_keys_from_keyring(request.subsystem_nqn, request.host_nqn)
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    assert not request.key_encrypted, "Encrypted keys can only come from update()"
                    assert not request.ctrlr_key_encrypted, "Encrypted keys can only come " \
                                                            "from update()"
                    assert not request.psk_encrypted, "Encrypted keys can only come from update()"
                    request.dhchap_key = dhchap_key_for_omap
                    request.key_encrypted = key_encrypted_for_omap
                    request.psk = psk_for_omap
                    request.psk_encrypted = psk_encrypted_for_omap
                    request.dhchap_ctrlr_key = dhchap_ctrlr_key_for_omap
                    request.ctrlr_key_encrypted = ctrlr_key_encrypted_for_omap
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_host(request.subsystem_nqn, request.host_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting host {request.host_nqn} access addition"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    self.remove_all_host_key_files(request.subsystem_nqn, request.host_nqn)
                    self.remove_all_host_keys_from_keyring(request.subsystem_nqn, request.host_nqn)
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def add_host(self, request, context=None):
        err_prefix = f"Failure adding host {request.host_nqn} to {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.add_host_safe, request, context, err_prefix)

    def remove_host_from_state(self, subsystem_nqn, host_nqn, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        assert context is None or self.omap_lock.write_locked_by_me(), \
            f"OMAP is unlocked when calling remove_host_from_state()\n" \
            f"in thread: {threading.get_native_id()}. Locked by: " \
            f"{self.omap_lock.locked_by}, with cookie: {self.omap_lock.lock_cookie}, " \
            f"locked: {self.omap_lock.is_exclusively_locked}"

        # Update gateway state
        try:
            self.gateway_state.remove_host(subsystem_nqn, host_nqn)
        except Exception as ex:
            errmsg = f"Error persisting host {host_nqn} access removal"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        return pb2.req_status(status=0, error_message=os.strerror(0))

    def remove_host_safe(self, request, context):
        """Removes a host from a subsystem."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling remove_host_safe()"
        peer_msg = self.get_peer_message(context)
        ns_using_host = []
        removed_host_is_connected = False
        all_host_failure_prefix = f"Failure disabling open host access to {request.subsystem_nqn}"
        host_failure_prefix = f"Failure removing host {request.host_nqn} access " \
                              f"from {request.subsystem_nqn}"

        if request.host_nqn == "*":
            self.logger.info(
                f"Received request to disable open host access to"
                f" {request.subsystem_nqn}, context: {context}{peer_msg}")
        else:
            self.logger.info(
                f"Received request to remove host {request.host_nqn} access from"
                f" {request.subsystem_nqn}, force: {request.force}, "
                f"context: {context}{peer_msg}")

        if self.verify_nqns:
            rc = GatewayService.is_valid_host_nqn(request.host_nqn)
            if rc.status != 0:
                errmsg = f"{host_failure_prefix}: {rc.error_message}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc.status, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: Can't disable open host access " \
                         f"to a discovery subsystem"
            else:
                errmsg = f"{host_failure_prefix}: Can't remove host access from a " \
                         f"discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: Can't use a discovery NQN as host's"
            else:
                errmsg = f"{host_failure_prefix}: Can't use a discovery NQN as host's"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if context:
            ns_using_host = self.subsystem_nsid_bdev_and_uuid.get_all_namespaces_with_host(
                request.host_nqn, request.subsystem_nqn)
        if len(ns_using_host) > 0 and not request.force:
            if request.host_nqn == "*":
                errmsg = f"{all_host_failure_prefix}: One of the hosts in the netmask of " \
                         f"namespace {ns_using_host[0][0]} relies on the subsystem " \
                         f"being open for all hosts. " \
                         f"Either clear the netmask or use the \"force\" parameter."
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)
            else:
                errmsg = f"{host_failure_prefix}: Host is included in the netmask of " \
                         f"namespace {ns_using_host[0][0]}. " \
                         f"Either remove it or use the \"force\" parameter."
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        removed_host_is_connected = self.is_host_connected(request.subsystem_nqn, request.host_nqn)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                if request.host_nqn == "*":  # Disable allow any host access
                    ret = self.spdk_rpc_client.nvmf_subsystem_allow_any_host(
                        nqn=request.subsystem_nqn,
                        allow_any_host=False,
                    )
                    self.logger.debug(f"remove_host *: {ret}")
                    self.host_info.disallow_any_host(request.subsystem_nqn)
                else:  # Remove single host access to subsystem
                    if not self.host_info.does_host_exist(request.subsystem_nqn,
                                                          request.host_nqn):
                        errmsg = f"{host_failure_prefix}: No such host"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
                    ret = self.spdk_rpc_client.nvmf_subsystem_remove_host(
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                    )
                    self.logger.debug(f"remove_host {request.host_nqn}: {ret}")
                    self.host_info.remove_psk_host(request.subsystem_nqn, request.host_nqn)
                    self.host_info.remove_dhchap_host(request.subsystem_nqn, request.host_nqn)
                    self.host_info.remove_dhchap_ctrlr_host(request.subsystem_nqn,
                                                            request.host_nqn)
                    self.remove_all_host_key_files(request.subsystem_nqn, request.host_nqn)
                    self.remove_all_host_keys_from_keyring(request.subsystem_nqn, request.host_nqn)
                    self.host_info.remove_host_nqn(request.subsystem_nqn, request.host_nqn)
            except Exception as ex:
                if request.host_nqn == "*":
                    self.logger.exception(all_host_failure_prefix)
                    errmsg = f"{all_host_failure_prefix}:\n{ex}"
                else:
                    self.logger.exception(host_failure_prefix)
                    errmsg = f"{host_failure_prefix}:\n{ex}"
                self.logger.error(errmsg)
                self.remove_host_from_state(request.subsystem_nqn, request.host_nqn, context)
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    if request.host_nqn == "*":
                        errmsg = f"{all_host_failure_prefix}: {resp['message']}"
                    else:
                        errmsg = f"{host_failure_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                if request.host_nqn == "*":
                    errmsg = all_host_failure_prefix
                else:
                    errmsg = host_failure_prefix
                self.logger.error(errmsg)
                self.remove_host_from_state(request.subsystem_nqn, request.host_nqn, context)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            rc = self.remove_host_from_state(request.subsystem_nqn, request.host_nqn, context)
            if rc.status == 0:
                err_msg_con = ""
                err_msg_ns = ""
                if removed_host_is_connected:
                    rc.status = errno.EBUSY
                    err_msg_con = \
                        f"Host {request.host_nqn} is still connected to " \
                        f"{request.subsystem_nqn}\n" \
                        f"Reconnecting the host would fail unless " \
                        f"it is re-added to the subsystem."
                    self.logger.warning(err_msg_con)
                if len(ns_using_host) > 0:
                    rc.status = errno.EBUSY
                    if request.host_nqn == "*":
                        err_msg_ns = \
                            f"One of the hosts in the netmask of " \
                            f"namespace {ns_using_host[0][0]} relies on the subsystem " \
                            f"being open for all hosts. "
                    else:
                        err_msg_ns = \
                            f"Host {request.host_nqn} is included in the netmask of " \
                            f"namespace {ns_using_host[0][0]} in subsystem " \
                            f"{ns_using_host[0][1]}. "

                    err_msg_ns += \
                        "Will continue as the \"force\" parameter " \
                        "was used but this might cause issues with the netmask later, in " \
                        "case the host is not removed from the netmask."
                    self.logger.warning(err_msg_ns)
                if err_msg_con:
                    rc.error_message = err_msg_con
                    if err_msg_ns:
                        rc.error_message += "\n" + err_msg_ns
                elif err_msg_ns:
                    rc.error_message = err_msg_ns
            return rc

    def remove_host(self, request, context=None):
        err_prefix = f"Failure removing host {request.host_nqn} access " \
                     f"from {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.remove_host_safe, request, context, err_prefix)

    def change_host_key_safe(self, request, context):
        """Changes host's inband authentication key and/or controller key."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling change_host_key_safe()"
        peer_msg = self.get_peer_message(context)
        cmd = "change" if request.dhchap_key else "delete"
        cmd2 = "changing" if request.dhchap_key else "deleting"
        ctrlr_cmd = "change" if request.dhchap_ctrlr_key else "delete"
        ctrlr_cmd2 = "changing" if request.dhchap_ctrlr_key else "deleting"
        host_failure_prefix = f"Failure {cmd2} DH-HMAC-CHAP key for host {request.host_nqn} " \
                              f"on subsystem {request.subsystem_nqn}"
        ctrlr_failure_prefix = f"Failure {ctrlr_cmd2} DH-HMAC-CHAP controller key for " \
                               f"host {request.host_nqn} on subsystem {request.subsystem_nqn}"
        both_failure_prefix = f"Failure changing DH-HMAC-CHAP key for host {request.host_nqn} " \
                              f"and its controller on subsystem {request.subsystem_nqn}"
        if request.dhchap_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
            if request.dhchap_ctrlr_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
                self.logger.info("No DH-HMAC-CHAP key change was requested, quit")
                return pb2.req_status(status=0, error_message=os.strerror(0))

        if request.dhchap_ctrlr_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
            self.logger.info(
                f"Received request to {cmd} inband authentication key for host {request.host_nqn} "
                f"on subsystem {request.subsystem_nqn}, context: {context}{peer_msg}")
            failure_prefix = host_failure_prefix
        elif request.dhchap_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
            self.logger.info(
                f"Received request to {ctrlr_cmd} inband authentication key for controller of "
                f"host {request.host_nqn} "
                f"on subsystem {request.subsystem_nqn}, context: {context}{peer_msg}")
            failure_prefix = ctrlr_failure_prefix
        else:
            self.logger.info(
                f"Received request to change inband authentication key for host {request.host_nqn} "
                f"and its controller on subsystem {request.subsystem_nqn}, "
                f"context: {context}{peer_msg}")
            failure_prefix = both_failure_prefix

        if request.host_nqn == "*":
            errmsg = f"{failure_prefix}: Host NQN can't be '*'"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not context:
            if request.dhchap_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{host_failure_prefix}: No valid DH-HMAC-CHAP key was found for host"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)
            if request.dhchap_ctrlr_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{ctrlr_failure_prefix}: No valid DH-HMAC-CHAP key was " \
                         f"found for controller"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EKEYREJECTED, error_message=errmsg)

        if context and self.verify_keys:
            if request.dhchap_key:
                if request.dhchap_key != GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
                    rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_key)
                    if rc[0] != 0:
                        errmsg = f"{host_failure_prefix}: {rc[1]}"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=rc[0], error_message=errmsg)
            if request.dhchap_ctrlr_key:
                if request.dhchap_ctrlr_key != GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
                    rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_ctrlr_key, True)
                    if rc[0] != 0:
                        errmsg = f"{ctrlr_failure_prefix}: {rc[1]}"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=rc[0], error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.host_nqn):
            errmsg = f"{failure_prefix}: Invalid host NQN \"{request.host_nqn}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Invalid subsystem NQN \"{request.subsystem_nqn}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

            rc = GatewayUtils.is_valid_nqn(request.host_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Can't use a discovery NQN as subsystem's"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            errmsg = f"{failure_prefix}: Can't use a discovery NQN as host's"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.dhchap_key:
            if request.dhchap_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
                dhchap_key_to_use = self.host_info.get_host_dhchap_key(
                    request.subsystem_nqn, request.host_nqn)
            else:
                dhchap_key_to_use = request.dhchap_key
        else:
            dhchap_key_to_use = None
        has_dhchap_key = True if dhchap_key_to_use else False

        if request.dhchap_ctrlr_key:
            if request.dhchap_ctrlr_key == GatewayUtilsCrypto.EXISTING_DHCHAP_KEY:
                dhchap_ctrlr_key_to_use = self.host_info.get_host_dhchap_ctrlr_key(
                    request.subsystem_nqn, request.host_nqn)
            else:
                dhchap_ctrlr_key_to_use = request.dhchap_ctrlr_key
        else:
            dhchap_ctrlr_key_to_use = None
        has_dhchap_ctrlr_key = True if dhchap_ctrlr_key_to_use else False

        if has_dhchap_ctrlr_key and not has_dhchap_key:
            errmsg = f"{failure_prefix}: Host must have a DH-HMAC-CHAP key if the " \
                     f"controller has one"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        ctrlr_key_taken_from_subsystem = False
        dhchap_subsystem_key = self.host_info.get_subsystem_dhchap_key(request.subsystem_nqn)
        if dhchap_subsystem_key:
            if not has_dhchap_key:
                errmsg = f"{failure_prefix}: Host must have a DH-HMAC-CHAP key if the " \
                         f"subsystem has one"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
            if has_dhchap_ctrlr_key:
                errmsg = f"{ctrlr_failure_prefix}: Can't set a host DH-HMAC-CHAP controller key " \
                         f"when the subsystem has a key as well, remove the subsystem's key first"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
            if not request.dhchap_ctrlr_key:
                errmsg = f"{failure_prefix}: Can't delete host DH-HMAC-CHAP controller key " \
                         f"as it was defined in the subsystem"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
            dhchap_ctrlr_key_to_use = dhchap_subsystem_key
            has_dhchap_ctrlr_key = True
            ctrlr_key_taken_from_subsystem = True

        if has_dhchap_key and not has_dhchap_ctrlr_key:
            self.logger.warning(f"Host {request.host_nqn} has a DH-HMAC-CHAP key but no "
                                f"controller key, and subsystem "
                                f"{request.subsystem_nqn} has no key, a unidirectional "
                                f"authentication will be used")

        if has_dhchap_key and self.host_info.is_any_host_allowed(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: DH-HMAC-CHAP key is " \
                     f"not allowed for hosts on subsystems which are open for all hosts. " \
                     f"You need to remove the open access in order to set a key for the host"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        if has_dhchap_ctrlr_key and self.host_info.is_any_host_allowed(request.subsystem_nqn):
            errmsg = f"{ctrlr_failure_prefix}: DH-HMAC-CHAP controller key is " \
                     f"not allowed for hosts on subsystems which are open for all hosts. " \
                     f"You need to remove the open access in order to set a controller " \
                     f"key for the host"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        host_already_exist = self.matching_host_exists(context, request.subsystem_nqn,
                                                       request.host_nqn)
        if not host_already_exist and context:
            errmsg = f"{failure_prefix}: Can't find host on subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        host_psk = None
        if context:
            host_psk = self.host_info.get_host_psk_key(request.subsystem_nqn, request.host_nqn)

        dhchap_key_for_omap = dhchap_key_to_use
        dhchap_ctrlr_key_for_omap = dhchap_ctrlr_key_to_use
        key_encrypted_for_omap = False
        ctrlr_key_encrypted_for_omap = False
        psk_for_omap = host_psk
        psk_encrypted_for_omap = False

        if context and self.enable_key_encryption:
            if dhchap_key_to_use:
                if self.gateway_state.crypto:
                    dhchap_key_for_omap = self.gateway_state.crypto.encrypt_text(dhchap_key_to_use)
                    key_encrypted_for_omap = True
                else:
                    errmsg = f"{host_failure_prefix}: No encryption key or the wrong key " \
                             f"was found but we need to encrypt host {request.host_nqn} " \
                             f"DH-HMAC-CHAP key"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

            if dhchap_ctrlr_key_to_use:
                if ctrlr_key_taken_from_subsystem:
                    dhchap_ctrlr_key_for_omap = None
                    ctrlr_key_encrypted_for_omap = False
                elif self.gateway_state.crypto:
                    dhchap_ctrlr_key_for_omap = self.gateway_state.crypto.encrypt_text(
                        dhchap_ctrlr_key_to_use)
                    ctrlr_key_encrypted_for_omap = True
                else:
                    errmsg = f"{ctrlr_failure_prefix}: No encryption key or the wrong key " \
                             f"was found but we need to encrypt host {request.host_nqn} " \
                             f"DH-HMAC-CHAP controller key"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

            if host_psk:
                if self.gateway_state.crypto:
                    psk_for_omap = self.gateway_state.crypto.encrypt_text(host_psk)
                    psk_encrypted_for_omap = True
                else:
                    errmsg = f"{failure_prefix}: No encryption key or the wrong key was found " \
                             f"but we need to encrypt host {request.host_nqn} PSK key"
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if not context:
            if dhchap_subsystem_key == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{failure_prefix}: No valid DH-HMAC-CHAP key was found for subsystem"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

            if host_psk == GatewayUtilsCrypto.INVALID_KEY_VALUE:
                errmsg = f"{failure_prefix}: No valid PSK key was found for subsystem"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        dhchap_file = None
        dhchap_key_name = None
        dhchap_ctrlr_file = None
        dhchap_ctrlr_key_name = None
        if request.dhchap_key:
            (key_files_status,
             key_file_errmsg,
             dhchap_file,
             dhchap_key_name,
             dhchap_ctrlr_file,
             dhchap_ctrlr_key_name) = self._create_dhchap_key_files(request.subsystem_nqn,
                                                                    request.host_nqn,
                                                                    dhchap_key_to_use,
                                                                    dhchap_ctrlr_key_to_use,
                                                                    failure_prefix)

            if key_files_status != 0:
                return pb2.req_status(status=key_files_status, error_message=key_file_errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                self._add_key_to_keyring("DH-HMAC-CHAP", dhchap_file, dhchap_key_name)
                self._add_key_to_keyring("DH-HMAC-CHAP controller",
                                         dhchap_ctrlr_file, dhchap_ctrlr_key_name)
                ret = self.spdk_rpc_client.nvmf_subsystem_set_keys(
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                    dhchap_key=dhchap_key_name,
                    dhchap_ctrlr_key=dhchap_ctrlr_key_name,
                )
            except Exception as ex:
                self.logger.exception(failure_prefix)
                errmsg = f"{failure_prefix}:\n{ex}"
                self.logger.error(errmsg)
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{failure_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                errmsg = failure_prefix
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if dhchap_key_name:
                self.host_info.add_dhchap_host(request.subsystem_nqn,
                                               request.host_nqn, dhchap_key_to_use)
            else:
                self.host_info.remove_dhchap_host(request.subsystem_nqn, request.host_nqn)
                self.remove_all_host_key_files(request.subsystem_nqn, request.host_nqn)
                self.remove_all_host_keys_from_keyring(request.subsystem_nqn, request.host_nqn)

            if dhchap_ctrlr_key_name and not ctrlr_key_taken_from_subsystem:
                self.host_info.add_dhchap_ctrlr_host(request.subsystem_nqn,
                                                     request.host_nqn, dhchap_ctrlr_key_to_use)
            else:
                self.host_info.remove_dhchap_ctrlr_host(request.subsystem_nqn, request.host_nqn)

            if context:
                # Update gateway state
                try:
                    add_req = pb2.add_host_req(subsystem_nqn=request.subsystem_nqn,
                                               host_nqn=request.host_nqn,
                                               psk=psk_for_omap,
                                               dhchap_key=dhchap_key_for_omap,
                                               key_encrypted=key_encrypted_for_omap,
                                               psk_encrypted=psk_encrypted_for_omap,
                                               dhchap_ctrlr_key=dhchap_ctrlr_key_for_omap,
                                               ctrlr_key_encrypted=ctrlr_key_encrypted_for_omap)
                    json_req = json_format.MessageToJson(
                        add_req, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_host(request.subsystem_nqn, request.host_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting host change key for host {request.host_nqn}" \
                             f" in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def change_host_key(self, request, context=None):
        """Changes host's inband authentication key."""
        cmd2 = "changing" if request.dhchap_key else "deleting"
        err_prefix = f"Failure {cmd2} DH-HMAC-CHAP key for host {request.host_nqn} " \
                     f"on subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.change_host_key_safe, request, context, err_prefix)

    def get_connection_io_statistics_safe(self, request, context):
        """Get connection's IO statistics."""

        def _get_int_from_dict(dic, fld) -> int:
            if not dic:
                return 0
            if not fld:
                return 0
            val = dic.get(fld)
            if not val:
                return 0
            return val

        def _get_bucket(bucket):
            def _get_latency_group(grp):
                def _get_latency_stats(lat):
                    if not lat:
                        return None
                    lat_min = _get_int_from_dict(lat, "min")
                    lat_max = _get_int_from_dict(lat, "max")
                    lat_mean = _get_int_from_dict(lat, "mean")
                    return pb2.latency_stats(min=lat_min, max=lat_max, mean=lat_mean)

                if not grp:
                    return None
                io_count = _get_int_from_dict(grp, "io_count")
                latency = grp.get("latency")
                if not latency:
                    return pb2.latency_group(io_count=io_count)
                total_lat = _get_latency_stats(latency.get("total"))
                bdev_lat = _get_latency_stats(latency.get("bdev"))
                net_lat = _get_latency_stats(latency.get("net"))
                qos_lat = _get_latency_stats(latency.get("qos"))
                return pb2.latency_group(io_count=io_count,
                                         total=total_lat,
                                         bdev=bdev_lat,
                                         net=net_lat,
                                         qos=qos_lat)
            if not bucket:
                return None
            size = _get_int_from_dict(bucket, "bucket-size (KB)")
            read_lat_grp = _get_latency_group(bucket.get("read"))
            write_lat_grp = _get_latency_group(bucket.get("write"))
            return pb2.bucket_info(size=size, read=read_lat_grp, write=write_lat_grp)

        assert self.rpc_lock.locked(), "RPC is unlocked when calling " \
                                       "get_connection_io_statistics_safe()"
        peer_msg = self.get_peer_message(context)
        cmd = "reset" if request.reset else "get"
        cmd2 = "resetting" if request.reset else "getting"
        self.logger.info(f"Received request to {cmd} IO statistics for host {request.host_nqn} "
                         f"on {request.subsystem_nqn}, "
                         f"context: {context}{peer_msg}")
        failure_prefix = f"Failure {cmd2} IO statistics for host {request.host_nqn} " \
                         f"on subsystem {request.subsystem_nqn}"

        if request.subsystem_nqn not in self.subsys_serial:
            errmsg = f"{failure_prefix}: No such subsystem"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.ENOENT, error_message=errmsg)

        if not self.io_stats_enabled:
            errmsg = f"{failure_prefix}: IO statistics is disabled or not supported"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.ENOTSUP, error_message=errmsg)

        if request.host_nqn == "*":
            errmsg = f"{failure_prefix}: Must specify a specific host NQN, \"*\" is invalid"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.host_nqn):
            errmsg = f"{failure_prefix}: Invalid host NQN \"{request.host_nqn}\", " \
                     f"contains invalid characters"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Invalid subsystem NQN \"{request.subsystem_nqn}\"," \
                     f" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=errmsg)

        if not self.host_info.is_any_host_allowed(request.subsystem_nqn):
            host_exists = self.host_info.does_host_exist(request.subsystem_nqn,
                                                         request.host_nqn)
            if not host_exists:
                errmsg = f"{failure_prefix}: Host is not allowed to access subsystem"
                self.logger.error(errmsg)
                return pb2.connection_io_statistics(status=errno.ENODEV, error_message=errmsg)

        host_is_connected = self.is_host_connected(request.subsystem_nqn, request.host_nqn)
        if not host_is_connected:
            errmsg = f"{failure_prefix}: Host is not connected"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.ENODEV, error_message=errmsg)

        try:
            ret = self.spdk_rpc_client.nvmf_get_ctrl_io_stats(nqn=request.subsystem_nqn,
                                                              host_nqn=request.host_nqn,
                                                              reset=request.reset)
            self.logger.debug(f"nvmf_get_ctrl_io_stats: {ret}")
        except Exception as ex:
            self.logger.exception(failure_prefix)
            errmsg = f"{failure_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{failure_prefix}: {resp['message']}"
            return pb2.connection_io_statistics(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not ret:
            self.logger.error(failure_prefix)
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=failure_prefix)

        not_supported_err = ret.get("supported")
        if not_supported_err:
            errmsg = f"{failure_prefix}: IO statistics is disabled or not supported"
            self.logger.error(errmsg)
            return pb2.connection_io_statistics(status=errno.ENOTSUP, error_message=errmsg)

        if request.reset:
            if ret.get("reset"):
                return pb2.connection_io_statistics(status=0, error_message=os.strerror(0))
            self.logger.error(failure_prefix)
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=failure_prefix)

        bucket_list = []
        try:
            total_num_ios = _get_int_from_dict(ret, "total_num_ios")
            buckets = ret.get("buckets")
            if not buckets:
                buckets = []
            for bucket in buckets:
                one_bucket = _get_bucket(bucket)
                if one_bucket:
                    bucket_list.append(one_bucket)
            return pb2.connection_io_statistics(status=0, error_message=os.strerror(0),
                                                subsystem_nqn=request.subsystem_nqn,
                                                host_nqn=request.host_nqn,
                                                total_num_ios=total_num_ios,
                                                buckets=bucket_list)
        except Exception as ex:
            self.logger.exception(f"Error parsing {ret}")
            errmsg = f"{failure_prefix}:\n{ex}"
            return pb2.connection_io_statistics(status=errno.EINVAL, error_message=errmsg)

        return pb2.connection_io_statistics(status=errno.ENOTSUP, error_message="TBD")

    def get_connection_io_statistics(self, request, context=None):
        """Get connection's IO statistics."""
        return self.execute_grpc_function(self.get_connection_io_statistics_safe, request, context)

    def list_hosts_safe(self, request, context):
        """List hosts."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling list_hosts_safe()"
        peer_msg = self.get_peer_message(context)
        log_level = logging.INFO if context else logging.DEBUG
        self.logger.log(log_level, f"Received request to list hosts for "
                                   f"{request.subsystem}, clear_alerts: {request.clear_alerts}, "
                                   f"context: {context}{peer_msg}")
        ret = None
        if not context:
            ret = self.subsystems_cache.get_one_subsystem(request.subsystem)
            self.logger.debug(f"list_hosts subsystem (cache): {ret}")
        if not ret:
            try:
                ret = self.spdk_rpc_client.nvmf_get_subsystems(nqn=request.subsystem)
                self.logger.debug(f"list_hosts subsystem: {ret}")
            except Exception as ex:
                errmsg = "Failure listing hosts, can't get subsystem"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure listing hosts, can't get subsystem: {resp['message']}"
                return pb2.hosts_info(status=status, error_message=errmsg, hosts=[])

            parsed_ret = []
            for s in ret:
                subsys = pb2.subsystem()
                try:
                    json_format.Parse(json.dumps(s), subsys, ignore_unknown_fields=True)
                except Exception:
                    self.logger.exception(f"Failure listing hosts, can't parse subsystem {s}")
                    return pb2.hosts_info(status=errno.EINVAL,
                                          error_message="Failure listing hosts, "
                                                        "can't parse subsystem",
                                          hosts=[])
                parsed_ret.append(subsys)
            ret = parsed_ret

        if not ret:
            ret = []
        hosts = []
        allow_any_host = False
        for s in ret:
            try:
                if s.nqn != request.subsystem:
                    self.logger.warning(f'Got subsystem {s.nqn} instead of '
                                        f'{request.subsystem}, ignore')
                    continue
                try:
                    allow_any_host = s.allow_any_host
                    host_nqns = s.hosts
                except Exception:
                    host_nqns = []
                    pass
                subsystem_has_dhchap_key = self.host_info.does_subsystem_have_dhchap_key(
                    request.subsystem)
                for h in host_nqns:
                    host_nqn = h.nqn
                    psk = self.host_info.is_psk_host(request.subsystem, host_nqn)
                    dhchap = self.host_info.is_dhchap_host(request.subsystem, host_nqn)
                    if self.host_info.is_dhchap_ctrlr_host(request.subsystem, host_nqn):
                        dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.host_specific
                    elif subsystem_has_dhchap_key:
                        dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.subsystem_implicit
                    else:
                        dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.no_key
                    was_ka_timeout = \
                        self.host_info.was_host_disconnected_due_to_keepalive_timeout(
                            request.subsystem, host_nqn)
                    one_host = pb2.host(nqn=host_nqn, use_psk=psk, use_dhchap=dhchap,
                                        dhchap_controller_origin=dhchap_ctrlr,
                                        disconnected_due_to_keepalive_timeout=was_ka_timeout)
                    hosts.append(one_host)
                    if was_ka_timeout and request.clear_alerts:
                        self.host_info.reset_host_keepalive_timeout_disconnection(
                            request.subsystem, host_nqn)
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return pb2.hosts_info(status=0, error_message=os.strerror(0), allow_any_host=allow_any_host,
                              subsystem_nqn=request.subsystem, hosts=hosts)

    def list_hosts(self, request, context=None):
        return self.execute_grpc_function(self.list_hosts_safe, request, context)

    def list_connections_safe(self, request, context):
        """List connections."""

        peer_msg = self.get_peer_message(context)
        log_level = logging.INFO if context else logging.DEBUG
        self.logger.log(log_level,
                        f"Received request to list connections for {request.subsystem}, "
                        f"clear_alerts: {request.clear_alerts}, context: {context}{peer_msg}")

        if not request.subsystem:
            request.subsystem = GatewayUtils.ALL_SUBSYSTEMS

        if request.subsystem != GatewayUtils.ALL_SUBSYSTEMS:
            return self.list_connection_for_one_subsystem(request.subsystem,
                                                          request.clear_alerts,
                                                          not context)

        subsystems = list(self.subsys_serial.keys())
        connections = []
        for subsys in subsystems:
            connections_info = self.list_connection_for_one_subsystem(subsys,
                                                                      request.clear_alerts,
                                                                      not context)
            if connections_info.status != 0:
                self.logger.warning(f"Failed listing connections for {subsys}, "
                                    f"will continue with the other subsystems")
            connections += connections_info.connections

        return pb2.connections_info(status=0, error_message=os.strerror(0),
                                    subsystem_nqn=GatewayUtils.ALL_SUBSYSTEMS,
                                    connections=connections)

    def is_host_connected(self, subsystem, hostnqn) -> bool:
        if not hostnqn:
            return False
        if hostnqn == "*":
            return False
        connections = self.list_connection_for_one_subsystem(subsystem, False, False)
        for one_conn in connections.connections:
            if one_conn.connected and one_conn.nqn == hostnqn:
                return True
        return False

    def list_connection_for_one_subsystem(self, subsystem, clear_alerts, use_cache):
        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling list_connection_for_one_subsystem()"
        try:
            qpair_ret = self.spdk_rpc_client.nvmf_subsystem_get_qpairs(nqn=subsystem)
            self.logger.debug(f"list_connections get_qpairs: {qpair_ret}")
        except Exception as ex:
            errmsg = f"Failure listing connections for {subsystem}, can't get qpairs"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing connections for {subsystem}, " \
                         f"can't get qpairs: {resp['message']}"
            return pb2.connections_info(status=status, error_message=errmsg, connections=[])

        try:
            ctrl_ret = self.spdk_rpc_client.nvmf_subsystem_get_controllers(nqn=subsystem)
            self.logger.debug(f"list_connections get_controllers: {ctrl_ret}")
        except Exception as ex:
            errmsg = f"Failure listing connections for {subsystem}, can't get controllers"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing connections for {subsystem}, " \
                         f"can't get controllers: {resp['message']}"
            return pb2.connections_info(status=status, error_message=errmsg, connections=[])

        subsys_ret = None
        if use_cache:
            subsys_ret = self.subsystems_cache.get_one_subsystem(subsystem)
            self.logger.debug(f"list_connections subsystems (cache): {subsys_ret}")

        if not subsys_ret:
            try:
                subsys_ret = self.spdk_rpc_client.nvmf_get_subsystems(nqn=subsystem)
                self.logger.debug(f"list_connections subsystems: {subsys_ret}")
            except Exception as ex:
                errmsg = f"Failure listing connections for {subsystem}, can't get subsystem"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure listing connections for {subsystem}, " \
                             f"can't get subsystem: {resp['message']}"
                return pb2.connections_info(status=status, error_message=errmsg, connections=[])

            parsed_ret = []
            for s in subsys_ret:
                subsys = pb2.subsystem()
                try:
                    json_format.Parse(json.dumps(s), subsys, ignore_unknown_fields=True)
                except Exception:
                    self.logger.exception(f"Failure listing connections, "
                                          f"can't parse subsystem {s}")
                    return pb2.connections_info(status=errno.EINVAL,
                                                error_message="Failure listing connections, "
                                                              "can't parse subsystem",
                                                connections=[])
                parsed_ret.append(subsys)
            subsys_ret = parsed_ret

        if not subsys_ret:
            subsys_ret = []
        connections = []
        host_nqns = []
        for s in subsys_ret:
            try:
                if s.nqn != subsystem:
                    self.logger.warning(f"Got subsystem {s.nqn} instead of {subsystem}, ignore")
                    continue
                try:
                    subsys_hosts = s.hosts
                except Exception:
                    subsys_hosts = []
                    pass
                for h in subsys_hosts:
                    try:
                        host_nqns.append(h.nqn)
                    except Exception:
                        pass
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        disconnected_hosts = self.host_info.get_keepalive_timeout_disconnections(subsystem)
        if disconnected_hosts:
            self.logger.debug(f"disconnected hosts: {disconnected_hosts}")
        for h in disconnected_hosts:
            if h not in host_nqns:
                host_nqns.append(h)

        subsystem_has_dhchap_key = self.host_info.does_subsystem_have_dhchap_key(subsystem)
        for conn in ctrl_ret:
            try:
                traddr = ""
                trsvcid = 0
                adrfam = ""
                trtype = "TCP"
                hostnqn = conn["hostnqn"]
                found = False
                secure = False
                psk = self.host_info.is_psk_host(subsystem, hostnqn)
                dhchap = self.host_info.is_dhchap_host(subsystem, hostnqn)
                if self.host_info.is_dhchap_ctrlr_host(subsystem, hostnqn):
                    dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.host_specific
                elif subsystem_has_dhchap_key:
                    dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.subsystem_implicit
                else:
                    dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.no_key

                for qp in qpair_ret:
                    try:
                        if qp["cntlid"] != conn["cntlid"]:
                            continue
                        if qp["state"] != "enabled":
                            self.logger.debug(f"Qpair {qp} is not enabled")
                            continue
                        addr = qp["listen_address"]
                        if not addr:
                            continue
                        traddr = addr["traddr"]
                        if not traddr:
                            continue
                        trsvcid = int(addr["trsvcid"])
                        try:
                            trtype = addr["trtype"].upper()
                        except Exception:
                            pass
                        try:
                            adrfam = addr["adrfam"].lower()
                        except Exception:
                            pass
                        found = True
                        break
                    except Exception:
                        self.logger.exception(f"Got exception while parsing qpair: {qp}")
                        pass

                if not found:
                    self.logger.debug(f"Can't find active qpair for connection {conn}")
                    continue

                if subsystem in self.subsystem_listeners:
                    for active in [False, True]:
                        lstnr = (adrfam, traddr, trsvcid, True, active)
                        if lstnr in self.subsystem_listeners[subsystem]:
                            secure = True
                            break

                if not trtype:
                    trtype = "TCP"
                if not adrfam:
                    adrfam = "ipv4"
                was_ka_timeout = \
                    self.host_info.was_host_disconnected_due_to_keepalive_timeout(
                        subsystem, hostnqn)
                one_conn = pb2.connection(nqn=hostnqn, connected=True,
                                          traddr=traddr, trsvcid=trsvcid,
                                          trtype=trtype, adrfam=adrfam,
                                          qpairs_count=conn["num_io_qpairs"],
                                          controller_id=conn["cntlid"],
                                          secure=secure, use_psk=psk, use_dhchap=dhchap,
                                          dhchap_controller_origin=dhchap_ctrlr,
                                          subsystem=subsystem,
                                          disconnected_due_to_keepalive_timeout=was_ka_timeout)
                connections.append(one_conn)
                if hostnqn in host_nqns:
                    host_nqns.remove(hostnqn)
            except Exception:
                self.logger.exception(f"{conn=} parse error")
                pass

        for nqn in host_nqns:
            psk = self.host_info.is_psk_host(subsystem, nqn)
            dhchap = self.host_info.is_dhchap_host(subsystem, nqn)
            if self.host_info.is_dhchap_ctrlr_host(subsystem, nqn):
                dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.host_specific
            elif subsystem_has_dhchap_key:
                dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.subsystem_implicit
            else:
                dhchap_ctrlr = pb2.DHCHAPControllerKeyOrigin.no_key
            was_ka_timeout = \
                self.host_info.was_host_disconnected_due_to_keepalive_timeout(
                    subsystem, nqn)
            one_conn = pb2.connection(nqn=nqn, connected=False, traddr="<n/a>", trsvcid=0,
                                      qpairs_count=-1, controller_id=-1,
                                      use_psk=psk, use_dhchap=dhchap,
                                      dhchap_controller_origin=dhchap_ctrlr,
                                      subsystem=subsystem,
                                      disconnected_due_to_keepalive_timeout=was_ka_timeout)
            connections.append(one_conn)

        if clear_alerts:
            self.host_info.reset_host_keepalive_timeout_disconnection(subsystem)

        return pb2.connections_info(status=0, error_message=os.strerror(0),
                                    subsystem_nqn=subsystem, connections=connections)

    def list_connections(self, request, context=None):
        err_prefix = "Failure listing connections: "
        return self.execute_grpc_function(self.list_connections_safe, request, context, err_prefix)

    def _check_for_listener_security_contradiction(self, req_addr, req_adrfam,
                                                   req_port, req_secure):
        any_host = GatewayUtils.is_any_host_address(req_addr, req_adrfam)
        for nqn in self.subsystem_listeners:
            for (adrfam, addr, port, secure, _) in self.subsystem_listeners[nqn]:
                if req_port != port:
                    continue
                if not any_host and not GatewayUtils.is_any_host_address(addr, adrfam):
                    if req_addr != addr:
                        continue
                if secure != req_secure:
                    return (nqn, addr, port, secure)
        return None

    def create_listener_safe(self, request, context):
        """Creates a listener for a subsystem at a given IP/Port."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling create_listener_safe()"
        ret = True
        if not request.trsvcid:
            if request.secure:
                request.trsvcid = GatewayService.SECURE_LISTENER_PORT_DEFAULT
            else:
                request.trsvcid = GatewayService.LISTENER_PORT_DEFAULT
            self.logger.debug(f"Port was set to default value {request.trsvcid}")
        create_listener_error_prefix = f"Failure adding {request.nqn} listener at " \
                                       f"{request.traddr}:{request.trsvcid}"

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam is None:
            errmsg = f"{create_listener_error_prefix}: Unknown address family {request.adrfam}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # Adding the listener to the OMAP for future use only makes sense when we're
        # not in update()
        if not context:
            request.verify_host_name = True

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to create {request.host_name}"
                         f" TCP {adrfam} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}, secure: {request.secure},"
                         f" verify host name: {request.verify_host_name},"
                         f" force: {request.force},"
                         f" context: {context}{peer_msg}")

        traddr = GatewayUtils.unescape_address(request.traddr)

        if not request.nqn:
            errmsg = f"{create_listener_error_prefix}: missing subsystem NQN"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.nqn):
            errmsg = f"{create_listener_error_prefix}: Can't create a " \
                     f"listener for a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        # If this is not set the subsystem was not created yet
        if request.nqn not in self.subsys_serial:
            errmsg = f"{create_listener_error_prefix}: can't find subsystem {request.nqn}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.host_name):
            errmsg = f"{create_listener_error_prefix}: Host name " \
                     f"\"{request.host_name}\" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayUtils.is_valid_host_name(request.host_name):
            errmsg = f"{create_listener_error_prefix}: Host name " \
                     f"\"{request.host_name}\" is invalid"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        err = GatewayUtils.is_valid_ip_address(traddr, adrfam)
        if err:
            errmsg = f"{create_listener_error_prefix}: {err}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.secure and self.host_info.is_any_host_allowed(request.nqn):
            errmsg = f"{create_listener_error_prefix}: Secure channel is only allowed " \
                     f"for subsystems in which \"allow any host\" is off"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if self.force_tls and not request.secure:
            errmsg = f"{create_listener_error_prefix}: Secure channel must be used"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.trsvcid and request.trsvcid > 0xffff:
            errmsg = f"{create_listener_error_prefix}: trsvcid value must be smaller than 65536"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        lstnr_contradiction = None
        if context:
            lstnr_contradiction = self._check_for_listener_security_contradiction(traddr,
                                                                                  adrfam,
                                                                                  request.trsvcid,
                                                                                  request.secure)
        if lstnr_contradiction is not None:
            self.logger.debug(f"listener contradiction: {lstnr_contradiction}")
            sec_txt = "secure" if lstnr_contradiction[3] else "insecure"
            if request.force:
                self.logger.warning(f"The listener clashes with the existing {sec_txt} listener"
                                    f" on {lstnr_contradiction[0]}, address "
                                    f"{lstnr_contradiction[1]}:"
                                    f"{lstnr_contradiction[2]}, will continue as the \"force\" "
                                    f"parameter was used")
            else:
                errmsg = f"{create_listener_error_prefix}: The listener clashes with the " \
                         f"existing {sec_txt} listener on {lstnr_contradiction[0]}, address " \
                         f"{lstnr_contradiction[1]}:" \
                         f"{lstnr_contradiction[2]}, either remove that listener or use the "\
                         f"\"force\" parameter"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EBUSY, error_message=errmsg)

        add_listener_args = {}
        add_listener_args["nqn"] = request.nqn
        add_listener_args["listen_address"] = {"trtype": "TCP",
                                               "traddr": traddr,
                                               "trsvcid": str(request.trsvcid),
                                               "adrfam": adrfam}
        add_listener_args["secure_channel"] = request.secure

        listener_created = False
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if request.verify_host_name and request.host_name != self.host_name:
                if context:
                    errmsg = f"{create_listener_error_prefix}: Gateway's host name must " \
                             f"match current host ({self.host_name})"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
                else:
                    errmsg = f"Listener not created as gateway's host name " \
                             f"{self.host_name} differs from requested host " \
                             f"{request.host_name}"
                    self.logger.info(errmsg)
                    return pb2.req_status(status=0, error_message=errmsg)

            assert (not request.verify_host_name) or request.host_name == self.host_name
            if request.host_name == self.host_name:
                try:
                    for secure in [False, True]:
                        for active in [False, True]:
                            lstnr = (adrfam, traddr, request.trsvcid, secure, active)
                            if lstnr in self.subsystem_listeners[request.nqn]:
                                errmsg = f"{create_listener_error_prefix}: Subsystem already " \
                                         f"listens on address {request.traddr}:{request.trsvcid}"
                                self.logger.error(errmsg)
                                return pb2.req_status(status=errno.EEXIST, error_message=errmsg)

                    if self.verify_listener_ip:
                        nics = NICS(self.logger, True)
                        if not nics.verify_ip_address(traddr, adrfam):
                            for dev in nics.adapters.values():
                                self.logger.debug(f"NIC: {dev}")
                            errmsg = f"{create_listener_error_prefix}: Address " \
                                     f"{traddr} is not available as an " \
                                     f"{adrfam.upper()} address"
                            self.logger.error(errmsg)
                            return pb2.req_status(status=errno.EADDRNOTAVAIL, error_message=errmsg)

                    ret = self.spdk_rpc_client.nvmf_subsystem_add_listener(**add_listener_args)
                    self.logger.debug(f"create_listener: {ret}")
                    listener_created = ret
                except Exception as ex:
                    self.logger.exception(create_listener_error_prefix)
                    errmsg = f"{create_listener_error_prefix}:\n{ex}"
                    resp = self.parse_json_exeption(ex)
                    status = errno.EINVAL
                    if resp:
                        status = resp["code"]
                        errmsg = f"{create_listener_error_prefix}: {resp['message']}"
                    return pb2.req_status(status=status, error_message=errmsg)
            elif not request.verify_host_name:
                self.logger.info(f"Gateway's host name \"{self.host_name}\" differs from "
                                 f"requested one \"{request.host_name}\". Listener will "
                                 f"be stashed to be used later by the right gateway.")
                ret = True

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(create_listener_error_prefix)
                return pb2.req_status(status=errno.EINVAL,
                                      error_message=create_listener_error_prefix)

            self.subsystem_listeners[request.nqn].add((adrfam, traddr,
                                                       request.trsvcid, request.secure,
                                                       request.host_name == self.host_name))
            if listener_created:
                try:
                    self.logger.debug(f"create_listener nvmf_subsystem_listener_set_ana_state "
                                      f"{request=} set inaccessible for all ana groups")
                    _ana_state = "inaccessible"
                    ret = self.spdk_rpc_client.nvmf_subsystem_listener_set_ana_state(
                        nqn=request.nqn,
                        ana_state=_ana_state,
                        listen_address={"trtype": "TCP",
                                        "traddr": traddr,
                                        "trsvcid": str(request.trsvcid),
                                        "adrfam": adrfam})
                    self.logger.debug(f"create_listener "
                                      f"nvmf_subsystem_listener_set_ana_state response {ret=}")

                    # have been provided with ana state for this nqn prior to creation
                    # update optimized ana groups
                    if self.ana_map[request.nqn]:
                        for x in range(self.subsys_max_ns[request.nqn]):
                            ana_grp = x + 1
                            if ana_grp in self.ana_map[request.nqn]:
                                if self.ana_map[request.nqn][ana_grp] == pb2.ana_state.OPTIMIZED:
                                    _ana_state = "optimized"
                                    self.logger.debug(f"using ana_map: set listener on nqn: "
                                                      f"{request.nqn} "
                                                      f"ana state: {_ana_state} for "
                                                      f"group: {ana_grp}")
                                    rc = self.spdk_rpc_client.nvmf_subsystem_listener_set_ana_state(
                                        nqn=request.nqn,
                                        ana_state=_ana_state,
                                        listen_address={"trtype": "TCP",
                                                        "traddr": traddr,
                                                        "trsvcid": str(request.trsvcid),
                                                        "adrfam": adrfam},
                                        anagrpid=ana_grp)
                                    self.logger.debug(f"create_listener "
                                                      f"nvmf_subsystem_listener_set_ana_state "
                                                      f"response {rc=}")

                except Exception as ex:
                    errmsg = f"{create_listener_error_prefix}: Error setting ANA state"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    resp = self.parse_json_exeption(ex)
                    status = errno.EINVAL
                    if resp:
                        status = resp["code"]
                        errmsg = f"{create_listener_error_prefix}: Error setting ANA state: " \
                                 f"{resp['message']}"
                    return pb2.req_status(status=status, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    # this is needed so 0 values will be written to output buffer
                    if not request.adrfam:
                        request.adrfam = 0
                    request.traddr = traddr
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True,
                        including_default_value_fields=True,
                        use_integers_for_enums=True)
                    self.gateway_state.add_listener(request.nqn,
                                                    request.host_name,
                                                    "TCP", traddr,
                                                    request.trsvcid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting listener {request.traddr}:{request.trsvcid}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if listener_created:
            return pb2.req_status(status=0, error_message=os.strerror(0))
        else:
            return pb2.req_status(status=errno.EREMOTE,
                                  error_message="Host name mismatch, listener will only be "
                                                "active when the appropriate gateway is up")

    def create_listener(self, request, context=None):
        err_prefix = f"Failure adding {request.nqn} listener at " \
                     f"{request.traddr}:{request.trsvcid}: "
        return self.execute_grpc_function(self.create_listener_safe, request, context, err_prefix)

    def remove_listener_from_state(self, nqn, host_name, traddr, port, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        assert context is None or self.omap_lock.write_locked_by_me(), \
            f"OMAP is unlocked when calling remove_listener_from_state()\n" \
            f"in thread: {threading.get_native_id()}. Locked by: " \
            f"{self.omap_lock.locked_by}, with cookie: {self.omap_lock.lock_cookie}, " \
            f"locked: {self.omap_lock.is_exclusively_locked}"

        host_name = host_name.strip()
        listener_hosts = []
        if host_name == "*":
            state = self.gateway_state.local.get_state()
            listener_prefix = GatewayState.build_partial_listener_key(nqn, None)
            for key, val in state.items():
                if not key.startswith(listener_prefix):
                    continue
                try:
                    listener = json_format.Parse(val, pb2.create_listener_req(),
                                                 ignore_unknown_fields=True)
                    listener_nqn = listener.nqn
                    if listener_nqn != nqn:
                        self.logger.warning(f"Got subsystem {listener_nqn} "
                                            f"instead of {nqn}, ignore")
                        continue
                    elif listener.traddr != traddr:
                        continue
                    elif listener.trsvcid != port:
                        continue
                    listener_hosts.append(listener.host_name)
                except Exception:
                    self.logger.exception(f"Got exception while parsing {val}")
        else:
            listener_hosts.append(host_name)

        # Update gateway state
        req_status = None
        for one_host in listener_hosts:
            try:
                self.gateway_state.remove_listener(nqn, one_host, "TCP", traddr, port)
            except Exception as ex:
                errmsg = f"Error persisting deletion of {one_host} listener " \
                         f"{traddr}:{port} from {nqn}"
                self.logger.exception(errmsg)
                if not req_status:
                    errmsg = f"{errmsg}:\n{ex}"
                    req_status = pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        if not req_status:
            req_status = pb2.req_status(status=0, error_message=os.strerror(0))

        return req_status

    def delete_listener_safe(self, request, context):
        """Deletes a listener from a subsystem at a given IP/Port."""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling delete_listener_safe()"
        ret = True
        delete_listener_error_prefix = f"Failed to delete listener {request.traddr}:" \
                                       f"{request.trsvcid} from {request.nqn}"

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam is None:
            errmsg = f"{delete_listener_error_prefix}: Unknown address family {request.adrfam}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        traddr = GatewayUtils.unescape_address(request.traddr)
        delete_listener_error_prefix = f"Failed to delete listener {traddr}:" \
                                       f"{request.trsvcid} from {request.nqn}"

        peer_msg = self.get_peer_message(context)
        force_msg = " forcefully" if request.force else ""
        host_msg = "all hosts" if request.host_name == "*" else f"host {request.host_name}"

        self.logger.info(f"Received request to delete TCP listener of {host_msg}"
                         f" for subsystem {request.nqn} at"
                         f" {traddr}:{request.trsvcid}{force_msg},"
                         f" context: {context}{peer_msg}")

        if request.host_name == "*" and not request.force:
            errmsg = f"{delete_listener_error_prefix}: Must use the \"--force\"" \
                     f" parameter when setting the host name to \"*\"."
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.nqn):
            errmsg = f"{delete_listener_error_prefix}: " \
                     f"Can't delete a listener from a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.trsvcid > 0xffff:
            errmsg = f"{delete_listener_error_prefix}: trsvcid value must be smaller than 65536"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.force:
            list_conn_req = pb2.list_connections_req(subsystem=request.nqn)
            list_conn_ret = self.list_connections_safe(list_conn_req, context)
            if list_conn_ret.status != 0:
                errmsg = f"{delete_listener_error_prefix}: " \
                         f"Can't verify there are no active connections for this address"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOTEMPTY, error_message=errmsg)
            for conn in list_conn_ret.connections:
                if not conn.connected:
                    continue
                if conn.traddr != traddr:
                    continue
                if conn.trsvcid != request.trsvcid:
                    continue
                errmsg = f"{delete_listener_error_prefix}: There are active connections for " \
                         f"{traddr}:{request.trsvcid}. Deleting the listener terminates " \
                         f"active connections. You can continue to delete the listener by " \
                         f"adding the \"--force\" parameter."
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOTEMPTY, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                is_there = False
                is_active = False
                if request.nqn in self.subsystem_listeners:
                    for secur in [False, True]:
                        if is_there:
                            break
                        for active in [False, True]:
                            lstnr = (adrfam, traddr, request.trsvcid, secur, active)
                            if lstnr in self.subsystem_listeners[request.nqn]:
                                is_there = True
                                is_active = active
                                break
                if not is_there:
                    errmsg = f"{delete_listener_error_prefix}: Listener not found"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

                if context:
                    state = self.gateway_state.local.get_state()
                    listener_prefix = GatewayState.build_partial_listener_key(
                        request.nqn, None)
                    is_in_omap = False
                    for key, val in state.items():
                        if not key.startswith(listener_prefix):
                            continue
                        try:
                            lstnr = json_format.Parse(val, pb2.create_listener_req(),
                                                      ignore_unknown_fields=True)
                            if lstnr.traddr == traddr and lstnr.trsvcid == request.trsvcid:
                                is_in_omap = True
                                break
                        except Exception:
                            self.logger.exception(f"Got exception while parsing {val}")
                            continue
                    if not is_in_omap:
                        errmsg = f"{delete_listener_error_prefix}: Listener was created " \
                                 f"automatically as part of the subsystem's network mask. " \
                                 f"To remove it, modify the network mask."
                        self.logger.error(errmsg)
                        return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

                if request.host_name == self.host_name or request.force:
                    if is_active:
                        ret = self.spdk_rpc_client.nvmf_subsystem_remove_listener(
                            nqn=request.nqn,
                            listen_address={"trtype": "TCP",
                                            "traddr": traddr,
                                            "trsvcid": str(request.trsvcid),
                                            "adrfam": adrfam}
                        )
                        self.logger.debug(f"delete_listener: {ret}")
                    if request.nqn in self.subsystem_listeners:
                        for secur in [False, True]:
                            for active in [False, True]:
                                lstnr = (adrfam, traddr, request.trsvcid, secur, active)
                                if lstnr in self.subsystem_listeners[request.nqn]:
                                    self.subsystem_listeners[request.nqn].remove(lstnr)
                else:
                    errmsg = f"{delete_listener_error_prefix}: Gateway's host name must " \
                             f"match current host ({self.host_name}). You can continue to " \
                             f"delete the listener by adding the \"--force\" parameter."
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            except Exception as ex:
                self.logger.exception(delete_listener_error_prefix)
                # It's OK for SPDK to fail in case we used a different host name,
                # just continue to remove from OMAP
                if request.host_name == self.host_name:
                    errmsg = f"{delete_listener_error_prefix}:\n{ex}"
                    self.remove_listener_from_state(request.nqn, request.host_name,
                                                    traddr, request.trsvcid, context)
                    resp = self.parse_json_exeption(ex)
                    status = errno.EINVAL
                    if resp:
                        status = resp["code"]
                        errmsg = f"{delete_listener_error_prefix}: {resp['message']}"
                    return pb2.req_status(status=status, error_message=errmsg)
                ret = True

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(delete_listener_error_prefix)
                self.remove_listener_from_state(request.nqn, request.host_name,
                                                traddr, request.trsvcid, context)
                return pb2.req_status(status=errno.EINVAL,
                                      error_message=delete_listener_error_prefix)

            return self.remove_listener_from_state(request.nqn, request.host_name,
                                                   traddr, request.trsvcid, context)

    def delete_listener(self, request, context=None):
        err_prefix = f"Failed to delete listener {request.traddr}:" \
                     f"{request.trsvcid} from {request.nqn}: "
        return self.execute_grpc_function(self.delete_listener_safe, request, context, err_prefix)

    def _is_active_listener(self, subsystem_nqn, listener):
        try:
            adrfam = listener.adrfam
            if isinstance(adrfam, int):
                adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, adrfam)
        except KeyError:
            adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, 0)
            self.logger.debug(f"Missing adrfam in entry use default value: {adrfam}")
        adrfam = adrfam.lower()
        secure = listener.secure
        active = False
        if subsystem_nqn in self.subsystem_listeners:
            traddr = GatewayUtils.unescape_address_if_ipv6(listener.traddr, adrfam)
            lookfor = (adrfam, traddr,
                       int(listener.trsvcid), secure, False)
            if lookfor in self.subsystem_listeners[subsystem_nqn]:
                active = False
            else:
                lookfor = (adrfam, traddr,
                           int(listener.trsvcid), secure, True)
                if lookfor in self.subsystem_listeners[subsystem_nqn]:
                    active = True
                else:
                    self.logger.warning(f"Can't find listener "
                                        f"{listener} in local list")
        return active

    def list_listeners(self, request, context):
        """List listeners."""

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to list listeners for {request.subsystem}, "
                         f"context: {context}{peer_msg}")

        # If this is not set the subsystem was not created yet
        if request.subsystem not in self.subsys_serial:
            errmsg = f"Failure listing listeners: No such subsystem \"{request.subsystem}\""
            self.logger.error(errmsg)
            return pb2.listeners_info(status=errno.ENOENT, error_message=errmsg, listeners=[])

        listeners = []
        omap_listeners = set()
        state = self.gateway_state.local.get_state()
        listener_prefix = GatewayState.build_partial_listener_key(request.subsystem, None)
        for key, val in state.items():
            if not key.startswith(listener_prefix):
                continue
            try:
                listener = json_format.Parse(val, pb2.create_listener_req(),
                                             ignore_unknown_fields=True)
                nqn = listener.nqn
                if nqn != request.subsystem:
                    self.logger.warning(f"Got subsystem {nqn} instead of "
                                        f"{request.subsystem}, ignore")
                    continue

                active = self._is_active_listener(request.subsystem, listener)
                one_listener = pb2.listener_info(host_name=listener.host_name,
                                                 trtype="TCP",
                                                 adrfam=listener.adrfam,
                                                 traddr=listener.traddr,
                                                 trsvcid=listener.trsvcid,
                                                 secure=listener.secure,
                                                 active=active,
                                                 manual=True)
                listeners.append(one_listener)
                listener_key = (listener.traddr, listener.trsvcid)
                omap_listeners.add(listener_key)
            except Exception:
                self.logger.exception(f"Got exception while parsing {val}")
                continue
        try:
            subsys_key = GatewayState.build_subsystem_key(request.subsystem)
            if subsys_key not in state:
                err_msg = (f"Subsystem {request.subsystem} not found in local gateway state")
                raise RuntimeError(err_msg)
            state_subsys = state[subsys_key]
            subsystem = json.loads(state_subsys)
            if subsystem and subsystem.get('network_mask'):
                pool = self.config.get("ceph", "pool")
                group = self.config.get("gateway", "group")
                nvmemon_listeners = self.ceph_utils.get_gw_listeners(pool, group)
                if request.subsystem in nvmemon_listeners:
                    subsystem_listeners = nvmemon_listeners[request.subsystem]
                    secure = subsystem.get('secure_listeners', False)
                    for _listener in subsystem_listeners:
                        listener = {
                            "host_name": _listener["gw_id"],
                            "adrfam": (_listener["address_family"] or '').lower(),
                            "trsvcid": int(_listener["svcid"] or 0),
                            "nqn": request.subsystem,
                            "traddr": _listener["address"],
                            "secure": secure,
                        }
                        listener_key = (listener["traddr"], listener["trsvcid"])
                        if listener_key in omap_listeners:
                            continue
                        hostname = GatewayUtils.get_hostname(listener["traddr"], self.logger)
                        if hostname:
                            listener["host_name"] = hostname
                        listener_json = json.dumps(listener)
                        listener = json_format.Parse(listener_json,
                                                     pb2.create_listener_req(),
                                                     ignore_unknown_fields=True)
                        active = self._is_active_listener(request.subsystem, listener)
                        one_listener = pb2.listener_info(
                            host_name=listener.host_name,
                            trtype="TCP",
                            adrfam=listener.adrfam,
                            traddr=listener.traddr,
                            trsvcid=listener.trsvcid,
                            secure=secure, active=active, manual=False)
                        listeners.append(one_listener)
        except Exception as e:
            errmsg = f"Failure when displaying listener info from 'nvme-gw listeners' cmd: {e}"
            self.logger.exception(errmsg)
            return pb2.listeners_info(status=errno.EINVAL, error_message=errmsg,
                                      listeners=listeners)

        return pb2.listeners_info(status=0, error_message=os.strerror(0), listeners=listeners)

    def show_gateway_listeners_info_safe(self, request, context):
        """Show gateway's listeners info."""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling show_gateway_listeners_info_safe()"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to show gateway listeners info for "
                         f"{request.subsystem_nqn}, context: {context}{peer_msg}")

        if self.ana_grp_state[0] != pb2.ana_state.INACCESSIBLE:
            errmsg = "Internal error, we shouldn't have a real state for load balancing group 0"
            self.logger.error(errmsg)
            return pb2.gateway_listeners_info(status=errno.EINVAL,
                                              error_message=errmsg,
                                              gw_listeners=[])

        try:
            ret = self.spdk_rpc_client.nvmf_subsystem_get_listeners(nqn=request.subsystem_nqn)
            self.logger.debug(f"get_listeners: {ret}")
        except Exception as ex:
            errmsg = "Failure listing gateway listeners"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.ENODEV
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing gateway listeners: {resp['message']}"
            return pb2.gateway_listeners_info(status=status,
                                              error_message=errmsg,
                                              gw_listeners=[])

        gw_listeners = []
        for lstnr in ret:
            try:
                is_secure = False
                is_active = False
                found = False
                if request.subsystem_nqn in self.subsystem_listeners:
                    for secure in [False, True]:
                        if found:
                            break
                        for active in [False, True]:
                            local_lstnr = (lstnr["address"]["adrfam"].lower(),
                                           lstnr["address"]["traddr"],
                                           int(lstnr["address"]["trsvcid"]),
                                           secure,
                                           active)
                            if local_lstnr in self.subsystem_listeners[request.subsystem_nqn]:
                                found = True
                                is_secure = secure
                                is_active = active
                                break
                if not found:
                    self.logger.warning(f"Can't find listener {lstnr} in local list")
                lstnr_part = pb2.listener_info(host_name=self.host_name,
                                               trtype=lstnr["address"]["trtype"].upper(),
                                               adrfam=lstnr["address"]["adrfam"].lower(),
                                               traddr=lstnr["address"]["traddr"],
                                               trsvcid=int(lstnr["address"]["trsvcid"]),
                                               secure=is_secure, active=is_active)
            except Exception:
                self.logger.exception(f"Error getting address from {lstnr}")
                continue

            ana_states = []
            try:
                for ana_state in lstnr["ana_states"]:
                    spdk_group = ana_state["ana_group"]
                    if spdk_group > self.max_ana_grps:
                        continue
                    spdk_state = ana_state["ana_state"]
                    spdk_state_enum_val = GatewayEnumUtils.get_value_from_key(pb2.ana_state,
                                                                              spdk_state.upper())
                    if spdk_state_enum_val is None:
                        self.logger.error(f"Unknown state \"{spdk_state}\" for "
                                          f"load balancing group {spdk_group} in SPDK")
                        continue

                    ana_states.append(pb2.ana_group_state(grp_id=spdk_group,
                                                          state=spdk_state_enum_val))
                    if spdk_group in self.ana_grp_state:
                        if self.ana_grp_state[spdk_group] != spdk_state_enum_val:
                            gw_state_str = GatewayEnumUtils.get_key_from_value(
                                pb2.ana_state, self.ana_grp_state[spdk_group])
                            if gw_state_str is None:
                                self.logger.error(f'State for load balancing group {spdk_group} '
                                                  f'is "{self.ana_grp_state[spdk_group]}" '
                                                  f'but is {spdk_state_enum_val} in SPDK')
                            else:
                                self.logger.error(f'State for load balancing group {spdk_group} '
                                                  f'is "{gw_state_str}" '
                                                  f'but is "{spdk_state}" in SPDK')
            except Exception:
                self.logger.exception(f"Error parsing load balancing state {ana_state}")
                continue

            gw_lstnr = pb2.gateway_listener_info(listener=lstnr_part, lb_states=ana_states)
            gw_listeners.append(gw_lstnr)

        return pb2.gateway_listeners_info(status=0, error_message=os.strerror(0),
                                          gw_listeners=gw_listeners)

    def show_gateway_listeners_info(self, request, context=None):
        return self.execute_grpc_function(self.show_gateway_listeners_info_safe, request, context)

    def list_subsystems_safe(self, request, context):
        """List subsystems."""

        assert self.spdk_rpc_subsystems_lock.locked(), "Subsystems RPC is unlocked when calling " \
                                                       "list_subsystems_safe()"
        peer_msg = self.get_peer_message(context)
        log_level = logging.INFO if context else logging.DEBUG
        if request.subsystem_nqn:
            self.logger.log(log_level,
                            f"Received request to list subsystem {request.subsystem_nqn}, "
                            f"context: {context}{peer_msg}")
        else:
            if request.serial_number:
                self.logger.log(log_level,
                                f"Received request to list the subsystem with serial number "
                                f"{request.serial_number}, context: {context}{peer_msg}")
            else:
                self.logger.log(log_level,
                                f"Received request to list all subsystems, context: "
                                f"{context}{peer_msg}")

        subsystems = []
        cache_subsystems = []
        try:
            if request.subsystem_nqn:
                ret = self.spdk_rpc_subsystems_client.nvmf_get_subsystems(nqn=request.subsystem_nqn)
            else:
                ret = self.spdk_rpc_subsystems_client.nvmf_get_subsystems()
            self.logger.debug(f"list_subsystems: {ret}")
        except Exception as ex:
            errmsg = "Failure listing subsystems"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.ENODEV
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing subsystems: {resp['message']}"
            return pb2.subsystems_info_cli(status=status, error_message=errmsg, subsystems=[])

        if not ret:
            ret = []
        for s in ret:
            try:
                if s["subtype"] == "NVMe":
                    s["namespace_count"] = len(s["namespaces"])
                    s["network_mask"] = []
                    if s["nqn"] in self.subsys_network:
                        s["network_mask"] = list(self.subsys_network[s['nqn']])
                    s["enable_ha"] = True
                    s["has_dhchap_key"] = self.host_info.does_subsystem_have_dhchap_key(s["nqn"])
                    s["created_without_key"] = \
                        self.host_info.was_subsystem_created_without_key(s["nqn"])
                    for n in s["namespaces"]:
                        bdev = n["bdev_name"]
                        with self.shared_state_lock:
                            nonce = self.cluster_nonce[self.bdev_cluster[bdev]]
                        n["nonce"] = nonce
                        find_ret = self.subsystem_nsid_bdev_and_uuid.find_namespace(
                            s["nqn"], n["nsid"])
                        n["auto_visible"] = find_ret.auto_visible
                        n["hosts"] = find_ret.host_list
                else:
                    s["namespace_count"] = 0
                    s["enable_ha"] = False
                    s["has_dhchap_key"] = False
                    s["network_mask"] = []
                # Parse the JSON dictionary into the protobuf message
                subsystem = pb2.subsystem_cli()
                json_format.Parse(json.dumps(s), subsystem, ignore_unknown_fields=True)
                if not request.serial_number or s["serial_number"] == request.serial_number:
                    subsystems.append(subsystem)
                if not request.subsystem_nqn:
                    cache_subsystem = pb2.subsystem()
                    json_format.Parse(json.dumps(s), cache_subsystem, ignore_unknown_fields=True)
                    cache_subsystems.append(cache_subsystem)
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        # Only set cache if we've listed all subsystems
        if not request.subsystem_nqn:
            self.subsystems_cache.set_subsystems(pb2.subsystems_info(subsystems=cache_subsystems))

        return pb2.subsystems_info_cli(status=0, error_message=os.strerror(0),
                                       subsystems=subsystems)

    def get_subsystems(self, request, context):
        return self.subsystems_cache.get_subsystems()

    def list_subsystems(self, request, context=None):
        with self.spdk_rpc_subsystems_lock:
            return self.list_subsystems_safe(request, context)

    def change_subsystem_key_safe(self, request, context):
        """Change subsystem key inband authentication key."""
        peer_msg = self.get_peer_message(context)
        cmd = "change" if request.dhchap_key else "delete"
        cmd2 = "changing" if request.dhchap_key else "deleting"
        failure_prefix = f"Failure {cmd2} DH-HMAC-CHAP key for subsystem {request.subsystem_nqn}"
        self.logger.info(
            f"Received request to {cmd} inband authentication key for subsystem "
            f"{request.subsystem_nqn}, context: {context}{peer_msg}")

        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Invalid subsystem NQN \"{request.subsystem_nqn}\"," \
                     f" contains invalid characters"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{failure_prefix}: {rc[1]}"
                self.logger.error(errmsg)
                return pb2.req_status(status=rc[0], error_message=errmsg)

        if context and self.verify_keys:
            if request.dhchap_key:
                rc = GatewayKeyUtils.is_valid_dhchap_key(request.dhchap_key)
                if rc[0] != 0:
                    errmsg = f"{failure_prefix}: {rc[1]}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=rc[0], error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: Can't change DH-HMAC-CHAP key for a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if request.dhchap_key and self.host_info.is_any_host_allowed(request.subsystem_nqn):
            errmsg = f"{failure_prefix}: DH-HMAC-CHAP key is " \
                     f"not allowed for subsystems which are open for all hosts. " \
                     f"You need to remove the open access in order to set a key for the subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EACCES, error_message=errmsg)

        if request.dhchap_key:
            hosts = self.host_info.get_hosts_with_dhchap_ctrlr_key(request.subsystem_nqn)
            if hosts:
                first_host = list(hosts.keys())[0]
                errmsg = f"{failure_prefix}: DH-HMAC-CHAP key is " \
                         f"not allowed for subsystems which have a host with a DH-HMAC-CHAP " \
                         f"controller key. " \
                         f"You need to remove host {first_host} DH-HMAC-CHAP controller key in " \
                         f"order to set a key for the subsystem"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            subsys_entry = None
            if context:
                # notice that the local state might not be up to date in case we're in the middle
                # of update() but as the context is not None, we are not in an update(), the OMAP
                # lock made sure that we got here with an updated local state
                state = self.gateway_state.local.get_state()
                if request.dhchap_key:
                    # We set the subsystem key, this requires that all hosts have keys too
                    all_subsys_hosts = self.get_subsystem_hosts(request.subsystem_nqn)
                    for hostnqn in all_subsys_hosts:
                        assert hostnqn, "Shouldn't get an empty host NQN"
                        if not self.host_info.is_dhchap_host(request.subsystem_nqn, hostnqn):
                            errmsg = f"{failure_prefix}: Can't set a subsystem's DH-HMAC-CHAP " \
                                     f"key when it has hosts with no key, like host {hostnqn}"
                            self.logger.error(errmsg)
                            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

                subsys_key = GatewayState.build_subsystem_key(request.subsystem_nqn)
                try:
                    state_subsys = state[subsys_key]
                    subsys_entry = json_format.Parse(state_subsys, pb2.create_subsystem_req(),
                                                     ignore_unknown_fields=True)
                except Exception:
                    errmsg = f"{failure_prefix}: Can't find entry for subsystem " \
                             f"{request.subsystem_nqn}"
                    self.logger.error(errmsg)
                    return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

                assert subsys_entry, f"Can't find entry for subsystem {request.subsystem_nqn}"
                try:
                    key_encrypted = False
                    dhchap_key_for_omap = request.dhchap_key
                    self.host_info.reset_subsystem_created_without_key(request.subsystem_nqn)
                    if context and self.enable_key_encryption and request.dhchap_key:
                        if self.gateway_state.crypto:
                            dhchap_key_for_omap = \
                                self.gateway_state.crypto.encrypt_text(request.dhchap_key)
                            key_encrypted = True
                        else:
                            self.logger.warning(f"No encryption key or the wrong key was found "
                                                f"but we need to encrypt subsystem "
                                                f"{request.subsystem_nqn} "
                                                f"DH-HMAC-CHAP key. Any attempt to add host "
                                                f"access using a DH-HMAC-CHAP key to the subsystem "
                                                f"would fail")
                            dhchap_key_for_omap = GatewayUtilsCrypto.INVALID_KEY_VALUE
                            key_encrypted = False
                            self.host_info.set_subsystem_created_without_key(request.subsystem_nqn)

                    subsys_entry.dhchap_key = dhchap_key_for_omap
                    subsys_entry.key_encrypted = key_encrypted
                    json_req = json_format.MessageToJson(
                        subsys_entry, preserving_proto_field_name=True,
                        including_default_value_fields=True)
                    self.gateway_state.add_subsystem(request.subsystem_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting subsystem key change for {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        hosts = self.host_info.get_hosts_with_dhchap_key(request.subsystem_nqn).copy()
        # We need to change the subsystem key before calling the host change key functions,
        # so the new subsystem key will be used
        # As we change the list now, we have to use a copy having the old values
        if request.dhchap_key:
            self.host_info.add_dhchap_key_to_subsystem(request.subsystem_nqn, request.dhchap_key)
        else:
            self.host_info.remove_dhchap_key_from_subsystem(request.subsystem_nqn)
        for hnqn in hosts.keys():
            change_req = pb2.change_host_key_req(
                subsystem_nqn=request.subsystem_nqn,
                host_nqn=hnqn,
                dhchap_key=hosts[hnqn],
                dhchap_ctrlr_key=GatewayUtilsCrypto.EXISTING_DHCHAP_KEY)
            try:
                self.change_host_key_safe(change_req, context)
            except Exception:
                pass

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def change_subsystem_key(self, request, context=None):
        """Change subsystem key."""
        cmd2 = "changing" if request.dhchap_key else "deleting"
        err_prefix = f"Failure {cmd2} DH-HMAC-CHAP key for subsystem {request.subsystem_nqn}: "
        return self.execute_grpc_function(self.change_subsystem_key_safe, request,
                                          context, err_prefix)

    def get_spdk_nvmf_log_flags_and_level_safe(self, request, context):
        """Gets spdk nvmf log flags, log level and log print level"""

        assert self.rpc_lock.locked(), \
            "RPC is unlocked when calling get_spdk_nvmf_log_flags_and_level_safe()"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get SPDK log flags and level, "
                         f"all_log_flags: {request.all_log_flags}{peer_msg}")
        log_flags = []
        try:
            nvmf_log_flags = {key: value for key,
                              value in self.spdk_rpc_client.log_get_flags().items()
                              if request.all_log_flags or key.startswith('nvmf')}
            for flag, flagvalue in nvmf_log_flags.items():
                pb2_log_flag = pb2.spdk_log_flag_info(name=flag, enabled=flagvalue)
                log_flags.append(pb2_log_flag)
            spdk_log_level = self.spdk_rpc_client.log_get_level()
            spdk_log_print_level = self.spdk_rpc_client.log_get_print_level()
            self.logger.debug(f"spdk log flags: {nvmf_log_flags}, "
                              f"spdk log level: {spdk_log_level}, "
                              f"spdk log print level: {spdk_log_print_level}")
        except Exception as ex:
            errmsg = "Failure getting SPDK log levels and nvmf log flags"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure getting SPDK log levels and nvmf log flags: {resp['message']}"
            return pb2.spdk_nvmf_log_flags_and_level_info(status=status, error_message=errmsg)

        return pb2.spdk_nvmf_log_flags_and_level_info(
            nvmf_log_flags=log_flags,
            log_level=spdk_log_level,
            log_print_level=spdk_log_print_level,
            status=0,
            error_message=os.strerror(0))

    def get_spdk_nvmf_log_flags_and_level(self, request, context=None):
        return self.execute_grpc_function(self.get_spdk_nvmf_log_flags_and_level_safe,
                                          request, context)

    def set_spdk_nvmf_logs_safe(self, request, context):
        """Enables spdk nvmf logs"""
        log_level = None
        print_level = None
        ret_log = False
        ret_print = False

        assert self.rpc_lock.locked(), "RPC is unlocked when calling set_spdk_nvmf_logs_safe()"
        peer_msg = self.get_peer_message(context)
        if GatewayService.is_optional_field_in_message(request, "log_level"):
            log_level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, request.log_level)
            if log_level is None:
                errmsg = f"Unknown log level {request.log_level}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayService.is_optional_field_in_message(request, "print_level"):
            print_level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, request.print_level)
            if print_level is None:
                errmsg = f"Unknown print level {request.print_level}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        self.logger.info(f"Received request to set SPDK logs: log_level: {log_level}, "
                         f"print_level: {print_level}, extra: {request.extra_log_flags}{peer_msg}")

        try:
            nvmf_log_flags = [key for key in self.spdk_rpc_client.log_get_flags().keys()
                              if key.startswith('nvmf')]
            nvmf_log_flags += request.extra_log_flags
            ret = [self.spdk_rpc_client.log_set_flag(
                flag=flag) for flag in nvmf_log_flags]
            self.logger.debug(f"Set SPDK log flags {nvmf_log_flags} to TRUE: {ret}")
            if log_level is not None:
                ret_log = self.spdk_rpc_client.log_set_level(level=log_level)
                self.logger.debug(f"Set log level to {log_level}: {ret_log}")
            if print_level is not None:
                ret_print = self.spdk_rpc_client.log_set_print_level(
                    level=print_level)
                self.logger.debug(f"Set log print level to {print_level}: {ret_print}")
        except Exception as ex:
            errmsg = "Failure setting SPDK log levels"
            self.logger.exception(errmsg)
            errmsg = "{errmsg}:\n{ex}"
            for flag in nvmf_log_flags:
                self.spdk_rpc_client.log_clear_flag(flag=flag)
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure setting SPDK log levels: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        status = 0
        errmsg = os.strerror(0)
        if log_level is not None and not ret_log:
            status = errno.EINVAL
            errmsg = "Failure setting SPDK log level"
        elif print_level is not None and not ret_print:
            status = errno.EINVAL
            errmsg = "Failure setting SPDK print log level"
        elif not all(ret):
            status = errno.EINVAL
            errmsg = "Failure setting some SPDK log flags"
        return pb2.req_status(status=status, error_message=errmsg)

    def set_spdk_nvmf_logs(self, request, context=None):
        return self.execute_grpc_function(self.set_spdk_nvmf_logs_safe, request, context)

    def disable_spdk_nvmf_logs_safe(self, request, context):
        """Disables spdk nvmf logs"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling disable_spdk_nvmf_logs_safe()"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to disable SPDK logs, "
                         f"extra: {request.extra_log_flags}{peer_msg}")

        try:
            nvmf_log_flags = [key for key in self.spdk_rpc_client.log_get_flags().keys()
                              if key.startswith('nvmf')]
            nvmf_log_flags += request.extra_log_flags
            ret = [self.spdk_rpc_client.log_clear_flag(flag=flag)
                   for flag in nvmf_log_flags]
            self.logger.debug(f"Set SPDK log flags {nvmf_log_flags} to FALSE: {ret}")
            logs_level = [self.spdk_rpc_client.log_set_level(level='NOTICE'),
                          self.spdk_rpc_client.log_set_print_level(level='INFO')]
            ret.extend(logs_level)
        except Exception as ex:
            errmsg = "Failure in disable SPDK log flags"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure in disable SPDK log flags: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        status = 0
        errmsg = os.strerror(0)
        if not all(ret):
            status = errno.EINVAL
            errmsg = "Failure in disable SPDK log flags"
        return pb2.req_status(status=status, error_message=errmsg)

    def disable_spdk_nvmf_logs(self, request, context=None):
        return self.execute_grpc_function(self.disable_spdk_nvmf_logs_safe, request, context)

    def parse_version(self, version):
        if not version:
            return None
        try:
            vlist = version.split(".")
            if len(vlist) != 3:
                raise Exception
            v1 = int(vlist[0])
            v2 = int(vlist[1])
            v3 = int(vlist[2])
        except Exception:
            self.logger.exception(f"Can't parse version \"{version}\"")
            return None
        return (v1, v2, v3)

    def get_gateway_info_safe(self, request, context):
        """Get gateway's info"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling get_gateway_info_safe()"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get gateway's info{peer_msg}")
        gw_version_string = os.getenv("NVMEOF_VERSION")
        if not self.spdk_version:
            try:
                ret = self.spdk_rpc_client.spdk_get_version()
                if ret:
                    self.spdk_version = ret["version"]
            except Exception:
                self.logger.exception("Error getting SPDK version")
                pass
        if self.spdk_version:
            spdk_version_string = self.spdk_version
        else:
            spdk_version_string = os.getenv("NVMEOF_SPDK_VERSION")
        cli_version_string = request.cli_version
        addr = self.config.get_with_default("gateway", "addr", "")
        port = self.config.get_with_default("gateway", "port", "")
        initialization_over = self.gateway_state.is_initialization_over()
        location = self.ceph_utils.get_gateway_location(self.gateway_pool,
                                                        self.gateway_group,
                                                        self.gateway_name)
        ret = pb2.gateway_info(cli_version=request.cli_version,
                               version=gw_version_string,
                               spdk_version=spdk_version_string,
                               name=self.gateway_name,
                               group=self.gateway_group,
                               addr=addr,
                               port=port,
                               load_balancing_group=self.group_id + 1,
                               bool_status=True,
                               hostname=self.host_name,
                               max_subsystems=self.max_subsystems,
                               max_namespaces=self.max_namespaces,
                               max_namespaces_per_subsystem=self.max_namespaces_per_subsystem,
                               max_hosts_per_subsystem=self.max_hosts_per_subsystem,
                               max_hosts=self.max_hosts,
                               gateway_initialization_over=initialization_over,
                               io_stats_enabled=self.io_stats_enabled,
                               location=location,
                               status=0,
                               error_message=os.strerror(0))
        cli_ver = self.parse_version(cli_version_string)
        gw_ver = self.parse_version(gw_version_string)
        if cli_ver is not None and gw_ver is not None and cli_ver < gw_ver:
            ret.bool_status = False
            ret.status = errno.EINVAL
            ret.error_message = f"CLI version {cli_version_string} is older " \
                                f"than gateway's version {gw_version_string}"
        elif not gw_version_string:
            ret.bool_status = False
            ret.status = errno.EINVAL
            ret.error_message = "Gateway's version not found"
        elif not gw_ver:
            ret.bool_status = False
            ret.status = errno.EINVAL
            ret.error_message = f"Invalid gateway's version {gw_version_string}"
        if not cli_version_string:
            self.logger.warning("No CLI version specified, can't check version compatibility")
        elif not cli_ver:
            self.logger.warning(f"Invalid CLI version {cli_version_string}, "
                                f"can't check version compatibility")
        if ret.status == 0:
            log_func = self.logger.debug
        else:
            log_func = self.logger.error
        log_func(f"Gateway's info:\n{ret}")
        return ret

    def get_gateway_info(self, request, context=None):
        """Get gateway's info"""

        return self.execute_grpc_function(self.get_gateway_info_safe, request, context)

    def set_gateway_io_stats_mode_safe(self, request, context):
        """Set gateway's IO statistics mode"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling " \
                                       "set_gateway_io_stats_mode_safe()"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to set gateway's IO statistics mode, enabled: "
                         f"{request.enabled}{peer_msg}")
        mode_str = "on" if request.enabled else "off"
        error_prefix = f"Failure setting gateway's IO statistics mode to \"{mode_str}\""
        assert context, "Should not set gateway's IO statistics mode on update"

        try:
            ret = self.spdk_rpc_client.nvmf_enable_ctrl_io_stats(trtype="TCP",
                                                                 enable=request.enabled)
            self.logger.debug(f"nvmf_enable_ctrl_io_stats: {ret}")
            self.io_stats_enabled = request.enabled
        except Exception as ex:
            self.logger.exception(error_prefix)
            errmsg = f"{error_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{error_prefix}: {resp['message']}"
            return pb2.req_status(status=status, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def set_gateway_io_stats_mode(self, request, context=None):
        """Set gateway's IO statistics mode"""

        return self.execute_grpc_function(self.set_gateway_io_stats_mode_safe, request, context)

    def get_gateway_stats_safe(self, request, context=None):
        """Get gateway's NVMf statistics"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling get_gateway_stats_safe()"
        error_prefix = "Failure getting gateway's NVMf statistics"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get gateway's NVMf statistics{peer_msg}")
        try:
            gw_stats = self.spdk_rpc_client.nvmf_get_stats()
            self.logger.debug(f"nvmf_get_stats: {gw_stats}")
        except Exception as ex:
            self.logger.exception(error_prefix)
            errmsg = f"{error_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{error_prefix}: {resp['message']}"
            return pb2.gateway_stats_info(status=status, error_message=errmsg)

        gw_stats_info = None
        try:
            tick_rate = gw_stats["tick_rate"]
            poll_groups = []
            for poll_grp in gw_stats["poll_groups"]:
                transports = []
                for trns in poll_grp["transports"]:
                    transports.append(pb2.poll_group_transport_info(trtype=trns["trtype"]))
                pg = pb2.poll_group_info(name=poll_grp["name"],
                                         admin_qpairs=poll_grp["admin_qpairs"],
                                         io_qpairs=poll_grp["io_qpairs"],
                                         current_admin_qpairs=poll_grp["current_admin_qpairs"],
                                         current_io_qpairs=poll_grp["current_io_qpairs"],
                                         pending_bdev_io=poll_grp["pending_bdev_io"],
                                         completed_nvme_io=poll_grp["completed_nvme_io"],
                                         transports=transports)
                poll_groups.append(pg)
            gw_stats_info = pb2.gateway_stats_info(status=0, error_message=os.strerror(0),
                                                   tick_rate=tick_rate,
                                                   poll_groups=poll_groups)
        except KeyError:
            self.logger.exception(f"Error parsing {gw_stats}")
            errmsg = f"{error_prefix}: Error parsing"
            return pb2.gateway_stats_info(status=errno.EINVAL, error_message=errmsg)

        return gw_stats_info

    def get_gateway_stats(self, request, context=None):
        """Get gateway's NVMf statistics"""

        return self.execute_grpc_function(self.get_gateway_stats_safe, request, context)

    def get_thread_stats_safe(self, request, context=None):
        """Get SPDK thread stats"""

        assert self.rpc_lock.locked(), "RPC is unlocked when calling get_thread_stats_safe()"
        error_prefix = "Failure getting SPDK thread statistics"
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get spdk thread stats {peer_msg}")
        try:
            thread_stats = self.spdk_rpc_client.thread_get_stats()
            self.logger.debug(f"thread_get_stats: {thread_stats}")
            threads = []
            for spdk_thread in thread_stats.get("threads", []):
                thread = pb2.spdk_thread_info(
                    name=spdk_thread.get("name"),
                    busy=spdk_thread.get("busy"),
                    idle=spdk_thread.get("idle"),
                )
                threads.append(thread)
            return pb2.thread_stats_info(
                status=0, error_message=os.strerror(0),
                threads=threads, tick_rate=thread_stats.get("tick_rate", 0))
        except Exception as ex:
            self.logger.exception(error_prefix)
            errmsg = f"{error_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{error_prefix}: {resp['message']}"
            return pb2.thread_stats_info(status=status, error_message=errmsg)

    def get_thread_stats(self, request, context=None):
        """Get spdk thread statistics"""

        return self.execute_grpc_function(self.get_thread_stats_safe, request, context)

    def get_gateway_log_level(self, request, context=None):
        """Get gateway's log level"""

        peer_msg = self.get_peer_message(context)
        try:
            log_level = GatewayEnumUtils.get_key_from_value(pb2.GwLogLevel, self.logger.level)
        except Exception:
            self.logger.exception(f"Can't get string value for log level {self.logger.level}")
            return pb2.gateway_log_level_info(status=errno.EINVAL,
                                              error_message="Invalid gateway log level")
        self.logger.info(f"Received request to get gateway's log level. "
                         f"Level is {log_level}{peer_msg}")
        return pb2.gateway_log_level_info(status=0, error_message=os.strerror(0),
                                          log_level=log_level)

    def set_gateway_log_level(self, request, context=None):
        """Set gateway's log level"""

        peer_msg = self.get_peer_message(context)
        log_level = GatewayEnumUtils.get_key_from_value(pb2.GwLogLevel, request.log_level)
        if log_level is None:
            errmsg = f"Unknown log level {request.log_level}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        log_level = log_level.upper()

        self.logger.info(f"Received request to set gateway's log level to {log_level}{peer_msg}")
        self.gw_logger_object.set_log_level(request.log_level)

        try:
            os.remove(GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH)
        except FileNotFoundError:
            pass
        except Exception:
            self.logger.exception(f"Failure removing "
                                  f"\"{GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH}\"")

        try:
            with open(GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH, "w") as f:
                f.write(str(request.log_level))
        except Exception:
            self.logger.exception(f"Failure writing log level to "
                                  f"\"{GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH}\"")

        return pb2.req_status(status=0, error_message=os.strerror(0))
