#
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
import contextlib
import time
from typing import Callable
from collections import defaultdict
import logging

import spdk.rpc.bdev as rpc_bdev
import spdk.rpc.nvmf as rpc_nvmf
import spdk.rpc.log as rpc_log
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
from .utils import GatewayLogger
from .state import GatewayState, GatewayStateHandler, OmapLock
from .cephutils import CephUtils

# Assuming max of 32 gateways and protocol min 1 max 65519
CNTLID_RANGE_SIZE = 2040
DEFAULT_MODEL_NUMBER = "Ceph bdev Controller"

class BdevStatus:
    def __init__(self, status, error_message, bdev_name = ""):
        self.status = status
        self.error_message = error_message
        self.bdev_name = bdev_name

class MonitorGroupService(monitor_pb2_grpc.MonitorGroupServicer):
    def __init__(self, set_group_id: Callable[[int], None]) -> None:
        self.set_group_id = set_group_id

    def group_id(self, request: monitor_pb2.group_id_req, context = None) -> Empty:
        self.set_group_id(request.id)
        return Empty()

class GatewayService(pb2_grpc.GatewayServicer):
    """Implements gateway service interface.

    Handles configuration of the SPDK NVMEoF target according to client requests.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_name: Gateway identifier
        gateway_state: Methods for target state persistence
        spdk_rpc_client: Client of SPDK RPC server
    """

    def __init__(self, config: GatewayConfig, gateway_state: GatewayStateHandler, rpc_lock, omap_lock: OmapLock, group_id: int, spdk_rpc_client, ceph_utils: CephUtils) -> None:
        """Constructor"""
        self.gw_logger_object = GatewayLogger(config)
        self.logger = self.gw_logger_object.logger
        config.display_environment_info(self.logger)
        self.ceph_utils = ceph_utils
        self.ceph_utils.fetch_and_display_ceph_version()
        requested_hugepages_val = os.getenv("HUGEPAGES", "")
        if not requested_hugepages_val:
            self.logger.warning("Can't get requested huge pages count")
        else:
            requested_hugepages_val = requested_hugepages_val.strip()
            try:
                requested_hugepages_val = int(requested_hugepages_val)
                self.logger.info(f"Requested huge pages count is {requested_hugepages_val}")
            except ValueError:
                self.logger.warning(f"Requested huge pages count value {requested_hugepages_val} is not numeric")
                requested_hugepages_val = None
        hugepages_file = os.getenv("HUGEPAGES_DIR", "")
        if not hugepages_file:
            hugepages_file = "/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
            self.logger.warning("No huge pages file defined, will use /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages")
        else:
            hugepages_file = hugepages_file.strip()
        if os.access(hugepages_file, os.F_OK):
            try:
                hugepages_val = ""
                with open(hugepages_file) as f:
                    hugepages_val = f.readline()
                hugepages_val = hugepages_val.strip()
                if hugepages_val:
                    try:
                        hugepages_val = int(hugepages_val)
                        self.logger.info(f"Actual huge pages count is {hugepages_val}")
                    except ValueError:
                        self.logger.warning(f"Actual huge pages count value {hugepages_val} is not numeric")
                        hugepages_val = ""
                    if requested_hugepages_val and hugepages_val != "" and requested_hugepages_val > hugepages_val:
                        self.logger.warning(f"The actual huge page count {hugepages_val} is smaller than the requested value of {requested_hugepages_val}")
                else:
                    self.logger.warning(f"Can't read actual huge pages count value from {hugepages_file}")
            except Exception as ex:
                self.logger.exception(f"Can't read actual huge pages count value from {hugepages_file}")
        else:
            self.logger.warning(f"Can't find huge pages file {hugepages_file}")
        self.config = config
        config.dump_config_file(self.logger)
        self.rpc_lock = rpc_lock
        self.gateway_state = gateway_state
        self.omap_lock = omap_lock
        self.group_id = group_id
        self.spdk_rpc_client = spdk_rpc_client
        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()
        self.gateway_group = self.config.get("gateway", "group")
        override_hostname = self.config.get_with_default("gateway", "override_hostname", "")
        if override_hostname:
            self.host_name = override_hostname
            self.logger.info(f"Gateway's host name was overridden to {override_hostname}")
        else:
            self.host_name = socket.gethostname()
        self.verify_nqns = self.config.getboolean_with_default("gateway", "verify_nqns", True)
        self.gateway_group = self.config.get_with_default("gateway", "group", "")
        self.gateway_pool =  self.config.get_with_default("ceph", "pool", "")
        self.ana_map = defaultdict(dict)
        self.cluster_nonce = {}
        self.bdev_cluster = {}
        self.bdev_params  = {}
        self.subsystem_nsid_bdev = defaultdict(dict)
        self.subsystem_nsid_anagrp = defaultdict(dict)
        self.subsystem_listeners = defaultdict(set)
        self._init_cluster_context()
        self.subsys_ha = {}
        self.subsys_max_ns = {}

    def is_valid_host_nqn(nqn):
        if nqn == "*":
            return pb2.req_status(status=0, error_message=os.strerror(0))
        rc = GatewayUtils.is_valid_nqn(nqn)
        return pb2.req_status(status=rc[0], error_message=rc[1])

    def parse_json_exeption(self, ex):
        if type(ex) != JSONRPCException:
            return None

        json_error_text = "Got JSON-RPC error response"
        resp = None
        try:
            resp_index = ex.message.find(json_error_text)
            if resp_index >= 0:
                resp_str = ex.message[resp_index + len(json_error_text) :]
                resp_index = resp_str.find("response:")
                if resp_index >= 0:
                    resp_str = resp_str[resp_index + len("response:") :]
                    resp = json.loads(resp_str)
        except Exception:
            self.logger.exception(f"Got exception parsing JSON exception")
            pass
        if resp:
            if resp["code"] < 0:
                resp["code"] = -resp["code"]
        else:
            resp={}
            if "timeout" in ex.message.lower():
                resp["code"] = errno.ETIMEDOUT
            else:
                resp["code"] = errno.EINVAL
            resp["message"] = ex.message

        return resp

    def _init_cluster_context(self) -> None:
        """Init cluster context management variables"""
        self.clusters = defaultdict(dict)
        self.bdevs_per_cluster = self.config.getint_with_default("spdk", "bdevs_per_cluster", 32)
        if self.bdevs_per_cluster < 1:
            raise Exception(f"invalid configuration: spdk.bdevs_per_cluster_contexts {self.bdevs_per_cluster} < 1")
        self.logger.info(f"NVMeoF bdevs per cluster: {self.bdevs_per_cluster}")
        self.librbd_core_mask = self.config.get_with_default("spdk", "librbd_core_mask", None)
        self.rados_id = self.config.get_with_default("ceph", "id", "")
        if self.rados_id == "":
            self.rados_id = None

    def _get_cluster(self, anagrp: int) -> str:
        """Returns cluster name, enforcing bdev per cluster context"""
        cluster_name = None
        for name in self.clusters[anagrp]:
            if self.clusters[anagrp][name] < self.bdevs_per_cluster:
                cluster_name = name
                break

        if not cluster_name:
            cluster_name = self._alloc_cluster(anagrp)
            self.clusters[anagrp][cluster_name] = 1
        else:
            self.clusters[anagrp][cluster_name] += 1
        self.logger.info(f"get_cluster {cluster_name=} number bdevs: {self.clusters[anagrp][cluster_name]}")
        return cluster_name

    def _put_cluster(self, name: str) -> None:
        for anagrp in self.clusters:
            if name in self.clusters[anagrp]:
                self.clusters[anagrp][name] -= 1
                assert self.clusters[anagrp][name] >= 0
                # free the cluster context if no longer used by any bdev
                if self.clusters[anagrp][name] == 0:
                    ret = rpc_bdev.bdev_rbd_unregister_cluster(
                        self.spdk_rpc_client,
                        name = name
                    )
                    self.logger.info(f"Free cluster {name=} {ret=}")
                    assert ret
                    self.clusters[anagrp].pop(name)
                else :
                   self.logger.info(f"put_cluster {name=} number bdevs: {self.clusters[anagrp][name]}")
                return

        assert False, f"Cluster {name} is not found"  # we should find the cluster in our state

    def _alloc_cluster_name(self, anagrp: int) -> str:
        """Allocates a new cluster name for ana group"""
        x = 0
        while True:
            name = f"cluster_context_{anagrp}_{x}"
            if name not in self.clusters[anagrp]:
                return name
            x += 1

    def _alloc_cluster(self, anagrp: int) -> str:
        """Allocates a new Rados cluster context"""
        name = self._alloc_cluster_name(anagrp)
        nonce = rpc_bdev.bdev_rbd_register_cluster(
            self.spdk_rpc_client,
            name = name,
            user = self.rados_id,
            core_mask = self.librbd_core_mask,
        )
        self.logger.info(f"Allocated cluster {name=} {nonce=} {anagrp=}")
        self.cluster_nonce[name] = nonce
        return name

    def _grpc_function_with_lock(self, func, request, context):
        with self.rpc_lock:
            rc = func(request, context)
            if not self.omap_lock.omap_file_disable_unlock:
                assert not self.omap_lock.locked(), f"OMAP is still locked when we're out of function {func}"
            return rc

    def execute_grpc_function(self, func, request, context):
        """This functions handles RPC lock by wrapping 'func' with
           self._grpc_function_with_lock, and assumes (?!) the function 'func'
           called might take OMAP lock internally, however does NOT ensure
           taking OMAP lock in any way.
        """
        return self.omap_lock.execute_omap_locking_function(self._grpc_function_with_lock, func, request, context)

    def create_bdev(self, anagrp: int, name, uuid, rbd_pool_name, rbd_image_name, block_size, create_image, rbd_image_size, context, peer_msg = ""):
        """Creates a bdev from an RBD image."""

        if create_image:
            cr_img_msg = "will create image if doesn't exist"
        else:
            cr_img_msg = "will not create image if doesn't exist"

        self.logger.info(f"Received request to create bdev {name} from"
                         f" {rbd_pool_name}/{rbd_image_name} (size {rbd_image_size} bytes)"
                         f" with block size {block_size}, {cr_img_msg}, context={context}{peer_msg}")

        if block_size == 0:
            return BdevStatus(status=errno.EINVAL,
                                   error_message=f"Failure creating bdev {name}: block size can't be zero")

        if create_image:
            if rbd_image_size <= 0:
                return BdevStatus(status=errno.EINVAL,
                                  error_message=f"Failure creating bdev {name}: image size must be positive")
            if rbd_image_size % (1024 * 1024):
                return BdevStatus(status=errno.EINVAL,
                                  error_message=f"Failure creating bdev {name}: image size must be aligned to MiBs")
            rc = self.ceph_utils.pool_exists(rbd_pool_name)
            if not rc:
                return BdevStatus(status=errno.ENODEV,
                                       error_message=f"Failure creating bdev {name}: RBD pool {rbd_pool_name} doesn't exist")

            try:
                rc = self.ceph_utils.create_image(rbd_pool_name, rbd_image_name, rbd_image_size)
                if rc:
                    self.logger.info(f"Image {rbd_pool_name}/{rbd_image_name} created, size is {rbd_image_size} bytes")
                else:
                    self.logger.info(f"Image {rbd_pool_name}/{rbd_image_name} already exists with size {rbd_image_size} bytes")
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
                errmsg = f"Can't create RBD image {rbd_pool_name}/{rbd_image_name}: {msg}"
                self.logger.exception(errmsg)
                return BdevStatus(status=errcode, error_message=f"Failure creating bdev {name}: {errmsg}")

        try:
            cluster_name=self._get_cluster(anagrp)
            bdev_name = rpc_bdev.bdev_rbd_create(
                self.spdk_rpc_client,
                name=name,
                cluster_name=cluster_name,
                pool_name=rbd_pool_name,
                rbd_name=rbd_image_name,
                block_size=block_size,
                uuid=uuid,
            )
            self.bdev_cluster[name] = cluster_name
            self.bdev_params[name]  = {'uuid':uuid, 'pool_name':rbd_pool_name, 'image_name':rbd_image_name, 'image_size':rbd_image_size, 'block_size': block_size}

            self.logger.debug(f"bdev_rbd_create: {bdev_name}, cluster_name {cluster_name}")
        except Exception as ex:
            self._put_cluster(cluster_name)
            errmsg = f"bdev_rbd_create {name} failed"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg} with:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.ENODEV
            if resp:
                status = resp["code"]
                errmsg = f"Failure creating bdev {name}: {resp['message']}"
            return BdevStatus(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not bdev_name:
            errmsg = f"Can't create bdev {name}"
            self.logger.error(errmsg)
            return BdevStatus(status=errno.ENODEV, error_message=errmsg)

        if name != bdev_name:
            self.logger.warning(f"Created bdev name {bdev_name} differs from requested name {name}")

        return BdevStatus(status=0, error_message=os.strerror(0), bdev_name=name)

    def resize_bdev(self, bdev_name, new_size, peer_msg = ""):
        """Resizes a bdev."""

        self.logger.info(f"Received request to resize bdev {bdev_name} to {new_size} MiB{peer_msg}")
        rbd_pool_name = None
        rbd_image_name = None
        bdev_info = self.get_bdev_info(bdev_name, True)
        if bdev_info is not None:
            try:
                drv_specific_info = bdev_info["driver_specific"]
                rbd_info = drv_specific_info["rbd"]
                rbd_pool_name = rbd_info["pool_name"]
                rbd_image_name = rbd_info["rbd_name"]
            except KeyError as err:
                self.logger.warning(f"Key {err} is not found, will not check size for shrinkage")
                pass
        else:
            self.logger.warning(f"Can't get information for associated block device {bdev_name}, won't check size for shrinkage")

        if rbd_pool_name and rbd_image_name:
            try:
                current_size = self.ceph_utils.get_image_size(rbd_pool_name, rbd_image_name)
                if current_size > new_size * 1024 * 1024:
                    return pb2.req_status(status=errno.EINVAL,
                                          error_message=f"new size {new_size * 1024 * 1024} bytes is smaller than current size {current_size} bytes")
            except Exception as ex:
                self.logger.warning(f"Error trying to get the size of image {rbd_pool_name}/{rbd_image_name}, won't check size for shrinkage:\n{ex}")
                pass

        with self.rpc_lock:
            try:
                ret = rpc_bdev.bdev_rbd_resize(
                    self.spdk_rpc_client,
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

        if not self.rpc_lock.locked():
            self.logger.error(f"A call to delete_bdev() without holding the RPC lock")
            assert self.rpc_lock.locked(), "RPC is unlocked when calling delete_bdev()"

        self.logger.info(f"Received request to delete bdev {bdev_name}{peer_msg}")
        try:
            ret = rpc_bdev.bdev_rbd_delete(
                self.spdk_rpc_client,
                bdev_name,
            )
            if not recycling_mode:
                del self.bdev_params[bdev_name]
            self.logger.debug(f"to delete_bdev {bdev_name} cluster {self.bdev_cluster[bdev_name]} ")
            self._put_cluster(self.bdev_cluster[bdev_name])
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

    def subsystem_already_exists(self, context, nqn) -> bool:
        if not context:
            return False
        state = self.gateway_state.local.get_state()
        for key, val in state.items():
            if not key.startswith(self.gateway_state.local.SUBSYSTEM_PREFIX):
                continue
            try:
                subsys = json.loads(val)
                subnqn = subsys["subsystem_nqn"]
                if subnqn == nqn:
                    return True
            except Exception:
                self.logger.exception(f"Got exception while parsing {val}, will continue")
                continue
        return False

    def serial_number_already_used(self, context, serial) -> str:
        if not context:
            return None
        state = self.gateway_state.local.get_state()
        for key, val in state.items():
            if not key.startswith(self.gateway_state.local.SUBSYSTEM_PREFIX):
                continue
            try:
                subsys = json.loads(val)
                if serial == subsys["serial_number"]:
                    return subsys["subsystem_nqn"]
            except Exception:
                self.logger.exception(f"Got exception while parsing {val}")
                continue
        return None

    def get_peer_message(self, context) -> str:
        if not context:
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
            self.logger.exception(f"Got exception trying to get peer's address")

        return ""

    def create_subsystem_safe(self, request, context):
        """Creates a subsystem."""

        create_subsystem_error_prefix = f"Failure creating subsystem {request.subsystem_nqn}"
        peer_msg = self.get_peer_message(context)

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}, enable_ha: {request.enable_ha}, max_namespaces: {request.max_namespaces}, context: {context}{peer_msg}")

        if not request.enable_ha:
            errmsg = f"{create_subsystem_error_prefix}: HA must be enabled for subsystems"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        errmsg = ""
        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{create_subsystem_error_prefix}: Invalid NQN \"{request.subsystem_nqn}\", contains invalid characters"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        if self.verify_nqns:
            rc = GatewayUtils.is_valid_nqn(request.subsystem_nqn)
            if rc[0] != 0:
                errmsg = f"{create_subsystem_error_prefix}: {rc[1]}"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status = rc[0], error_message = errmsg)
        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{create_subsystem_error_prefix}: Can't create a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        # Set client ID range according to group id assigned by the monitor
        offset = self.group_id * CNTLID_RANGE_SIZE
        min_cntlid = offset + 1
        max_cntlid = offset + CNTLID_RANGE_SIZE

        if not request.serial_number:
            random.seed()
            randser = random.randint(2, 99999999999999)
            request.serial_number = f"Ceph{randser}"
            self.logger.info(f"No serial number specified for {request.subsystem_nqn}, will use {request.serial_number}")

        ret = False
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            errmsg = ""
            try:
                subsys_using_serial = None
                subsys_already_exists = self.subsystem_already_exists(context, request.subsystem_nqn)
                if subsys_already_exists:
                    errmsg = f"Subsystem already exists"
                else:
                    subsys_using_serial = self.serial_number_already_used(context, request.serial_number)
                    if subsys_using_serial:
                        errmsg = f"Serial number {request.serial_number} already used by subsystem {subsys_using_serial}"
                if subsys_already_exists or subsys_using_serial:
                    errmsg = f"{create_subsystem_error_prefix}: {errmsg}"
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.EEXIST, error_message=errmsg)
                enable_ha = (request.enable_ha == True)
                ret = rpc_nvmf.nvmf_create_subsystem(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    serial_number=request.serial_number,
                    model_number=DEFAULT_MODEL_NUMBER,
                    max_namespaces=request.max_namespaces,
                    min_cntlid=min_cntlid,
                    max_cntlid=max_cntlid,
                    ana_reporting = enable_ha,
                )
                self.subsys_ha[request.subsystem_nqn] = enable_ha
                self.subsys_max_ns[request.subsystem_nqn] = request.max_namespaces if request.max_namespaces else 32
                self.logger.debug(f"create_subsystem {request.subsystem_nqn}: {ret}")
            except Exception as ex:
                self.logger.exception(create_subsystem_error_prefix)
                errmsg = f"{create_subsystem_error_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{create_subsystem_error_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(create_subsystem_error_prefix)
                return pb2.req_status(status=errno.EINVAL, error_message=create_subsystem_error_prefix)

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_subsystem(request.subsystem_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting subsystem {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def create_subsystem(self, request, context=None):
        return self.execute_grpc_function(self.create_subsystem_safe, request, context)

    def get_subsystem_namespaces(self, nqn) -> list:
        ns_list = []
        local_state_dict = self.gateway_state.local.get_state()
        for key, val in local_state_dict.items():
            if not key.startswith(self.gateway_state.local.NAMESPACE_PREFIX):
                continue
            try:
                ns = json.loads(val)
                if ns["subsystem_nqn"] == nqn:
                    nsid = ns["nsid"]
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
                lsnr = json.loads(val)
                if lsnr["nqn"] == nqn:
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

        delete_subsystem_error_prefix = f"Failure deleting subsystem {request.subsystem_nqn}"

        ret = False
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                ret = rpc_nvmf.nvmf_delete_subsystem(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                )
                self.subsys_ha.pop(request.subsystem_nqn)
                self.subsys_max_ns.pop(request.subsystem_nqn)
                if request.subsystem_nqn in self.subsystem_listeners:
                    self.subsystem_listeners.pop(request.subsystem_nqn)
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
                self.remove_subsystem_from_state( request.subsystem_nqn, context)
                return pb2.req_status(status=errno.EINVAL, error_message=delete_subsystem_error_prefix)

            return self.remove_subsystem_from_state(request.subsystem_nqn, context)

    def delete_subsystem(self, request, context=None):
        """Deletes a subsystem."""

        peer_msg = self.get_peer_message(context)
        delete_subsystem_error_prefix = f"Failure deleting subsystem {request.subsystem_nqn}"
        self.logger.info(f"Received request to delete subsystem {request.subsystem_nqn}, context: {context}{peer_msg}")

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            errmsg = f"{delete_subsystem_error_prefix}: Can't delete a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        ns_list = []
        if context:
            if self.subsystem_has_listeners(request.subsystem_nqn):
                self.logger.warning(f"About to delete subsystem {request.subsystem_nqn} which has a listener defined")
            ns_list = self.get_subsystem_namespaces(request.subsystem_nqn)

        # We found a namespace still using this subsystem and --force wasn't used fail with EBUSY
        if not request.force and len(ns_list) > 0:
            errmsg = f"{delete_subsystem_error_prefix}: Namespace {ns_list[0]} is still using the subsystem. Either remove it or use the '--force' command line option"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EBUSY, error_message=errmsg)

        for nsid in ns_list:
            # We found a namespace still using this subsystem and --force was used so we will try to remove the namespace
            self.logger.warning(f"Will remove namespace {nsid} from {request.subsystem_nqn}")
            ret = self.namespace_delete(pb2.namespace_delete_req(subsystem_nqn=request.subsystem_nqn, nsid=nsid), context)
            if ret.status == 0:
                self.logger.info(f"Automatically removed namespace {nsid} from {request.subsystem_nqn}")
            else:
                self.logger.error(f"Failure removing namespace {nsid} from {request.subsystem_nqn}:\n{ret.error_message}")
                self.logger.warning(f"Will continue deleting {request.subsystem_nqn} anyway")
        return self.execute_grpc_function(self.delete_subsystem_safe, request, context)

    def check_if_image_used(self, pool_name, image_name):
        """Check if image is used by any other namespace."""

        errmsg = ""
        nqn = None
        state = self.gateway_state.local.get_state()
        for key, val in state.items():
            if not key.startswith(self.gateway_state.local.NAMESPACE_PREFIX):
                continue
            try:
                ns = json.loads(val)
                ns_pool = ns["rbd_pool_name"]
                ns_image = ns["rbd_image_name"]
                if pool_name and pool_name == ns_pool and image_name and image_name == ns_image:
                    nqn = ns["subsystem_nqn"]
                    errmsg = f"RBD image {ns_pool}/{ns_image} is already used by a namespace in subsystem {nqn}"
                    break
            except Exception:
                self.logger.exception(f"Got exception while parsing {val}, will continue")
                continue
        return errmsg, nqn

    def create_namespace(self, subsystem_nqn, bdev_name, nsid, anagrpid, uuid, context):
        """Adds a namespace to a subsystem."""
 
        if context:
            assert self.omap_lock.locked(), "OMAP is unlocked when calling create_namespace()"
        nsid_msg = ""
        if nsid and uuid:
            nsid_msg = f" using NSID {nsid} and UUID {uuid}"
        elif nsid:
            nsid_msg = f" using NSID {nsid} "
        elif uuid:
            nsid_msg = f" using UUID {uuid} "

        add_namespace_error_prefix = f"Failure adding namespace{nsid_msg}to {subsystem_nqn}"

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to add {bdev_name} to {subsystem_nqn} with ANA group id {anagrpid}{nsid_msg}, context: {context}{peer_msg}")

        if anagrpid > self.subsys_max_ns[subsystem_nqn]:
            errmsg = f"{add_namespace_error_prefix}: Group ID {anagrpid} is bigger than configured maximum {self.subsys_max_ns[subsystem_nqn]}"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(subsystem_nqn):
            errmsg = f"{add_namespace_error_prefix}: Can't add namespaces to a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.nsid_status(status=errno.EINVAL, error_message=errmsg)

        try:
            nsid = rpc_nvmf.nvmf_subsystem_add_ns(
                self.spdk_rpc_client,
                nqn=subsystem_nqn,
                bdev_name=bdev_name,
                nsid=nsid,
                anagrpid=anagrpid,
                uuid=uuid,
            )
            self.subsystem_nsid_bdev[subsystem_nqn][nsid] = bdev_name
            self.subsystem_nsid_anagrp[subsystem_nqn][nsid] = anagrpid
            self.logger.debug(f"subsystem_add_ns: {nsid}")
        except Exception as ex:
            self.logger.exception(add_namespace_error_prefix)
            errmsg = f"{add_namespace_error_prefix}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"{add_namespace_error_prefix}: {resp['message']}"
            return pb2.nsid_status(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not nsid:
            self.logger.error(add_namespace_error_prefix)
            return pb2.nsid_status(status=errno.EINVAL, error_message=add_namespace_error_prefix)

        return pb2.nsid_status(nsid=nsid, status=0, error_message=os.strerror(0))

    def find_unique_bdev_name(self, uuid) -> str:
        if not uuid:
            uuid = str(uuid.uuid4())
        return f"bdev_{uuid}"

    def get_ns_id_message(self, nsid, uuid):
        ns_id_msg = ""
        if nsid and uuid:
            ns_id_msg = f"using NSID {nsid} and UUID {uuid} "
        elif nsid:
            ns_id_msg = f"using NSID {nsid} "
        elif uuid:
            ns_id_msg = f"using UUID {uuid} "
        return ns_id_msg

    def set_ana_state(self, request, context=None):
        return self.execute_grpc_function(self.set_ana_state_safe, request, context)

    def set_ana_state_safe(self, ana_info: pb2.ana_info, context=None):
        peer_msg = self.get_peer_message(context)
        """Sets ana state for this gateway."""
        self.logger.info(f"Received request to set ana states {ana_info.states}, {peer_msg}")

        state = self.gateway_state.local.get_state()
        inaccessible_ana_groups = {}
        optimized_ana_groups = set()
        # Iterate over nqn_ana_states in ana_info
        for nas in ana_info.states:

            # fill the static gateway dictionary per nqn and grp_id
            nqn = nas.nqn
            for gs in nas.states:
                self.ana_map[nqn][gs.grp_id]  = gs.state

            # could mean also that the subsystem is not created yet
            if not self.get_subsystem_ha_status(nqn):
                continue

            self.logger.debug(f"Iterate over {nqn=} {self.subsystem_listeners[nqn]=}")
            for listener in self.subsystem_listeners[nqn]:
                self.logger.debug(f"{listener=}")

                # Iterate over ana_group_state in nqn_ana_states
                for gs in nas.states:
                    # Access grp_id and state
                    grp_id = gs.grp_id
                    # The gateway's interface gRPC ana_state into SPDK JSON RPC values,
                    # see nvmf_subsystem_listener_set_ana_state method https://spdk.io/doc/jsonrpc.html
                    ana_state = "optimized" if gs.state == pb2.ana_state.OPTIMIZED else "inaccessible"
                    try:
                        # Need to wait for the latest OSD map, for each RADOS
                        # cluster context before becoming optimized,
                        # part of bocklist logic
                        if gs.state == pb2.ana_state.OPTIMIZED:
                            if grp_id not in optimized_ana_groups:
                                for cluster in self.clusters[grp_id]:
                                    if not rpc_bdev.bdev_rbd_wait_for_latest_osdmap(self.spdk_rpc_client, name=cluster):
                                        raise Exception(f"bdev_rbd_wait_for_latest_osdmap({cluster=}) error")
                                    self.logger.debug(f"set_ana_state bdev_rbd_wait_for_latest_osdmap {cluster=}")
                                optimized_ana_groups.add(grp_id)

                        self.logger.debug(f"set_ana_state nvmf_subsystem_listener_set_ana_state {nqn=} {listener=} {ana_state=} {grp_id=}")
                        (adrfam, traddr, trsvcid) = listener
                        ret = rpc_nvmf.nvmf_subsystem_listener_set_ana_state(
                            self.spdk_rpc_client,
                            nqn=nqn,
                            trtype="TCP",
                            traddr=traddr,
                            trsvcid=str(trsvcid),
                            adrfam=adrfam,
                            ana_state=ana_state,
                            anagrpid=grp_id)
                        if ana_state == "inaccessible" :
                            inaccessible_ana_groups[grp_id] = True
                        self.logger.debug(f"set_ana_state nvmf_subsystem_listener_set_ana_state response {ret=}")
                        if not ret:
                            raise Exception(f"nvmf_subsystem_listener_set_ana_state({nqn=}, {listener=}, {ana_state=}, {grp_id=}) error")
                    except Exception as ex:
                        self.logger.exception("nvmf_subsystem_listener_set_ana_state()")
                        if context:
                            context.set_code(grpc.StatusCode.INTERNAL)
                            context.set_details(f"{ex}")
                        return pb2.req_status()
        return pb2.req_status(status=True)

    def choose_anagrpid_for_namespace(self, nsid) ->int:
        grps_list = self.ceph_utils.get_number_created_gateways(self.gateway_pool, self.gateway_group)
        for ana_grp in grps_list:
            if not self.clusters[ana_grp]: # still no namespaces in this ana-group - probably the new GW  added
                self.logger.info(f"New GW created: chosen ana group {ana_grp} for ns {nsid} ")
                return ana_grp
        #not found ana_grp .To calulate it.  Find minimum loaded ana_grp cluster
        ana_load = {}
        min_load = 2000
        chosen_ana_group = 0
        for ana_grp in self.clusters:
            if ana_grp in grps_list: #to take into consideration only valid groups
                ana_load[ana_grp] = 0;
                for name in self.clusters[ana_grp]:
                    ana_load[ana_grp] += self.clusters[ana_grp][name] # accumulate the total load per ana group for all valid ana_grp clusters
        for ana_grp in ana_load :
            self.logger.info(f" ana group {ana_grp} load =  {ana_load[ana_grp]}  ")
            if ana_load[ana_grp] <=  min_load:
                min_load = ana_load[ana_grp]
                chosen_ana_group = ana_grp
                self.logger.info(f" ana group {ana_grp} load =  {ana_load[ana_grp]} set as min {min_load} ")
        self.logger.info(f"Found min loaded cluster: chosen ana group {chosen_ana_group} for ns {nsid} ")
        return chosen_ana_group

    def namespace_add_safe(self, request, context):
        """Adds a namespace to a subsystem."""

        grps_list = []
        anagrp = 0
        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        self.logger.info(f"Received request to add a namespace {nsid_msg}to {request.subsystem_nqn}, ana group {request.anagrpid}  context: {context}{peer_msg}")

        if not request.uuid:
            request.uuid = str(uuid.uuid4())

        if context:
            if request.anagrpid != 0:
                grps_list = self.ceph_utils.get_number_created_gateways(self.gateway_pool, self.gateway_group)
            else:
                anagrp = self.choose_anagrpid_for_namespace(request.nsid)
                assert anagrp != 0, "Chosen ANA group is 0"

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if context:
                errmsg, ns_nqn = self.check_if_image_used(request.rbd_pool_name, request.rbd_image_name)
                if errmsg and ns_nqn:
                    if request.force:
                        self.logger.warning(f"{errmsg}, will continue as the \"force\" argument was used")
                    else:
                        errmsg = f"{errmsg}, either delete the namespace or use the \"force\" argument,\nyou can find the offending namespace by using the \"namespace list --subsystem {ns_nqn}\" CLI command"
                        self.logger.error(errmsg)
                        return pb2.nsid_status(status=errno.EEXIST, error_message=errmsg)

            bdev_name = self.find_unique_bdev_name(request.uuid)

            create_image = request.create_image
            if not context:
                create_image = False
            else: # new namespace
                # If an explicit load balancing group was passed, make sure it exists
                if request.anagrpid != 0:
                    if request.anagrpid not in grps_list:
                        self.logger.debug(f"ANA groups: {grps_list}")
                        errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: Load balancing group {request.anagrpid} doesn't exist"
                        self.logger.error(errmsg)
                        return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
                else:
                   request.anagrpid = anagrp

            anagrp = request.anagrpid
            ret_bdev = self.create_bdev(anagrp, bdev_name, request.uuid, request.rbd_pool_name,
                                        request.rbd_image_name, request.block_size, create_image, request.size, context, peer_msg)
            if ret_bdev.status != 0:
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}: {ret_bdev.error_message}"
                self.logger.error(errmsg)
                # Delete the bdev just to be on the safe side
                ns_bdev = self.get_bdev_info(bdev_name, False)
                if ns_bdev != None:
                    try:
                        ret_del = self.delete_bdev(bdev_name, peer_msg = peer_msg)
                        self.logger.debug(f"delete_bdev({bdev_name}): {ret_del.status}")
                    except AssertionError:
                        self.logger.exception(f"Got an assert while trying to delete bdev {bdev_name}")
                        raise
                    except Exception:
                        self.logger.exception(f"Got exception while trying to delete bdev {bdev_name}")
                return pb2.nsid_status(status=ret_bdev.status, error_message=errmsg)

            if ret_bdev.bdev_name != bdev_name:
                self.logger.warning(f"Returned bdev name {ret_bdev.bdev_name} differs from requested one {bdev_name}")

            ret_ns = self.create_namespace(request.subsystem_nqn, bdev_name, request.nsid, anagrp, request.uuid, context)

            if ret_ns.status == 0 and request.nsid and ret_ns.nsid != request.nsid:
                errmsg = f"Returned NSID {ret_ns.nsid} differs from requested one {request.nsid}"
                self.logger.error(errmsg)
                ret_ns.status = errno.ENODEV
                ret_ns.error_message = errmsg

            if ret_ns.status != 0:
                try:
                    ret_del = self.delete_bdev(bdev_name, peer_msg = peer_msg)
                    if ret_del.status != 0:
                        self.logger.warning(f"Failure {ret_del.status} deleting bdev {bdev_name}: {ret_del.error_message}")
                except AssertionError:
                    self.logger.exception(f"Got an assert while trying to delete bdev {bdev_name}")
                    raise
                except Exception:
                    self.logger.exception(f"Got exception while trying to delete bdev {bdev_name}")
                errmsg = f"Failure adding namespace {nsid_msg}to {request.subsystem_nqn}:{ret_ns.error_message}"
                self.logger.error(errmsg)
                return pb2.nsid_status(status=ret_ns.status, error_message=errmsg)

            if context:
                # Update gateway state
                request.nsid = ret_ns.nsid
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, ret_ns.nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting namespace {nsid_msg}on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.nsid_status(status=0, error_message=os.strerror(0), nsid=ret_ns.nsid)

    def namespace_add(self, request, context=None):
        """Adds a namespace to a subsystem."""
        return self.execute_grpc_function(self.namespace_add_safe, request, context)

    def namespace_change_load_balancing_group_safe(self, request, context):
        """Changes a namespace load balancing group."""

        grps_list = []
        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        self.logger.info(f"Received request to change load balancing group for namespace {nsid_msg}in {request.subsystem_nqn} to {request.anagrpid}, context: {context}{peer_msg}")

        grps_list = self.ceph_utils.get_number_created_gateways(self.gateway_pool, self.gateway_group)
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            if request.anagrpid not in grps_list:
                self.logger.debug(f"ANA groups: {grps_list}")
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}: Load balancing group {request.anagrpid} doesn't exist"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
            find_ret = self.find_namespace_and_bdev_name(request.subsystem_nqn, request.nsid, request.uuid, False,
                            f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}")
            if not find_ret[0]:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}: Can't find namespace"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
            try:
                uuid = find_ret[0]["uuid"]
            except KeyError:
                uuid = None
                self.logger.warning(f"Can't get UUID value for namespace {nsid_msg}in {request.subsystem_nqn}:\n{find_ret[0]}")
            try:
                nsid = find_ret[0]["nsid"]
            except KeyError:
                nsid = 0
                self.logger.warning(f"Can't get NSID value for namespace {nsid_msg}in {request.subsystem_nqn}:\n{find_ret[0]}")

            if request.nsid and request.nsid != nsid:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}: Returned NSID {nsid} differs from requested one {request.nsid}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

            if request.uuid and request.uuid != uuid:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}: Returned UUID {uuid} differs from requested one {request.uuid}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

            bdev_name = find_ret[1]
            if not bdev_name:
                bdev_name = self.find_unique_bdev_name(uuid)
                self.logger.warning(f"Failure finding namespace's associated block device name, will use {bdev_name} instead")

            ns_entry = None
            state = self.gateway_state.local.get_state()
            ns_key = GatewayState.build_namespace_key(request.subsystem_nqn, nsid)
            try:
                state_ns = state[ns_key]
                ns_entry = json.loads(state_ns)
            except Exception as ex:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}. Can't get namespace entry from local state"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

            if not ns_entry:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}. Can't get namespace entry from local state"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOENT, error_message=errmsg)

            ret_del = self.remove_namespace(request.subsystem_nqn, nsid, context)
            if ret_del.status != 0:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}. Can't delete namespace: {ret_del.error_message}"
                self.logger.error(errmsg)
                return pb2.req_status(status=ret_del.status, error_message=errmsg)

            if nsid:
                self.remove_namespace_from_state(request.subsystem_nqn, nsid, context)

            ret_ns = self.create_namespace(request.subsystem_nqn, bdev_name, nsid, request.anagrpid, uuid, context)
            if ret_ns.status != 0:
                errmsg = f"Failure changing load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}:{ret_ns.error_message}"
                self.logger.error(errmsg)
                return pb2.req_status(status=ret_ns.status, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    namespace_add_req = pb2.namespace_add_req()
                    try:
                        namespace_add_req.rbd_pool_name=ns_entry["rbd_pool_name"]
                    except KeyError:
                        pass
                    try:
                        namespace_add_req.rbd_image_name=ns_entry["rbd_image_name"]
                    except KeyError:
                        pass
                    try:
                        namespace_add_req.subsystem_nqn=ns_entry["subsystem_nqn"]
                    except KeyError:
                        pass
                    try:
                        namespace_add_req.nsid=ns_entry["nsid"]
                    except KeyError:
                        pass
                    try:
                        namespace_add_req.block_size=ns_entry["block_size"]
                    except KeyError:
                        pass
                    try:
                        namespace_add_req.uuid=ns_entry["uuid"]
                    except KeyError:
                        pass
                    namespace_add_req.anagrpid=request.anagrpid
                    json_req = json_format.MessageToJson(
                            namespace_add_req, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn, nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting change load balancing group for namespace {nsid_msg}in {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_change_load_balancing_group(self, request, context=None):
        """Changes a namespace load balancing group."""
        return self.execute_grpc_function(self.namespace_change_load_balancing_group_safe, request, context)

    def remove_namespace_from_state(self, nqn, nsid, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        # If we got here context is not None, so we must hold the OMAP lock
        assert self.omap_lock.locked(), "OMAP is unlocked when calling remove_namespace_from_state()"

        # Update gateway state
        try:
            self.gateway_state.remove_namespace_qos(nqn, str(nsid))
        except Exception:
            self.logger.warning(f"Error removing namespace's QOS limits, they might not have been set")
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

        if context:
            assert self.omap_lock.locked(), "OMAP is unlocked when calling remove_namespace()"
        peer_msg = self.get_peer_message(context)
        namespace_failure_prefix = f"Failure removing namespace {nsid} from {subsystem_nqn}"
        self.logger.info(f"Received request to remove namespace {nsid} from {subsystem_nqn}{peer_msg}")

        if GatewayUtils.is_discovery_nqn(subsystem_nqn):
            errmsg=f"{namespace_failure_prefix}: Can't remove a namespace from a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        try:
            ret = rpc_nvmf.nvmf_subsystem_remove_ns(
                self.spdk_rpc_client,
                nqn=subsystem_nqn,
                nsid=nsid,
            )
            self.logger.debug(f"remove_namespace {nsid}: {ret}")
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

    def get_bdev_info(self, bdev_name, need_to_lock):
        """Get bdev info"""

        ret_bdev = None
        if need_to_lock:
            lock_to_use = self.rpc_lock
        else:
            lock_to_use = contextlib.suppress()

        with lock_to_use:
            try:
                bdevs = rpc_bdev.bdev_get_bdevs(self.spdk_rpc_client, name=bdev_name)
                if (len(bdevs) > 1):
                    self.logger.warning(f"Got {len(bdevs)} bdevs for bdev name {bdev_name}, will use the first one")
                ret_bdev = bdevs[0]
            except Exception:
                self.logger.exception(f"Got exception while getting bdev {bdev_name} info")

        return ret_bdev

    def list_namespaces(self, request, context=None):
        """List namespaces."""

        peer_msg = self.get_peer_message(context)
        if request.nsid == None or request.nsid == 0:
            if request.uuid:
                nsid_msg = f"namespace with UUID {request.uuid}"
            else:
                nsid_msg = "all namespaces"
        else:
            if request.uuid:
                nsid_msg = f"namespace with NSID {request.nsid} and UUID {request.uuid}"
            else:
                nsid_msg = f"namespace with NSID {request.nsid}"
        self.logger.info(f"Received request to list {nsid_msg} for {request.subsystem}, context: {context}{peer_msg}")

        with self.rpc_lock:
            try:
                ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client, nqn=request.subsystem)
                self.logger.debug(f"list_namespaces: {ret}")
            except Exception as ex:
                errmsg = f"Failure listing namespaces"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure listing namespaces: {resp['message']}"
                return pb2.namespaces_info(status=status, error_message=errmsg, subsystem_nqn=request.subsystem, namespaces=[])

        namespaces = []
        for s in ret:
            try:
                if s["nqn"] != request.subsystem:
                    self.logger.warning(f'Got subsystem {s["nqn"]} instead of {request.subsystem}, ignore')
                    continue
                try:
                    ns_list = s["namespaces"]
                except Exception:
                    ns_list = []
                    pass
                for n in ns_list:
                    if request.nsid and request.nsid != n["nsid"]:
                        self.logger.debug(f'Filter out namespace {n["nsid"]} which is different than requested nsid {request.nsid}')
                        continue
                    if request.uuid and request.uuid != n["uuid"]:
                        self.logger.debug(f'Filter out namespace with UUID {n["uuid"]} which is different than requested UUID {request.uuid}')
                        continue
                    bdev_name = n["bdev_name"]
                    ns_bdev = self.get_bdev_info(bdev_name, True)
                    lb_group = 0
                    try:
                        lb_group = n["anagrpid"]
                    except KeyError:
                        pass
                    one_ns = pb2.namespace_cli(nsid = n["nsid"],
                                           bdev_name = bdev_name,
                                           uuid = n["uuid"],
                                           load_balancing_group = lb_group)
                    if ns_bdev == None:
                        self.logger.warning(f"Can't find namespace's bdev {bdev_name}, will not list bdev's information")
                    else:
                        try:
                            drv_specific_info = ns_bdev["driver_specific"]
                            rbd_info = drv_specific_info["rbd"]
                            one_ns.rbd_image_name = rbd_info["rbd_name"]
                            one_ns.rbd_pool_name = rbd_info["pool_name"]
                            one_ns.block_size = ns_bdev["block_size"]
                            one_ns.rbd_image_size = ns_bdev["block_size"] * ns_bdev["num_blocks"]
                            assigned_limits = ns_bdev["assigned_rate_limits"]
                            one_ns.rw_ios_per_second=assigned_limits["rw_ios_per_sec"]
                            one_ns.rw_mbytes_per_second=assigned_limits["rw_mbytes_per_sec"]
                            one_ns.r_mbytes_per_second=assigned_limits["r_mbytes_per_sec"]
                            one_ns.w_mbytes_per_second=assigned_limits["w_mbytes_per_sec"]
                        except KeyError as err:
                            self.logger.warning(f"Key {err} is not found, will not list bdev's information") 
                            pass
                        except Exception:
                            self.logger.exception(f"{ns_bdev=} parse error") 
                            pass
                    namespaces.append(one_ns)
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return pb2.namespaces_info(status = 0, error_message = os.strerror(0), subsystem_nqn=request.subsystem, namespaces=namespaces)

    def namespace_get_io_stats(self, request, context=None):
        """Get namespace's IO stats."""

        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        self.logger.info(f"Received request to get IO stats for namespace {nsid_msg}on {request.subsystem_nqn}, context: {context}{peer_msg}")

        with self.rpc_lock:
            find_ret = self.find_namespace_and_bdev_name(request.subsystem_nqn, request.nsid, request.uuid, False,
                                                     "Failure getting namespace's IO stats")
            ns = find_ret[0]
            if not ns:
                errmsg = f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}: Can't find namespace"
                self.logger.error(errmsg)
                return pb2.namespace_io_stats_info(status=errno.ENODEV, error_message=errmsg)
            bdev_name = find_ret[1]
            if not bdev_name:
                errmsg = f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}: Can't find associated block device"
                self.logger.error(errmsg)
                return pb2.namespace_io_stats_info(status=errno.ENODEV, error_message=errmsg)

            try:
                ret = rpc_bdev.bdev_get_iostat(
                    self.spdk_rpc_client,
                    name=bdev_name,
                )
                self.logger.debug(f"get_bdev_iostat {bdev_name}: {ret}")
            except Exception as ex:
                errmsg = f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}: {resp['message']}"
                return pb2.namespace_io_stats_info(status=status, error_message=errmsg)

        # Just in case SPDK failed with no exception
        if not ret:
            errmsg = f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}"
            self.logger.error(errmsg)
            return pb2.namespace_io_stats_info(status=errno.EINVAL, error_message=errmsg)

        exmsg = ""
        try:
            bdevs = ret["bdevs"]
            if not bdevs:
                return pb2.namespace_io_stats_info(status=errno.ENODEV,
                                                   error_message=f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}: No associated block device found")
            if len(bdevs) > 1:
                self.logger.warning(f"More than one associated block device found for namespace, will use the first one")
            bdev = bdevs[0]
            io_errs = []
            try:
                io_error=bdev["io_error"]
                for err_name in io_error.keys():
                    one_error = pb2.namespace_io_error(name=err_name, value=io_error[err_name])
                    io_errs.append(one_error)
            except Exception:
                self.logger.exception(f"failure getting io errors")
            io_stats = pb2.namespace_io_stats_info(status=0,
                               error_message=os.strerror(0),
                               subsystem_nqn=request.subsystem_nqn,
                               nsid=ns["nsid"],
                               uuid=ns["uuid"],
                               bdev_name=bdev_name,
                               tick_rate=ret["tick_rate"],
                               ticks=ret["ticks"],
                               bytes_read=bdev["bytes_read"],
                               num_read_ops=bdev["num_read_ops"],
                               bytes_written=bdev["bytes_written"],
                               num_write_ops=bdev["num_write_ops"],
                               bytes_unmapped=bdev["bytes_unmapped"],
                               num_unmap_ops=bdev["num_unmap_ops"],
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
            return io_stats
        except Exception as ex:
            self.logger.exception(f"parse error")
            exmsg = str(ex)
            pass

        return pb2.namespace_io_stats_info(status=errno.EINVAL,
                               error_message=f"Failure getting IO stats for namespace {nsid_msg}on {request.subsystem_nqn}: Error parsing returned stats:\n{exmsg}") 

    def get_qos_limits_string(self, request):
        limits_to_set = ""
        if request.HasField("rw_ios_per_second"):
            limits_to_set += f" R/W IOs per second: {request.rw_ios_per_second}"
        if request.HasField("rw_mbytes_per_second"):
            limits_to_set += f" R/W megabytes per second: {request.rw_mbytes_per_second}"
        if request.HasField("r_mbytes_per_second"):
            limits_to_set += f" Read megabytes per second: {request.r_mbytes_per_second}"
        if request.HasField("w_mbytes_per_second"):
            limits_to_set += f" Write megabytes per second: {request.w_mbytes_per_second}"

        return limits_to_set

    def namespace_set_qos_limits_safe(self, request, context):
        """Set namespace's qos limits."""

        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        limits_to_set = self.get_qos_limits_string(request)
        self.logger.info(f"Received request to set QOS limits for namespace {nsid_msg}on {request.subsystem_nqn},{limits_to_set}, context: {context}{peer_msg}")

        find_ret = self.find_namespace_and_bdev_name(request.subsystem_nqn, request.nsid, request.uuid, False,
                                                 "Failure setting namespace's QOS limits")
        if not find_ret[0]:
            errmsg = f"Failure setting QOS limits for namespace {nsid_msg}on {request.subsystem_nqn}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        bdev_name = find_ret[1]
        if not bdev_name:
            errmsg = f"Failure setting QOS limits for namespace {nsid_msg}on {request.subsystem_nqn}: Can't find associated block device"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        nsid = find_ret[0]["nsid"]

        set_qos_limits_args = {}
        set_qos_limits_args["name"] = bdev_name
        if request.HasField("rw_ios_per_second"):
            set_qos_limits_args["rw_ios_per_sec"] = request.rw_ios_per_second
        if request.HasField("rw_mbytes_per_second"):
            set_qos_limits_args["rw_mbytes_per_sec"] = request.rw_mbytes_per_second
        if request.HasField("r_mbytes_per_second"):
            set_qos_limits_args["r_mbytes_per_sec"] = request.r_mbytes_per_second
        if request.HasField("w_mbytes_per_second"):
            set_qos_limits_args["w_mbytes_per_sec"] = request.w_mbytes_per_second

        ns_qos_entry = None
        if context:
            state = self.gateway_state.local.get_state()
            ns_qos_key = GatewayState.build_namespace_qos_key(request.subsystem_nqn, nsid)
            try:
                state_ns_qos = state[ns_qos_key]
                ns_qos_entry = json.loads(state_ns_qos)
            except Exception as ex:
                self.logger.info(f"No previous QOS limits found, this is the first time the limits are set for namespace {nsid_msg}on {request.subsystem_nqn}")

        # Merge current limits with previous ones, if exist
        if ns_qos_entry:
            if not request.HasField("rw_ios_per_second") and ns_qos_entry.get("rw_ios_per_second") != None:
                request.rw_ios_per_second = int(ns_qos_entry["rw_ios_per_second"])
            if not request.HasField("rw_mbytes_per_second") and ns_qos_entry.get("rw_mbytes_per_second") != None:
                request.rw_mbytes_per_second = int(ns_qos_entry["rw_mbytes_per_second"])
            if not request.HasField("r_mbytes_per_second") and ns_qos_entry.get("r_mbytes_per_second") != None:
                request.r_mbytes_per_second = int(ns_qos_entry["r_mbytes_per_second"])
            if not request.HasField("w_mbytes_per_second") and ns_qos_entry.get("w_mbytes_per_second") != None:
                request.w_mbytes_per_second = int(ns_qos_entry["w_mbytes_per_second"])

            limits_to_set = self.get_qos_limits_string(request)
            self.logger.debug(f"After merging current QOS limits with previous ones for namespace {nsid_msg}on {request.subsystem_nqn},{limits_to_set}")

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                ret = rpc_bdev.bdev_set_qos_limit(
                    self.spdk_rpc_client,
                    **set_qos_limits_args)
                self.logger.debug(f"bdev_set_qos_limit {bdev_name}: {ret}")
            except Exception as ex:
                errmsg = f"Failure setting QOS limits for namespace {nsid_msg}on {request.subsystem_nqn}"
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
                errmsg = f"Failure setting QOS limits for namespace {nsid_msg}on {request.subsystem_nqn}"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_namespace_qos(request.subsystem_nqn, nsid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting namespace QOS settings {nsid_msg}on {request.subsystem_nqn}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_set_qos_limits(self, request, context=None):
        """Set namespace's qos limits."""
        return self.execute_grpc_function(self.namespace_set_qos_limits_safe, request, context)

    def find_namespace_and_bdev_name(self, nqn, nsid, uuid, needs_lock, err_prefix):
        if nsid <= 0 and not uuid:
           self.logger.error(f"{err_prefix}: At least one of NSID or UUID should be specified for finding a namesapce")
           return (None, None)

        if needs_lock:
            lock_to_use = self.rpc_lock
        else:
            if not self.rpc_lock.locked():
                self.logger.error(f"A call to find_namespace_and_bdev_name() with 'needs_lock' set to False and without holding the RPC lock")
                assert self.rpc_lock.locked(), "RPC is unlocked when calling find_namespace_and_bdev_name()"
            lock_to_use = contextlib.suppress()

        with lock_to_use:
            try:
                ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client, nqn=nqn)
                self.logger.debug(f"find_namespace_and_bdev_name: {ret}")
            except Exception as ex:
                self.logger.exception(err_prefix)
                errmsg = f"{err_prefix}:\n{ex}"
                return (None, None)

        if not ret:
           return (None, None)

        bdev_name = None
        found_ns = None
        for s in ret:
            try:
                if s["nqn"] != nqn:
                    self.logger.warning(f'Got subsystem {s["nqn"]} instead of {nqn}, ignore')
                    continue
                try:
                    ns_list = s["namespaces"]
                except Exception:
                    ns_list = []
                    pass
                for n in ns_list:
                    if nsid > 0 and nsid != n["nsid"]:
                        continue
                    if uuid and uuid != n["uuid"]:
                        continue
                    found_ns = n
                    bdev_name = n["bdev_name"]
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return (found_ns, bdev_name)

    def namespace_resize(self, request, context=None):
        """Resize a namespace."""

        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        self.logger.info(f"Received request to resize namespace {nsid_msg}on {request.subsystem_nqn} to {request.new_size} MiB, context: {context}{peer_msg}")

        if request.new_size <= 0:
            return pb2.req_status(status=errno.EINVAL,
                                  error_message=f"Failure resizing namespace {nsid_msg}on {request.subsystem_nqn}: New size must be positive")

        find_ret = self.find_namespace_and_bdev_name(request.subsystem_nqn, request.nsid, request.uuid, True, "Failure resizing namespace")
        if not find_ret[0]:
            errmsg = f"Failure resizing namespace {nsid_msg}on {request.subsystem_nqn}: Can't find namespace"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
        bdev_name = find_ret[1]
        if not bdev_name:
            errmsg = f"Failure resizing namespace {nsid_msg}on {request.subsystem_nqn}: Can't find associated block device"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENODEV, error_message=errmsg)

        ret = self.resize_bdev(bdev_name, request.new_size, peer_msg)

        if ret.status == 0:
            errmsg = os.strerror(0)
        else:
            errmsg = f"Failure resizing namespace {nsid_msg}on {request.subsystem_nqn}: {ret.error_message}"
            self.logger.error(errmsg)

        return pb2.req_status(status=ret.status, error_message=errmsg)

    def namespace_delete_safe(self, request, context):
        """Delete a namespace."""

        peer_msg = self.get_peer_message(context)
        nsid_msg = self.get_ns_id_message(request.nsid, request.uuid)
        self.logger.info(f"Received request to delete namespace {nsid_msg}from {request.subsystem_nqn}, context: {context}{peer_msg}")

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            find_ret = self.find_namespace_and_bdev_name(request.subsystem_nqn, request.nsid, request.uuid, False,
                                                         "Failure deleting namespace")
            if not find_ret[0]:
                errmsg = f"Failure deleting namespace: Can't find namespace"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
            bdev_name = find_ret[1]
            if not bdev_name:
                self.logger.warning(f"Can't find namespace's bdev name, will try to delete namespace anyway")

            ns = find_ret[0]
            nsid = ns["nsid"]
            ret = self.remove_namespace(request.subsystem_nqn, nsid, context)
            if ret.status != 0:
                return ret

            self.remove_namespace_from_state(request.subsystem_nqn, nsid, context)
            if bdev_name:
                ret_del = self.delete_bdev(bdev_name, peer_msg = peer_msg)
                if ret_del.status != 0:
                    errmsg = f"Failure deleting namespace {nsid_msg}from {request.subsystem_nqn}: {ret_del.error_message}"
                    self.logger.error(errmsg)
                    return pb2.nsid_status(status=ret_del.status, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def namespace_delete(self, request, context=None):
        """Delete a namespace."""
        return self.execute_grpc_function(self.namespace_delete_safe, request, context)

    def matching_host_exists(self, context, subsys_nqn, host_nqn) -> bool:
        if not context:
            return False
        host_key = GatewayState.build_host_key(subsys_nqn, host_nqn)
        state = self.gateway_state.local.get_state()
        if state.get(host_key):
            return True
        else:
            return False

    def add_host_safe(self, request, context):
        """Adds a host to a subsystem."""

        peer_msg = self.get_peer_message(context)
        all_host_failure_prefix=f"Failure allowing open host access to {request.subsystem_nqn}"
        host_failure_prefix=f"Failure adding host {request.host_nqn} to {request.subsystem_nqn}"

        if not GatewayState.is_key_element_valid(request.host_nqn):
            errmsg = f"{host_failure_prefix}: Invalid host NQN \"{request.host_nqn}\", contains invalid characters"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        if not GatewayState.is_key_element_valid(request.subsystem_nqn):
            errmsg = f"{host_failure_prefix}: Invalid subsystem NQN \"{request.subsystem_nqn}\", contains invalid characters"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status = errno.EINVAL, error_message = errmsg)

        if self.verify_nqns:
            rc = GatewayService.is_valid_host_nqn(request.host_nqn)
            if rc.status != 0:
                errmsg = f"{host_failure_prefix}: {rc.error_message}"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status = rc.status, error_message = errmsg)

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            if request.host_nqn == "*":
                errmsg=f"{all_host_failure_prefix}: Can't allow host access to a discovery subsystem"
            else:
                errmsg=f"{host_failure_prefix}: Can't add host to a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            errmsg=f"{host_failure_prefix}: Can't use a discovery NQN as host's"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                host_already_exist = self.matching_host_exists(context, request.subsystem_nqn, request.host_nqn)
                if host_already_exist:
                    if request.host_nqn == "*":
                        errmsg = f"{all_host_failure_prefix}: Open host access is already allowed"
                        self.logger.error(f"{errmsg}")
                        return pb2.req_status(status=errno.EEXIST, error_message=errmsg)
                    else:
                        errmsg = f"{host_failure_prefix}: Host is already added"
                        self.logger.error(f"{errmsg}")
                        return pb2.req_status(status=errno.EEXIST, error_message=errmsg)

                if request.host_nqn == "*":  # Allow any host access to subsystem
                    self.logger.info(f"Received request to allow any host access for {request.subsystem_nqn}, context: {context}{peer_msg}")
                    ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        disable=False,
                    )
                    self.logger.debug(f"add_host *: {ret}")
                else:  # Allow single host access to subsystem
                    self.logger.info(
                        f"Received request to add host {request.host_nqn} to {request.subsystem_nqn}, context: {context}{peer_msg}")
                    ret = rpc_nvmf.nvmf_subsystem_add_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                    )
                    self.logger.debug(f"add_host {request.host_nqn}: {ret}")
            except Exception as ex:
                if request.host_nqn == "*":
                    self.logger.exception(all_host_failure_prefix)
                    errmsg = f"{all_host_failure_prefix}:\n{ex}"
                else:
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
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_host(request.subsystem_nqn,
                                                request.host_nqn, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting host {request.host_nqn} access addition"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def add_host(self, request, context=None):
        return self.execute_grpc_function(self.add_host_safe, request, context)

    def remove_host_from_state(self, subsystem_nqn, host_nqn, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        if context:
            assert self.omap_lock.locked(), "OMAP is unlocked when calling remove_host_from_state()"
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

        peer_msg = self.get_peer_message(context)
        all_host_failure_prefix=f"Failure disabling open host access to {request.subsystem_nqn}"
        host_failure_prefix=f"Failure removing host {request.host_nqn} access from {request.subsystem_nqn}"

        if GatewayUtils.is_discovery_nqn(request.subsystem_nqn):
            if request.host_nqn == "*":
                errmsg=f"{all_host_failure_prefix}: Can't disable open host access to a discovery subsystem"
            else:
                errmsg=f"{host_failure_prefix}: Can't remove host access from a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.host_nqn):
            if request.host_nqn == "*":
                errmsg=f"{all_host_failure_prefix}: Can't use a discovery NQN as host's"
            else:
                errmsg=f"{host_failure_prefix}: Can't use a discovery NQN as host's"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                if request.host_nqn == "*":  # Disable allow any host access
                    self.logger.info(
                        f"Received request to disable open host access to"
                        f" {request.subsystem_nqn}, context: {context}{peer_msg}")
                    ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        disable=True,
                    )
                    self.logger.debug(f"remove_host *: {ret}")
                else:  # Remove single host access to subsystem
                    self.logger.info(
                        f"Received request to remove host {request.host_nqn} access from"
                        f" {request.subsystem_nqn}, context: {context}{peer_msg}")
                    ret = rpc_nvmf.nvmf_subsystem_remove_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                    )
                    self.logger.debug(f"remove_host {request.host_nqn}: {ret}")
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

            return self.remove_host_from_state(request.subsystem_nqn, request.host_nqn, context)

    def remove_host(self, request, context=None):
        return self.execute_grpc_function(self.remove_host_safe, request, context)

    def list_hosts_safe(self, request, context):
        """List hosts."""

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to list hosts for {request.subsystem}, context: {context}{peer_msg}")
        try:
            ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client, nqn=request.subsystem)
            self.logger.debug(f"list_hosts: {ret}")
        except Exception as ex:
            errmsg = f"Failure listing hosts, can't get subsystems"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing hosts, can't get subsystems: {resp['message']}"
            return pb2.hosts_info(status=status, error_message=errmsg, hosts=[])

        hosts = []
        allow_any_host = False
        for s in ret:
            try:
                if s["nqn"] != request.subsystem:
                    self.logger.warning(f'Got subsystem {s["nqn"]} instead of {request.subsystem}, ignore')
                    continue
                try:
                    allow_any_host = s["allow_any_host"]
                    host_nqns = s["hosts"]
                except Exception:
                    host_nqns = []
                    pass
                for h in host_nqns:
                    one_host = pb2.host(nqn = h["nqn"])
                    hosts.append(one_host)
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return pb2.hosts_info(status = 0, error_message = os.strerror(0), allow_any_host=allow_any_host,
                              subsystem_nqn=request.subsystem, hosts=hosts)

    def list_hosts(self, request, context=None):
        return self.execute_grpc_function(self.list_hosts_safe, request, context)

    def list_connections_safe(self, request, context):
        """List connections."""

        peer_msg = self.get_peer_message(context)
        log_level = logging.INFO if context else logging.DEBUG
        self.logger.log(log_level, f"Received request to list connections for {request.subsystem}, context: {context}{peer_msg}")
        try:
            qpair_ret = rpc_nvmf.nvmf_subsystem_get_qpairs(self.spdk_rpc_client, nqn=request.subsystem)
            self.logger.debug(f"list_connections get_qpairs: {qpair_ret}")
        except Exception as ex:
            errmsg = f"Failure listing connections, can't get qpairs"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing connections, can't get qpairs: {resp['message']}"
            return pb2.connections_info(status=status, error_message=errmsg, connections=[])

        try:
            ctrl_ret = rpc_nvmf.nvmf_subsystem_get_controllers(self.spdk_rpc_client, nqn=request.subsystem)
            self.logger.debug(f"list_connections get_controllers: {ctrl_ret}")
        except Exception as ex:
            errmsg = f"Failure listing connections, can't get controllers"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing connections, can't get controllers: {resp['message']}"
            return pb2.connections_info(status=status, error_message=errmsg, connections=[])

        try:
            subsys_ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client, nqn=request.subsystem)
            self.logger.debug(f"list_connections subsystems: {subsys_ret}")
        except Exception as ex:
            errmsg = f"Failure listing connections, can't get subsystems"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.EINVAL
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing connections, can't get subsystems: {resp['message']}"
            return pb2.connections_info(status=status, error_message=errmsg, connections=[])

        connections = []
        host_nqns = []
        for s in subsys_ret:
            try:
                if s["nqn"] != request.subsystem:
                    self.logger.warning(f'Got subsystem {s["nqn"]} instead of {request.subsystem}, ignore')
                    continue
                try:
                    subsys_hosts = s["hosts"]
                except Exception:
                    subsys_hosts = []
                    pass
                for h in subsys_hosts:
                    try:
                        host_nqns.append(h["nqn"])
                    except Exception:
                        pass
                break
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        for conn in ctrl_ret:
            try:
                traddr = ""
                trsvcid = 0
                adrfam = ""
                trtype = "TCP"
                hostnqn = conn["hostnqn"]
                connected = False
                found = False

                for qp in qpair_ret:
                    try:
                        if qp["cntlid"] != conn["cntlid"]:
                            continue
                        if qp["state"] != "active":
                            self.logger.debug(f"Qpair {qp} is not active")
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
                if not trtype:
                    trtype = "TCP"
                if not adrfam:
                    adrfam = "ipv4"
                one_conn = pb2.connection(nqn=hostnqn, connected=True,
                                          traddr=traddr, trsvcid=trsvcid, trtype=trtype, adrfam=adrfam,
                                          qpairs_count=conn["num_io_qpairs"], controller_id=conn["cntlid"])
                connections.append(one_conn)
                if hostnqn in host_nqns:
                    host_nqns.remove(hostnqn)
            except Exception:
                self.logger.exception(f"{conn=} parse error")
                pass

        for nqn in host_nqns:
            one_conn = pb2.connection(nqn=nqn, connected=False, traddr="<n/a>", trsvcid=0,
                                      qpairs_count=-1, controller_id=-1)
            connections.append(one_conn)

        return pb2.connections_info(status = 0, error_message = os.strerror(0),
                              subsystem_nqn=request.subsystem, connections=connections)

    def list_connections(self, request, context=None):
        return self.execute_grpc_function(self.list_connections_safe, request, context)

    def get_subsystem_ha_status(self, nqn) -> bool:
        if nqn not in self.subsys_ha:
            self.logger.warning(f"Subsystem {nqn} not found")
            return False

        enable_ha = self.subsys_ha[nqn]
        self.logger.debug(f"Subsystem {nqn} enable_ha: {enable_ha}")
        return enable_ha

    def create_listener_safe(self, request, context):
        """Creates a listener for a subsystem at a given IP/Port."""

        ret = True
        create_listener_error_prefix = f"Failure adding {request.nqn} listener at {request.traddr}:{request.trsvcid}"

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam == None:
            errmsg=f"{create_listener_error_prefix}: Unknown address family {request.adrfam}"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to create {request.host_name}"
                         f" TCP {adrfam} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}, context: {context}{peer_msg}")

        if GatewayUtils.is_discovery_nqn(request.nqn):
            errmsg=f"{create_listener_error_prefix}: Can't create a listener for a discovery subsystem"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not GatewayState.is_key_element_valid(request.host_name):
            errmsg=f"{create_listener_error_prefix}: Host name \"{request.host_name}\" contains invalid characters"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                if request.host_name == self.host_name:
                    if (adrfam, request.traddr, request.trsvcid) in self.subsystem_listeners[request.nqn]:
                        self.logger.error(f"{request.nqn} already listens on address {request.traddr}:{request.trsvcid}")
                        return pb2.req_status(status=errno.EEXIST,
                                  error_message=f"{create_listener_error_prefix}: Subsystem already listens on this address")
                    ret = rpc_nvmf.nvmf_subsystem_add_listener(
                        self.spdk_rpc_client,
                        nqn=request.nqn,
                        trtype="TCP",
                        traddr=request.traddr,
                        trsvcid=str(request.trsvcid),
                        adrfam=adrfam,
                    )
                    self.logger.debug(f"create_listener: {ret}")
                    self.subsystem_listeners[request.nqn].add((adrfam, request.traddr, request.trsvcid))
                else:
                    if context:
                        errmsg=f"{create_listener_error_prefix}: Gateway's host name must match current host ({self.host_name})"
                        self.logger.error(f"{errmsg}")
                        return pb2.req_status(status=errno.ENODEV, error_message=errmsg)
                    else:
                        errmsg=f"Listener not created as gateway's host name {self.host_name} differs from requested host {request.host_name}"
                        self.logger.debug(f"{errmsg}")
                        return pb2.req_status(status=0, error_message=errmsg)
            except Exception as ex:
                self.logger.exception(create_listener_error_prefix)
                errmsg = f"{create_listener_error_prefix}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"{create_listener_error_prefix}: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

            # Just in case SPDK failed with no exception
            if not ret:
                self.logger.error(create_listener_error_prefix)
                return pb2.req_status(status=errno.EINVAL, error_message=create_listener_error_prefix)

            enable_ha = self.get_subsystem_ha_status(request.nqn)

            if enable_ha:
                try:
                    self.logger.debug(f"create_listener nvmf_subsystem_listener_set_ana_state {request=} set inaccessible for all ana groups")
                    _ana_state = "inaccessible"
                    ret = rpc_nvmf.nvmf_subsystem_listener_set_ana_state(
                      self.spdk_rpc_client,
                      nqn=request.nqn,
                      ana_state=_ana_state,
                      trtype="TCP",
                      traddr=request.traddr,
                      trsvcid=str(request.trsvcid),
                      adrfam=adrfam)
                    self.logger.debug(f"create_listener nvmf_subsystem_listener_set_ana_state response {ret=}")

                    # have been provided with ana state for this nqn prior to creation
                    # update optimized ana groups
                    if self.ana_map[request.nqn]:
                        for x in range (self.subsys_max_ns[request.nqn]):
                            ana_grp = x+1
                            if ana_grp in self.ana_map[request.nqn] and self.ana_map[request.nqn][ana_grp] == pb2.ana_state.OPTIMIZED:
                                _ana_state = "optimized"
                                self.logger.debug(f"using ana_map: set listener on nqn : {request.nqn}  ana state : {_ana_state} for group : {ana_grp}")
                                ret = rpc_nvmf.nvmf_subsystem_listener_set_ana_state(
                                  self.spdk_rpc_client,
                                  nqn=request.nqn,
                                  ana_state=_ana_state,
                                  trtype="TCP",
                                  traddr=request.traddr,
                                  trsvcid=str(request.trsvcid),
                                  adrfam=adrfam,
                                  anagrpid=ana_grp )
                                self.logger.debug(f"create_listener nvmf_subsystem_listener_set_ana_state response {ret=}")

                except Exception as ex:
                    errmsg=f"{create_listener_error_prefix}: Error setting ANA state"
                    self.logger.exception(errmsg)
                    errmsg=f"{errmsg}:\n{ex}"
                    resp = self.parse_json_exeption(ex)
                    status = errno.EINVAL
                    if resp:
                        status = resp["code"]
                        errmsg = f"{create_listener_error_prefix}: Error setting ANA state: {resp['message']}"
                    return pb2.req_status(status=status, error_message=errmsg)

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_listener(request.nqn,
                                                    request.host_name,
                                                    "TCP", request.traddr,
                                                    request.trsvcid, json_req)
                except Exception as ex:
                    errmsg = f"Error persisting listener {request.traddr}:{request.trsvcid}"
                    self.logger.exception(errmsg)
                    errmsg = f"{errmsg}:\n{ex}"
                    return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        return pb2.req_status(status=0, error_message=os.strerror(0))

    def create_listener(self, request, context=None):
        return self.execute_grpc_function(self.create_listener_safe, request, context)

    def remove_listener_from_state(self, nqn, host_name, traddr, port, context):
        if not context:
            return pb2.req_status(status=0, error_message=os.strerror(0))

        if context:
            assert self.omap_lock.locked(), "OMAP is unlocked when calling remove_listener_from_state()"

        host_name = host_name.strip()
        listener_hosts = []
        if host_name == "*":
            state = self.gateway_state.local.get_state()
            listener_prefix = GatewayState.build_partial_listener_key(nqn)
            for key, val in state.items():
                if not key.startswith(listener_prefix):
                    continue
                try:
                    listener = json.loads(val)
                    listener_nqn = listener["nqn"]
                    if listener_nqn != nqn:
                        self.logger.warning(f"Got subsystem {listener_nqn} instead of {nqn}, ignore")
                        continue
                    if listener["traddr"] != traddr:
                        continue
                    if listener["trsvcid"] != port:
                        continue
                    listener_hosts.append(listener["host_name"])
                except Exception:
                    self.logger.exception(f"Got exception while parsing {val}")
                    continue
        else:
            listener_hosts.append(host_name)

        # Update gateway state
        req_status = None
        for one_host in listener_hosts:
            try:
                self.gateway_state.remove_listener(nqn, one_host, "TCP", traddr, port)
            except Exception as ex:
                errmsg = f"Error persisting deletion of {one_host} listener {traddr}:{port} from {nqn}"
                self.logger.exception(errmsg)
                if not req_status:
                    errmsg = f"{errmsg}:\n{ex}"
                    req_status = pb2.req_status(status=errno.EINVAL, error_message=errmsg)
        if not req_status:
            req_status = pb2.req_status(status=0, error_message=os.strerror(0))

        return req_status

    def delete_listener_safe(self, request, context):
        """Deletes a listener from a subsystem at a given IP/Port."""

        ret = True
        traddr = GatewayUtils.escape_address_if_ipv6(request.traddr)
        delete_listener_error_prefix = f"Listener {traddr}:{request.trsvcid} failed to delete from {request.nqn}"

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam == None:
            errmsg=f"{delete_listener_error_prefix}. Unknown address family {request.adrfam}"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        peer_msg = self.get_peer_message(context)
        force_msg = " forcefully" if request.force else ""
        host_msg = "all hosts" if request.host_name == "*" else f"host {request.host_name}"

        self.logger.info(f"Received request to delete TCP listener of {host_msg}"
                         f" for subsystem {request.nqn} at"
                         f" {traddr}:{request.trsvcid}{force_msg}, context: {context}{peer_msg}")

        if request.host_name == "*" and not request.force:
            errmsg=f"{delete_listener_error_prefix}. Must use the \"--force\" parameter when setting the host name to \"*\"."
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if GatewayUtils.is_discovery_nqn(request.nqn):
            errmsg=f"{delete_listener_error_prefix}. Can't delete a listener from a discovery subsystem"
            self.logger.error(errmsg)
            return pb2.req_status(status=errno.EINVAL, error_message=errmsg)

        if not request.force:
            list_conn_req = pb2.list_connections_req(subsystem=request.nqn)
            list_conn_ret = self.list_connections_safe(list_conn_req, context)
            if list_conn_ret.status != 0:
                errmsg=f"{delete_listener_error_prefix}. Can't verify there are no active connections for this address"
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOTEMPTY, error_message=errmsg)
            for conn in list_conn_ret.connections:
                if not conn.connected:
                    continue
                if conn.traddr != request.traddr:
                    continue
                if conn.trsvcid != request.trsvcid:
                    continue
                errmsg=f"{delete_listener_error_prefix} due to active connections for {request.traddr}:{request.trsvcid}. Deleting the listener terminates active connections. You can continue to delete the listener by adding the `--force` parameter."
                self.logger.error(errmsg)
                return pb2.req_status(status=errno.ENOTEMPTY, error_message=errmsg)

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                if request.host_name == self.host_name or request.force:
                    ret = rpc_nvmf.nvmf_subsystem_remove_listener(
                        self.spdk_rpc_client,
                        nqn=request.nqn,
                        trtype="TCP",
                        traddr=request.traddr,
                        trsvcid=str(request.trsvcid),
                        adrfam=adrfam,
                    )
                    self.logger.debug(f"delete_listener: {ret}")
                    self.subsystem_listeners[request.nqn].remove((adrfam, request.traddr, request.trsvcid))
                else:
                    errmsg=f"{delete_listener_error_prefix}. Gateway's host name must match current host ({self.host_name}). You can continue to delete the listener by adding the `--force` parameter."
                    self.logger.error(f"{errmsg}")
                    return pb2.req_status(status=errno.ENOENT, error_message=errmsg)
            except Exception as ex:
                self.logger.exception(delete_listener_error_prefix)
                # It's OK for SPDK to fail in case we used a different host name, just continue to remove from OMAP
                if request.host_name == self.host_name:
                    errmsg = f"{delete_listener_error_prefix}:\n{ex}"
                    self.remove_listener_from_state(request.nqn, request.host_name,
                                                    request.traddr, request.trsvcid, context)
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
                                                request.traddr, request.trsvcid, context)
                return pb2.req_status(status=errno.EINVAL, error_message=delete_listener_error_prefix)

            return self.remove_listener_from_state(request.nqn, request.host_name,
                                                   request.traddr, request.trsvcid, context)

    def delete_listener(self, request, context=None):
        return self.execute_grpc_function(self.delete_listener_safe, request, context)

    def list_listeners_safe(self, request, context):
        """List listeners."""

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to list listeners for {request.subsystem}, context: {context}{peer_msg}")

        listeners = []
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            state = self.gateway_state.local.get_state()
            listener_prefix = GatewayState.build_partial_listener_key(request.subsystem)
            for key, val in state.items():
                if not key.startswith(listener_prefix):
                    continue
                try:
                    listener = json.loads(val)
                    nqn = listener["nqn"]
                    if nqn != request.subsystem:
                        self.logger.warning(f"Got subsystem {nqn} instead of {request.subsystem}, ignore")
                        continue
                    one_listener = pb2.listener_info(host_name = listener["host_name"],
                                                     trtype = "TCP",
                                                     adrfam = listener["adrfam"],
                                                     traddr = listener["traddr"],
                                                     trsvcid = listener["trsvcid"])
                    listeners.append(one_listener)
                except Exception:
                    self.logger.exception(f"Got exception while parsing {val}")
                    continue

        return pb2.listeners_info(status = 0, error_message = os.strerror(0), listeners=listeners)

    def list_listeners(self, request, context=None):
        return self.execute_grpc_function(self.list_listeners_safe, request, context)

    def list_subsystems_safe(self, request, context):
        """List subsystems."""

        peer_msg = self.get_peer_message(context)
        log_level = logging.INFO if context else logging.DEBUG
        if request.subsystem_nqn:
            self.logger.log(log_level, f"Received request to list subsystem {request.subsystem_nqn}, context: {context}{peer_msg}")
        else:
            if request.serial_number:
                self.logger.log(log_level, f"Received request to list the subsystem with serial number {request.serial_number}, context: {context}{peer_msg}")
            else:
                self.logger.log(log_level, f"Received request to list all subsystems, context: {context}{peer_msg}")

        subsystems = []
        try:
            if request.subsystem_nqn:
                ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client, nqn=request.subsystem_nqn)
            else:
                ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.debug(f"list_subsystems: {ret}")
        except Exception as ex:
            errmsg = f"Failure listing subsystems"
            self.logger.exception(errmsg)
            errmsg = f"{errmsg}:\n{ex}"
            resp = self.parse_json_exeption(ex)
            status = errno.ENODEV
            if resp:
                status = resp["code"]
                errmsg = f"Failure listing subsystems: {resp['message']}"
            return pb2.subsystems_info_cli(status=status, error_message=errmsg, subsystems=[])

        for s in ret:
            try:
                if request.serial_number:
                    if s["serial_number"] != request.serial_number:
                        continue
                if s["subtype"] == "NVMe":
                    s["namespace_count"] = len(s["namespaces"])
                    s["enable_ha"] = self.get_subsystem_ha_status(s["nqn"])
                else:
                    s["namespace_count"] = 0
                    s["enable_ha"] = False
                # Parse the JSON dictionary into the protobuf message
                subsystem = pb2.subsystem_cli()
                json_format.Parse(json.dumps(s), subsystem, ignore_unknown_fields=True)
                subsystems.append(subsystem)
            except Exception:
                self.logger.exception(f"{s=} parse error")
                pass

        return pb2.subsystems_info_cli(status = 0, error_message = os.strerror(0), subsystems=subsystems)

    def get_subsystems_safe(self, request, context):
        """Gets subsystems."""

        peer_msg = self.get_peer_message(context)
        self.logger.debug(f"Received request to get subsystems, context: {context}{peer_msg}")
        subsystems = []
        try:
            ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
        except Exception as ex:
            self.logger.exception(f"get_subsystems failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        for s in ret:
            try:
                ns_key = "namespaces"
                if ns_key in s:
                    for n in s[ns_key]:
                        nqn = s["nqn"]
                        nsid = n["nsid"]
                        n["anagrpid"] = self.subsystem_nsid_anagrp[nqn][nsid]
                        bdev = self.subsystem_nsid_bdev[nqn][nsid]
                        nonce = self.cluster_nonce[self.bdev_cluster[bdev]]
                        n["nonce"] = nonce
                # Parse the JSON dictionary into the protobuf message
                subsystem = pb2.subsystem()
                json_format.Parse(json.dumps(s), subsystem, ignore_unknown_fields=True)
                subsystems.append(subsystem)
            except Exception:
                self.logger.exception(f"{s=} parse error")
                raise

        return pb2.subsystems_info(subsystems=subsystems)

    def get_subsystems(self, request, context):
        with self.rpc_lock:
            return self.get_subsystems_safe(request, context)

    def list_subsystems(self, request, context=None):
        return self.execute_grpc_function(self.list_subsystems_safe, request, context)

    def get_spdk_nvmf_log_flags_and_level_safe(self, request, context):
        """Gets spdk nvmf log flags, log level and log print level"""
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get SPDK nvmf log flags and level{peer_msg}")
        log_flags = []
        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                nvmf_log_flags = {key: value for key, value in rpc_log.log_get_flags(
                    self.spdk_rpc_client).items() if key.startswith('nvmf')}
                for flag, flagvalue in nvmf_log_flags.items():
                    pb2_log_flag = pb2.spdk_log_flag_info(name = flag, enabled = flagvalue)
                    log_flags.append(pb2_log_flag)
                spdk_log_level = rpc_log.log_get_level(self.spdk_rpc_client)
                spdk_log_print_level = rpc_log.log_get_print_level(self.spdk_rpc_client)
                self.logger.debug(f"spdk log flags: {nvmf_log_flags}, " 
                                 f"spdk log level: {spdk_log_level}, "
                                 f"spdk log print level: {spdk_log_print_level}")
            except Exception as ex:
                errmsg = f"Failure getting SPDK log levels and nvmf log flags"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.ENOKEY
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure getting SPDK log levels and nvmf log flags: {resp['message']}"
                return pb2.spdk_nvmf_log_flags_and_level_info(status = status, error_message = errmsg)

        return pb2.spdk_nvmf_log_flags_and_level_info(
            nvmf_log_flags=log_flags,
            log_level = spdk_log_level,
            log_print_level = spdk_log_print_level,
            status = 0,
            error_message = os.strerror(0))

    def get_spdk_nvmf_log_flags_and_level(self, request, context=None):
        return self.execute_grpc_function(self.get_spdk_nvmf_log_flags_and_level_safe, request, context)

    def set_spdk_nvmf_logs_safe(self, request, context):
        """Enables spdk nvmf logs"""
        log_level = None
        print_level = None
        ret_log = False
        ret_print = False

        peer_msg = self.get_peer_message(context)
        if request.HasField("log_level"):
            log_level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, request.log_level)
            if log_level == None:
                errmsg=f"Unknown log level {request.log_level}"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        if request.HasField("print_level"):
            print_level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, request.print_level)
            if print_level == None:
                errmsg=f"Unknown print level {request.print_level}"
                self.logger.error(f"{errmsg}")
                return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)

        self.logger.info(f"Received request to set SPDK nvmf logs: log_level: {log_level}, print_level: {print_level}{peer_msg}")

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                nvmf_log_flags = [key for key in rpc_log.log_get_flags(self.spdk_rpc_client).keys() \
                                  if key.startswith('nvmf')]
                ret = [rpc_log.log_set_flag(
                    self.spdk_rpc_client, flag=flag) for flag in nvmf_log_flags]
                self.logger.debug(f"Set SPDK nvmf log flags {nvmf_log_flags} to TRUE: {ret}")
                if log_level != None:
                    ret_log = rpc_log.log_set_level(self.spdk_rpc_client, level=log_level)
                    self.logger.debug(f"Set log level to {log_level}: {ret_log}")
                if print_level != None:
                    ret_print = rpc_log.log_set_print_level(
                        self.spdk_rpc_client, level=print_level)
                    self.logger.debug(f"Set log print level to {print_level}: {ret_print}")
            except Exception as ex:
                errmsg="Failure setting SPDK log levels"
                self.logger.exception(errmsg)
                errmsg="{errmsg}:\n{ex}"
                for flag in nvmf_log_flags:
                    rpc_log.log_clear_flag(self.spdk_rpc_client, flag=flag)
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure setting SPDK log levels: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

        status = 0
        errmsg = os.strerror(0)
        if log_level != None and not ret_log:
            status = errno.EINVAL
            errmsg = "Failure setting SPDK log level"
        elif print_level != None and not ret_print:
            status = errno.EINVAL
            errmsg = "Failure setting SPDK print log level"
        elif not all(ret):
            status = errno.EINVAL
            errmsg = "Failure setting some SPDK nvmf log flags"
        return pb2.req_status(status=status, error_message=errmsg)

    def set_spdk_nvmf_logs(self, request, context=None):
        return self.execute_grpc_function(self.set_spdk_nvmf_logs_safe, request, context)

    def disable_spdk_nvmf_logs_safe(self, request, context):
        """Disables spdk nvmf logs"""
        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to disable SPDK nvmf logs{peer_msg}")

        omap_lock = self.omap_lock.get_omap_lock_to_use(context)
        with omap_lock:
            try:
                nvmf_log_flags = [key for key in rpc_log.log_get_flags(self.spdk_rpc_client).keys() \
                                  if key.startswith('nvmf')]
                ret = [rpc_log.log_clear_flag(self.spdk_rpc_client, flag=flag) for flag in nvmf_log_flags]
                logs_level = [rpc_log.log_set_level(self.spdk_rpc_client, level='NOTICE'),
                              rpc_log.log_set_print_level(self.spdk_rpc_client, level='INFO')]
                ret.extend(logs_level)
            except Exception as ex:
                errmsg = f"Failure in disable SPDK nvmf log flags"
                self.logger.exception(errmsg)
                errmsg = f"{errmsg}:\n{ex}"
                resp = self.parse_json_exeption(ex)
                status = errno.EINVAL
                if resp:
                    status = resp["code"]
                    errmsg = f"Failure in disable SPDK nvmf log flags: {resp['message']}"
                return pb2.req_status(status=status, error_message=errmsg)

        status = 0
        errmsg = os.strerror(0)
        if not all(ret):
            status = errno.EINVAL
            errmsg = "Failure in disable SPDK nvmf log flags"
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

        peer_msg = self.get_peer_message(context)
        self.logger.info(f"Received request to get gateway's info{peer_msg}")
        gw_version_string = os.getenv("NVMEOF_VERSION")
        spdk_version_string = os.getenv("NVMEOF_SPDK_VERSION")
        cli_version_string = request.cli_version
        addr = self.config.get_with_default("gateway", "addr", "")
        port = self.config.get_with_default("gateway", "port", "")
        ret = pb2.gateway_info(cli_version = request.cli_version,
                               version = gw_version_string,
                               spdk_version = spdk_version_string,
                               name = self.gateway_name,
                               group = self.gateway_group,
                               addr = addr,
                               port = port,
                               load_balancing_group = self.group_id + 1,
                               bool_status = True,
                               hostname = self.host_name,
                               status = 0,
                               error_message = os.strerror(0))
        cli_ver = self.parse_version(cli_version_string)
        gw_ver = self.parse_version(gw_version_string)
        if cli_ver != None and gw_ver != None and cli_ver < gw_ver:
            ret.bool_status = False
            ret.status = errno.EINVAL
            ret.error_message = f"CLI version {cli_version_string} is older than gateway's version {gw_version_string}"
        elif not gw_version_string:
            ret.bool_status = False
            ret.status = errno.ENOKEY
            ret.error_message = "Gateway's version not found"
        elif not gw_ver:
            ret.bool_status = False
            ret.status = errno.EINVAL
            ret.error_message = f"Invalid gateway's version {gw_version_string}"
        if not cli_version_string:
            self.logger.warning(f"No CLI version specified, can't check version compatibility")
        elif not cli_ver:
            self.logger.warning(f"Invalid CLI version {cli_version_string}, can't check version compatibility")
        if ret.status == 0:
            log_func = self.logger.debug
        else:
            log_func = self.logger.error
        log_func(f"Gateway's info:\n{ret}")
        return ret

    def get_gateway_info(self, request, context=None):
        """Get gateway's info"""
        return self.execute_grpc_function(self.get_gateway_info_safe, request, context)

    def get_gateway_log_level(self, request, context=None):
        """Get gateway's log level"""
        peer_msg = self.get_peer_message(context)
        try:
            log_level = GatewayEnumUtils.get_key_from_value(pb2.GwLogLevel, self.logger.level)
        except Exception:
            self.logger.exception(f"Can't get string value for log level {self.logger.level}")
            return pb2.gateway_log_level_info(status = errno.ENOKEY,
                                              error_message=f"Invalid gateway log level")
        self.logger.info(f"Received request to get gateway's log level. Level is {log_level}{peer_msg}")
        return pb2.gateway_log_level_info(status = 0, error_message=os.strerror(0), log_level=log_level)

    def set_gateway_log_level(self, request, context=None):
        """Set gateway's log level"""

        peer_msg = self.get_peer_message(context)
        log_level = GatewayEnumUtils.get_key_from_value(pb2.GwLogLevel, request.log_level)
        if log_level == None:
            errmsg=f"Unknown log level {request.log_level}"
            self.logger.error(f"{errmsg}")
            return pb2.req_status(status=errno.ENOKEY, error_message=errmsg)
        log_level = log_level.upper()

        self.logger.info(f"Received request to set gateway's log level to {log_level}{peer_msg}")
        self.gw_logger_object.set_log_level(request.log_level)

        try:
            os.remove(GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH)
        except FileNotFoundError:
            pass
        except Exception:
            self.logger.exception(f"Failure removing \"{GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH}\"")

        try:
            with open(GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH, "w") as f:
                f.write(str(request.log_level))
        except Exception:
            self.logger.exception(f"Failure writing log level to \"{GatewayLogger.NVME_GATEWAY_LOG_LEVEL_FILE_PATH}\"")

        return pb2.req_status(status=0, error_message=os.strerror(0))
