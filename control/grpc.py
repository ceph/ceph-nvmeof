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
import logging
import os
import threading
import errno

import spdk.rpc.bdev as rpc_bdev
import spdk.rpc.nvmf as rpc_nvmf
import spdk.rpc.log as rpc_log

from google.protobuf import json_format
from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc
from .config import GatewayConfig
from .state import GatewayState

MAX_ANA_GROUPS = 4

class GatewayEnumUtils:
    def get_value_from_key(e_type, keyval, ignore_case = False):
        val = None
        try:
            key_index = e_type.keys().index(keyval)
            val = e_type.values()[key_index]
        except ValueError:
            pass
        except IndexError:
            pass

        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.lower(), False)
        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.upper(), False)

        return val

    def get_key_from_value(e_type, val):
        keyval = None
        try:
            val_index = e_type.values().index(val)
            keyval = e_type.keys()[val_index]
        except ValueError:
            pass
        except IndexError:
            pass
        return keyval

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

    def __init__(self, config, gateway_state, omap_lock, spdk_rpc_client) -> None:
        """Constructor"""
        self.logger = logging.getLogger(__name__)
        ver = os.getenv("NVMEOF_VERSION")
        if ver:
            self.logger.info(f"Using NVMeoF gateway version {ver}")
        spdk_ver = os.getenv("NVMEOF_SPDK_VERSION")
        if spdk_ver:
            self.logger.info(f"Using SPDK version {spdk_ver}")
        ceph_ver = os.getenv("NVMEOF_CEPH_VERSION")
        if ceph_ver:
            self.logger.info(f"Using vstart cluster version based on {ceph_ver}")
        build_date = os.getenv("BUILD_DATE")
        if build_date:
            self.logger.info(f"NVMeoF gateway built on: {build_date}")
        git_rep = os.getenv("NVMEOF_GIT_REPO")
        if git_rep:
            self.logger.info(f"NVMeoF gateway Git repository: {git_rep}")
        git_branch = os.getenv("NVMEOF_GIT_BRANCH")
        if git_branch:
            self.logger.info(f"NVMeoF gateway Git branch: {git_branch}")
        git_commit = os.getenv("NVMEOF_GIT_COMMIT")
        if git_commit:
            self.logger.info(f"NVMeoF gateway Git commit: {git_commit}")
        git_modified = os.getenv("NVMEOF_GIT_MODIFIED_FILES")
        if git_modified:
            self.logger.info(f"NVMeoF gateway uncommitted modified files: {git_modified}")
        self.config = config
        config.dump_config_file(self.logger)
        self.rpc_lock = threading.Lock()
        self.gateway_state = gateway_state
        self.omap_lock = omap_lock
        self.spdk_rpc_client = spdk_rpc_client
        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()
        self.gateway_group = self.config.get("gateway", "group")
        self._init_cluster_context()

    def _init_cluster_context(self) -> None:
        """Init cluster context management variables"""
        self.clusters = {}
        self.current_cluster = None
        self.bdevs_per_cluster = self.config.getint_with_default("spdk", "bdevs_per_cluster", 8)
        if self.bdevs_per_cluster < 1:
            raise Exception(f"invalid configuration: spdk.bdevs_per_cluster_contexts {self.bdevs_per_cluster} < 1")
        self.librbd_core_mask = self.config.get_with_default("spdk", "librbd_core_mask", None)
        self.rados_id = self.config.get_with_default("ceph", "id", "")
        if self.rados_id == "":
            self.rados_id = None

    def _get_cluster(self) -> str:
        """Returns cluster name, enforcing bdev per cluster context"""
        cluster_name = None
        if self.current_cluster is None:
            cluster_name = self._alloc_cluster()
            self.current_cluster = cluster_name
            self.clusters[cluster_name] = 1
        elif self.clusters[self.current_cluster] >= self.bdevs_per_cluster:
            self.current_cluster = None
            cluster_name = self._get_cluster()
        else:
            cluster_name = self.current_cluster
            self.clusters[cluster_name] += 1

        return cluster_name

    def _alloc_cluster(self) -> str:
        """Allocates a new Rados cluster context"""
        name = f"cluster_context_{len(self.clusters)}"
        self.logger.info(f"Allocating cluster {name=}")
        rpc_bdev.bdev_rbd_register_cluster(
            self.spdk_rpc_client,
            name = name,
            user = self.rados_id,
            core_mask = self.librbd_core_mask,
        )
        return name

    def _grpc_function_with_lock(self, func, request, context):
        with self.rpc_lock:
            return func(request, context)

    def execute_grpc_function(self, func, request, context):
        """This functions handles both the RPC and OMAP locks. It first takes the OMAP lock and then calls a
           help function which takes the RPC lock and call the GRPC function passes as a parameter. So, the GRPC
           function runs with both the OMAP and RPC locks taken
        """
        return self.omap_lock.execute_omap_locking_function(self._grpc_function_with_lock, func, request, context)

    def create_bdev_safe(self, request, context=None):
        """Creates a bdev from an RBD image."""

        if not request.uuid:
            request.uuid = str(uuid.uuid4())

        name = request.uuid if not request.bdev_name else request.bdev_name
        self.logger.info(f"Received request to create bdev {name} from"
                         f" {request.rbd_pool_name}/{request.rbd_image_name}"
                         f" with block size {request.block_size}, context: {context}")
        with self.omap_lock(context=context):
            try:
                bdev_name = rpc_bdev.bdev_rbd_create(
                    self.spdk_rpc_client,
                    name=name,
                    cluster_name=self._get_cluster(),
                    pool_name=request.rbd_pool_name,
                    rbd_name=request.rbd_image_name,
                    block_size=request.block_size,
                    uuid=request.uuid,
                )
                self.logger.info(f"create_bdev: {bdev_name}")
            except Exception as ex:
                self.logger.error(f"create_bdev failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.bdev()

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_bdev(bdev_name, json_req)
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting create_bdev {bdev_name}: {ex}")
                    raise

        return pb2.bdev(bdev_name=bdev_name, status=True)

    def create_bdev(self, request, context=None):
        return self.execute_grpc_function(self.create_bdev_safe, request, context)

    def resize_bdev_safe(self, request):
        """Resizes a bdev."""

        self.logger.info(f"Received request to resize bdev {request.bdev_name} to size {request.new_size} MiB")
        try:
            ret = rpc_bdev.bdev_rbd_resize(
                self.spdk_rpc_client,
                name=request.bdev_name,
                new_size=request.new_size,
            )
            self.logger.info(f"resize_bdev: {request.bdev_name}: {ret}")
        except Exception as ex:
            self.logger.error(f"resize_bdev failed with: \n {ex}")
            return pb2.req_status()

        return pb2.req_status(status=ret)

    def resize_bdev(self, request, context=None):
        with self.rpc_lock:
            return self.resize_bdev_safe(request)

    def get_bdev_namespaces(self, bdev_name) -> list:
        ns_list = []
        local_state_dict = self.gateway_state.local.get_state()
        for key, val in local_state_dict.items():
            if not key.startswith(self.gateway_state.local.NAMESPACE_PREFIX):
                continue
            try:
                ns = json.loads(val)
                if ns["bdev_name"] == bdev_name:
                    nsid = ns["nsid"]
                    nqn = ns["subsystem_nqn"]
                    ns_list.insert(0, {"nqn" : nqn, "nsid" : nsid})
            except Exception as ex:
                self.logger.error(f"Got exception trying to get bdev {bdev_name} namespaces: {ex}")
                pass

        return ns_list

    def delete_bdev_handle_exception(self, context, ex):
        self.logger.error(f"delete_bdev failed with: \n {ex}")
        if context:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
        return pb2.req_status()

    def delete_bdev_safe(self, request, context=None):
        """Deletes a bdev."""

        self.logger.info(f"Received request to delete bdev {request.bdev_name}, context: {context}")
        ns_list = []
        with self.omap_lock(context=context):
            if context:
                ns_list = self.get_bdev_namespaces(request.bdev_name)
            for namespace in ns_list:
                # We found a namespace still using this bdev. If --force was used we will try to remove the namespace from OMAP.
                # Otherwise fail with EBUSY
                try:
                    ns_nsid = namespace["nsid"]
                    ns_nqn = namespace["nqn"]
                except Exception as ex:
                    self.logger.error(f"Got exception while trying to remove namespace: {namespace} which stil uses bdev {request.bdev_name}: {ex}")
                    continue

                if request.force:
                    self.logger.info(f"Will remove namespace {ns_nsid} from {ns_nqn} as it is using bdev {request.bdev_name}")
                    try:
                        self.gateway_state.remove_namespace(ns_nqn, str(ns_nsid))
                        self.logger.info(f"Removed namespace {ns_nsid} from {ns_nqn}")
                    except Exception as ex:
                        self.logger.error(f"Error removing namespace {ns_nsid} from {ns_nqn}, will delete bdev {request.bdev_name} anyway: {ex}")
                        pass
                else:
                    self.logger.error(f"Namespace {ns_nsid} from {ns_nqn} is still using bdev {request.bdev_name}. You need to either remove it or use the '--force' command line option")
                    req = {"name": request.bdev_name, "method": "bdev_rbd_delete", "req_id": 0}
                    ret = {"code": -errno.EBUSY, "message": os.strerror(errno.EBUSY)}
                    msg = "\n".join(["request:", "%s" % json.dumps(req, indent = 2),
                        "Got JSON-RPC error response", "response:", json.dumps(ret, indent = 2)])
                    return self.delete_bdev_handle_exception(context, Exception(msg))

            try:
                ret = rpc_bdev.bdev_rbd_delete(
                    self.spdk_rpc_client,
                    request.bdev_name,
                )
                self.logger.info(f"delete_bdev {request.bdev_name}: {ret}")
            except Exception as ex:
                return self.delete_bdev_handle_exception(context, ex)

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_bdev(request.bdev_name)
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting delete_bdev {request.bdev_name}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def delete_bdev(self, request, context=None):
        return self.execute_grpc_function(self.delete_bdev_safe, request, context)

    def is_discovery_nqn(self, nqn) -> bool:
        return nqn == GatewayConfig.DISCOVERY_NQN

    def serial_number_already_used(self, context, serial) -> str:
        if not context:
            return None
        state = self.gateway_state.local.get_state()
        for key, val in state.items():
            if not key.startswith(self.gateway_state.local.SUBSYSTEM_PREFIX):
                continue
            try:
                subsys = json.loads(val)
                sn = subsys["serial_number"]
                if serial == sn:
                    return subsys["subsystem_nqn"]
            except Exception:
                self.logger.warning("Got exception while parsing {val}: {ex}")
                continue
        return None

    def create_subsystem_safe(self, request, context=None):
        """Creates a subsystem."""

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}, enable_ha: {request.enable_ha}, ana reporting: {request.ana_reporting}, context: {context}")

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't create a discovery subsystem")
        if request.enable_ha == True  and request.ana_reporting == False:
            raise Exception(f"Validation Error: HA enabled but ANA-reporting is disabled ")

        min_cntlid = self.config.getint_with_default("gateway", "min_controller_id", 1)
        max_cntlid = self.config.getint_with_default("gateway", "max_controller_id", 65519)
        if not request.serial_number:
            random.seed()
            randser = random.randint(2, 99999999999999)
            request.serial_number = f"SPDK{randser}"
            self.logger.info(f"No serial number specified, will use {request.serial_number}")

        with self.omap_lock(context=context):
            try:
                subsys_using_serial = self.serial_number_already_used(context, request.serial_number)
                if subsys_using_serial:
                    self.logger.error(f"Serial number {request.serial_number} already used by subsystem {subsys_using_serial}")
                    req = {"subsystem_nqn": request.subsystem_nqn,
                           "serial_number": request.serial_number,
                           "max_namespaces": request.max_namespaces,
                           "ana_reporting": request.ana_reporting,
                           "enable_ha": request.enable_ha,
                           "method": "nvmf_create_subsystem", "req_id": 0}
                    ret = {"code": -errno.EEXIST, "message": f"Serial number {request.serial_number} already used by subsystem {subsys_using_serial}"}
                    msg = "\n".join(["request:", "%s" % json.dumps(req, indent=2),
                                    "Got JSON-RPC error response",
                                    "response:",
                                    json.dumps(ret, indent=2)])
                    raise Exception(msg)
                ret = rpc_nvmf.nvmf_create_subsystem(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    serial_number=request.serial_number,
                    max_namespaces=request.max_namespaces,
                    min_cntlid=min_cntlid,
                    max_cntlid=max_cntlid,
                    ana_reporting = request.ana_reporting,
                )
                self.logger.info(f"create_subsystem {request.subsystem_nqn}: {ret}")
            except Exception as ex:
                self.logger.error(f"create_subsystem failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_subsystem(request.subsystem_nqn,
                                                     json_req)
                except Exception as ex:
                    self.logger.error(f"Error persisting create_subsystem"
                                      f" {request.subsystem_nqn}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def create_subsystem(self, request, context=None):
        return self.execute_grpc_function(self.create_subsystem_safe, request, context)

    def delete_subsystem_safe(self, request, context=None):
        """Deletes a subsystem."""

        self.logger.info(
            f"Received request to delete subsystem {request.subsystem_nqn}, context: {context}")

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't delete a discovery subsystem")

        with self.omap_lock(context=context):
            try:
                ret = rpc_nvmf.nvmf_delete_subsystem(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                )
                self.logger.info(f"delete_subsystem {request.subsystem_nqn}: {ret}")
            except Exception as ex:
                self.logger.error(f"delete_subsystem failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_subsystem(request.subsystem_nqn)
                except Exception as ex:
                    self.logger.error(f"Error persisting delete_subsystem"
                                      f" {request.subsystem_nqn}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def delete_subsystem(self, request, context=None):
        return self.execute_grpc_function(self.delete_subsystem_safe, request, context)

    def add_namespace_safe(self, request, context=None):
        """Adds a namespace to a subsystem."""
              
        self.logger.info(f"Received request to add {request.bdev_name} to"
                         f" {request.subsystem_nqn}, context: {context}")

        if request.anagrpid > MAX_ANA_GROUPS:
            raise Exception(f"Error group ID {request.anagrpid} is more than configured maximum {MAX_ANA_GROUPS}")

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't add a namespace to a discovery subsystem")

        with self.omap_lock(context=context):
            try:
                nsid = rpc_nvmf.nvmf_subsystem_add_ns(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    bdev_name=request.bdev_name,
                    nsid=request.nsid,
                    anagrpid=request.anagrpid,
                )
                self.logger.info(f"add_namespace: {nsid}")
            except Exception as ex:
                self.logger.error(f"add_namespace failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.nsid_status()

            if context:
                # Update gateway state
                try:
                    if not request.nsid:
                        request.nsid = nsid
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_namespace(request.subsystem_nqn,
                                                     str(nsid), json_req)
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting add_namespace {nsid}: {ex}")
                    raise

        return pb2.nsid_status(nsid=nsid, status=True)

    def add_namespace(self, request, context=None):
        return self.execute_grpc_function(self.add_namespace_safe, request, context)

    def remove_namespace_safe(self, request, context=None):
        """Removes a namespace from a subsystem."""

        self.logger.info(f"Received request to remove nsid {request.nsid} from"
                         f" {request.subsystem_nqn}, context: {context}")

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't remove a namespace from a discovery subsystem")

        with self.omap_lock(context=context):
            try:
                ret = rpc_nvmf.nvmf_subsystem_remove_ns(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    nsid=request.nsid,
                )
                self.logger.info(f"remove_namespace {request.nsid}: {ret}")
            except Exception as ex:
                self.logger.error(f"remove_namespace failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_namespace(request.subsystem_nqn,
                                                        str(request.nsid))
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting remove_namespace {request.nsid}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def remove_namespace(self, request, context=None):
        return self.execute_grpc_function(self.remove_namespace_safe, request, context)

    def matching_host_exists(self, context, subsys_nqn, host_nqn) -> bool:
        if not context:
            return False
        host_key = GatewayState.build_host_key(subsys_nqn, host_nqn)
        state = self.gateway_state.local.get_state()
        if state.get(host_key):
            return True
        else:
            return False

    def add_host_safe(self, request, context=None):
        """Adds a host to a subsystem."""

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't allow a host to a discovery subsystem")

        if self.is_discovery_nqn(request.host_nqn):
            raise Exception(f"Can't use a discovery NQN as host NQN")

        with self.omap_lock(context=context):
            try:
                host_already_exist = self.matching_host_exists(context, request.subsystem_nqn, request.host_nqn)
                if host_already_exist:
                    if request.host_nqn == "*":
                        self.logger.error(f"All hosts already allowed to {request.subsystem_nqn}")
                        req = {"subsystem_nqn": request.subsystem_nqn, "host_nqn": request.host_nqn,
                               "method": "nvmf_subsystem_allow_any_host", "req_id": 0}
                        ret = {"code": -errno.EEXIST, "message": f"All hosts already allowed to {request.subsystem_nqn}"}
                    else:
                        self.logger.error(f"Host {request.host_nqn} already added to {request.subsystem_nqn}")
                        req = {"subsystem_nqn": request.subsystem_nqn, "host_nqn": request.host_nqn,
                               "method": "nvmf_subsystem_add_host", "req_id": 0}
                        ret = {"code": -errno.EEXIST, "message": f"Host {request.host_nqn} already added to {request.subsystem_nqn}"}
                    msg = "\n".join(["request:", "%s" % json.dumps(req, indent=2),
                                    "Got JSON-RPC error response",
                                    "response:",
                                    json.dumps(ret, indent=2)])
                    raise Exception(msg)
                if request.host_nqn == "*":  # Allow any host access to subsystem
                    self.logger.info(f"Received request to allow any host to"
                                     f" {request.subsystem_nqn}, context: {context}")
                    ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        disable=False,
                    )
                    self.logger.info(f"add_host *: {ret}")
                else:  # Allow single host access to subsystem
                    self.logger.info(
                        f"Received request to add host {request.host_nqn} to"
                        f" {request.subsystem_nqn}, context: {context}")
                    ret = rpc_nvmf.nvmf_subsystem_add_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                    )
                    self.logger.info(f"add_host {request.host_nqn}: {ret}")
            except Exception as ex:
                self.logger.error(f"add_host failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_host(request.subsystem_nqn,
                                                request.host_nqn, json_req)
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting add_host {request.host_nqn}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def add_host(self, request, context=None):
        return self.execute_grpc_function(self.add_host_safe, request, context)

    def remove_host_safe(self, request, context=None):
        """Removes a host from a subsystem."""

        if self.is_discovery_nqn(request.subsystem_nqn):
            raise Exception(f"Can't remove a host from a discovery subsystem")

        if self.is_discovery_nqn(request.host_nqn):
            raise Exception(f"Can't use a discovery NQN as host NQN")

        with self.omap_lock(context=context):
            try:
                if request.host_nqn == "*":  # Disable allow any host access
                    self.logger.info(
                        f"Received request to disable any host access to"
                        f" {request.subsystem_nqn}, context: {context}")
                    ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        disable=True,
                    )
                    self.logger.info(f"remove_host *: {ret}")
                else:  # Remove single host access to subsystem
                    self.logger.info(
                        f"Received request to remove host_{request.host_nqn} from"
                        f" {request.subsystem_nqn}, context: {context}")
                    ret = rpc_nvmf.nvmf_subsystem_remove_host(
                        self.spdk_rpc_client,
                        nqn=request.subsystem_nqn,
                        host=request.host_nqn,
                    )
                    self.logger.info(f"remove_host {request.host_nqn}: {ret}")
            except Exception as ex:
                self.logger.error(f"remove_host failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_host(request.subsystem_nqn,
                                                   request.host_nqn)
                except Exception as ex:
                    self.logger.error(f"Error persisting remove_host: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def remove_host(self, request, context=None):
        return self.execute_grpc_function(self.remove_host_safe, request, context)

    def matching_listener_exists(self, context, nqn, gw_name, trtype, traddr, trsvcid) -> bool:
        if not context:
            return False
        listener_key = GatewayState.build_listener_key(nqn, gw_name, trtype, traddr, trsvcid)
        state = self.gateway_state.local.get_state()
        if state.get(listener_key):
            return True
        else:
            return False

    def create_listener_safe(self, request, context=None):
        """Creates a listener for a subsystem at a given IP/Port."""
        ret = True
        traddr = GatewayConfig.escape_address_if_ipv6(request.traddr)

        trtype = GatewayEnumUtils.get_key_from_value(pb2.TransportType, request.trtype)
        if trtype == None:
            raise Exception(f"Unknown transport type {request.trtype}")

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam == None:
            raise Exception(f"Unknown address family {request.adrfam}")

        auto_ha_state = GatewayEnumUtils.get_key_from_value(pb2.AutoHAState, request.auto_ha_state)
        if auto_ha_state == None:
            raise Exception(f"Unknown auto HA state {request.auto_ha_state}")

        self.logger.info(f"Received request to create {request.gateway_name}"
                         f" {trtype} {adrfam} listener for {request.nqn} at"
                         f" {traddr}:{request.trsvcid}, auto HA state: {auto_ha_state}, context: {context}")

        if self.is_discovery_nqn(request.nqn):
            raise Exception(f"Can't create a listener for a discovery subsystem")

        with self.omap_lock(context=context):
            try:
                if request.gateway_name == self.gateway_name:
                    listener_already_exist = self.matching_listener_exists(
                            context, request.nqn, request.gateway_name, trtype, request.traddr, request.trsvcid)
                    if listener_already_exist:
                        self.logger.error(f"{request.nqn} already listens on address {request.traddr} port {request.trsvcid}")
                        req = {"nqn": request.nqn, "trtype": trtype, "traddr": request.traddr,
                               "gateway_name": request.gateway_name,
                               "trsvcid": request.trsvcid, "adrfam": adrfam,
                               "method": "nvmf_subsystem_add_listener", "req_id": 0}
                        ret = {"code": -errno.EEXIST, "message": f"{request.nqn} already listens on address {request.traddr} port {request.trsvcid}"}
                        msg = "\n".join(["request:", "%s" % json.dumps(req, indent=2),
                                        "Got JSON-RPC error response",
                                        "response:",
                                        json.dumps(ret, indent=2)])
                        raise Exception(msg)
                    ret = rpc_nvmf.nvmf_subsystem_add_listener(
                        self.spdk_rpc_client,
                        nqn=request.nqn,
                        trtype=trtype,
                        traddr=request.traddr,
                        trsvcid=request.trsvcid,
                        adrfam=adrfam,
                    )
                    self.logger.info(f"create_listener: {ret}")
                else:
                    raise Exception(f"Gateway name must match current gateway"
                                    f" ({self.gateway_name})")
            except Exception as ex:
                self.logger.error(f"create_listener failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            enable_ha = False
            if auto_ha_state == "AUTO_HA_UNSET":
                if context == None:
                    self.logger.error(f"auto_ha_state is not set but we are in an update()")
                state = self.gateway_state.local.get_state()
                subsys_str = state.get(GatewayState.build_subsystem_key(request.nqn))
                if subsys_str:
                    self.logger.debug(f"value of sub-system: {subsys_str}")
                    try:
                        subsys_dict = json.loads(subsys_str)
                        try:
                            enable_ha = subsys_dict["enable_ha"]
                            auto_ha_state_key = "AUTO_HA_ON" if enable_ha else "AUTO_HA_OFF"
                            request.auto_ha_state = GatewayEnumUtils.get_value_from_key(pb2.AutoHAState, auto_ha_state_key)
                        except KeyError:
                            enable_ha = False
                        self.logger.info(f"enable_ha: {enable_ha}")
                    except Exception as ex:
                        self.logger.error(f"Got exception trying to parse subsystem {request.nqn}: {ex}")
                        pass
                else:
                    self.logger.info(f"No subsystem for {request.nqn}")
            else:
                if context != None:
                    self.logger.error(f"auto_ha_state is set to {auto_ha_state} but we are not in an update()")
                if auto_ha_state == "AUTO_HA_OFF":
                    enable_ha = False
                elif auto_ha_state == "AUTO_HA_ON":
                    enable_ha = True

            if enable_ha:
                  for x in range (MAX_ANA_GROUPS):
                       try:
                          ret = rpc_nvmf.nvmf_subsystem_listener_set_ana_state(
                            self.spdk_rpc_client,
                            nqn=request.nqn,
                            ana_state="inaccessible",
                            trtype=trtype,
                            traddr=request.traddr,
                            trsvcid=request.trsvcid,
                            adrfam=adrfam,
                            anagrpid=(x+1) )
                       except Exception as ex:
                            self.logger.error(f"set_listener_ana_state failed with:\n{ex}")
                            raise

            if context:
                # Update gateway state
                try:
                    json_req = json_format.MessageToJson(
                        request, preserving_proto_field_name=True, including_default_value_fields=True)
                    self.gateway_state.add_listener(request.nqn,
                                                    request.gateway_name,
                                                    trtype, request.traddr,
                                                    request.trsvcid, json_req)
                except Exception as ex:
                    self.logger.error(f"Error persisting add_listener {request.trsvcid}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def create_listener(self, request, context=None):
        return self.execute_grpc_function(self.create_listener_safe, request, context)

    def delete_listener_safe(self, request, context=None):
        """Deletes a listener from a subsystem at a given IP/Port."""

        ret = True
        traddr = GatewayConfig.escape_address_if_ipv6(request.traddr)

        trtype = GatewayEnumUtils.get_key_from_value(pb2.TransportType, request.trtype)
        if trtype == None:
            raise Exception(f"Unknown transport type {request.trtype}")

        adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, request.adrfam)
        if adrfam == None:
            raise Exception(f"Unknown address family {request.adrfam}")

        self.logger.info(f"Received request to delete {request.gateway_name}"
                         f" {trtype} listener for {request.nqn} at"
                         f" {traddr}:{request.trsvcid}, context: {context}")

        if self.is_discovery_nqn(request.nqn):
            raise Exception(f"Can't delete a listener from a discovery subsystem")

        with self.omap_lock(context=context):
            try:
                if request.gateway_name == self.gateway_name:
                    ret = rpc_nvmf.nvmf_subsystem_remove_listener(
                        self.spdk_rpc_client,
                        nqn=request.nqn,
                        trtype=trtype,
                        traddr=request.traddr,
                        trsvcid=request.trsvcid,
                        adrfam=adrfam,
                    )
                    self.logger.info(f"delete_listener: {ret}")
                else:
                    raise Exception(f"Gateway name must match current gateway"
                                    f" ({self.gateway_name})")
            except Exception as ex:
                self.logger.error(f"delete_listener failed with: \n {ex}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(f"{ex}")
                return pb2.req_status()

            if context:
                # Update gateway state
                try:
                    self.gateway_state.remove_listener(request.nqn,
                                                       request.gateway_name,
                                                       trtype,
                                                       request.traddr,
                                                       request.trsvcid)
                except Exception as ex:
                    self.logger.error(
                        f"Error persisting delete_listener {request.trsvcid}: {ex}")
                    raise

        return pb2.req_status(status=ret)

    def delete_listener(self, request, context=None):
        return self.execute_grpc_function(self.delete_listener_safe, request, context)

    def get_subsystems_safe(self, request, context):
        """Gets subsystems."""

        self.logger.info(f"Received request to get subsystems, context: {context}")
        subsystems = []
        try:
            ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.info(f"get_subsystems: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        for s in ret:
            try:
                # Need to adjust values to fit enum constants
                try:
                    listen_addrs = s["listen_addresses"]
                except Exception:
                    listen_addrs = []
                    pass
                for addr in listen_addrs:
                    try:
                        addr["trtype"] = addr["trtype"].upper()
                    except Exception:
                        pass
                    try:
                        addr["adrfam"] = addr["adrfam"].lower()
                    except Exception:
                        pass
                # Parse the JSON dictionary into the protobuf message
                subsystem = pb2.subsystem()
                json_format.Parse(json.dumps(s), subsystem)
                subsystems.append(subsystem)
            except Exception:
                self.logger.exception(f"{s=} parse error: ")
                raise

        return pb2.subsystems_info(subsystems=subsystems)

    def get_subsystems(self, request, context):
        with self.rpc_lock:
            return self.get_subsystems_safe(request, context)

    def get_spdk_nvmf_log_flags_and_level_safe(self, request, context):
        """Gets spdk nvmf log flags, log level and log print level"""
        self.logger.info(f"Received request to get SPDK nvmf log flags and level")
        try:
            nvmf_log_flags = {key: value for key, value in rpc_log.log_get_flags(
                self.spdk_rpc_client).items() if key.startswith('nvmf')}
            spdk_log_level = {'log_level': rpc_log.log_get_level(self.spdk_rpc_client)}
            spdk_log_print_level = {'log_print_level': rpc_log.log_get_print_level(
                self.spdk_rpc_client)}
            flags_log_level = {**nvmf_log_flags, **spdk_log_level, **spdk_log_print_level}
            self.logger.info(f"spdk log flags: {nvmf_log_flags}, " 
                             f"spdk log level: {spdk_log_level}, "
                             f"spdk log print level: {spdk_log_print_level}")
        except Exception as ex:
            self.logger.error(f"get_spdk_nvmf_log_flags_and_level failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.spdk_nvmf_log_flags_and_level_info()

        return pb2.spdk_nvmf_log_flags_and_level_info(
            flags_level=json.dumps(flags_log_level))

    def get_spdk_nvmf_log_flags_and_level(self, request, context):
        with self.rpc_lock:
            return self.get_spdk_nvmf_log_flags_and_level_safe(request, context)

    def set_spdk_nvmf_logs_safe(self, request, context):
        """Enables spdk nvmf logs"""
        self.logger.info(f"Received request to set SPDK nvmf logs")
        log_level = None
        print_level = None
        if request.log_level:
            try:
                log_level = pb2.LogLevel.keys()[request.log_level]
            except Exception:
                raise Exception(f"Unknown log level {request.log_level}")

        if request.print_level:
            try:
                print_level = pb2.LogLevel.keys()[request.print_level]
            except Exception:
                raise Exception(f"Unknown print level {request.print_level}")

        try:
            nvmf_log_flags = [key for key in rpc_log.log_get_flags(self.spdk_rpc_client).keys() \
                              if key.startswith('nvmf')]
            ret = [rpc_log.log_set_flag(
                self.spdk_rpc_client, flag=flag) for flag in nvmf_log_flags]
            self.logger.info(f"Set SPDK log flags {nvmf_log_flags} to TRUE")
            if log_level:
                ret_log = rpc_log.log_set_level(self.spdk_rpc_client, level=log_level)
                self.logger.info(f"Set log level to: {log_level}")
                ret.append(ret_log)
            if print_level:
                ret_print = rpc_log.log_set_print_level(
                    self.spdk_rpc_client, level=print_level)
                self.logger.info(f"Set log print level to: {print_level}")
                ret.append(ret_print)
        except Exception as ex:
            self.logger.error(f"set_spdk_nvmf_logs failed with:\n{ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            for flag in nvmf_log_flags:
                rpc_log.log_clear_flag(self.spdk_rpc_client, flag=flag)
            return pb2.req_status()

        return pb2.req_status(status=all(ret))

    def set_spdk_nvmf_logs(self, request, context):
        with self.rpc_lock:
            return self.set_spdk_nvmf_logs_safe(request, context)

    def disable_spdk_nvmf_logs_safe(self, request, context):
        """Disables spdk nvmf logs"""
        self.logger.info(f"Received request to disable SPDK nvmf logs")
        try:
            nvmf_log_flags = [key for key in rpc_log.log_get_flags(self.spdk_rpc_client).keys() \
                              if key.startswith('nvmf')]
            ret = [rpc_log.log_clear_flag(
                self.spdk_rpc_client, flag=flag) for flag in nvmf_log_flags]
            logs_level = [rpc_log.log_set_level(self.spdk_rpc_client, level='NOTICE'),
                          rpc_log.log_set_print_level(self.spdk_rpc_client, level='INFO')]
            ret.extend(logs_level)
        except Exception as ex:
            self.logger.error(f"disable_spdk_nvmf_logs failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.req_status()

        return pb2.req_status(status=all(ret))

    def disable_spdk_nvmf_logs(self, request, context):
        with self.rpc_lock:
            return self.disable_spdk_nvmf_logs_safe(request, context)

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
            self.logger.error(f"Can't parse version \"{version}\"")
            return None
        return (v1, v2, v3)

    def get_gateway_info(self, request, context):
        """Return gateway's info"""
        self.logger.info(f"Received request to get gateway's info")
        gw_version_string = os.getenv("NVMEOF_VERSION")
        cli_version_string = request.cli_version
        addr = self.config.get_with_default("gateway", "addr", "")
        port = self.config.get_with_default("gateway", "port", "")
        ret = pb2.gateway_info(cli_version = request.cli_version,
                               gateway_version = gw_version_string,
                               gateway_name = self.gateway_name,
                               gateway_group = self.gateway_group,
                               gateway_addr = addr,
                               gateway_port = port,
                               status = True)
        cli_ver = self.parse_version(cli_version_string)
        gw_ver = self.parse_version(gw_version_string)
        if cli_ver != None and gw_ver != None and cli_ver < gw_ver:
            self.logger.error(f"CLI version {cli_version_string} is older than gateway's version {gw_version_string}")
            ret.status = False
        if not cli_version_string:
            self.logger.error(f"No CLI version specified")
            ret.status = False
        if not gw_version_string:
            self.logger.error(f"Gateway version not found")
            ret.status = False
        if not cli_ver or not gw_ver:
            ret.status = False
        self.logger.info(f"Gateway's info:\n{ret}")
        return ret
