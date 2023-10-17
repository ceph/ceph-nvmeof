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

import spdk.rpc.bdev as rpc_bdev
import spdk.rpc.nvmf as rpc_nvmf

from google.protobuf import json_format
from .proto import gateway_pb2 as pb2
from .proto import gateway_pb2_grpc as pb2_grpc


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

    def __init__(self, config, gateway_state, spdk_rpc_client) -> None:
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
            self.logger.info(f"Using Ceph version {ceph_ver}")
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
        self.logger.info(f"Using configuration file {config.filepath}")
        try:
            with open(config.filepath) as f:
                self.logger.info(f"Configuration file content:")
                self.logger.info(f"============================================================================")
                for line in f:
                    line = line.rstrip()
                    self.logger.info(f"{line}")
                self.logger.info(f"============================================================================")
        except Exception:
            pass
        self.rpc_lock = threading.Lock()
        self.gateway_state = gateway_state
        self.spdk_rpc_client = spdk_rpc_client
        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()
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

    def create_bdev_safe(self, request, context=None):
        """Creates a bdev from an RBD image."""

        if not request.uuid:
            request.uuid = str(uuid.uuid4())

        name = request.uuid if not request.bdev_name else request.bdev_name
        self.logger.info(f"Received request to create bdev {name} from"
                         f" {request.rbd_pool_name}/{request.rbd_image_name}"
                         f" with block size {request.block_size}")
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
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_bdev(bdev_name, json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting create_bdev {bdev_name}: {ex}")
                raise

        return pb2.bdev(bdev_name=bdev_name, status=True)

    def create_bdev(self, request, context=None):
        with self.rpc_lock:
            return self.create_bdev_safe(request, context)

    def delete_bdev_safe(self, request, context=None):
        """Deletes a bdev."""

        self.logger.info(f"Received request to delete bdev {request.bdev_name}")
        use_excep = None
        req_get_subsystems = pb2.get_subsystems_req()
        # We already hold the lock, so call the safe version, do not try lock again
        ret = self.get_subsystems_safe(req_get_subsystems, context)
        subsystems = json.loads(ret.subsystems)
        for subsystem in subsystems:
            for namespace in subsystem['namespaces']:
                if namespace['bdev_name'] == request.bdev_name:
                    # We found a namespace still using this bdev. If --force was used we will try to remove this namespace.
                    # Otherwise fail with EBUSY
                    if request.force:
                        self.logger.info(f"Will remove namespace {namespace['nsid']} from {subsystem['nqn']} as it is using bdev {request.bdev_name}")
                        try:
                            req_rm_ns = pb2.remove_namespace_req(subsystem_nqn=subsystem['nqn'], nsid=namespace['nsid'])
                            # We already hold the lock, so call the safe version, do not try lock again
                            ret = self.remove_namespace_safe(req_rm_ns, context)
                            self.logger.info(
                                    f"Removed namespace {namespace['nsid']} from {subsystem['nqn']}: {ret.status}")
                        except Exception as ex:
                            self.logger.error(f"Error removing namespace {namespace['nsid']} from {subsystem['nqn']}, will delete bdev {request.bdev_name} anyway: {ex}")
                            pass
                    else:
                        self.logger.error(f"Namespace {namespace['nsid']} from {subsystem['nqn']} is still using bdev {request.bdev_name}. You need to either remove it or use the '--force' command line option")
                        req = {"name": request.bdev_name, "method": "bdev_rbd_delete", "req_id": 0}
                        ret = {"code": -16, "message": "Device or resource busy"}
                        msg = "\n".join(["request:", "%s" % json.dumps(req, indent=2),
                            "Got JSON-RPC error response",
                            "response:",
                            json.dumps(ret, indent=2)])
                        use_excep = Exception(msg)

        try:
            if use_excep:
                raise use_excep
            ret = rpc_bdev.bdev_rbd_delete(
                self.spdk_rpc_client,
                request.bdev_name,
            )
            self.logger.info(f"delete_bdev {request.bdev_name}: {ret}")
        except Exception as ex:
            self.logger.error(f"delete_bdev failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

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
        with self.rpc_lock:
            return self.delete_bdev_safe(request, context)

    def create_subsystem_safe(self, request, context=None):
        """Creates a subsystem."""

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}")
        min_cntlid = self.config.getint_with_default("gateway", "min_controller_id", 1)
        max_cntlid = self.config.getint_with_default("gateway", "max_controller_id", 65519)
        if not request.serial_number:
            random.seed()
            randser = random.randint(2, 99999999999999)
            request.serial_number = f"SPDK{randser}"
        try:
            ret = rpc_nvmf.nvmf_create_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                serial_number=request.serial_number,
                max_namespaces=request.max_namespaces,
                min_cntlid=min_cntlid,
                max_cntlid=max_cntlid,
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
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_subsystem(request.subsystem_nqn,
                                                 json_req)
            except Exception as ex:
                self.logger.error(f"Error persisting create_subsystem"
                                  f" {request.subsystem_nqn}: {ex}")
                raise

        return pb2.req_status(status=ret)

    def create_subsystem(self, request, context=None):
        with self.rpc_lock:
            return self.create_subsystem_safe(request, context)

    def delete_subsystem_safe(self, request, context=None):
        """Deletes a subsystem."""

        self.logger.info(
            f"Received request to delete subsystem {request.subsystem_nqn}")
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
        with self.rpc_lock:
            return self.delete_subsystem_safe(request, context)

    def add_namespace_safe(self, request, context=None):
        """Adds a namespace to a subsystem."""

        self.logger.info(f"Received request to add {request.bdev_name} to"
                         f" {request.subsystem_nqn}")
        try:
            nsid = rpc_nvmf.nvmf_subsystem_add_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                bdev_name=request.bdev_name,
                nsid=request.nsid,
            )
            self.logger.info(f"add_namespace: {nsid}")
        except Exception as ex:
            self.logger.error(f"add_namespace failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.nsid()

        if context:
            # Update gateway state
            try:
                if not request.nsid:
                    request.nsid = nsid
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_namespace(request.subsystem_nqn,
                                                 str(nsid), json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_namespace {nsid}: {ex}")
                raise

        return pb2.nsid(nsid=nsid, status=True)

    def add_namespace(self, request, context=None):
        with self.rpc_lock:
            return self.add_namespace_safe(request, context)

    def remove_namespace_safe(self, request, context=None):
        """Removes a namespace from a subsystem."""

        self.logger.info(f"Received request to remove {request.nsid} from"
                         f" {request.subsystem_nqn}")
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
        with self.rpc_lock:
            return self.remove_namespace_safe(request, context)

    def add_host_safe(self, request, context=None):
        """Adds a host to a subsystem."""

        try:
            if request.host_nqn == "*":  # Allow any host access to subsystem
                self.logger.info(f"Received request to allow any host to"
                                 f" {request.subsystem_nqn}")
                ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=False,
                )
                self.logger.info(f"add_host *: {ret}")
            else:  # Allow single host access to subsystem
                self.logger.info(
                    f"Received request to add host {request.host_nqn} to"
                    f" {request.subsystem_nqn}")
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
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_host(request.subsystem_nqn,
                                            request.host_nqn, json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_host {request.host_nqn}: {ex}")
                raise

        return pb2.req_status(status=ret)

    def add_host(self, request, context=None):
        with self.rpc_lock:
            return self.add_host_safe(request, context)

    def remove_host_safe(self, request, context=None):
        """Removes a host from a subsystem."""

        try:
            if request.host_nqn == "*":  # Disable allow any host access
                self.logger.info(
                    f"Received request to disable any host access to"
                    f" {request.subsystem_nqn}")
                ret = rpc_nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=True,
                )
                self.logger.info(f"remove_host *: {ret}")
            else:  # Remove single host access to subsystem
                self.logger.info(
                    f"Received request to remove host_{request.host_nqn} from"
                    f" {request.subsystem_nqn}")
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
        with self.rpc_lock:
            return self.remove_host_safe(request, context)

    def create_listener_safe(self, request, context=None):
        """Creates a listener for a subsystem at a given IP/Port."""

        ret = True
        self.logger.info(f"Received request to create {request.gateway_name}"
                         f" {request.trtype} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}.")
        try:
            if request.gateway_name == self.gateway_name:
                ret = rpc_nvmf.nvmf_subsystem_add_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=request.traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
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

        if context:
            # Update gateway state
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_listener(request.nqn,
                                                request.gateway_name,
                                                request.trtype, request.traddr,
                                                request.trsvcid, json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_listener {request.trsvcid}: {ex}")
                raise

        return pb2.req_status(status=ret)

    def create_listener(self, request, context=None):
        with self.rpc_lock:
            return self.create_listener_safe(request, context)

    def delete_listener_safe(self, request, context=None):
        """Deletes a listener from a subsystem at a given IP/Port."""

        ret = True
        self.logger.info(f"Received request to delete {request.gateway_name}"
                         f" {request.trtype} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}.")
        try:
            if request.gateway_name == self.gateway_name:
                ret = rpc_nvmf.nvmf_subsystem_remove_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=request.traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
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
                                                   request.trtype,
                                                   request.traddr,
                                                   request.trsvcid)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting delete_listener {request.trsvcid}: {ex}")
                raise

        return pb2.req_status(status=ret)

    def delete_listener(self, request, context=None):
        with self.rpc_lock:
            return self.delete_listener_safe(request, context)

    def get_subsystems_safe(self, request, context):
        """Gets subsystems."""

        self.logger.info(f"Received request to get subsystems")
        try:
            ret = rpc_nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.info(f"get_subsystems: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        return pb2.subsystems_info(subsystems=json.dumps(ret))

    def get_subsystems(self, request, context):
        with self.rpc_lock:
            return self.get_subsystems_safe(request, context)
