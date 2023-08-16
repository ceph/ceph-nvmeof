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
import logging

import spdk.rpc.bdev as rpc_bdev
import spdk.rpc.nvmf as rpc_nvmf

from google.protobuf import json_format
from proto import gateway_pb2 as pb2
from proto import gateway_pb2_grpc as pb2_grpc


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

    def __init__(self, config, gateway_state, spdk_rpc_client):

        self.logger = logging.getLogger(__name__)
        self.config = config
        self.gateway_state = gateway_state
        self.spdk_rpc_client = spdk_rpc_client

        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()

    def create_bdev(self, request, context=None):
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

    def delete_bdev(self, request, context=None):
        """Deletes a bdev."""

        self.logger.info(f"Received request to delete bdev {request.bdev_name}")
        try:
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

    def create_subsystem(self, request, context=None):
        """Creates a subsystem."""

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}")
        min_cntlid = self.config.getint_with_default("gateway", "min_cntlid", 1)
        max_cntlid = self.config.getint_with_default("gateway", "max_cntlid", 65519)
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

    def delete_subsystem(self, request, context=None):
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

    def add_namespace(self, request, context=None):
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

    def remove_namespace(self, request, context=None):
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

    def add_host(self, request, context=None):
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

    def remove_host(self, request, context=None):
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

    def create_listener(self, request, context=None):
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

    def delete_listener(self, request, context=None):
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

    def get_subsystems(self, request, context):
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
