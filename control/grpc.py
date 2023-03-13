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
from enum import Enum
from google.protobuf import json_format
from .generated import gateway_pb2 as pb2
from .generated import gateway_pb2_grpc as pb2_grpc


class RequestStatus(Enum):
    INIT = 1
    ABORT = 2
    SUCCESS = 3


class GatewayService(pb2_grpc.GatewayServicer):
    """Implements gateway service interface.

    Handles configuration of the SPDK NVMEoF target according to client requests.

    Instance attributes:
        config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_name: Gateway identifier
        gateway_state: Methods for target state persistence
        spdk_rpc: Module methods for SPDK
        spdk_rpc_client: Client of SPDK RPC server
    """

    def __init__(self, config, gateway_state, spdk_rpc, spdk_rpc_client):

        self.logger = logging.getLogger(__name__)
        self.config = config
        self.gateway_state = gateway_state
        self.spdk_rpc = spdk_rpc
        self.spdk_rpc_client = spdk_rpc_client

        self.gateway_name = self.config.get("gateway", "name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()

    def create_bdev(self, request, context=None):
        """Creates a bdev from an RBD image."""

        if not request.bdev_name:
            request.bdev_name = str(uuid.uuid4())
        self.logger.info(
            f"Received request to create bdev {request.bdev_name} from"
            f" {request.rbd_pool_name}/{request.rbd_image_name}"
            f" with block size {request.block_size}")
        json_req = json_format.MessageToJson(request,
                                             preserving_proto_field_name=True)
        if context:  # Update state with incomplete request
            self.gateway_state.add_bdev(request.bdev_name, json_req,
                                        RequestStatus.INIT)

        try:
            bdev_name = self.spdk_rpc.bdev.bdev_rbd_create(
                self.spdk_rpc_client,
                name=request.bdev_name,
                pool_name=request.rbd_pool_name,
                rbd_name=request.rbd_image_name,
                block_size=request.block_size,
            )
            self.logger.info(f"create_bdev: {bdev_name}")
        except Exception as ex:
            self.logger.error(f"create_bdev failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.add_bdev(request.bdev_name, json_req,
                                            RequestStatus.ABORT)
            return pb2.bdev()

        if context:  # Update state with complete request
            self.gateway_state.add_bdev(request.bdev_name, json_req,
                                        RequestStatus.SUCCESS)

        return pb2.bdev(bdev_name=bdev_name, status=True)

    def delete_bdev(self, request, context=None):
        """Deletes a bdev."""

        self.logger.info(f"Received request to delete bdev {request.bdev_name}")
        if context:  # Update state with incomplete request
            self.gateway_state.remove_bdev(request.bdev_name,
                                           RequestStatus.INIT)

        try:
            ret = self.spdk_rpc.bdev.bdev_rbd_delete(
                self.spdk_rpc_client,
                request.bdev_name,
            )
            self.logger.info(f"delete_bdev {request.bdev_name}: {ret}")
        except Exception as ex:
            self.logger.error(f"delete_bdev failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.remove_bdev(request.bdev_name,
                                               RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.remove_bdev(request.bdev_name,
                                           RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def create_subsystem(self, request, context=None):
        """Creates a subsystem."""

        self.logger.info(
            f"Received request to create subsystem {request.subsystem_nqn}")
        json_req = json_format.MessageToJson(request,
                                             preserving_proto_field_name=True)
        if context:  # Update state with incomplete request
            self.gateway_state.add_subsystem(request.subsystem_nqn, json_req,
                                             RequestStatus.INIT)

        try:
            ret = self.spdk_rpc.nvmf.nvmf_create_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                serial_number=request.serial_number,
            )
            self.logger.info(f"create_subsystem {request.subsystem_nqn}: {ret}")
        except Exception as ex:
            self.logger.error(f"create_subsystem failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.add_subsystem(request.subsystem_nqn,
                                                 json_req, RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.add_subsystem(request.subsystem_nqn, json_req,
                                             RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def delete_subsystem(self, request, context=None):
        """Deletes a subsystem."""

        self.logger.info(
            f"Received request to delete subsystem {request.subsystem_nqn}")
        if context:  # Update state with incomplete request
            self.gateway_state.remove_subsystem(request.subsystem_nqn,
                                                RequestStatus.INIT)

        try:
            ret = self.spdk_rpc.nvmf.nvmf_delete_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
            )
            self.logger.info(f"delete_subsystem {request.subsystem_nqn}: {ret}")
        except Exception as ex:
            self.logger.error(f"delete_subsystem failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.remove_subsystem(request.subsystem_nqn,
                                                    RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.remove_subsystem(request.subsystem_nqn,
                                                RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def add_namespace(self, request, context=None):
        """Adds a namespace to a subsystem."""

        self.logger.info(f"Received request to add {request.bdev_name} to"
                         f" {request.subsystem_nqn}")
        json_req = json_format.MessageToJson(request,
                                             preserving_proto_field_name=True)
        if context:  # Update state with incomplete request
            self.gateway_state.add_namespace(request.subsystem_nqn,
                                             str(request.nsid), json_req,
                                             RequestStatus.INIT)

        try:
            nsid = self.spdk_rpc.nvmf.nvmf_subsystem_add_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                bdev_name=request.bdev_name,
                nsid=request.nsid,
            )
            self.logger.info(f"add_namespace: {nsid}")
        except Exception as ex:
            self.logger.error(f"add_namespace failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.add_namespace(request.subsystem_nqn,
                                                 str(request.nsid), json_req,
                                                 RequestStatus.ABORT)
            return pb2.nsid()

        if context:  # Update state with complete request
            if not request.nsid:
                request.nsid = nsid
            self.gateway_state.add_namespace(request.subsystem_nqn,
                                             str(request.nsid), json_req,
                                             RequestStatus.SUCCESS)

        return pb2.nsid(nsid=nsid, status=True)

    def remove_namespace(self, request, context=None):
        """Removes a namespace from a subsystem."""

        self.logger.info(f"Received request to remove {request.nsid} from"
                         f" {request.subsystem_nqn}")
        if context:  # Update state with incomplete request
            self.gateway_state.remove_namespace(request.subsystem_nqn,
                                                str(request.nsid),
                                                RequestStatus.INIT)
        try:
            ret = self.spdk_rpc.nvmf.nvmf_subsystem_remove_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                nsid=request.nsid,
            )
            self.logger.info(f"remove_namespace {request.nsid}: {ret}")
        except Exception as ex:
            self.logger.error(f"remove_namespace failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.remove_namespace(request.subsystem_nqn,
                                                    str(request.nsid),
                                                    RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.remove_namespace(request.subsystem_nqn,
                                                str(request.nsid),
                                                RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def add_host(self, request, context=None):
        """Adds a host to a subsystem."""

        json_req = json_format.MessageToJson(request,
                                             preserving_proto_field_name=True)
        if context:  # Update state with incomplete request
            self.gateway_state.add_host(request.subsystem_nqn, request.host_nqn,
                                        json_req, RequestStatus.INIT)

        try:
            if request.host_nqn == "*":  # Allow any host access to subsystem
                self.logger.info(f"Received request to allow any host to"
                                 f" {request.subsystem_nqn}")
                ret = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=False,
                )
                self.logger.info(f"add_host *: {ret}")
            else:  # Allow single host access to subsystem
                self.logger.info(
                    f"Received request to add host {request.host_nqn} to"
                    f" {request.subsystem_nqn}")
                ret = self.spdk_rpc.nvmf.nvmf_subsystem_add_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
                self.logger.info(f"add_host {request.host_nqn}: {ret}")
        except Exception as ex:
            self.logger.error(f"add_host failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.add_host(request.subsystem_nqn,
                                            request.host_nqn, json_req,
                                            RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.add_host(request.subsystem_nqn, request.host_nqn,
                                        json_req, RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def remove_host(self, request, context=None):
        """Removes a host from a subsystem."""

        if context:  # Update state with incomplete request
            self.gateway_state.remove_host(request.subsystem_nqn,
                                           request.host_nqn, RequestStatus.INIT)

        try:
            if request.host_nqn == "*":  # Disable allow any host access
                self.logger.info(
                    f"Received request to disable any host access to"
                    f" {request.subsystem_nqn}")
                ret = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=True,
                )
                self.logger.info(f"remove_host *: {ret}")
            else:  # Remove single host access to subsystem
                self.logger.info(
                    f"Received request to remove host_{request.host_nqn} from"
                    f" {request.subsystem_nqn}")
                ret = self.spdk_rpc.nvmf.nvmf_subsystem_remove_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
                self.logger.info(f"remove_host {request.host_nqn}: {ret}")
        except Exception as ex:
            self.logger.error(f"remove_host failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.remove_host(request.subsystem_nqn,
                                               request.host_nqn,
                                               RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.remove_host(request.subsystem_nqn,
                                           request.host_nqn,
                                           RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def create_listener(self, request, context=None):
        """Creates a listener for a subsystem at a given IP/Port."""

        ret = True
        self.logger.info(f"Received request to create {request.gateway_name}"
                         f" {request.trtype} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}.")
        json_req = json_format.MessageToJson(request,
                                             preserving_proto_field_name=True)
        if context:  # Update state with complete request
            self.gateway_state.add_listener(request.nqn, request.gateway_name,
                                            request.trtype, request.traddr,
                                            request.trsvcid, json_req,
                                            RequestStatus.INIT)

        try:
            if (request.gateway_name and not request.traddr) or \
               (not request.gateway_name and request.traddr):
                raise Exception(
                    "both gateway_name and traddr or neither must be specified")

            if not request.gateway_name or \
               request.gateway_name == self.gateway_name:
                if not request.traddr:
                    traddr = self.config.get("gateway", "addr")
                    if not traddr:
                        raise Exception("gateway.addr option is not set")
                else:
                    traddr = request.traddr

                ret = self.spdk_rpc.nvmf.nvmf_subsystem_add_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
                )
                self.logger.info(f"create_listener: {ret}")
        except Exception as ex:
            self.logger.error(f"create_listener failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.add_listener(request.nqn,
                                                request.gateway_name,
                                                request.trtype, request.traddr,
                                                request.trsvcid, json_req,
                                                RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.add_listener(request.nqn, request.gateway_name,
                                            request.trtype, request.traddr,
                                            request.trsvcid, json_req,
                                            RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def delete_listener(self, request, context=None):
        """Deletes a listener from a subsystem at a given IP/Port."""

        ret = True
        self.logger.info(f"Received request to delete {request.gateway_name}"
                         f" {request.trtype} listener for {request.nqn} at"
                         f" {request.traddr}:{request.trsvcid}.")
        if context:  # Update state with complete request
            self.gateway_state.remove_listener(request.nqn,
                                               request.gateway_name,
                                               request.trtype, request.traddr,
                                               request.trsvcid,
                                               RequestStatus.ABORT)
        try:
            if (request.gateway_name and not request.traddr) or \
               (not request.gateway_name and request.traddr):
                raise Exception(
                    "both gateway_name and traddr or neither must be specified")

            if not request.gateway_name or \
               request.gateway_name == self.gateway_name:
                if not request.traddr:
                    traddr = self.config.get("gateway", "addr")
                    if not traddr:
                        raise Exception("gateway.addr option is not set")
                else:
                    traddr = request.traddr

                ret = self.spdk_rpc.nvmf.nvmf_subsystem_remove_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
                )
                self.logger.info(f"delete_listener: {ret}")
        except Exception as ex:
            self.logger.error(f"delete_listener failed with: \n {ex}")
            if context:  # Set context and remove incomplete request
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
                self.gateway_state.remove_listener(
                    request.nqn, request.gateway_name, request.trtype,
                    request.traddr, request.trsvcid, RequestStatus.ABORT)
            return pb2.req_status()

        if context:  # Update state with complete request
            self.gateway_state.remove_listener(request.nqn,
                                               request.gateway_name,
                                               request.trtype, request.traddr,
                                               request.trsvcid,
                                               RequestStatus.SUCCESS)

        return pb2.req_status(status=ret)

    def get_subsystems(self, request, context):
        """Gets subsystems."""

        self.logger.info(f"Received request to get subsystems")
        try:
            ret = self.spdk_rpc.nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.info(f"get_subsystems: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        return pb2.subsystems_info(subsystems=json.dumps(ret))
