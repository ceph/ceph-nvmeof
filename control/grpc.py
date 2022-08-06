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
from google.protobuf import json_format
from .generated import gateway_pb2 as pb2
from .generated import gateway_pb2_grpc as pb2_grpc


class GatewayService(pb2_grpc.NVMEGatewayServicer):
    """Implements gateway service interface.

    Handles configuration of the SPDK NVMEoF target according to client requests.

    Instance attributes:
        nvme_config: Basic gateway parameters
        logger: Logger instance to track server events
        gateway_name: Gateway identifier
        gateway_state: Methods for target state persistence
        spdk_rpc: Module methods for SPDK
        spdk_rpc_client: Client of SPDK RPC server
    """

    def __init__(self, nvme_config, gateway_state, spdk_rpc, spdk_rpc_client):

        self.logger = nvme_config.logger
        self.nvme_config = nvme_config
        self.gateway_state = gateway_state
        self.spdk_rpc = spdk_rpc
        self.spdk_rpc_client = spdk_rpc_client

        self.gateway_name = self.nvme_config.get("config", "gateway_name")
        if not self.gateway_name:
            self.gateway_name = socket.gethostname()

    def bdev_rbd_create(self, request, context=None):
        """Creates bdev from a given RBD image."""
        self.logger.info({
            f"Received request to create bdev {request.bdev_name} from",
            f" {request.ceph_pool_name}/{request.rbd_name}",
            f" with block size {request.block_size}",
        })
        try:
            bdev_name = self.spdk_rpc.bdev.bdev_rbd_create(
                self.spdk_rpc_client,
                name=request.bdev_name,
                pool_name=request.ceph_pool_name,
                rbd_name=request.rbd_name,
                block_size=request.block_size,
            )
            self.logger.info(f"Created bdev {bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev create failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.bdev_info()

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

        return pb2.bdev_info(bdev_name=bdev_name)

    def bdev_rbd_delete(self, request, context=None):
        """Deletes bdev."""
        self.logger.info({
            f"Received request to delete bdev: {request.bdev_name}",
        })
        try:
            return_string = self.spdk_rpc.bdev.bdev_rbd_delete(
                self.spdk_rpc_client,
                request.bdev_name,
            )
            self.logger.info(f"Deleted bdev {request.bdev_name}")

        except Exception as ex:
            self.logger.error(f"bdev delete failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update gateway state
            try:
                self.gateway_state.delete_bdev(request.bdev_name)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting delete_bdev {request.bdev_name}: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_create_subsystem(self, request, context=None):
        """Creates an NVMe subsystem."""
        self.logger.info({
            f"Received request to create: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_create_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                serial_number=request.serial_number,
                max_namespaces=request.max_namespaces,
            )
            self.logger.info(f"returned with status: {return_string}")
            return_status = return_string != "none"
        except Exception as ex:
            self.logger.error(f"create_subsystem failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.subsystem_info()

        if context:
            # Update gateway state
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_subsystem(request.subsystem_nqn,
                                                 json_req)
            except Exception as ex:
                self.logger.error(f"Error persisting create_subsystem" +
                                  f" {request.subsystem_nqn}: {ex}")
                raise

        return pb2.subsystem_info(subsystem_nqn=request.subsystem_nqn,
                                  created=return_status)

    def nvmf_delete_subsystem(self, request, context=None):
        """Deletes an NVMe subsystem."""
        self.logger.info({
            f"Received request to delete: {request.subsystem_nqn}",
        })

        try:
            return_string = self.spdk_rpc.nvmf.nvmf_delete_subsystem(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
            )
            self.logger.info(f"returned with status: {return_string}")
        except Exception as ex:
            self.logger.error(f"delete_subsystem failed with: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update gateway state
            try:
                self.gateway_state.delete_subsystem(request.subsystem_nqn)
            except Exception as ex:
                self.logger.error(f"Error persisting delete_subsystem" +
                                  f" {request.subsystem_nqn}: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_add_ns(self, request, context=None):
        """Adds a given namespace to a given subsystem."""
        self.logger.info({
            f"Received request to add: {request.bdev_name} to {request.subsystem_nqn}",
        })

        try:
            nsid = self.spdk_rpc.nvmf.nvmf_subsystem_add_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                bdev_name=request.bdev_name,
                nsid=request.nsid,
            )
            self.logger.info(f"returned with nsid: {nsid}")
        except Exception as ex:
            self.logger.error(f"Add NS returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.nsid()

        if context:
            # Update gateway state
            try:
                json_req = json_format.MessageToJson(
                    request, preserving_proto_field_name=True)
                self.gateway_state.add_namespace(request.subsystem_nqn,
                                                 str(nsid), json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_namespace {nsid}: {ex}")
                raise

        return pb2.nsid(nsid=nsid)

    def nvmf_subsystem_remove_ns(self, request, context=None):
        """Removes a given namespace from a given subsystem."""
        self.logger.info({
            f"Received request to remove: {request.nsid} from {request.subsystem_nqn}",
        })

        try:
            status = self.spdk_rpc.nvmf.nvmf_subsystem_remove_ns(
                self.spdk_rpc_client,
                nqn=request.subsystem_nqn,
                nsid=request.nsid)
            self.logger.info(f"Returned with status: {status}")
        except Exception as ex:
            self.logger.error(f"Remove namespace returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update gateway state
            try:
                self.gateway_state.delete_namespace(request.subsystem_nqn,
                                                    str(request.nsid))
            except Exception as ex:
                self.logger.error(
                    f"Error persisting remove_namespace {request.nsid}: {ex}")
                raise

        return pb2.req_status(status=status)

    def nvmf_subsystem_add_host(self, request, context=None):
        """Grants host access to a given subsystem."""

        try:
            if request.host_nqn == "*":  # Allow any host access to subsystem
                self.logger.info({
                    f"Received request: allow any host to {request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=False,
                )
            else:  # Allow single host access to subsystem
                self.logger.info({
                    f"Received request: add host {request.host_nqn} to {request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
        except Exception as ex:
            self.logger.error(f"Add host access returned with error: \n {ex}")
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
                                            request.host_nqn,
                                            json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_host {request.host_nqn}: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_remove_host(self, request, context=None):
        """Removes host access from a given subsystem."""

        try:
            if request.host_nqn == "*":  # Disable allow any host access
                self.logger.info({
                    f"Received request: disable any host access to ",
                    f"{request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_allow_any_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    disable=True,
                )
            else:  # Remove single host access to subsystem
                self.logger.info({
                    f"Received request: remove host {request.host_nqn} from ",
                    f"{request.subsystem_nqn}",
                })
                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_remove_host(
                    self.spdk_rpc_client,
                    nqn=request.subsystem_nqn,
                    host=request.host_nqn,
                )
        except Exception as ex:
            self.logger.error(
                f"Remove host access returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update gateway state
            try:
                self.gateway_state.delete_host(request.subsystem_nqn,
                                               request.host_nqn)
            except Exception as ex:
                self.logger.error(f"Error persisting remove_host: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_add_listener(self, request, context=None):
        """Adds a listener at the given TCP/IP address for the given subsystem."""
        self.logger.info({
            f"Adding {request.gateway_name} {request.trtype} listener at {request.traddr}:{request.trsvcid} for {request.nqn}"
        })

        try:
            if (request.gateway_name and not request.traddr) or \
               (not request.gateway_name and request.traddr):
                raise Exception(
                    "both gateway_name and traddr or neither must be specified")

            if not request.gateway_name or \
               request.gateway_name == self.gateway_name:
                if not request.traddr:
                    traddr = self.nvme_config.get("config", "gateway_addr")
                    if not traddr:
                        raise Exception("config.gateway_addr option is not set")
                else:
                    traddr = request.traddr

                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_add_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
                )
                self.logger.info(f"Status of add listener: {return_string}")
        except Exception as ex:
            self.logger.error(f"Add Listener failed: \n {ex}")
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
                                                request.trtype,
                                                request.traddr,
                                                request.trsvcid,
                                                json_req)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting add_listener {request.trsvcid}: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_subsystem_remove_listener(self, request, context=None):
        """Removes a listener at the given TCP/IP address for the given subsystem."""
        self.logger.info(
            {f"Removing {request.gateway_name} {request.trtype} listener at {request.traddr}:{request.trsvcid} for {request.nqn}"})

        try:
            if (request.gateway_name and not request.traddr) or \
               (not request.gateway_name and request.traddr):
                raise Exception(
                    "both gateway_name and traddr or neither must be specified")

            if not request.gateway_name or \
               request.gateway_name == self.gateway_name:
                if not request.traddr:
                    traddr = self.nvme_config.get("config", "gateway_addr")
                    if not traddr:
                        raise Exception("config.gateway_addr option is not set")
                else:
                    traddr = request.traddr

                return_string = self.spdk_rpc.nvmf.nvmf_subsystem_remove_listener(
                    self.spdk_rpc_client,
                    nqn=request.nqn,
                    trtype=request.trtype,
                    traddr=traddr,
                    trsvcid=request.trsvcid,
                    adrfam=request.adrfam,
                )
                self.logger.info(f"Status of remove listener: {return_string}")
        except Exception as ex:
            self.logger.error(f"Remove listener returned with error: \n {ex}")
            if context:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"{ex}")
            return pb2.req_status()

        if context:
            # Update gateway state
            try:
                self.gateway_state.delete_listener(request.nqn,
                                                   request.gateway_name,
                                                   request.trtype,
                                                   request.traddr,
                                                   request.trsvcid)
            except Exception as ex:
                self.logger.error(
                    f"Error persisting remove_listener {request.trsvcid}: {ex}")
                raise

        return pb2.req_status(status=return_string)

    def nvmf_get_subsystems(self, request, context):
        """Gets NVMe subsystems."""
        self.logger.info({
            f"Received request to get subsystems",
        })

        try:
            ret = self.spdk_rpc.nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
            self.logger.info(f"returned with: {ret}")
        except Exception as ex:
            self.logger.error(f"get_subsystems failed with: \n {ex}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{ex}")
            return pb2.subsystems_info()

        return pb2.subsystems_info(subsystems=json.dumps(ret))
