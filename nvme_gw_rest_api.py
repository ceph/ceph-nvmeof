#
#  Copyright (c) 2022 clyso GmbH
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: Mykola Golub <mykola.golub@clyso.com>
#

import grpc
import json
import nvme_gw_pb2_grpc as pb2_grpc
import nvme_gw_pb2 as pb2
import nvme_gw_config

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

class GatewayClient:
    """Client for gRPC functionality with a gateway server.

    Contains methods to send RPC calls to the server and specifications for the
    associated REST API requests.

    """

    def __init__(self):
        self._stub = None
        self._logger = None

    @property
    def stub(self):
        """Object on which to call server methods."""

        if self._stub is None:
            raise AttributeError("stub is None. Set with connect method.")
        return self._stub

    @property
    def logger(self):
        """Logger instance to track client events."""

        if self._logger is None:
            raise AttributeError("logger is None. Set with connect method.")
        return self._logger

    def connect(self, nvme_config):
        """ Connects to server and sets stub and logger."""

        # Read in configuration parameters
        host = nvme_config.get("config", "gateway_addr")
        port = nvme_config.get("config", "gateway_port")
        enable_auth = nvme_config.getboolean("config", "enable_auth")
        server = "{}:{}".format(host, port)

        if enable_auth:

            # Create credentials for mutual TLS and a secure channel
            with open(nvme_config.get("mtls", "client_cert"), "rb") as f:
                client_cert = f.read()
            with open(nvme_config.get("mtls", "client_key"), "rb") as f:
                client_key = f.read()
            with open(nvme_config.get("mtls", "server_cert"), "rb") as f:
                server_cert = f.read()

            credentials = grpc.ssl_channel_credentials(
                root_certificates=server_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )
            channel = grpc.secure_channel(server, credentials)
        else:

            # Instantiate a channel without credentials
            channel = grpc.insecure_channel(server)

        # Bind the client and the server
        self._stub = pb2_grpc.NVMEGatewayStub(channel)
        # Set up logging
        self._logger = nvme_config.logger


class Bdev(BaseModel):
    name: str
    pool: str
    image: str
    user: Optional[str] = None
    block_size: Optional[int] = 4096


@app.get("/bdevs")
def list_bdevs():
    """Lists bdevs."""

    return [] # TODO


@app.get("/bdevs/{bdev_name}")
def get_bdev(bdev_name: str):
    """Gets a bdev."""

    return {"name" : bdev_name} # TODO


@app.post("/bdevs")
def create_bdev(bdev: Bdev):
    """Creates a bdev from a Ceph RBD."""

    try:
        create_req = pb2.bdev_create_req(
            bdev_name=bdev.name,
            ceph_pool_name=bdev.pool,
            rbd_name=bdev.image,
            block_size=bdev.block_size,
        )
        ret = client.stub.bdev_rbd_create(create_req)
        logger.info(f"Created bdev: {ret.bdev_name}")
    except Exception as error:
        logger.error(f"Failed to create bdev: \n {error}")
        raise

    return bdev.dict()


@app.delete("/bdevs/{bdev_name}")
def delete_bdev(bdev_name: str):
    """Deletes a bdev."""

    try:
        delete_req = pb2.bdev_delete_req(bdev_name=bdev_name)
        ret = client.stub.bdev_rbd_delete(delete_req)
        logger.info(f"Deleted bdev: {delete_req.bdev_name}")
    except Exception as error:
        logger.error(f"Failed to delete bdev: \n {error}")
        raise

    return {"name" : bdev_name}


class Subsystem(BaseModel):
    nqn: str
    serial: str


@app.get("/subsystems")
def list_subsystems():
    """Lists subsystems."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        logger.info(f"Get subsystems:\n{subsystems}")
    except Exception as error:
        logger.error(f"Failed to get subsystems: \n {error}")
        raise

    return subsystems


@app.get("/subsystems/{nqn}")
def get_subsystem(nqn: str):
    """Gets a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")
        logger.info(f"Get subsystem:\n{subsystem}")
    except Exception as error:
        logger.error(f"Failed to get subsystems: \n {error}")
        raise

    return subsystem


@app.post("/subsystems")
def create_subsystem(subsystem: Subsystem):
    """Creates a new subsystem."""

    try:
        create_req = pb2.subsystem_create_req(subsystem_nqn=subsystem.nqn,
                                              serial_number=subsystem.serial)
        ret = client.stub.nvmf_create_subsystem(create_req)
        logger.info(f"Created subsystem: {ret.subsystem_nqn}")
    except Exception as error:
        logger.error(f"Failed to create subsystem: \n {error}")
        raise

    return subsystem.dict()


@app.delete("/subsystems/{nqn}")
def delete_subsystem(nqn: str):
    """Deletes a subsystem."""

    try:
        delete_req = pb2.subsystem_delete_req(subsystem_nqn=nqn)
        ret = client.stub.nvmf_delete_subsystem(delete_req)
        logger.info(f"Deleted subsystem: {delete_req.subsystem_nqn}")
    except Exception as error:
        logger.error(f"Failed to delete subsystem: \n {error}")
        raise

    return {"nqn" : nqn}


class Namespace(BaseModel):
    bdev_name: str


@app.get("/subsystems/{nqn}/namespaces")
def list_namespaces(nqn: str):
    """Lists namespaces of a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")

        namespaces = subsystem.get('namespaces', [])

        logger.info(f"Get namespaces:\n{namespaces}")
    except Exception as error:
        logger.error(f"Failed to get namespaces: \n {error}")
        raise

    return namespaces


@app.get("/subsystems/{nqn}/namespaces/{nsid}")
def get_namespace(nqn: str, nsid: str):
    """Gets a namespace of a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")

        namespaces = subsystem.get('namespaces', [])
        namespace = next(filter(lambda n: n['nsid'] == int(nsid), namespaces), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="namespace not found")

        logger.info(f"Get namespace:\n{namespace}")
    except Exception as error:
        logger.error(f"Failed to get namespace: \n {error}")
        raise

    return namespace


@app.post("/subsystems/{nqn}/namespaces")
def create_namespace(nqn: str, namespace: Namespace):
    """Adds a namespace to a subsystem."""

    try:
        create_req = pb2.subsystem_add_ns_req(subsystem_nqn=nqn,
                                              bdev_name=namespace.bdev_name)
        ret = client.stub.nvmf_subsystem_add_ns(create_req)
        logger.info(f"Added namespace {ret.nsid} to {nqn}")
    except Exception as error:
        logger.error(f"Failed to add namespace: \n {error}")
        raise

    return {"nqn" : nqn, "nsid" : ret.nsid, **namespace.dict()}


@app.delete("/subsystems/{nqn}/namespaces/{nsid}")
def delete_namespace(nqn: str, nsid: str):
    """Deletes a namespace from a subsystem."""

    try:
        delete_req = pb2.ns_delete_req(subsystem_nqn=nqn, nsid=int(nsid))
        ret = client.stub.nvmf_subsystem_remove_ns(delete_req)
        logger.info(f"Deleted namespace {delete_req.nsid}: {ret}")
    except Exception as error:
        logger.error(f"Failed to remove namespace: \n {error}")
        raise

    return {"nqn" : nqn, "nsid" : nsid}


class Host(BaseModel):
    nqn: str


@app.get("/subsystems/{nqn}/hosts")
def list_hosts(nqn: str):
    """Lists allowed hosts for a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")

        hosts = subsystem.get('hosts', [])
        if subsystem.get('allow_any_host'):
            hosts.append("*")

        logger.info(f"Get hosts:\n{hosts}")
    except Exception as error:
        logger.error(f"Failed to get hosts: \n {error}")
        raise

    return hosts


@app.get("/subsystems/{nqn}/hosts/{host_nqn}")
def get_host(nqn: str, host_nqn: str):
    """Gets an allowed host for a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")

        if host_nqn == "*":
            host = subsystem.get('allow_any_host') and "*" or None
        else:
            hosts = subsystem.get('hosts', [])
            host = next(filter(lambda h: h['nqn'] == host_nqn, hosts), None)
        if not host:
            raise HTTPException(status_code=404, detail="host not found")

        logger.info(f"Get host:\n{host}")
    except Exception as error:
        logger.error(f"Failed to get host: \n {error}")
        raise

    return host


@app.post("/subsystems/{nqn}/hosts")
def add_host(nqn: str, host: Host):
    """Adds a host to a subsystem."""

    try:
        create_req = pb2.subsystem_add_host_req(subsystem_nqn=nqn,
                                                host_nqn=host.nqn)
        ret = client.stub.nvmf_subsystem_add_host(create_req)
        if host.nqn == "*":
            logger.info(f"Allowed open host access to {nqn}: {ret.status}")
        else:
            logger.info(
                f"Added host {host.nqn} access to {nqn}: {ret.status}")
    except Exception as error:
        logger.error(f"Failed to add host: \n {error}")
        raise

    return {"nqn" : nqn, "host_nqn" : host.nqn}


@app.delete("/subsystems/{nqn}/hosts/{host_nqn}")
def delete_host(nqn: str, host_nqn: str):
    """Deletes a host from a subsystem."""

    try:
        delete_req = pb2.host_delete_req(subsystem_nqn=nqn,
                                         host_nqn=host_nqn)
        ret = client.stub.nvmf_subsystem_remove_host(delete_req)
        if host_nqn == "*":
            logger.info(f"Disabled open host access to {nqn}: {ret.status}")
        else:
            logger.info(
                f"Removed host {host_nqn} access from {nqn}: {ret.status}")
    except Exception as error:
        logger.error(f"Failed to remove host: \n {error}")
        raise

    return {"nqn" : nqn, "host_nqn" : host_nqn}


class Listener(BaseModel):
    gateway_name: Optional[str] = ""
    trtype: Optional[str] = "tcp"
    adrfam: Optional[str] = "ipv4"
    traddr: Optional[str] = ""
    trsvcid: str


@app.get("/subsystems/{nqn}/listeners")
def list_listeners(nqn: str):
    """Lists listeners for a subsystem."""

    try:
        get_req = pb2.subsystems_get_req()
        ret = client.stub.nvmf_get_subsystems(get_req)
        subsystems = json.loads(ret.subsystems)
        subsystem = next(filter(lambda s: s['nqn'] == nqn, subsystems), None)
        if not subsystem:
            raise HTTPException(status_code=404, detail="subsystem not found")

        listeners = subsystem.get('listen_addresses', [])

        logger.info(f"Get listeners:\n{listeners}")
    except Exception as error:
        logger.error(f"Failed to get listeners: \n {error}")
        raise

    return listeners


@app.post("/subsystems/{nqn}/listeners")
def add_listeners(nqn: str, listener: Listener):
    """Adds a listener for a given subsystem."""

    try:
        create_req = pb2.subsystem_add_listener_req(
            nqn=nqn,
            gateway_name=listener.gateway_name,
            trtype=listener.trtype,
            adrfam=listener.adrfam,
            traddr=listener.traddr,
            trsvcid=listener.trsvcid,
        )
        ret = client.stub.nvmf_subsystem_add_listener(create_req)
        logger.info(f"Created {nqn} listener: {ret.status}")
    except Exception as error:
        logger.error(f"Failed to create listener: \n {error}")
        raise

    return listener

@app.delete("/subsystems/{nqn}/listeners")
def delete_listener(nqn: str, listener: Listener):
    """Deletes a listener for a given subsystem."""

    try:
        delete_req = pb2.listener_delete_req(
            nqn=nqn,
            gateway_name=listener.gateway_name,
            trtype=listener.trtype,
            adrfam=listener.adrfam,
            traddr=listener.traddr,
            trsvcid=listener.trsvcid,
        )
        ret = client.stub.nvmf_subsystem_remove_listener(delete_req)
        logger.info(f"Deleted {listener.traddr} from {nqn}: {ret.status}")
    except Exception as error:
        logger.error(f"Failed to delete listener: \n {error}")
        raise

    return listener

client = GatewayClient()
nvme_config = nvme_gw_config.NVMeGWConfig("nvme_gw.config") # TODO: fix hardcoded config name/path
logger = nvme_config.logger
client.connect(nvme_config)
