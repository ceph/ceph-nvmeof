#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

from threading import TIMEOUT_MAX
import grpc

from distutils import util
import configparser
import sys
import os

# read in configuration parameters
thisfolder = os.path.dirname(os.path.abspath(__file__))
config_file = os.path.join(thisfolder, "nvme_client.config")

assert os.path.isfile(config_file)

gateway_config = configparser.ConfigParser()
gateway_config.read(config_file)

enable_auth = gateway_config.getboolean("config", "enable_auth")
gateway_addr = gateway_config.get("config", "gateway_addr")
gateway_port = gateway_config.get("config", "gateway_port")

nvme_host = gateway_config.get("config", "nvme_host")
nvme_port = gateway_config.get("config", "nvme_port")

client_key = gateway_config.get("mtls", "client_key")
client_cert = gateway_config.get("mtls", "client_cert")
server_cert = gateway_config.get("mtls", "server_cert")

proto_path = gateway_config.get("config", "proto_path")

sys.path.append(proto_path)

import nvme_gw_pb2_grpc as pb2_grpc
import nvme_gw_pb2 as pb2

created_subsystem = []


class nvme_gw_client(object):
    # Client for gRPC functionality

    def __init__(self):

        nvme_gateway_server = "{}:{}".format(gateway_addr, gateway_port)

        if enable_auth == True:

            with open(client_cert, "rb") as f:
                client_crt = f.read()
            with open(client_key, "rb") as f:
                private_key = f.read()
            with open(server_cert, "rb") as f:
                server_crt = f.read()

            # create credentials for mutual TLS
            credentials = grpc.ssl_channel_credentials(
                root_certificates=server_crt,
                private_key=private_key,
                certificate_chain=client_crt,
            )

            # create channel using ssl credentials
            self.channel = grpc.secure_channel(nvme_gateway_server,
                                               credentials)
        else:

            # instantiate a channel
            self.channel = grpc.insecure_channel(nvme_gateway_server)

        # bind the client and the server
        self.stub = pb2_grpc.NVMEGatewayStub(self.channel)

    def Callbdev_rbd_create(self):
        # Client function to call the rpc for creating a bdev from a ceph rbd

        rbd_name = "mytestdevimage"
        try:
            create_req = pb2.bdev_create_req(
                # bdev_name="ceph0",
                # user_id="admin",
                ceph_pool_name="rbd",
                rbd_name=rbd_name,
                block_size=4096,
            )
        except Exception as ex:
            print(f"Unable create rbd {rbd_name} {ex}. Exiting!")
            sys.exit(-1)

        return self.stub.bdev_rbd_create(create_req)

    def Callnvmf_create_subsystem(self):
        create_req = pb2.subsystem_create_req(
            subsystem_nqn="nqn.2016-06.io.spdk:cnode1",
            serial_number="SPDK00000000000001",
            # max_namespaces=10,
        )
        return self.stub.nvmf_create_subsystem(create_req)

    def Callnvmf_subsystem_add_ns(self):
        # Client funtion to add a NS to a previously created subsystem

        create_req = pb2.subsystem_add_ns_req(
            subsystem_nqn=created_subsystem.subsystem_nqn,
            bdev_name=created_bdev.bdev_name,
        )
        return self.stub.nvmf_subsystem_add_ns(create_req)

    def CallAllowAnyHost(self, disabled):
        # Client funtion to add a NS to a previously created subsystem

        create_req = pb2.subsystem_allow_any_host_req(
            subsystem_nqn=created_subsystem.subsystem_nqn,
            disable=disabled,
        )
        return self.stub.nvmf_subsystem_allow_any_host(create_req)

    def CallCreateTransport(self, tr_type):
        # Client funtion to add a NS to a previously created subsystem

        create_req = pb2.create_transport_req(trtype=tr_type, )
        return self.stub.nvmf_create_transport(create_req)

    def CallSubsystemAddListener(self):
        # Client Function to add a listener at a particular TCP/IP address for a given subsystem

        create_req = pb2.subsystem_add_listener_req(
            nqn=created_subsystem.subsystem_nqn,
            trtype="tcp",
            adrfam="ipv4",
            traddr=nvme_host,
            trsvcid=nvme_port,
        )
        return self.stub.nvmf_subsystem_add_listener(create_req)


if __name__ == "__main__":
    client = nvme_gw_client()

    created_bdev = client.Callbdev_rbd_create()
    print(
        f"returned from rbd create request. Created Bdev {created_bdev.bdev_name}"
    )

    created_subsystem = client.Callnvmf_create_subsystem()
    print(
        f"Returned from create subsystem. Created subsys {created_subsystem.subsystem_nqn} status {created_subsystem.created}"
    )

    created_nsid = client.Callnvmf_subsystem_add_ns()
    print(f"Returned from adding NS to subsystem. Generated nsid: ",
          {created_nsid.nsid})

    req_status = client.CallAllowAnyHost(False)
    print(f"Allow AnyHost Request returned {req_status.status}")

    req_status = client.CallCreateTransport("tcp")
    print(f"Create Transport type tcp returned: {req_status.status}")

    req_status = client.CallSubsystemAddListener()
    print(
        f"Successfully added listener for the subsystem: {req_status.status}")
