#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import argparse
import grpc
import nvme_gw_pb2_grpc as pb2_grpc
import nvme_gw_pb2 as pb2
import nvme_gw_config


def argument(*name_or_flags, **kwargs):
    """Helper function to format arguments for argparse command decorator."""

    return (list(name_or_flags), kwargs)


class Parser:
    """Class to simplify creation of client CLI.

    Instance attributes:
        parser: ArgumentParser object.
        subparsers: Action object to add subcommands to main argument parser.
    """
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            prog="python3 ./nvme_gw_cli.py",
            description="CLI to manage NVMe gateways")
        self.parser.add_argument(
            "-c",
            "--config",
            default="nvme_gw.config",
            type=str,
            help="Path to config file",
        )

        self.subparsers = self.parser.add_subparsers(dest="subcommand")

    def cmd(self, args=[]):
        """Decorator to create an argparse command.

        The arguments to this decorator are used as arguments for the argparse
        command.
        """
        def decorator(func):
            parser = self.subparsers.add_parser(func.__name__,
                                                description=func.__doc__)
            # Add specified arguments to the parser and set the function
            # attribute to point to the subcommand's associated function
            for arg in args:
                parser.add_argument(*arg[0], **arg[1])
                parser.set_defaults(func=func)
            return func

        return decorator


class GatewayClient:
    """Client for gRPC functionality with a gateway server.

    Contains methods to send RPC calls to the server and specifications for the
    associated command line arguments.

    Class attributes:
        cli: Parser object

    Instance attributes: * Must be initialized with GatewayClient.connect *
        stub: Object on which to call server methods
        logger: Logger instance to track client events
    """

    cli = Parser()

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

    @cli.cmd([
        argument("-i", "--image", help="RBD image name", required=True),
        argument("-p", "--pool", help="Ceph pool name", required=True),
        argument("-b", "--bdev", help="Bdev name"),
        argument("-u", "--user", help="User ID"),
        argument("-s", "--block-size", help="Block size", default=4096),
    ])
    def create_bdev(self, args):
        """Creates a bdev from a Ceph RBD."""

        try:
            create_req = pb2.bdev_create_req(
                ceph_pool_name=args.pool,
                rbd_name=args.image,
                block_size=int(args.block_size),
            )
            ret = self.stub.bdev_rbd_create(create_req)
            self.logger.info(f"Created bdev: {ret.bdev_name}")
        except Exception as error:
            self.logger.error(f"Failed to create bdev: \n {error}")

    @cli.cmd([
        argument("-b", "--bdev", help="Bdev name", required=True),
    ])
    def delete_bdev(self, args):
        """Deletes a bdev."""

        try:
            delete_req = pb2.bdev_delete_req(
                bdev_name=args.bdev,
            )
            ret = self.stub.bdev_rbd_delete(delete_req)
            self.logger.info(f"Deleted bdev: {delete_req.bdev_name}")
        except Exception as error:
            self.logger.error(f"Failed to delete bdev: \n {error}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-s", "--serial", help="Serial number", required=True),
    ])
    def create_subsystem(self, args):
        """Creates a new subsystem."""

        try:
            create_req = pb2.subsystem_create_req(subsystem_nqn=args.subnqn,
                                                  serial_number=args.serial)
            ret = self.stub.nvmf_create_subsystem(create_req)
            self.logger.info(f"Created subsystem: {ret.subsystem_nqn}")
        except Exception as error:
            self.logger.error(f"Failed to create subsystem: \n {error}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
    ])
    def delete_subsystem(self, args):
        """Deletes a new subsystem."""

        try:
            delete_req = pb2.subsystem_delete_req(subsystem_nqn=args.subnqn)
            ret = self.stub.nvmf_delete_subsystem(delete_req)
            self.logger.info(f"Deleted subsystem: {delete_req.subsystem_nqn}")
        except Exception as error:
            self.logger.error(f"Failed to delete subsystem: \n {error}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-b", "--bdev", help="Bdev name", required=True),
    ])
    def create_namespace(self, args):
        """Adds a namespace to a previously created subsystem."""

        try:
            create_req = pb2.subsystem_add_ns_req(subsystem_nqn=args.subnqn,
                                                  bdev_name=args.bdev)
            ret = self.stub.nvmf_subsystem_add_ns(create_req)
            self.logger.info(f"Added namespace {ret.nsid} to {args.subnqn}")
        except Exception as error:
            self.logger.error(f"Failed to add namespace: \n {error}")

    @cli.cmd([argument("-n", "--subnqn", help="Subsystem NQN", required=True)])
    def allow_any_hosts(self, args):
        """Allows any host to access a subsystem."""

        try:
            allow_req = pb2.subsystem_allow_any_host_req(
                subsystem_nqn=args.subnqn, disable=0)
            ret = self.stub.nvmf_subsystem_allow_any_host(allow_req)
            self.logger.info(f"All host access to {args.subnqn}: {ret.status}")
        except Exception as error:
            self.logger.error(f"Failed to allow any host: \n {error}")

    @cli.cmd(
        [argument("-t", "--trtype", help="Transport type", default="TCP")])
    def create_transport(self, args):
        """Sets a transport type."""

        try:
            create_req = pb2.create_transport_req(trtype=args.trtype)
            ret = self.stub.nvmf_create_transport(create_req)
            self.logger.info(f"Created {args.trtype} transport: {ret.status}")
        except Exception as error:
            self.logger.error(f"Failed to create transport: \n {error}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-a", "--traddr", help="NVMe host IP", required=True),
        argument("-s", "--trsvcid", help="Port number", required=True),
        argument("-t", "--trtype", help="Transport type", default="TCP"),
        argument("-f", "--adrfam", help="Address family", default="ipv4"),
    ])
    def create_listener(self, args):
        """Adds a listener at a particular TCP/IP address for a given subsystem."""

        try:
            create_req = pb2.subsystem_add_listener_req(
                nqn=args.subnqn,
                trtype=args.trtype,
                adrfam=args.adrfam,
                traddr=args.traddr,
                trsvcid=args.trsvcid,
            )
            ret = self.stub.nvmf_subsystem_add_listener(create_req)
            self.logger.info(f"Created {args.subnqn} listener: {ret.status}")
        except Exception as error:
            self.logger.error(f"Failed to create listener: \n {error}")


def main():
    client = GatewayClient()
    args = client.cli.parser.parse_args()
    nvme_config = nvme_gw_config.NVMeGWConfig(args.config)
    client.connect(nvme_config)
    if args.subcommand is None:
        client.cli.parser.print_help()
    else:
        call_function = getattr(client, args.func.__name__)
        call_function(args)


if __name__ == "__main__":
    main()
