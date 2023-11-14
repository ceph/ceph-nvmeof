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
import json
import logging
import sys

from functools import wraps

from .proto import gateway_pb2_grpc as pb2_grpc
from .proto import gateway_pb2 as pb2
from .config import GatewayConfig

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
            prog="python3 -m control.cli",
            description="CLI to manage NVMe gateways")
        self.parser.add_argument(
            "--server-address",
            default="localhost",
            type=str,
            help="Server address",
        )
        self.parser.add_argument(
            "--server-port",
            default=5500,
            type=int,
            help="Server port",
        )
        self.parser.add_argument(
            "--client-key",
            type=argparse.FileType("rb"),
            help="Path to the client key file")
        self.parser.add_argument(
            "--client-cert",
            type=argparse.FileType("rb"),
            help="Path to the client certificate file")
        self.parser.add_argument(
            "--server-cert",
            type=argparse.FileType("rb"),
            help="Path to the server certificate file"
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

            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except grpc.RpcError as e:
                    self.parser.error(
                        f"{func.__name__} failed: code={e.code()} message={e.details()}")
            parser.set_defaults(func=wrapper)
            return wrapper

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
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

    @property
    def stub(self):
        """Object on which to call server methods."""

        if self._stub is None:
            raise AttributeError("stub is None. Set with connect method.")
        return self._stub

    def connect(self, host, port, client_key, client_cert, server_cert):
        """Connects to server and sets stub."""
        # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
        host = GatewayConfig.escape_address_if_ipv6(host)
        server = f"{host}:{port}"

        if client_key and client_cert:
            # Create credentials for mutual TLS and a secure channel
            self.logger.info("Enable server auth since both --client-key and --client-cert are provided")
            with client_cert as f:
                client_cert = f.read()
            with client_key as f:
                client_key = f.read()
            if server_cert:
                with server_cert as f:
                    server_cert = f.read()
            else:
                self.logger.warn("No server certificate file was provided")

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
        self._stub = pb2_grpc.GatewayStub(channel)

    @cli.cmd([
        argument("-i", "--image", help="RBD image name", required=True),
        argument("-p", "--pool", help="RBD pool name", required=True),
        argument("-b", "--bdev", help="Bdev name"),
        argument("-s",
                 "--block-size",
                 help="Block size",
                 type=int,
                 default=512),
    ])
    def create_bdev(self, args):
        """Creates a bdev from an RBD image."""
        req = pb2.create_bdev_req(
            rbd_pool_name=args.pool,
            rbd_image_name=args.image,
            block_size=args.block_size,
            bdev_name=args.bdev,
        )
        ret = self.stub.create_bdev(req)
        self.logger.info(f"Created bdev {ret.bdev_name}: {ret.status}")

    @cli.cmd([
        argument("-b", "--bdev", help="Bdev name", required=True),
        argument("-s", "--size", help="New size in MiB", type=int, required=True),
    ])
    def resize_bdev(self, args):
        """Resizes a bdev."""
        req = pb2.resize_bdev_req(
            bdev_name=args.bdev,
            new_size=args.size,
        )
        ret = self.stub.resize_bdev(req)
        self.logger.info(f"Resized bdev {args.bdev}: {ret.status}")

    @cli.cmd([
        argument("-b", "--bdev", help="Bdev name", required=True),
        argument("-f", "--force", help="Delete any namespace using this bdev before deleting bdev", action='store_true', required=False),
    ])
    def delete_bdev(self, args):
        """Deletes a bdev."""
        req = pb2.delete_bdev_req(bdev_name=args.bdev, force=args.force)
        ret = self.stub.delete_bdev(req)
        self.logger.info(f"Deleted bdev {args.bdev}: {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-s", "--serial", help="Serial number", required=False),
        argument("-m", "--max-namespaces", help="Maximum number of namespaces", type=int, default=0, required=False),
        argument("-a", "--ana-reporting", help="Enable ANA reporting", action='store_true', required=False),
        argument("-t", "--enable-ha", help="Enable automatic HA", action='store_true', required=False),
    ])
    def create_subsystem(self, args):
        """Creates a subsystem."""
        req = pb2.create_subsystem_req(subsystem_nqn=args.subnqn,
                                        serial_number=args.serial,
                                        max_namespaces=args.max_namespaces,
                                        ana_reporting=args.ana_reporting,
                                        enable_ha=args.enable_ha)
        ret = self.stub.create_subsystem(req)
        self.logger.info(f"Created subsystem {args.subnqn}: {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
    ])
    def delete_subsystem(self, args):
        """Deletes a subsystem."""
        req = pb2.delete_subsystem_req(subsystem_nqn=args.subnqn)
        ret = self.stub.delete_subsystem(req)
        self.logger.info(f"Deleted subsystem {args.subnqn}: {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-b", "--bdev", help="Bdev name", required=True),
        argument("-i", "--nsid", help="Namespace ID", type=int),
        argument("-a", "--anagrpid", help="ANA group ID", type=int),
    ])
    def add_namespace(self, args):
        """Adds a namespace to a subsystem."""
        if args.anagrpid == 0:
	        args.anagrpid = 1

        req = pb2.add_namespace_req(subsystem_nqn=args.subnqn,
                                    bdev_name=args.bdev,
                                    nsid=args.nsid,
                                    anagrpid=args.anagrpid)
        ret = self.stub.add_namespace(req)
        self.logger.info(
            f"Added namespace {ret.nsid} to {args.subnqn}, ANA group id {args.anagrpid} : {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-i", "--nsid", help="Namespace ID", type=int, required=True),
    ])
    def remove_namespace(self, args):
        """Removes a namespace from a subsystem."""
        req = pb2.remove_namespace_req(subsystem_nqn=args.subnqn,
                                        nsid=args.nsid)
        ret = self.stub.remove_namespace(req)
        self.logger.info(
            f"Removed namespace {args.nsid} from {args.subnqn}:"
            f" {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-t", "--host", help="Host NQN", required=True),
    ])
    def add_host(self, args):
        """Adds a host to a subsystem."""
        req = pb2.add_host_req(subsystem_nqn=args.subnqn,
                                host_nqn=args.host)
        ret = self.stub.add_host(req)
        if args.host == "*":
            self.logger.info(
                f"Allowed open host access to {args.subnqn}: {ret.status}")
        else:
            self.logger.info(
                f"Added host {args.host} access to {args.subnqn}:"
                f" {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-t", "--host", help="Host NQN", required=True),
    ])
    def remove_host(self, args):
        """Removes a host from a subsystem."""
        req = pb2.remove_host_req(subsystem_nqn=args.subnqn,
                                    host_nqn=args.host)
        ret = self.stub.remove_host(req)
        if args.host == "*":
            self.logger.info(
                f"Disabled open host access to {args.subnqn}: {ret.status}")
        else:
            self.logger.info(
                f"Removed host {args.host} access from {args.subnqn}:"
                f" {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-g", "--gateway-name", help="Gateway name", required=True),
        argument("-t", "--trtype", help="Transport type", default="TCP"),
        argument("-f", "--adrfam", help="Address family", default="ipv4"),
        argument("-a", "--traddr", help="NVMe host IP", required=True),
        argument("-s", "--trsvcid", help="Port number", required=True),
    ])
    def create_listener(self, args):
        """Creates a listener for a subsystem at a given IP/Port."""
        traddr = GatewayConfig.escape_address_if_ipv6(args.traddr)
        req = pb2.create_listener_req(
            nqn=args.subnqn,
            gateway_name=args.gateway_name,
            trtype=args.trtype,
            adrfam=args.adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
        )
        ret = self.stub.create_listener(req)
        self.logger.info(f"Created {args.subnqn} listener at {traddr}:{args.trsvcid}: {ret.status}")

    @cli.cmd([
        argument("-n", "--subnqn", help="Subsystem NQN", required=True),
        argument("-g", "--gateway-name", help="Gateway name", required=True),
        argument("-t", "--trtype", help="Transport type", default="TCP"),
        argument("-f", "--adrfam", help="Address family", default="ipv4"),
        argument("-a", "--traddr", help="NVMe host IP", required=True),
        argument("-s", "--trsvcid", help="Port number", required=True),
    ])
    def delete_listener(self, args):
        """Deletes a listener from a subsystem at a given IP/Port."""
        traddr = GatewayConfig.escape_address_if_ipv6(args.traddr)
        req = pb2.delete_listener_req(
            nqn=args.subnqn,
            gateway_name=args.gateway_name,
            trtype=args.trtype,
            adrfam=args.adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
        )
        ret = self.stub.delete_listener(req)
        self.logger.info(f"Deleted {traddr}:{args.trsvcid} from {args.subnqn}: {ret.status}")

    @cli.cmd()
    def get_subsystems(self, args):
        """Gets subsystems."""
        req = pb2.get_subsystems_req()
        ret = self.stub.get_subsystems(req)
        subsystems = json.loads(ret.subsystems)
        formatted_subsystems = json.dumps(subsystems, indent=4)
        self.logger.info(f"Get subsystems:\n{formatted_subsystems}")

    @cli.cmd()
    def get_spdk_nvmf_log_flags_and_level(self, args):
        """Gets spdk nvmf log levels and flags"""
        req = pb2.get_spdk_nvmf_log_flags_and_level_req()
        ret = self.stub.get_spdk_nvmf_log_flags_and_level(req)
        formatted_flags_log_level = json.dumps(json.loads(ret.flags_level), indent=4)
        self.logger.info(
            f"Get SPDK nvmf log flags and level:\n{formatted_flags_log_level}")

    @cli.cmd()
    def disable_spdk_nvmf_logs(self, args):
        """Disables spdk nvmf logs and flags"""
        req = pb2.disable_spdk_nvmf_logs_req()
        ret = self.stub.disable_spdk_nvmf_logs(req)
        self.logger.info(
            f"Disable SPDK nvmf logs: {ret.status}")

    @cli.cmd([
        argument("-f", "--flags", help="SPDK nvmf enable flags", \
                 action='store_true', required=True),
        argument("-l", "--log_level", \
                 help="SPDK nvmf log level (ERROR, WARNING, NOTICE, INFO, DEBUG)", required=False),
        argument("-p", "--log_print_level", \
                 help="SPDK nvmf log print level (ERROR, WARNING, NOTICE, INFO, DEBUG)", \
                    required=False),
    ])
    def set_spdk_nvmf_logs(self, args):
        """Set spdk nvmf log and flags"""
        req = pb2.set_spdk_nvmf_logs_req(flags=args.flags, log_level=args.log_level, \
                                         print_level=args.log_print_level)
        ret = self.stub.set_spdk_nvmf_logs(req)
        self.logger.info(
            f"Set SPDK nvmf logs : {ret.status}")

def main(args=None):
    client = GatewayClient()
    parsed_args = client.cli.parser.parse_args(args)
    if parsed_args.subcommand is None:
        client.cli.parser.print_help()
        return 0
    server_address = parsed_args.server_address
    server_port = parsed_args.server_port
    client_key = parsed_args.client_key
    client_cert = parsed_args.client_cert
    server_cert = parsed_args.server_cert
    client.connect(server_address, server_port, client_key, client_cert, server_cert)
    call_function = getattr(client, parsed_args.func.__name__)
    call_function(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
