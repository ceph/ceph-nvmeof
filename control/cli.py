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
import errno
import os
import yaml

from functools import wraps
from google.protobuf import json_format
from tabulate import tabulate

from .proto import gateway_pb2_grpc as pb2_grpc
from .proto import gateway_pb2 as pb2
from .config import GatewayConfig
from .utils import GatewayUtils
from .utils import GatewayEnumUtils

BASE_GATEWAY_VERSION="1.0.0"

def errprint(msg):
    print(msg, file = sys.stderr)

def argument(*name_or_flags, **kwargs):
    """Helper function to format arguments for argparse command decorator."""
    return (list(name_or_flags), kwargs)

def get_enum_keys_list(e_type):
    k_list = []
    for k in e_type.keys():
        k_list.append(k.lower())
        k_list.append(k.upper())

    return k_list

def break_string(s, delim, count):
    start = 0
    for i in range(count):
        ind = s.find(delim, start)
        if ind < 0:
            return s
        start = ind + 1
    return s[0:ind + 1] + "\n" + s[ind + 1:]

class ErrorCatchingArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(__name__)
        super(ErrorCatchingArgumentParser, self).__init__(*args, **kwargs)

    def exit(self, status = 0, message = None):
        if status != 0:
            if message:
                self.logger.error(message)
        else:
            if message:
                self.logger.info(message)
        exit(status)

    def error(self, message):
        self.print_usage()
        if message:
            self.logger.error(f"error: {message}")
        exit(2)

class Parser:
    """Class to simplify creation of client CLI.

    Instance attributes:
        parser: ArgumentParser object.
        subparsers: Action object to add subcommands to main argument parser.
    """

    def __init__(self):
        self.parser = ErrorCatchingArgumentParser(
            prog="python3 -m control.cli",
            description="CLI to manage NVMe gateways")
        self.parser.add_argument(
            "--format",
            help="CLI output format",
            type=str,
            default="text",
            choices=["text", "json", "yaml", "plain", "python"],
            required=False)
        self.parser.add_argument(
            "--output",
            help="CLI output method",
            type=str,
            default="log",
            choices=["log", "stdio"],
            required=False)
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

        self.subparsers = self.parser.add_subparsers(title="Commands", dest="subcommand")

    def cmd(self, actions=[], aliases=[], hlp=None):
        """Decorator to create an argparse command.

        The arguments to this decorator are used as arguments for the argparse
        command.
        """

        def decorator(func):
            helpstr = func.__doc__
            if hlp:
                helpstr = hlp

            parser = self.subparsers.add_parser(func.__name__,
                                                description=helpstr, aliases=aliases, help=helpstr)
            subp = parser.add_subparsers(title="Action", dest="action")
            for act in actions:
                act_name = act["name"]
                act_args = act["args"]
                act_help = act["help"]
                pr = subp.add_parser(act_name, description=act_help, help=act_help)
                for arg in act_args:
                    pr.add_argument(*arg[0], **arg[1])

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
        logging.basicConfig(format='%(message)s', level=logging.DEBUG)
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
        host = GatewayUtils.escape_address_if_ipv6(host)
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

    def get_actions(act_list):
        acts = ""
        for a in act_list:
            acts += ", '" + a["name"] + "'"
        return acts[2:]

    def format_adrfam(self, adrfam):
        adrfam = adrfam.upper()
        if adrfam == "IPV4":
            adrfam = "IPv4"
        elif adrfam == "IPV6":
            adrfam = "IPv6"

        return adrfam

    def get_output_functions(self, args):
        if args.output == "log":
            return (self.logger.info, self.logger.error)
        elif args.output == "stdio":
            return (print, errprint)
        else:
            self.cli.parser.error("invalid --output value")

    @cli.cmd()
    def version(self, args):
        """Get CLI version"""
        rc = 0
        out_func, err_func = self.get_output_functions(args)
        errmsg = ""
        ver = os.getenv("NVMEOF_VERSION")
        if not ver:
            rc = errno.ENOKEY
            errmsg = "Can't get CLI version"
        else:
            rc = 0
            errmsg = os.strerror(0)
        if args.format == "text" or args.format == "plain":
            if not ver:
                err_func(errmsg)
            else:
                out_func(f"CLI version: {ver}")
        elif args.format == "json" or args.format == "yaml":
            cli_ver = pb2.cli_version(status=rc, error_message=errmsg, version=ver)
            out_ver = json_format.MessageToJson(cli_ver,
                                                indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{out_ver}")
            elif args.format == "yaml":
                obj = json.loads(out_ver)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return pb2.cli_version(status=rc, error_message=errmsg, version=ver)
        else:
            assert False

        return rc

    def parse_version_string(self, version):
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
            return None
        return (v1, v2, v3)

    def gw_get_info(self):
        ver = os.getenv("NVMEOF_VERSION")
        req = pb2.get_gateway_info_req(cli_version=ver)
        gw_info = self.stub.get_gateway_info(req)
        if gw_info.status == 0:
            base_ver = self.parse_version_string(BASE_GATEWAY_VERSION)
            assert base_ver != None
            gw_ver = self.parse_version_string(gw_info.version)
            if gw_ver == None:
                gw_info.status = errno.EINVAL
                gw_info.bool_status = False
                gw_info.error_message = f"Can't parse gateway version \"{gw_info.version}\"."
            elif gw_ver < base_ver:
                gw_info.status = errno.EINVAL
                gw_info.bool_status = False
                gw_info.error_message = f"Can't work with gateway version older than {BASE_GATEWAY_VERSION}"
        return gw_info

    def gw_info(self, args):
        """Get gateway's information"""

        out_func, err_func = self.get_output_functions(args)
        try:
            gw_info = self.gw_get_info()
        except Exception as ex:
            gw_info = pb2.gateway_info(status = errno.EINVAL, error_message = f"Failure getting gateway's information:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if gw_info.status == 0:
                if gw_info.cli_version:
                    out_func(f"CLI's version: {gw_info.cli_version}")
                if gw_info.version:
                    out_func(f"Gateway's version: {gw_info.version}")
                if gw_info.name:
                    out_func(f"Gateway's name: {gw_info.name}")
                if gw_info.group:
                    out_func(f"Gateway's group: {gw_info.group}")
                out_func(f"Gateway's address: {gw_info.addr}")
                out_func(f"Gateway's port: {gw_info.port}")
                if gw_info.spdk_version:
                    out_func(f"SPDK version: {gw_info.spdk_version}")
                if not gw_info.bool_status:
                    err_func(f"Getting gateway's information returned status mismatch")
            else:
                err_func(f"{gw_info.error_message}")
                if gw_info.bool_status:
                    err_func(f"Getting gateway's information returned status mismatch")
        elif args.format == "json" or args.format == "yaml":
            gw_info_str = json_format.MessageToJson(
                        gw_info,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{gw_info_str}")
            elif args.format == "yaml":
                obj = json.loads(gw_info_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return gw_info
        else:
            assert False

        return gw_info.status

    def gw_version(self, args):
        """Get gateway's version"""

        out_func, err_func = self.get_output_functions(args)
        try:
            gw_info = self.gw_get_info()
        except Exception as ex:
            gw_info = pb2.gateway_info(status = errno.EINVAL, error_message = f"Failure getting gateway's version:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if gw_info.status == 0:
                out_func(f"Gateway's version: {gw_info.version}")
            else:
                err_func(f"{gw_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            gw_ver = pb2.gw_version(status=gw_info.status, error_message=gw_info.error_message, version=gw_info.version)
            out_ver = json_format.MessageToJson(gw_ver,
                                                indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{out_ver}")
            elif args.format == "yaml":
                obj = json.loads(out_ver)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return pb2.gw_version(status=gw_info.status, error_message=gw_info.error_message, version=gw_info.version)
        else:
            assert False

        return gw_info.status

    gw_actions = []
    gw_actions.append({"name" : "version", "args" : [], "help" : "Display gateway's version"})
    gw_actions.append({"name" : "info", "args" : [], "help" : "Display gateway's information"})
    gw_choices = get_actions(gw_actions)
    @cli.cmd(gw_actions)
    def gw(self, args):
        """Gateway commands"""

        if args.action == "info":
            return self.gw_info(args)
        elif args.action == "version":
            return self.gw_version(args)
        if not args.action:
            self.cli.parser.error(f"missing action for gw command (choose from {GatewayClient.gw_choices})")

    def log_level_disable(self, args):
        """Disable SPDK nvmf log flags"""

        out_func, err_func = self.get_output_functions(args)

        req = pb2.disable_spdk_nvmf_logs_req()
        try:
            ret = self.stub.disable_spdk_nvmf_logs(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure disabling SPDK nvmf log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Disable SPDK nvmf log flags: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def log_level_get(self, args):
        """Get SPDK log levels and nvmf log flags"""

        out_func, err_func = self.get_output_functions(args)

        req = pb2.get_spdk_nvmf_log_flags_and_level_req()
        try:
            ret = self.stub.get_spdk_nvmf_log_flags_and_level(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure getting SPDK log levels and nvmf log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                for flag in ret.nvmf_log_flags:
                    enabled_str = "enabled" if flag.enabled else "disabled"
                    out_func(f"SPDK nvmf log flag \"{flag.name}\" is {enabled_str}")
                level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, ret.log_level)
                out_func(f"SPDK log level is {level}")
                level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, ret.log_print_level)
                out_func(f"SPDK log print level is {level}")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            out_log_level = json_format.MessageToJson(ret,
                                                indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{out_log_level}")
            elif args.format == "yaml":
                obj = json.loads(out_log_level)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def log_level_set(self, args):
        """Set SPDK log levels and nvmf log flags"""
        rc = 0
        errmsg = ""

        out_func, err_func = self.get_output_functions(args)
        log_level = None
        print_level = None

        if args.level:
            log_level = args.level.upper()

        if args.print:
            print_level = args.print.upper()

        try:
            req = pb2.set_spdk_nvmf_logs_req(log_level=log_level, print_level=print_level)
        except ValueError as err:
            errstr = str(err)
            if errstr.startswith("unknown enum label "):
                errstr = errstr.removeprefix("unknown enum label ")
                self.cli.parser.error(f"invalid log level {errstr}")
            else:
                self.cli.parser.error(f"{err}")

        try:
            ret = self.stub.set_spdk_nvmf_logs(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure setting SPDK log levels and nvmf log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Set SPDK log levels and nvmf log flags: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    log_get_args = []
    log_set_args = [
        argument("--level", "-l", help="SPDK nvmf log level", required=False,
                 type=str, choices=get_enum_keys_list(pb2.LogLevel)),
        argument("--print", "-p", help="SPDK nvmf log print level", required=False,
                 type=str, choices=get_enum_keys_list(pb2.LogLevel)),
    ]
    log_disable_args = []
    log_actions = []
    log_actions.append({"name" : "get", "args" : log_get_args, "help" : "Get SPDK log levels and nvmf log flags"})
    log_actions.append({"name" : "set", "args" : log_set_args, "help" : "Set SPDK log levels and nvmf log flags"})
    log_actions.append({"name" : "disable", "args" : log_disable_args, "help" : "Disable SPDK nvmf log flags"})
    log_choices = get_actions(log_actions)
    @cli.cmd(log_actions)
    def log_level(self, args):
        """SPDK nvmf log level commands"""
        if args.action == "get":
            return self.log_level_get(args)
        elif args.action == "set":
            return self.log_level_set(args)
        elif args.action == "disable":
            return self.log_level_disable(args)
        if not args.action:
            self.cli.parser.error(f"missing action for log_level command (choose from {GatewayClient.log_choices})")

    def subsystem_add(self, args):
        """Create a subsystem"""

        out_func, err_func = self.get_output_functions(args)
        if args.max_namespaces == None:
            args.max_namespaces = 256
        if args.max_namespaces <= 0:
            self.cli.parser.error("--max-namespaces value must be positive")
        if args.subsystem == GatewayUtils.DISCOVERY_NQN:
            self.cli.parser.error("Can't add a discovery subsystem")

        req = pb2.create_subsystem_req(subsystem_nqn=args.subsystem,
                                        serial_number=args.serial_number,
                                        max_namespaces=args.max_namespaces,
                                        enable_ha=args.enable_ha)
        try:
            ret = self.stub.create_subsystem(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure adding subsystem {args.subsystem}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Adding subsystem {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_del(self, args):
        """Delete a subsystem"""

        out_func, err_func = self.get_output_functions(args)
        if args.subsystem == GatewayUtils.DISCOVERY_NQN:
            self.cli.parser.error("Can't delete a discovery subsystem")

        req = pb2.delete_subsystem_req(subsystem_nqn=args.subsystem, force=args.force)
        try:
            ret = self.stub.delete_subsystem(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure deleting subsystem {args.subsystem}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Deleting subsystem {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_list(self, args):
        """List subsystems"""

        out_func, err_func = self.get_output_functions(args)

        subsystems = None
        try:
            subsystems = self.stub.list_subsystems(pb2.list_subsystems_req(subsystem_nqn=args.subsystem, serial_number=args.serial_number))
        except Exception as ex:
            subsystems = pb2.subsystems_info_cli(status = errno.EINVAL, error_message = f"Failure listing subsystems:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if subsystems.status == 0:
                subsys_list = []
                for s in subsystems.subsystems:
                    if args.subsystem and args.subsystem != s.nqn:
                        err_func("Failure listing subsystem {args.subsystem}: Got subsystem {s.nqn} instead")
                        return errno.ENODEV
                    if args.serial_number and args.serial_number != s.serial_number:
                        err_func("Failure listing subsystem with serial number {args.serial_number}: Got serial number {s.serial_number} instead")
                        return errno.ENODEV
                    ctrls_id = f"{s.min_cntlid}-{s.max_cntlid}"
                    ha_str = "enabled" if s.enable_ha else "disabled"
                    one_subsys = [s.subtype, s.nqn, ha_str, s.serial_number, s.model_number, ctrls_id, s.namespace_count, s.max_namespaces]
                    subsys_list.append(one_subsys)
                if len(subsys_list) > 0:
                    if args.format == "text":
                        table_format = "fancy_grid"
                    else:
                        table_format = "plain"
                    subsys_out = tabulate(subsys_list,
                                      headers = ["Subtype", "NQN", "HA State", "Serial Number", "Model Number", "Controller IDs",
                                                 "Namespace\nCount", "Max\nNamespaces"],
                                      tablefmt=table_format)
                    prefix = "Subsystems"
                    if args.subsystem:
                        prefix = f"Subsystem {args.subsystem}"
                    if args.serial_number:
                        prefix = prefix + f" with serial number {args.serial_number}"
                    out_func(f"{prefix}:\n{subsys_out}")
                else:
                    if args.subsystem:
                        out_func(f"No subsystem {args.subsystem}")
                    elif args.serial_number:
                        out_func(f"No subsystem with serial number {args.serial_number}")
                    else:
                        out_func(f"No subsystems")
            else:
                err_func(f"{subsystems.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        subsystems,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return subsystems
        else:
            assert False

        return subsystems.status

    subsys_add_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
        argument("--serial-number", "-s", help="Serial number", required=False),
        argument("--max-namespaces", "-m", help="Maximum number of namespaces", type=int, required=False),
        argument("--enable-ha", "-t", help="Enable automatic HA", action='store_true', required=False),
    ]
    subsys_del_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
        argument("--force", help="Delete subsytem's namespaces if any, then delete subsystem. If not set a subsystem deletion would fail in case it contains namespaces", action='store_true', required=False),
    ]
    subsys_list_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=False),
        argument("--serial-number", "-s", help="Serial number", required=False),
    ]
    subsystem_actions = []
    subsystem_actions.append({"name" : "add", "args" : subsys_add_args, "help" : "Create a subsystem"})
    subsystem_actions.append({"name" : "del", "args" : subsys_del_args, "help" : "Delete a subsystem"})
    subsystem_actions.append({"name" : "list", "args" : subsys_list_args, "help" : "List subsystems"})
    subsystem_choices = get_actions(subsystem_actions)
    @cli.cmd(subsystem_actions)
    def subsystem(self, args):
        """Subsystem commands"""
        if args.action == "add":
            return self.subsystem_add(args)
        elif args.action == "del":
            return self.subsystem_del(args)
        elif args.action == "list":
            return self.subsystem_list(args)
        if not args.action:
            self.cli.parser.error(f"missing action for subsystem command (choose from {GatewayClient.subsystem_choices})")

    def listener_add(self, args):
        """Create a listener"""

        out_func, err_func = self.get_output_functions(args)

        if args.trsvcid == None:
            args.trsvcid = 4420
        elif args.trsvcid <= 0:
            self.cli.parser.error("trsvcid value must be positive")
        elif args.trsvcid > 0xffff:
            self.cli.parser.error("trsvcid value must be smaller than 65536")
        if not args.adrfam:
            args.adrfam = "IPV4"

        traddr = GatewayUtils.escape_address_if_ipv6(args.traddr)
        adrfam = None
        if args.adrfam:
            adrfam = args.adrfam.lower()

        req = pb2.create_listener_req(
            nqn=args.subsystem,
            gateway_name=args.gateway_name,
            adrfam=adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
        )

        try:
            ret = self.stub.create_listener(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL,
                                 error_message = f"Failure adding {args.subsystem} listener at {traddr}:{args.trsvcid}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Adding {args.subsystem} listener at {traddr}:{args.trsvcid}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def listener_del(self, args):
        """Delete a listener"""

        out_func, err_func = self.get_output_functions(args)
        if args.trsvcid <= 0:
            self.cli.parser.error("trsvcid value must be positive")
        elif args.trsvcid > 0xffff:
            self.cli.parser.error("trsvcid value must be smaller than 65536")
        if not args.adrfam:
            args.adrfam = "IPV4"

        traddr = GatewayUtils.escape_address_if_ipv6(args.traddr)
        adrfam = None
        if args.adrfam:
            adrfam = args.adrfam.lower()

        req = pb2.delete_listener_req(
            nqn=args.subsystem,
            gateway_name=args.gateway_name,
            adrfam=adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
        )

        try:
            ret = self.stub.delete_listener(req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL,
                                 error_message = f"Failure deleting listener {traddr}:{args.trsvcid} from {args.subsystem}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Deleting listener {traddr}:{args.trsvcid} from {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def listener_list(self, args):
        """List listeners"""

        out_func, err_func = self.get_output_functions(args)
        listeners_info = None
        try:
            listeners_info = self.stub.list_listeners(pb2.list_listeners_req(subsystem=args.subsystem))
        except Exception as ex:
            listeners_info = pb2.listeners_info(status = errno.EINVAL, error_message = f"Failure listing listeners:\n{ex}", listeners=[])

        if args.format == "text" or args.format == "plain":
            if listeners_info.status == 0:
                listeners_list = []
                for l in listeners_info.listeners:
                    adrfam = GatewayEnumUtils.get_key_from_value(pb2.AddressFamily, l.adrfam)
                    adrfam = self.format_adrfam(adrfam)
                    listeners_list.append([l.gateway_name, l.trtype, adrfam, f"{l.traddr}:{l.trsvcid}"])
                if len(listeners_list) > 0:
                    if args.format == "text":
                        table_format = "fancy_grid"
                    else:
                        table_format = "plain"
                    listeners_out = tabulate(listeners_list,
                                      headers = ["Gateway", "Transport", "Address Family", "Address"],
                                      tablefmt=table_format)
                    out_func(f"Listeners for {args.subsystem}:\n{listeners_out}")
                else:
                    out_func(f"No listeners for {args.subsystem}")
            else:
                err_func(f"{listeners_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        listeners_info,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return listeners_info
        else:
            assert False

        return listeners_info.status

    listener_common_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
    ]
    listener_add_args = listener_common_args + [
        argument("--gateway-name", "-g", help="Gateway name", required=True),
        argument("--traddr", "-a", help="NVMe host IP", required=True),
        argument("--trsvcid", "-s", help="Port number", type=int, required=False),
        argument("--adrfam", "-f", help="Address family", default="", choices=get_enum_keys_list(pb2.AddressFamily)),
    ]
    listener_del_args = listener_common_args + [
        argument("--gateway-name", "-g", help="Gateway name", required=True),
        argument("--traddr", "-a", help="NVMe host IP", required=True),
        argument("--trsvcid", "-s", help="Port number", type=int, required=True),
        argument("--adrfam", "-f", help="Address family", default="", choices=get_enum_keys_list(pb2.AddressFamily)),
    ]
    listener_list_args = listener_common_args + [
    ]
    listener_actions = []
    listener_actions.append({"name" : "add", "args" : listener_add_args, "help" : "Create a listener"})
    listener_actions.append({"name" : "del", "args" : listener_del_args, "help" : "Delete a listener"})
    listener_actions.append({"name" : "list", "args" : listener_list_args, "help" : "List listeners"})
    listener_choices = get_actions(listener_actions)
    @cli.cmd(listener_actions)
    def listener(self, args):
        """Listener commands"""
        if args.action == "add":
            return self.listener_add(args)
        elif args.action == "del":
            return self.listener_del(args)
        elif args.action == "list":
            return self.listener_list(args)
        if not args.action:
            self.cli.parser.error(f"missing action for listener command (choose from {GatewayClient.listener_choices})")

    def host_add(self, args):
        """Add a host to a subsystem."""

        out_func, err_func = self.get_output_functions(args)
        if not args.host:
            self.cli.parser.error("--host argument is mandatory for add command")
        req = pb2.add_host_req(subsystem_nqn=args.subsystem, host_nqn=args.host)
        try:
            ret = self.stub.add_host(req)
        except Exception as ex:
            if args.host == "*":
                errmsg = f"Failure allowing open host access to {args.subsystem}"
            else:
                errmsg = f"Failure adding host {args.host} to {args.subsystem}"
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.host == "*":
                    out_func(f"Allowing open host access to {args.subsystem}: Successful")
                else:
                    out_func(f"Adding host {args.host} to {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def host_del(self, args):
        """Delete a host from a subsystem."""

        out_func, err_func = self.get_output_functions(args)
        if not args.host:
            self.cli.parser.error("--host argument is mandatory for del command")
        req = pb2.remove_host_req(subsystem_nqn=args.subsystem, host_nqn=args.host)

        try:
            ret = self.stub.remove_host(req)
        except Exception as ex:
            if args.host == "*":
                errmsg = f"Failure disabling open host access to {args.subsystem}"
            else:
                errmsg = f"Failure removing host {args.host} access to {args.subsystem}"
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.host == "*":
                    out_func(f"Disabling open host access to {args.subsystem}: Successful")
                else:
                    out_func(f"Removing host {args.host} access from {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def host_list(self, args):
        """List a host for a subsystem."""

        out_func, err_func = self.get_output_functions(args)

        hosts_info = None
        try:
            hosts_info = self.stub.list_hosts(pb2.list_hosts_req(subsystem=args.subsystem))
        except Exception as ex:
            hosts_info = pb2.hosts_info(status = errno.EINVAL, error_message = f"Failure listing hosts:\n{ex}", hosts=[])

        if args.format == "text" or args.format == "plain":
            if hosts_info.status == 0:
                hosts_list = []
                if hosts_info.allow_any_host:
                    hosts_list.append(["Any host"])
                for h in hosts_info.hosts:
                    hosts_list.append([h.nqn])
                if len(hosts_list) > 0:
                    if args.format == "text":
                        table_format = "fancy_grid"
                    else:
                        table_format = "plain"
                    hosts_out = tabulate(hosts_list,
                                      headers = [f"Host NQN"],
                                      tablefmt=table_format, stralign="center")
                    out_func(f"Hosts allowed to access {args.subsystem}:\n{hosts_out}")
                else:
                    out_func(f"No hosts are allowed to access {args.subsystem}")
            else:
                err_func(f"{hosts_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        hosts_info,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return hosts_info
        else:
            assert False

        return hosts_info.status

    host_common_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
    ]
    host_add_args = host_common_args + [
        argument("--host", "-t", help="Host NQN", required=True),
    ]
    host_del_args = host_common_args + [
        argument("--host", "-t", help="Host NQN", required=True),
    ]
    host_list_args = host_common_args + [
    ]
    host_actions = []
    host_actions.append({"name" : "add", "args" : host_add_args, "help" : "Add host access to a subsystem"})
    host_actions.append({"name" : "del", "args" : host_del_args, "help" : "Remove host access from a subsystem"})
    host_actions.append({"name" : "list", "args" : host_list_args, "help" : "List subsystem's host access"})
    host_choices = get_actions(host_actions)
    @cli.cmd(host_actions)
    def host(self, args):
        """Host commands"""
        if args.action == "add":
            return self.host_add(args)
        elif args.action == "del":
            return self.host_del(args)
        elif args.action == "list":
            return self.host_list(args)
        if not args.action:
            self.cli.parser.error(f"missing action for host command (choose from {GatewayClient.host_choices})")

    def connection_list(self, args):
        """List connections for a subsystem."""

        out_func, err_func = self.get_output_functions(args)
        connections_info = None
        try:
            connections_info = self.stub.list_connections(pb2.list_connections_req(subsystem=args.subsystem))
        except Exception as ex:
            connections_info = pb2.connections_info(status = errno.EINVAL,
                                                    error_message = f"Failure listing hosts:\n{ex}", connections=[])

        if args.format == "text" or args.format == "plain":
            if connections_info.status == 0:
                connections_list = []
                for conn in connections_info.connections:
                    connections_list.append([conn.nqn,
                                            f"{conn.traddr}:{conn.trsvcid}" if conn.connected else "<n/a>",
                                            "Yes" if conn.connected else "No",
                                            conn.qpairs_count if conn.connected else "<n/a>",
                                            conn.controller_id if conn.connected else "<n/a>"])
                if len(connections_list) > 0:
                    if args.format == "text":
                        table_format = "fancy_grid"
                    else:
                        table_format = "plain"
                    connections_out = tabulate(connections_list,
                                      headers = ["Host NQN", "Address", "Connected", "QPairs Count", "Controller ID"],
                                      tablefmt=table_format)
                    out_func(f"Connections for {args.subsystem}:\n{connections_out}")
                else:
                    out_func(f"No connections for {args.subsystem}")
            else:
                err_func(f"{connections_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        connections_info,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return connections_info
        else:
            assert False

        return connections_info.status

    connection_list_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
    ]
    connection_actions = []
    connection_actions.append({"name" : "list", "args" : connection_list_args, "help" : "List active connections"})
    connection_choices = get_actions(connection_actions)
    @cli.cmd(connection_actions)
    def connection(self, args):
        """Connection commands"""
        if args.action == "list":
            return self.connection_list(args)
        if not args.action:
            self.cli.parser.error(f"missing action for connection command (choose from {GatewayClient.connection_choices})")

    def ns_add(self, args):
        """Adds a namespace to a subsystem."""

        img_size = 0
        out_func, err_func = self.get_output_functions(args)
        if args.block_size == None:
            args.block_size = 512
        if args.block_size <= 0:
            self.cli.parser.error("block-size value must be positive")
        if args.load_balancing_group == None:
            args.load_balancing_group = 1
        if args.load_balancing_group <= 0:
            self.cli.parser.error("load-balancing-group value must be positive")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.rbd_create_image:
            if args.size == None:
                self.cli.parser.error("--size argument is mandatory for add command when RBD image creation is enabled")
            img_size = self.get_size_in_bytes(args.size)
            if img_size <= 0:
                self.cli.parser.error("size value must be positive")
        else:
            if args.size != None:
                self.cli.parser.error("--size argument is not allowed for add command when RBD image creation is disabled")

        req = pb2.namespace_add_req(rbd_pool_name=args.rbd_pool,
                                            rbd_image_name=args.rbd_image,
                                            subsystem_nqn=args.subsystem,
                                            nsid=args.nsid,
                                            block_size=args.block_size,
                                            uuid=args.uuid,
                                            anagrpid=args.load_balancing_group,
                                            create_image=args.rbd_create_image,
                                            size=img_size,
                                            force=args.force)
        try:
            ret = self.stub.namespace_add(req)
        except Exception as ex:
            nsid_msg = ""
            if args.nsid:
                nsid_msg = f"using NSID {args.nsid} "
            errmsg = f"Failure adding namespace {nsid_msg}to {args.subsystem}"
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Adding namespace {ret.nsid} to {args.subsystem}, load balancing group {args.load_balancing_group}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_del(self, args):
        """Deletes a namespace from a subsystem."""

        out_func, err_func = self.get_output_functions(args)
        if args.nsid == None and args.uuid == None:
            self.cli.parser.error("At least one of --nsid or --uuid arguments is mandatory for del command")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            ret = self.stub.namespace_delete(pb2.namespace_delete_req(subsystem_nqn=args.subsystem, nsid=args.nsid, uuid=args.uuid))
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure deleting namespace:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.nsid:
                    ns_id_str = f"{args.nsid}"
                elif args.uuid:
                    ns_id_str = f"with UUID {args.uuid}"
                else:
                    assert False
                out_func(f"Deleting namespace {ns_id_str} from {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_resize(self, args):
        """Resizes a namespace."""

        ns_size = 0
        out_func, err_func = self.get_output_functions(args)
        if args.nsid == None and args.uuid == None:
            self.cli.parser.error("At least one of --nsid or --uuid arguments is mandatory for resize command")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        ns_size = self.get_size_in_bytes(args.size)
        if ns_size <= 0:
            self.cli.parser.error("size value must be positive")
        mib = 1024 * 1024
        ns_size = int((ns_size + mib - 1) / mib)    # Convert to MiB

        try:
            ret = self.stub.namespace_resize(pb2.namespace_resize_req(subsystem_nqn=args.subsystem, nsid=args.nsid,
                                             uuid=args.uuid, new_size=ns_size))
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure resizing namespace:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.nsid:
                    ns_id_str = f"{args.nsid}"
                elif args.uuid:
                    ns_id_str = f"with UUID {args.uuid}"
                else:
                    assert False
                out_func(f"Resizing namespace {ns_id_str} in {args.subsystem} to {ns_size} MiB: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def format_size(self, sz):
        if sz < 1024:
            return str(sz) + " B"
        if sz < 1024 * 1024:
            sz = sz / 1024.0
            if sz == int(sz):
                sz = int(sz)
                return f"{sz} KiB"
            return f"{sz:2.1f} KiB"
        if sz < 1024 * 1024 * 1024:
            sz = sz / (1024.0 * 1024.0)
            if sz == int(sz):
                sz = int(sz)
                return f"{sz} MiB"
            return f"{sz:2.1f} MiB"
        if sz < 1024 * 1024 * 1024 * 1024:
            sz = sz / (1024.0 * 1024.0 * 1024.0)
            if sz == int(sz):
                sz = int(sz)
                return f"{sz} GiB"
            return f"{sz:2.1f} GiB"
        if sz < 1024 * 1024 * 1024 * 1024 * 1024:
            sz = sz / (1024.0 * 1024.0 * 1024.0 * 1024.0)
            if sz == int(sz):
                sz = int(sz)
                return f"{sz} TiB"
        return f"{sz:2.1f} PiB"

    def get_size_in_bytes(self, sz):
        multiply = 1
        sz = sz.strip()
        if sz.endswith("K"):
            sz = sz[:-1]
            multiply = 1024
        elif sz.endswith("KB"):
            sz = sz[:-2]
            multiply = 1000
        elif sz.endswith("KiB"):
            sz = sz[:-3]
            multiply = 1024
        elif sz.endswith("M"):
            sz = sz[:-1]
            multiply = 1024 * 1024
        elif sz.endswith("MB"):
            sz = sz[:-2]
            multiply = 1000 * 1000
        elif sz.endswith("MiB"):
            sz = sz[:-3]
            multiply = 1024 * 1024
        elif sz.endswith("G"):
            sz = sz[:-1]
            multiply = 1024 * 1024 * 1024
        elif sz.endswith("GB"):
            sz = sz[:-2]
            multiply = 1000 * 1000 * 1000
        elif sz.endswith("GiB"):
            sz = sz[:-3]
            multiply = 1024 * 1024 * 1024
        elif sz.endswith("T"):
            sz = sz[:-1]
            multiply = 1024 * 1024 * 1024 * 1024
        elif sz.endswith("TB"):
            sz = sz[:-2]
            multiply = 1000 * 1000 * 1000 * 1000
        elif sz.endswith("TiB"):
            sz = sz[:-3]
            multiply = 1024 * 1024 * 1024 * 1024
        elif sz.endswith("P"):
            sz = sz[:-1]
            multiply = 1024 * 1024 * 1024 * 1024 * 1024
        elif sz.endswith("PB"):
            sz = sz[:-2]
            multiply = 1000 * 1000 * 1000 * 1000 * 1000
        elif sz.endswith("PiB"):
            sz = sz[:-3]
            multiply = 1024 * 1024 * 1024 * 1024 * 1024

        try:
            int_size = int(sz)
        except:
            self.cli.parser.error(f"Size {sz} must be numeric")

        int_size *= multiply
        return int_size

    def ns_list(self, args):
        """Lists namespaces on a subsystem."""

        out_func, err_func = self.get_output_functions(args)
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            namespaces_info = self.stub.list_namespaces(pb2.list_namespaces_req(subsystem=args.subsystem,
                                                        nsid=args.nsid, uuid=args.uuid))
        except Exception as ex:
            namespaces_info = pb2.namespaces_info(status = errno.EINVAL, error_message = f"Failure listing namespaces:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if namespaces_info.status == 0:
                if args.nsid and len(namespaces_info.namespaces) > 1:
                    err_func(f"Got more than one namespace for NSID {args.nsid}")
                if args.uuid and len(namespaces_info.namespaces) > 1:
                    err_func(f"Got more than one namespace for UUID {args.uuid}")
                namespaces_list = []
                for ns in namespaces_info.namespaces:
                    if args.nsid and args.nsid != ns.nsid:
                        err_func("Failure listing namespace {args.nsid}: Got namespace {ns.nsid} instead")
                        return errno.ENODEV
                    if args.uuid and args.uuid != ns.uuid:
                        err_func("Failure listing namespace with UUID {args.uuid}: Got namespace {ns.uuid} instead")
                        return errno.ENODEV
                    if ns.load_balancing_group == 0:
                        lb_group = "<n/a>"
                    else:
                        lb_group = str(ns.load_balancing_group)
                    namespaces_list.append([ns.nsid,
                                            break_string(ns.bdev_name, "-", 2),
                                            ns.rbd_pool_name,
                                            ns.rbd_image_name,
                                            self.format_size(ns.rbd_image_size),
                                            self.format_size(ns.block_size),
                                            break_string(ns.uuid, "-", 3),
                                            lb_group,
                                            self.get_qos_limit_str_value(ns.rw_ios_per_second),
                                            self.get_qos_limit_str_value(ns.rw_mbytes_per_second),
                                            self.get_qos_limit_str_value(ns.r_mbytes_per_second),
                                            self.get_qos_limit_str_value(ns.w_mbytes_per_second)])

                if len(namespaces_list) > 0:
                    if args.format == "text":
                        table_format = "fancy_grid"
                    else:
                        table_format = "plain"
                    namespaces_out = tabulate(namespaces_list,
                                      headers = ["NSID", "Bdev\nName", "RBD\nPool", "RBD\nImage",
                                                 "Image\nSize", "Block\nSize", "UUID", "Load\nBalancing\nGroup",
                                                 "R/W IOs\nper\nsecond", "R/W MBs\nper\nsecond",
                                                 "Read MBs\nper\nsecond", "Write MBs\nper\nsecond"],
                                      tablefmt=table_format)
                    if args.nsid:
                        prefix = f"Namespace {args.nsid} in"
                    elif args.uuid:
                        prefix = f"Namespace with UUID {args.uuid} in"
                    else:
                        prefix = "Namespaces in"
                    out_func(f"{prefix} subsystem {args.subsystem}:\n{namespaces_out}")
                else:
                    if args.nsid:
                        out_func(f"No namespace {args.nsid} in subsystem {args.subsystem}")
                    elif args.uuid:
                        out_func(f"No namespace with UUID {args.uuid} in subsystem {args.subsystem}")
                    else:
                        out_func(f"No namespaces in subsystem {args.subsystem}")
            else:
                err_func(f"{namespaces_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        namespaces_info,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return namespaces_info
        else:
            assert False

        return namespaces_info.status

    def ns_get_io_stats(self, args):
        """Get namespace IO statistics."""

        out_func, err_func = self.get_output_functions(args)
        if args.nsid == None and args.uuid == None:
            self.cli.parser.error("At least one of --nsid or --uuid arguments is mandatory for get_io_stats command")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            get_stats_req = pb2.namespace_get_io_stats_req(subsystem_nqn=args.subsystem, nsid=args.nsid, uuid=args.uuid)
            ns_io_stats = self.stub.namespace_get_io_stats(get_stats_req)
        except Exception as ex:
            ns_io_stats = pb2.namespace_io_stats_info(status = errno.EINVAL, error_message = f"Failure getting namespace's IO stats:\n{ex}")

        if ns_io_stats.status == 0:
            if ns_io_stats.subsystem_nqn != args.subsystem:
                ns_io_stats.status = errno.ENODEV
                ns_io_stats.error_message = f"Failure getting namespace's IO stats: Returned subsystem {ns_io_stats.subsystem_nqn} differs from requested one {args.subsystem}"
            elif args.nsid and args.nsid != ns_io_stats.nsid:
                ns_io_stats.status = errno.ENODEV
                ns_io_stats.error_message = f"Failure getting namespace's IO stats: Returned namespace NSID {ns_io_stats.nsid} differs from requested one {args.nsid}"
            elif args.uuid and args.uuid != ns_io_stats.uuid:
                ns_io_stats.status = errno.ENODEV
                ns_io_stats.error_message = f"Failure getting namespace's IO stats: Returned namespace UUID {ns_io_stats.uuid} differs from requested one {args.uuid}"

        if args.format == "text" or args.format == "plain":
            if ns_io_stats.status == 0:
                stats_list = []
                stats_list.append(["Tick Rate", ns_io_stats.tick_rate])
                stats_list.append(["Ticks", ns_io_stats.ticks])
                stats_list.append(["Bytes Read", ns_io_stats.bytes_read])
                stats_list.append(["Num Read Ops", ns_io_stats.num_read_ops])
                stats_list.append(["Bytes Written", ns_io_stats.bytes_written])
                stats_list.append(["Num Write Ops", ns_io_stats.num_write_ops])
                stats_list.append(["Bytes Unmapped", ns_io_stats.bytes_unmapped])
                stats_list.append(["Num Unmap Ops", ns_io_stats.num_unmap_ops])
                stats_list.append(["Read Latency Ticks", ns_io_stats.read_latency_ticks])
                stats_list.append(["Max Read Latency Ticks", ns_io_stats.max_read_latency_ticks])
                stats_list.append(["Min Read Latency Ticks", ns_io_stats.min_read_latency_ticks])
                stats_list.append(["Write Latency Ticks", ns_io_stats.write_latency_ticks])
                stats_list.append(["Max Write Latency Ticks", ns_io_stats.max_write_latency_ticks])
                stats_list.append(["Min Write Latency Ticks", ns_io_stats.min_write_latency_ticks])
                stats_list.append(["Unmap Latency Ticks", ns_io_stats.unmap_latency_ticks])
                stats_list.append(["Max Unmap Latency Ticks", ns_io_stats.max_unmap_latency_ticks])
                stats_list.append(["Min Unmap Latency Ticks", ns_io_stats.min_unmap_latency_ticks])
                stats_list.append(["Copy Latency Ticks", ns_io_stats.copy_latency_ticks])
                stats_list.append(["Max Copy Latency Ticks", ns_io_stats.max_copy_latency_ticks])
                stats_list.append(["Min Copy Latency Ticks", ns_io_stats.min_copy_latency_ticks])
                stats_list.append(["IO Error", str(ns_io_stats.io_error)])

                if args.format == "text":
                    table_format = "fancy_grid"
                else:
                    table_format = "plain"
                stats_out = tabulate(stats_list, headers = ["Stat", "Value"], tablefmt=table_format)
                if args.nsid:
                    ns_id_str = f"{args.nsid}"
                elif args.uuid:
                    ns_id_str = f"with UUID {args.uuid}"
                else:
                    assert False
                out_func(f"IO statistics for namespace {ns_id_str} in {args.subsystem}, bdev {ns_io_stats.bdev_name}:\n{stats_out}")
            else:
                err_func(f"{ns_io_stats.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ns_io_stats,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ns_io_stats
        else:
            assert False

        return ns_io_stats.status

    def ns_change_load_balancing_group(self, args):
        """Change namespace load balancing group."""

        out_func, err_func = self.get_output_functions(args)
        if args.nsid == None and args.uuid == None:
            self.cli.parser.error("At least one of --nsid or --uuid arguments is mandatory for change_load_balancing_group command")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.load_balancing_group <= 0:
            self.cli.parser.error("load-balancing-group value must be positive")

        try:
            change_lb_group_req = pb2.namespace_change_load_balancing_group_req(subsystem_nqn=args.subsystem,
                                                                                nsid=args.nsid, uuid=args.uuid,
                                                                                anagrpid=args.load_balancing_group)
            ret = self.stub.namespace_change_load_balancing_group(change_lb_group_req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure changing namespace load balancing group:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.nsid:
                    ns_id_str = f"{args.nsid}"
                elif args.uuid:
                    ns_id_str = f"with UUID {args.uuid}"
                else:
                    assert False
                out_func(f"Changing load balancing group of namespace {ns_id_str} in {args.subsystem} to {args.load_balancing_group}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def get_qos_limit_str_value(self, qos_limit):
        if qos_limit == 0:
            return "unlimited"
        else:
            return str(qos_limit)

    def ns_set_qos(self, args):
        """Set namespace QOS limits."""

        out_func, err_func = self.get_output_functions(args)
        if args.nsid == None and args.uuid == None:
            self.cli.parser.error("At least one of --nsid or --uuid arguments is mandatory for set_qos command")
        if args.nsid != None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.rw_ios_per_second == None and args.rw_megabytes_per_second == None and args.r_megabytes_per_second == None and args.w_megabytes_per_second == None:
            self.cli.parser.error("At least one QOS limit should be set")

        if args.format == "text" or args.format == "plain":
            if args.rw_ios_per_second and (args.rw_ios_per_second % 1000) != 0:
                rounded_rate = int((args.rw_ios_per_second + 1000) / 1000) * 1000
                err_func(f"IOs per second {args.rw_ios_per_second} will be rounded up to {rounded_rate}")

        qos_args = {}
        qos_args["subsystem_nqn"] = args.subsystem
        if args.nsid:
            qos_args["nsid"] = args.nsid
        if args.uuid:
            qos_args["uuid"] = args.uuid
        if args.rw_ios_per_second != None:
            qos_args["rw_ios_per_second"] = args.rw_ios_per_second
        if args.rw_megabytes_per_second != None:
            qos_args["rw_mbytes_per_second"] = args.rw_megabytes_per_second
        if args.r_megabytes_per_second != None:
            qos_args["r_mbytes_per_second"] = args.r_megabytes_per_second
        if args.w_megabytes_per_second != None:
            qos_args["w_mbytes_per_second"] = args.w_megabytes_per_second
        try:
            set_qos_req = pb2.namespace_set_qos_req(**qos_args)
            ret = self.stub.namespace_set_qos_limits(set_qos_req)
        except Exception as ex:
            ret = pb2.req_status(status = errno.EINVAL, error_message = f"Failure setting namespaces QOS limits:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if args.nsid:
                    ns_id_str = f"{args.nsid}"
                elif args.uuid:
                    ns_id_str = f"with UUID {args.uuid}"
                else:
                    assert False
                out_func(f"Setting QOS limits of namespace {ns_id_str} in {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(
                        ret,
                        indent=4,
                        including_default_value_fields=True,
                        preserving_proto_field_name=True)
            if args.format == "json":
                out_func(f"{ret_str}")
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    ns_common_args = [
        argument("--subsystem", "-n", help="Subsystem NQN", required=True),
        argument("--uuid", "-u", help="UUID"),
        argument("--nsid", help="Namespace ID", type=int),
    ]
    ns_add_args_list = ns_common_args + [
        argument("--rbd-pool", "-p", help="RBD pool name", required=True),
        argument("--rbd-image", "-i", help="RBD image name", required=True),
        argument("--rbd-create-image", "-c", help="Create RBD image if needed", action='store_true', required=False),
        argument("--block-size", "-s", help="Block size", type=int),
        argument("--load-balancing-group", "-l", help="Load balancing group", type=int),
        argument("--size", help="Size in bytes or specified unit (KB, KiB, MB, MiB, GB, GiB, TB, TiB)"),
        argument("--force", help="Create a namespace even its image is already used by another namespace", action='store_true', required=False),
    ]
    ns_del_args_list = ns_common_args + [
    ]
    ns_resize_args_list = ns_common_args + [
        argument("--size", help="Size in bytes or specified unit (KB, KiB, MB, MiB, GB, GiB, TB, TiB)", required=True),
    ]
    ns_list_args_list = ns_common_args + [
    ]
    ns_get_io_stats_args_list = ns_common_args + [
    ]
    ns_change_load_balancing_group_args_list = ns_common_args + [
        argument("--load-balancing-group", "-l", help="Load balancing group", type=int, required=True),
    ]
    ns_set_qos_args_list = ns_common_args + [
        argument("--rw-ios-per-second", help="R/W IOs per second limit, 0 means unlimited", type=int),
        argument("--rw-megabytes-per-second", help="R/W megabytes per second limit, 0 means unlimited", type=int),
        argument("--r-megabytes-per-second", help="Read megabytes per second limit, 0 means unlimited", type=int),
        argument("--w-megabytes-per-second", help="Write megabytes per second limit, 0 means unlimited", type=int),
    ]
    ns_actions = []
    ns_actions.append({"name" : "add", "args" : ns_add_args_list, "help" : "Create a namespace"})
    ns_actions.append({"name" : "del", "args" : ns_del_args_list, "help" : "Delete a namespace"})
    ns_actions.append({"name" : "resize", "args" : ns_resize_args_list, "help" : "Resize a namespace"})
    ns_actions.append({"name" : "list", "args" : ns_list_args_list, "help" : "List namespaces"})
    ns_actions.append({"name" : "get_io_stats", "args" : ns_get_io_stats_args_list, "help" : "Get I/O stats for a namespace"})
    ns_actions.append({"name" : "change_load_balancing_group", "args" : ns_change_load_balancing_group_args_list, "help" : "Change load balancing group for a namespace"})
    ns_actions.append({"name" : "set_qos", "args" : ns_set_qos_args_list, "help" : "Set QOS limits for a namespace"})
    ns_choices = get_actions(ns_actions)
    @cli.cmd(ns_actions, ["ns"])
    def namespace(self, args):
        """Namespace commands"""
        if args.action == "add":
            return self.ns_add(args)
        elif args.action == "del":
            return self.ns_del(args)
        elif args.action == "resize":
            return self.ns_resize(args)
        elif args.action == "list":
            return self.ns_list(args)
        elif args.action == "get_io_stats":
            return self.ns_get_io_stats(args)
        elif args.action == "change_load_balancing_group":
            return self.ns_change_load_balancing_group(args)
        elif args.action == "set_qos":
            return self.ns_set_qos(args)
        if not args.action:
            self.cli.parser.error(f"missing action for namespace command (choose from {GatewayClient.ns_choices})")

    @cli.cmd()
    def get_subsystems(self, args):
        """Get subsystems"""
        subsystems = json_format.MessageToJson(
                        self.stub.get_subsystems(pb2.get_subsystems_req()),
                        indent=4, including_default_value_fields=True,
                        preserving_proto_field_name=True)
        self.logger.info(f"Get subsystems:\n{subsystems}")

def main_common(client, args):
    server_address = args.server_address
    server_port = args.server_port
    client_key = args.client_key
    client_cert = args.client_cert
    server_cert = args.server_cert
    client.connect(server_address, server_port, client_key, client_cert, server_cert)
    call_function = getattr(client, args.func.__name__)
    rc = call_function(args)
    return rc

def main_test(args):
    if not args:
        return None
    try:
        i = args.index("--format")
        del args[i:i + 2]
    except Exception:
        pass
    args = ["--format", "python"] + args
    client = GatewayClient()
    parsed_args = client.cli.parser.parse_args(args)
    if parsed_args.subcommand is None:
        return None

    return main_common(client, parsed_args)

def main(args=None) -> int:
    client = GatewayClient()
    parsed_args = client.cli.parser.parse_args(args)
    if parsed_args.subcommand is None:
        client.cli.parser.print_help()
        return -1

    return main_common(client, parsed_args)

if __name__ == "__main__":
    sys.exit(main())
