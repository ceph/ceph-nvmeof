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
from .utils import GatewayUtils
from .utils import GatewayUtilsCrypto
from .utils import GatewayEnumUtils


def errprint(msg):
    print(msg, file=sys.stderr)


def argument(*name_or_flags, **kwargs):
    """Helper function to format arguments for argparse command decorator."""
    return (list(name_or_flags), kwargs)


def get_enum_keys_list(e_type, include_first=True):
    k_list = []
    for k in e_type.keys():
        k_list.append(k.lower())
        k_list.append(k.upper())
    if not include_first:
        k_list = k_list[2:]

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

    def exit(self, status=0, message=None):
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
            type=str.lower,
            default="text",
            choices=["text", "json", "yaml", "plain", "python"],
            required=False)
        self.parser.add_argument(
            "--output",
            help="CLI output method",
            type=str.lower,
            default="log",
            choices=["log", "stdio"],
            required=False)
        self.parser.add_argument(
            "--log-level",
            help="CLI log level",
            type=str.lower,
            default="info",
            choices=get_enum_keys_list(pb2.GwLogLevel, False),
            required=False)
        self.parser.add_argument(
            "--server-address",
            default=(os.getenv('CEPH_NVMEOF_SERVER_ADDRESS') or "localhost"),
            type=str,
            help="Server address (default: CEPH_NVMEOF_SERVER_ADDRESS env variable or 'localhost')",
        )
        self.parser.add_argument(
            "--server-port",
            default=int(os.getenv('CEPH_NVMEOF_SERVER_PORT') or "5500"),
            type=int,
            help="Server port (default: CEPH_NVMEOF_SERVER_PORT env variable or '5500')",
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
        self.parser.add_argument(
            "--max-message-length",
            default=GatewayUtils.MAX_MESSAGE_LENGTH_DEFAULT,
            type=int,
            help="Max message length, in MB",
        )
        self.parser.add_argument(
            "--verbose",
            help="Run CLI in verbose mode",
            action='store_true'
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

    SIZE_UNITS = ["K", "M", "G", "T", "P"]
    MAX_MB_PER_SECOND = int(0xffffffffffffffff / (1024 * 1024))
    cli = Parser()

    def __init__(self):
        self._stub = None
        logging.basicConfig(format='%(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @property
    def stub(self):
        """Object on which to call server methods."""

        if self._stub is None:
            raise AttributeError("stub is None. Set with connect method.")
        return self._stub

    def connect(self, args, host, port, client_key, client_cert, server_cert, msg_len):
        """Connects to server and sets stub."""
        out_func, err_func, _ = self.get_output_functions(args)
        if args.format == "json" or args.format == "yaml" or args.format == "python":
            out_func = None

        # We need to enclose IPv6 addresses in brackets before
        # concatenating a colon and port number to it
        host = GatewayUtils.escape_address_if_ipv6(host)
        server = f"{host}:{port}"

        msg_len *= 1024 * 1024
        if client_key and client_cert:
            # Create credentials for mutual TLS and a secure channel
            if out_func:
                out_func("Enable server auth since both --client-key and "
                         "--client-cert are provided")
            with client_cert as f:
                client_cert = f.read()
            with client_key as f:
                client_key = f.read()
            if server_cert:
                with server_cert as f:
                    server_cert = f.read()
            else:
                err_func("No server certificate file was provided")

            credentials = grpc.ssl_channel_credentials(
                root_certificates=server_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )
            channel = grpc.secure_channel(server, credentials,
                                          options=[('grpc.max_receive_message_length', msg_len)])
        else:
            # Instantiate a channel without credentials
            channel = grpc.insecure_channel(server,
                                            options=[('grpc.max_receive_message_length', msg_len)])

        # Bind the client and the server
        self._stub = pb2_grpc.GatewayStub(channel)

    def get_actions(act_list):
        acts = ""
        for a in act_list:
            acts += ", '" + a["name"] + "'"
        return acts[2:]

    @staticmethod
    def format_adrfam(adrfam: pb2.AddressFamily) -> str:
        if adrfam == pb2.ipv4:
            return "IPv4"
        elif adrfam == pb2.ipv6:
            return "IPv6"

        return f"Unknown address family ({adrfam})"

    @staticmethod
    def get_dhchap_controller_text(val: pb2.DHCHAPControllerKeyOrigin) -> str:
        if val == pb2.DHCHAPControllerKeyOrigin.no_key:
            return "No Key"
        elif val == pb2.DHCHAPControllerKeyOrigin.host_specific:
            return "Host Specific"
        elif val == pb2.DHCHAPControllerKeyOrigin.subsystem_implicit:
            return "Subsystem Implicit"
        return "<unknown>"

    def get_output_functions(self, args):
        if args.output == "log":
            return (self.logger.info, self.logger.error, self.logger.warning)
        elif args.output == "stdio":
            return (print, errprint, errprint)
        else:
            self.cli.parser.error("invalid --output value")

    @staticmethod
    def parse_boolean(val: str) -> bool:
        val = val.lower()
        rc = val == "yes" or val == "true" or val == "1"
        return rc

    def validate_ip_address(self, addr, family):
        err = GatewayUtils.is_valid_ip_address(addr, family)
        if err:
            self.cli.parser.error(err)

    @cli.cmd()
    def version(self, args):
        """Get CLI version"""
        rc = 0
        out_func, err_func, _ = self.get_output_functions(args)
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
            gw_ver = self.parse_version_string(gw_info.version)
            if gw_ver is None:
                gw_info.status = errno.EINVAL
                gw_info.bool_status = False
                gw_info.error_message = f"Can't parse gateway version \"{gw_info.version}\"."
        return gw_info

    def gw_info(self, args):
        """Get gateway's information"""

        out_func, err_func, _ = self.get_output_functions(args)
        try:
            gw_info = self.gw_get_info()
        except Exception as ex:
            gw_info = pb2.gateway_info(
                status=errno.EINVAL,
                error_message=f"Failure getting gateway's information:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if gw_info.status == 0:
                if gw_info.cli_version:
                    out_func(f"CLI's version: {gw_info.cli_version}")
                if gw_info.version:
                    out_func(f"Gateway's version: {gw_info.version}")
                if gw_info.name:
                    out_func(f"Gateway's name: {gw_info.name}")
                location = gw_info.location if gw_info.location else "<default>"
                out_func(f"Gateway's location: {location}")
                if gw_info.group:
                    out_func(f"Gateway's group: {gw_info.group}")
                if gw_info.hostname:
                    out_func(f"Gateway's host name: {gw_info.hostname}")
                if not gw_info.gateway_initialization_over:
                    out_func("Gateway's initialization is still in progress")
                out_func(f"Gateway's load balancing group: {gw_info.load_balancing_group}")
                out_func(f"Gateway's address: {gw_info.addr}")
                out_func(f"Gateway's port: {gw_info.port}")
                if gw_info.max_subsystems:
                    out_func(f"Gateway's max subsystems: {gw_info.max_subsystems}")
                if gw_info.max_hosts:
                    out_func(f"Gateway's max hosts: {gw_info.max_hosts}")
                if gw_info.max_namespaces:
                    out_func(f"Gateway's max namespaces: {gw_info.max_namespaces}")
                if gw_info.max_namespaces_per_subsystem:
                    out_func(f"Gateway's max namespaces per subsystem: "
                             f"{gw_info.max_namespaces_per_subsystem}")
                if gw_info.max_hosts_per_subsystem:
                    out_func(f"Gateway's max hosts per subsystem: "
                             f"{gw_info.max_hosts_per_subsystem}")
                io_stats_txt = "enabled" if gw_info.io_stats_enabled else "disabled"
                out_func(f"Gateway's IO statistics is {io_stats_txt}")
                if gw_info.spdk_version:
                    out_func(f"SPDK version: {gw_info.spdk_version}")
                if not gw_info.bool_status:
                    err_func("Getting gateway's information returned status mismatch")
            else:
                err_func(gw_info.error_message)
                if gw_info.bool_status:
                    err_func("Getting gateway's information returned status mismatch")
        elif args.format == "json" or args.format == "yaml":
            gw_info_str = json_format.MessageToJson(gw_info, indent=4,
                                                    including_default_value_fields=True,
                                                    preserving_proto_field_name=True)
            if args.format == "json":
                out_func(gw_info_str)
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

        out_func, err_func, _ = self.get_output_functions(args)
        try:
            gw_info = self.gw_get_info()
        except Exception as ex:
            gw_info = pb2.gateway_info(status=errno.EINVAL,
                                       error_message=f"Failure getting gateway's version:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if gw_info.status == 0:
                out_func(f"Gateway's version: {gw_info.version}")
            else:
                err_func(f"{gw_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            gw_ver = pb2.gw_version(status=gw_info.status,
                                    error_message=gw_info.error_message,
                                    version=gw_info.version)
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
            return pb2.gw_version(status=gw_info.status,
                                  error_message=gw_info.error_message,
                                  version=gw_info.version)
        else:
            assert False

        return gw_info.status

    def gw_get_log_level(self, args):
        """Get gateway's log level"""

        out_func, err_func, _ = self.get_output_functions(args)
        req = pb2.get_gateway_log_level_req()
        try:
            ret = self.stub.get_gateway_log_level(req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure getting gateway log level:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                level = GatewayEnumUtils.get_key_from_value(pb2.GwLogLevel, ret.log_level)
                out_func(f"Gateway log level is \"{level}\"")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            out_log_level = json_format.MessageToJson(ret, indent=4,
                                                      including_default_value_fields=True,
                                                      preserving_proto_field_name=True)
            if args.format == "json":
                out_func(out_log_level)
            elif args.format == "yaml":
                obj = json.loads(out_log_level)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def gw_set_log_level(self, args):
        """Set gateway's log level"""

        out_func, err_func, _ = self.get_output_functions(args)
        log_level = None

        if args.level:
            log_level = args.level.lower()

        try:
            req = pb2.set_gateway_log_level_req(log_level=log_level)
        except ValueError as err:
            self.cli.parser.error(f"invalid log level {log_level}, error {err}")

        try:
            ret = self.stub.set_gateway_log_level(req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure setting gateway log level:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Set gateway log level to \"{log_level}\": Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def gw_get_thread_stats(self, args):
        """Show NVMf thread statistics for the gateway"""

        out_func, err_func, _ = self.get_output_functions(args)
        thread_stats = None
        try:
            get_thread_stats_req = pb2.get_thread_stats_req()
            thread_stats = self.stub.get_thread_stats(get_thread_stats_req)
        except Exception as ex:
            err_msg = f"Failure getting gateway's NVMf thread statistics:\n{ex}"
            thread_stats = pb2.thread_stats_info(status=errno.EINVAL,
                                                 error_message=err_msg)
        if args.format == "text" or args.format == "plain":
            if thread_stats.status == 0:
                table_format = "fancy_grid" if args.format == "text" else "plain"
                stats_list = []
                tick_rate = thread_stats.tick_rate
                for thread in thread_stats.threads:
                    stats_list.append([thread.name, thread.busy, thread.idle])
                stats_out = tabulate(stats_list,
                                     headers=["Name", "Busy", "Idle"],
                                     tablefmt=table_format)
                out_func(f"NVMf thread statistics for gateway (tick: "
                         f"{tick_rate}):\n{stats_out}")
            else:
                err_func(f"{thread_stats.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(thread_stats, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return thread_stats
        else:
            assert False

        return thread_stats.status

    def gw_set_io_stats_mode(self, args):
        """Set gateway IO statistics on or off"""

        out_func, err_func, _ = self.get_output_functions(args)
        ret = None
        if args.enable and args.disable:
            self.cli.parser.error("\"--enable\" and \"--disable\" are mutually exclusive")
        if not args.enable and not args.disable:
            self.cli.parser.error("One of \"--enable\" or \"--disable\" must be specified")

        enabled = args.enable or False
        try:
            set_io_stats_req = pb2.set_gateway_io_stats_mode_req(enabled=enabled)
            ret = self.stub.set_gateway_io_stats_mode(set_io_stats_req)
        except Exception as ex:
            ret = pb2.gateway_stats_info(status=errno.EINVAL,
                                         error_message=f"Failure setting gateway's "
                                                       f"IO statistics mode:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                mode_str = "enabled" if enabled else "disabled"
                out_func(f"Set gateway IO statistics mode to \"{mode_str}\": Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def gw_get_stats(self, args):
        """Show NVMf statistics for the gateway"""

        out_func, err_func, _ = self.get_output_functions(args)
        gw_stats = None
        try:
            get_stats_req = pb2.get_gateway_stats_req()
            gw_stats = self.stub.get_gateway_stats(get_stats_req)
        except Exception as ex:
            gw_stats = pb2.gateway_stats_info(status=errno.EINVAL,
                                              error_message=f"Failure getting gateway's "
                                                            f"NVMf statistics:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if gw_stats.status == 0:
                table_format = "fancy_grid" if args.format == "text" else "plain"
                stats_list = []
                for pg in gw_stats.poll_groups:
                    transports = ""
                    for trns in pg.transports:
                        transports += trns.trtype + ", "
                    if transports:
                        transports = transports.removesuffix(", ")
                    stats_list.append([pg.name, gw_stats.tick_rate,
                                       pg.admin_qpairs, pg.io_qpairs,
                                       pg.current_admin_qpairs, pg.current_io_qpairs,
                                       pg.pending_bdev_io, pg.completed_nvme_io,
                                       transports])
                stats_out = tabulate(stats_list,
                                     headers=["Poll\nGroup", "Tick\nRate",
                                              "Admin\nQPairs", "IO\nQPairs",
                                              "Current\nAdmin\nQPairs",
                                              "Current\nIO\nQPairs",
                                              "Pending\nBdev\nIO",
                                              "Completed\nNVMe\nIO",
                                              "Transports"],
                                     tablefmt=table_format)
                out_func(f"NVMf statistics for gateway:\n{stats_out}")
            else:
                err_func(f"{gw_stats.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(gw_stats, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return gw_stats
        else:
            assert False

        return gw_stats.status

    def gw_listener_info(self, args):
        """Show gateway's listeners info"""

        out_func, err_func, _ = self.get_output_functions(args)
        listeners_info = None
        try:
            list_req = pb2.show_gateway_listeners_info_req(subsystem_nqn=args.subsystem)
            listeners_info = self.stub.show_gateway_listeners_info(list_req)
        except Exception as ex:
            listeners_info = pb2.gateway_listeners_info(status=errno.EINVAL,
                                                        error_message=f"Failure listing gateway "
                                                                      f"listeners info:\n{ex}",
                                                        gw_listeners=[])

        if args.format == "text" or args.format == "plain":
            if listeners_info.status == 0:
                listeners_list = []
                for lstnr in listeners_info.gw_listeners:
                    ana_states = ""
                    for ana in lstnr.lb_states:
                        if not args.verbose and ana.state != pb2.ana_state.OPTIMIZED:
                            continue
                        state_str = GatewayEnumUtils.get_key_from_value(pb2.ana_state, ana.state)
                        if state_str is None:
                            ana_states += str(ana.grp_id) + ": " + str(ana.state) + "\n"
                        else:
                            ana_states += str(ana.grp_id) + ": " + state_str.title() + "\n"
                    adrfam = GatewayClient.format_adrfam(lstnr.listener.adrfam)
                    traddr = lstnr.listener.traddr
                    if lstnr.listener.adrfam == pb2.ipv6:
                        traddr = GatewayUtils.escape_address_if_ipv6(traddr)
                    secure = "Yes" if lstnr.listener.secure else "No"
                    active = "Yes" if lstnr.listener.active else "No"
                    ana_states = ana_states.removesuffix("\n")
                    listeners_list.append([lstnr.listener.host_name,
                                           lstnr.listener.trtype,
                                           adrfam,
                                           f"{traddr}:{lstnr.listener.trsvcid}",
                                           secure,
                                           active,
                                           ana_states])
                if len(listeners_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    listeners_out = tabulate(listeners_list,
                                             headers=["Host",
                                                      "Transport",
                                                      "Address Family",
                                                      "Address",
                                                      "Secure",
                                                      "Active",
                                                      "Load Balancing\nGroup ID/State"],
                                             tablefmt=table_format)
                    out_func(f"Gateway listeners for {args.subsystem}:\n{listeners_out}")
                else:
                    out_func(f"No gateway listeners for {args.subsystem}")
            else:
                err_func(f"{listeners_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(listeners_info, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return listeners_info
        else:
            assert False

        return listeners_info.status

    gw_set_log_level_args = [
        argument("--level", "-l", help="Gateway log level", required=True,
                 type=str.lower, choices=get_enum_keys_list(pb2.GwLogLevel, False)),
    ]
    gw_listener_info_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
    ]
    gw_set_io_stats_mode_args = [
        argument("--enable", "-e", help="Enable the IO statistics for the gateway",
                 action='store_true', required=False),
        argument("--disable", "-d", help="Enable the IO statistics for the gateway",
                 action='store_true', required=False),
    ]
    gw_actions = []
    gw_actions.append({"name": "version",
                       "args": [],
                       "help": "Display gateway's version"})
    gw_actions.append({"name": "info",
                       "args": [],
                       "help": "Display gateway's information"})
    gw_actions.append({"name": "get_log_level",
                       "args": [],
                       "help": "Get gateway's log level"})
    gw_actions.append({"name": "set_log_level",
                       "args": gw_set_log_level_args,
                       "help": "Set gateway's log level"})
    gw_actions.append({"name": "listener_info",
                       "args": gw_listener_info_args,
                       "help": "Show listeners information for the gateway"})
    gw_actions.append({"name": "get_thread_stats",
                       "args": [],
                       "help": "Show NVMf thread statistics for the gateway"})
    gw_actions.append({"name": "get_stats",
                       "args": [],
                       "help": "Show NVMf statistics for the gateway"})
    gw_actions.append({"name": "set_io_stats_mode",
                       "args": gw_set_io_stats_mode_args,
                       "help": "Set gateway IO statistics on or off"})
    gw_choices = get_actions(gw_actions)

    @cli.cmd(gw_actions, ["gw"])
    def gateway(self, args):
        """Gateway commands"""

        if args.action == "info":
            return self.gw_info(args)
        elif args.action == "version":
            return self.gw_version(args)
        elif args.action == "get_log_level":
            return self.gw_get_log_level(args)
        elif args.action == "set_log_level":
            return self.gw_set_log_level(args)
        elif args.action == "listener_info":
            return self.gw_listener_info(args)
        elif args.action == "get_stats":
            return self.gw_get_stats(args)
        elif args.action == "get_thread_stats":
            return self.gw_get_thread_stats(args)
        elif args.action == "set_io_stats_mode":
            return self.gw_set_io_stats_mode(args)
        if not args.action:
            self.cli.parser.error(f"missing action for gw command (choose from "
                                  f"{GatewayClient.gw_choices})")

    def spdk_log_level_disable(self, args):
        """Disable SPDK log flags"""

        out_func, err_func, _ = self.get_output_functions(args)

        req = pb2.disable_spdk_nvmf_logs_req(extra_log_flags=args.extra_log_flags)
        try:
            ret = self.stub.disable_spdk_nvmf_logs(req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure disabling SPDK log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func("Disable SPDK log flags: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def spdk_log_level_get(self, args):
        """Get SPDK log levels and nvmf log flags"""

        out_func, err_func, _ = self.get_output_functions(args)

        req = pb2.get_spdk_nvmf_log_flags_and_level_req(all_log_flags=args.all_log_flags)
        try:
            ret = self.stub.get_spdk_nvmf_log_flags_and_level(req)
        except Exception as ex:
            ret = pb2.req_status(
                status=errno.EINVAL,
                error_message=f"Failure getting SPDK log levels and nvmf log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                for flag in ret.nvmf_log_flags:
                    enabled_str = "enabled" if flag.enabled else "disabled"
                    out_func(f"SPDK log flag \"{flag.name}\" is {enabled_str}")
                level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, ret.log_level)
                out_func(f"SPDK log level is {level}")
                level = GatewayEnumUtils.get_key_from_value(pb2.LogLevel, ret.log_print_level)
                out_func(f"SPDK log print level is {level}")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            out_log_level = json_format.MessageToJson(ret, indent=4,
                                                      including_default_value_fields=True,
                                                      preserving_proto_field_name=True)
            if args.format == "json":
                out_func(out_log_level)
            elif args.format == "yaml":
                obj = json.loads(out_log_level)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def spdk_log_level_set(self, args):
        """Set SPDK log levels and nvmf log flags"""

        out_func, err_func, _ = self.get_output_functions(args)
        log_level = None
        print_level = None

        if args.level:
            log_level = args.level.upper()

        if args.print:
            print_level = args.print.upper()

        try:
            req = pb2.set_spdk_nvmf_logs_req(log_level=log_level,
                                             print_level=print_level,
                                             extra_log_flags=args.extra_log_flags)
        except ValueError as err:
            self.cli.parser.error(f"invalid log level {log_level}, error {err}")

        try:
            ret = self.stub.set_spdk_nvmf_logs(req)
        except Exception as ex:
            ret = pb2.req_status(
                status=errno.EINVAL,
                error_message=f"Failure setting SPDK log levels and nvmf log flags:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func("Set SPDK log levels and nvmf log flags: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    spdk_log_get_args = [
        argument("--all-log-flags", "-a",
                 help="Get all log flags, not just the NVMF ones",
                 action='store_true',
                 required=False),
    ]
    spdk_log_set_args = [
        argument("--level", "-l", help="SPDK log level", required=False,
                 type=str.lower, choices=get_enum_keys_list(pb2.LogLevel)),
        argument("--print", "-p", help="SPDK log print level", required=False,
                 type=str.lower, choices=get_enum_keys_list(pb2.LogLevel)),
        argument("--extra-log-flags", "-e", help="Extra log flags to set, not NVMF ones",
                 type=str, nargs="+", required=False),
    ]
    spdk_log_disable_args = [
        argument("--extra-log-flags", "-e", help="Extra log flags to reset, not NVMF ones",
                 type=str, nargs="+", required=False),
    ]
    spdk_log_actions = []
    spdk_log_actions.append({"name": "get",
                             "args": spdk_log_get_args,
                             "help": "Get SPDK log levels and nvmf log flags"})
    spdk_log_actions.append({"name": "set",
                             "args": spdk_log_set_args,
                             "help": "Set SPDK log levels and nvmf log flags"})
    spdk_log_actions.append({"name": "disable",
                             "args": spdk_log_disable_args,
                             "help": "Disable SPDK log flags"})
    spdk_log_choices = get_actions(spdk_log_actions)

    @cli.cmd(spdk_log_actions)
    def spdk_log_level(self, args):
        """SPDK log level commands"""
        if args.action == "get":
            return self.spdk_log_level_get(args)
        elif args.action == "set":
            return self.spdk_log_level_set(args)
        elif args.action == "disable":
            return self.spdk_log_level_disable(args)
        if not args.action:
            self.cli.parser.error(f"missing action for spdk_log_level command "
                                  f"(choose from {GatewayClient.spdk_log_choices})")

    def subsystem_add(self, args):
        """Create a subsystem"""

        out_func, err_func, wrn_func = self.get_output_functions(args)
        if args.max_namespaces is not None and args.max_namespaces <= 0:
            self.cli.parser.error("--max-namespaces value must be positive")
        if args.subsystem == GatewayUtils.DISCOVERY_NQN:
            self.cli.parser.error("Can't add a discovery subsystem")
        if args.dhchap_key == "":
            self.cli.parser.error("DH-HMAC-CHAP key can't be empty")
        if args.port is not None:
            if not args.network_mask:
                self.cli.parser.error("Port cannot be set without a network mask")
            if args.port <= 0:
                self.cli.parser.error("Port value must be positive")
            elif args.port > 0xffff:
                self.cli.parser.error("Port value must be smaller than 65536")
        if args.secure_listeners and not args.network_mask:
            self.cli.parser.error("Secure listeners cannot be set without a network mask")

        req = pb2.create_subsystem_req(subsystem_nqn=args.subsystem,
                                       serial_number=args.serial_number,
                                       max_namespaces=args.max_namespaces,
                                       enable_ha=True,
                                       no_group_append=args.no_group_append,
                                       dhchap_key=args.dhchap_key,
                                       network_mask=args.network_mask,
                                       port=args.port,
                                       secure_listeners=args.secure_listeners)
        try:
            ret = self.stub.create_subsystem(req)
        except Exception as ex:
            ret = pb2.subsys_status(
                status=errno.EINVAL,
                error_message=f"Failure adding subsystem {args.subsystem}:\n{ex}",
                nqn=args.subsystem)

        orig_status = ret.status
        if ret.status == errno.EAGAIN:
            ret.status = 0

        new_nqn = ""
        try:
            new_nqn = ret.nqn
        except Exception:
            # In case of an old gateway the returned value wouldn't have the nqn field
            pass
        if not new_nqn:
            new_nqn = args.subsystem

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Adding subsystem {new_nqn}: Successful")
                if orig_status != 0:
                    wrn_func(ret.error_message)
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)
        if args.subsystem == GatewayUtils.DISCOVERY_NQN:
            self.cli.parser.error("Can't delete a discovery subsystem")

        req = pb2.delete_subsystem_req(subsystem_nqn=args.subsystem,
                                       force=args.force,
                                       i_am_sure=args.i_am_sure)
        try:
            ret = self.stub.delete_subsystem(req)
        except Exception as ex:
            ret = pb2.req_status(
                status=errno.EINVAL,
                error_message=f"Failure deleting subsystem {args.subsystem}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Deleting subsystem {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)

        subsystems = None
        try:
            list_req = pb2.list_subsystems_req(subsystem_nqn=args.subsystem,
                                               serial_number=args.serial_number)
            subsystems = self.stub.list_subsystems(list_req)
        except Exception as ex:
            subsystems = pb2.subsystems_info_cli(
                status=errno.EINVAL,
                error_message=f"Failure listing subsystems:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if subsystems.status == 0:
                subsys_list = []
                created_without_key = False
                for s in subsystems.subsystems:
                    if s.created_without_key:
                        created_without_key = True
                        break
                for s in subsystems.subsystems:
                    if args.subsystem and args.subsystem != s.nqn:
                        err_func(f"Failure listing subsystem {args.subsystem}: "
                                 f"Got subsystem {s.nqn} instead")
                        return errno.ENODEV
                    if args.serial_number and args.serial_number != s.serial_number:
                        err_func(f"Failure listing subsystem with serial number "
                                 f"{args.serial_number}: Got serial number "
                                 f"{s.serial_number} instead")
                        return errno.ENODEV
                    ctrls_id = f"{s.min_cntlid}-{s.max_cntlid}"
                    has_dhchap = "Yes" if s.has_dhchap_key else "No"
                    allow_any = "Yes" if s.allow_any_host else "No"
                    net_mask = '\n'.join(s.network_mask) if s.network_mask else ""
                    one_subsys = [s.subtype,
                                  s.nqn,
                                  s.serial_number,
                                  ctrls_id,
                                  s.namespace_count,
                                  s.max_namespaces,
                                  allow_any,
                                  has_dhchap,
                                  net_mask]
                    if created_without_key:
                        one_subsys.append("Yes" if s.created_without_key else "No")
                    subsys_list.append(one_subsys)
                if len(subsys_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    headers_list = ["Subtype", "NQN", "Serial\nNumber", "Controller IDs",
                                    "Namespace\nCount", "Max\nNamespaces", "Allow\nAny Host",
                                    "DHCHAP\nKey", "Network\nMask"]
                    if created_without_key:
                        headers_list.append("Created\nWithout Key")
                    subsys_out = tabulate(subsys_list,
                                          headers=headers_list,
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
                        out_func("No subsystems")
            else:
                err_func(f"{subsystems.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(subsystems, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return subsystems
        else:
            assert False

        return subsystems.status

    def subsystem_del_key(self, args):
        """Delete subsystem's inband authentication key."""

        args.dhchap_key = None
        return self.subsystem_change_key(args)

    def subsystem_change_key(self, args):
        """Change subsystem's inband authentication key."""

        out_func, err_func, _ = self.get_output_functions(args)

        cmd = "deleting" if args.dhchap_key is None else "changing"
        cmd2 = "Deleting" if args.dhchap_key is None else "Changing"

        if args.dhchap_key is not None and args.dhchap_key == "":
            self.cli.parser.error("DH-HMAC-CHAP key can't be empty")

        req = pb2.change_subsystem_key_req(subsystem_nqn=args.subsystem, dhchap_key=args.dhchap_key)
        try:
            ret = self.stub.change_subsystem_key(req)
        except Exception as ex:
            errmsg = f"Failure {cmd} DH-HMAC-CHAP key for subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"{cmd2} DH-HMAC-CHAP key for subsystem {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_add_network_mask(self, args):
        """Add subsystem's network mask"""

        out_func, err_func, _ = self.get_output_functions(args)

        req = pb2.add_subsystem_network_req(subsystem_nqn=args.subsystem,
                                            network_mask=args.network_mask)
        try:
            ret = self.stub.add_subsystem_network(req)
        except Exception as ex:
            errmsg = f"Failure in adding network for subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Network mask {args.network_mask} added to subsystem "
                         f"{args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_del_network_mask(self, args):
        """Delete subsystem's network mask"""

        out_func, err_func, _ = self.get_output_functions(args)

        req = pb2.del_subsystem_network_req(subsystem_nqn=args.subsystem,
                                            network_mask=args.network_mask)
        try:
            ret = self.stub.del_subsystem_network(req)
        except Exception as ex:
            err = f"Failure in deleting network {args.network_mask} for subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{err}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Network mask {args.network_mask} deleted for subsystem "
                         f"{args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_add_kmip_server_endpoint(self, args):
        """Add a KMIP server endpoint to the subsystem"""

        out_func, err_func, wrn_func = self.get_output_functions(args)

        if not args.server_name or not args.server_name.strip():
            self.cli.parser.error("Server's name can't be empty")

        if args.port is not None:
            if args.port <= 0:
                self.cli.parser.error("Endpoint's port must be positive")
            elif args.port > 0xffff:
                self.cli.parser.error("Endpoint's port must be smaller than 65536")
        if not GatewayUtils.is_valid_host_name(args.address):
            self.cli.parser.error(f"Invalid endpoint address {args.address}")
        endpoint = pb2.kmip_server_endpoint(address=args.address, port=args.port)
        req = pb2.add_kmip_server_endpoints_req(subsystem_nqn=args.subsystem,
                                                server_name=args.server_name,
                                                endpoints=[endpoint])
        endpoint_addr = f"{args.address}:{args.port}" if args.port else args.address
        try:
            ret = self.stub.add_kmip_server_endpoints(req)
        except Exception as ex:
            errmsg = f"Failure adding an endpoint, with address {endpoint_addr}, to " \
                     f"KMIP server {args.server_name} on subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        orig_status = ret.status
        if ret.status == errno.EEXIST:
            ret.status = 0

        if args.format == "text" or args.format == "plain":
            if orig_status == 0:
                out_func(f"Adding an endpoint, with address {endpoint_addr}, to KMIP server "
                         f"{args.server_name} on subsystem {args.subsystem}: Successful")
            elif orig_status == errno.EEXIST:
                wrn_func(f"The endpoint, with address {endpoint_addr}, was not added "
                         f"to KMIP server {args.server_name} on subsystem {args.subsystem} "
                         f"as it's already there")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_del_kmip_server_endpoint(self, args):
        """Delete a KMIP server endpoint from the subsystem"""

        out_func, err_func, wrn_func = self.get_output_functions(args)

        if not args.server_name or not args.server_name.strip():
            self.cli.parser.error("Server's name can't be empty")

        if args.port is not None:
            if args.port <= 0:
                self.cli.parser.error("Endpoint's port must be positive")
            elif args.port > 0xffff:
                self.cli.parser.error("Endpoint's port must be smaller than 65536")
        if not GatewayUtils.is_valid_host_name(args.address):
            self.cli.parser.error(f"Invalid endpoint address {args.address}")
        endpoint = pb2.kmip_server_endpoint(address=args.address, port=args.port)
        req = pb2.del_kmip_server_endpoints_req(subsystem_nqn=args.subsystem,
                                                server_name=args.server_name,
                                                endpoints=[endpoint])
        endpoint_addr = f"{args.address}:{args.port}" if args.port else args.address
        try:
            ret = self.stub.del_kmip_server_endpoints(req)
        except Exception as ex:
            err = f"Failure deleting endpoint, with address {endpoint_addr}, from " \
                  f"KMIP server {args.server_name} on subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{err}:\n{ex}")

        orig_status = ret.status
        if ret.status == errno.ENOENT:
            ret.status = 0

        if args.format == "text" or args.format == "plain":
            if orig_status == 0:
                out_func(f"Deleting endpoint, with address {endpoint_addr}, from KMIP server "
                         f"{args.server_name} on subsystem {args.subsystem}: Successful")
            elif orig_status == errno.ENOENT:
                wrn_func(f"An endpoint with address {endpoint_addr} was not found "
                         f"for KMIP server {args.server_name} on subsystem {args.subsystem}")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def subsystem_list_kmip_server_endpoints(self, args):
        """List the KMIP server endpoints of a subsystem"""

        out_func, err_func, _ = self.get_output_functions(args)

        if not args.subsystem:
            args.subsystem = GatewayUtils.ALL_SUBSYSTEMS

        try:
            endpoints = self.stub.list_kmip_server_endpoints(pb2.list_kmip_server_endpoints_req(
                subsystem_nqn=args.subsystem, server_name=args.server_name))
        except Exception as ex:
            endpoints = pb2.kmip_server_endpoints_info(status=errno.EINVAL,
                                                       error_message=f"Failure listing KMIP "
                                                                     f"server endpoints:\n{ex}",
                                                       endpoints=[])

        if args.format == "text" or args.format == "plain":
            if endpoints.status == 0:
                endpoint_list = []
                for s in endpoints.endpoints:
                    endpoint_list.append([s.subsystem_nqn,
                                          s.server_name,
                                          s.address,
                                          s.port])
                if len(endpoint_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    headers_list = ["Subsystem", "Server Name", "Address", "Port"]
                    endpoint_out = tabulate(endpoint_list,
                                            headers=headers_list,
                                            tablefmt=table_format, stralign="center")
                    out_func(endpoint_out)
                else:
                    if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                        if args.server_name:
                            out_func(f"No endpoints for KMIP server {args.server_name} "
                                     f"on any subsystem")
                        else:
                            out_func("No KMIP server endpoints on any subsystem")
                    else:
                        if args.server_name:
                            out_func(f"No endpoints for KMIP server {args.server_name} on "
                                     f"subsystem {args.subsystem}")
                        else:
                            out_func(f"No KMIP server endpoints on subsystem {args.subsystem}")
            else:
                err_func(f"{endpoints.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(endpoints, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return endpoints
        else:
            assert False

        return endpoints.status

    subsys_add_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--serial-number",
                 "-s",
                 help="Serial number",
                 required=False),
        argument("--max-namespaces",
                 "-m",
                 help="Maximum number of namespaces",
                 type=int,
                 required=False),
        argument("--no-group-append",
                 help="Do not append gateway group name to the NQN",
                 action='store_true',
                 required=False),
        argument("--dhchap-key",
                 "-k",
                 help="Subsystem DH-HMAC-CHAP key",
                 required=False),
        argument("--network-mask",
                 help="For this subnet, automatically create listeners for this subsystem",
                 nargs='+',
                 required=False),
        argument("--port",
                 "-p",
                 help="Port to use for the created listeners",
                 type=int,
                 required=False),
        argument("--secure-listeners",
                 help="Make all the auto-listeners for this subsystem secure",
                 action='store_true',
                 required=False),
    ]
    subsys_del_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--force",
                 help="Delete subsystem's namespaces if any, then delete subsystem. If not set "
                      "a subsystem deletion would fail in case it contains namespaces",
                 action='store_true', required=False),
        argument("--i-am-sure",
                 help="Confirmation for deleting the namespace associated RBD image",
                 action='store_true',
                 required=False),
    ]
    subsys_list_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=False),
        argument("--serial-number",
                 "-s",
                 help="Serial number",
                 required=False),
    ]
    subsys_change_key_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--dhchap-key",
                 "-k",
                 help="Subsystem DH-HMAC-CHAP key",
                 required=True),
    ]
    subsys_del_key_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
    ]
    subsys_del_network_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--network-mask",
                 help="Existing network mask to delete",
                 required=True),
    ]
    subsys_add_network_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--network-mask",
                 help="New network mask to add",
                 required=True),
    ]
    subsys_del_kmip_server_endpoint_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--server-name",
                 "-s",
                 help="name of the KMIP server the endpoint points to",
                 required=True),
        argument("--address",
                 "-a",
                 help="KMIP server endpoint address",
                 required=True),
        argument("--port",
                 "-p",
                 type=int,
                 help="KMIP server endpoint port",
                 required=False),
    ]
    subsys_add_kmip_server_endpoint_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--server-name",
                 "-s",
                 help="name of the KMIP server the endpoint points to",
                 required=True),
        argument("--address",
                 "-a",
                 help="KMIP server endpoint address",
                 required=True),
        argument("--port",
                 "-p",
                 type=int,
                 help="KMIP server endpoint port",
                 required=False),
    ]
    subsys_list_kmip_server_endpoints_args = [
        argument("--subsystem",
                 "-n",
                 help="only show endpoints for this subsystem NQN",
                 required=False),
        argument("--server-name",
                 "-s",
                 help="only show endpoints for this KMIP server name",
                 required=False),
    ]
    subsystem_actions = []
    subsystem_actions.append({"name": "add",
                              "args": subsys_add_args,
                              "help": "Create a subsystem"})
    subsystem_actions.append({"name": "del",
                              "args": subsys_del_args,
                              "help": "Delete a subsystem"})
    subsystem_actions.append({"name": "list",
                              "args": subsys_list_args,
                              "help": "List subsystems"})
    subsystem_actions.append({"name": "change_key",
                              "args": subsys_change_key_args,
                              "help": "Change subsystem inband authentication key"})
    subsystem_actions.append({"name": "del_key",
                              "args": subsys_del_key_args,
                              "help": "Delete subsystem inband authentication key"})
    subsystem_actions.append({"name": "add_network",
                              "args": subsys_add_network_args,
                              "help": "Add a network mask in the subsystem"})
    subsystem_actions.append({"name": "del_network",
                              "args": subsys_del_network_args,
                              "help": "Delete a network mask in the subsystem"})
    subsystem_actions.append({"name": "add_kmip_server_endpoint",
                              "args": subsys_add_kmip_server_endpoint_args,
                              "help": "Add a KMIP server endpoint to the subsystem"})
    subsystem_actions.append({"name": "del_kmip_server_endpoint",
                              "args": subsys_del_kmip_server_endpoint_args,
                              "help": "Delete a KMIP server endpoint from the subsystem"})
    subsystem_actions.append({"name": "list_kmip_server_endpoints",
                              "args": subsys_list_kmip_server_endpoints_args,
                              "help": "List KMIP server endpoints for a subsystem or "
                                      "all subsystems"})
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
        elif args.action == "change_key":
            return self.subsystem_change_key(args)
        elif args.action == "del_key":
            return self.subsystem_del_key(args)
        elif args.action == "add_network":
            return self.subsystem_add_network_mask(args)
        elif args.action == "del_network":
            return self.subsystem_del_network_mask(args)
        elif args.action == "add_kmip_server_endpoint":
            return self.subsystem_add_kmip_server_endpoint(args)
        elif args.action == "del_kmip_server_endpoint":
            return self.subsystem_del_kmip_server_endpoint(args)
        elif args.action == "list_kmip_server_endpoints":
            return self.subsystem_list_kmip_server_endpoints(args)
        if not args.action:
            self.cli.parser.error(f"missing action for subsystem command (choose "
                                  f"from {GatewayClient.subsystem_choices})")

    def listener_add(self, args):
        """Create a listener"""

        out_func, err_func, wrn_func = self.get_output_functions(args)

        if args.trsvcid is not None:
            if args.trsvcid <= 0:
                self.cli.parser.error("trsvcid value must be positive")
            elif args.trsvcid > 0xffff:
                self.cli.parser.error("trsvcid value must be smaller than 65536")
        if not args.adrfam:
            args.adrfam = "IPV4"

        if not GatewayUtils.is_valid_host_name(args.host_name):
            self.cli.parser.error(f"invalid host name {args.host_name}")

        self.validate_ip_address(args.traddr, args.adrfam)
        traddr = GatewayUtils.escape_address_if_ipv6(args.traddr)
        adrfam = None
        if args.adrfam:
            adrfam = args.adrfam.lower()

        req = pb2.create_listener_req(
            nqn=args.subsystem,
            host_name=args.host_name,
            adrfam=adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
            secure=args.secure,
            verify_host_name=args.verify_host_name,
            force=args.force
        )

        lstnr_addr = f"{traddr}:{args.trsvcid}" if args.trsvcid else traddr
        try:
            ret = self.stub.create_listener(req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure adding {traddr} listener at "
                                               f"{lstnr_addr}:\n{ex}")

        orig_status = ret.status
        if ret.status == errno.EREMOTE:
            ret.status = 0

        if args.format == "text" or args.format == "plain":
            if orig_status == 0:
                out_func(f"Adding {args.subsystem} listener at {lstnr_addr}: "
                         f"Successful")
            elif orig_status == errno.EREMOTE:
                wrn_func(f"Adding {args.subsystem} listener at {lstnr_addr}: "
                         f"listener will only be active when appropriate gateway is up")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)

        if args.trsvcid is not None:
            if args.trsvcid <= 0:
                self.cli.parser.error("trsvcid value must be positive")
            elif args.trsvcid > 0xffff:
                self.cli.parser.error("trsvcid value must be smaller than 65536")
        if not args.adrfam:
            args.adrfam = "IPV4"

        self.validate_ip_address(args.traddr, args.adrfam)
        if args.host_name == "*" and not args.force:
            self.cli.parser.error("must use --force when setting host name to *")

        traddr = GatewayUtils.escape_address_if_ipv6(args.traddr)
        adrfam = None
        if args.adrfam:
            adrfam = args.adrfam.lower()

        req = pb2.delete_listener_req(
            nqn=args.subsystem,
            host_name=args.host_name,
            adrfam=adrfam,
            traddr=traddr,
            trsvcid=args.trsvcid,
            force=args.force,
        )

        try:
            ret = self.stub.delete_listener(req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure deleting listener {traddr}:{args.trsvcid}"
                                               f" from {args.subsystem}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                host_msg = f"for host {args.host_name}"
                if args.host_name == "*":
                    host_msg = "for all hosts"
                out_func(f"Deleting listener {traddr}:{args.trsvcid} from {args.subsystem} "
                         f"{host_msg}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)
        listeners_info = None
        try:
            list_req = pb2.list_listeners_req(subsystem=args.subsystem)
            listeners_info = self.stub.list_listeners(list_req)
        except Exception as ex:
            listeners_info = pb2.listeners_info(status=errno.EINVAL,
                                                error_message=f"Failure listing listeners:\n{ex}",
                                                listeners=[])

        if args.format == "text" or args.format == "plain":
            if listeners_info.status == 0:
                listeners_list = []
                for lstnr in listeners_info.listeners:
                    adrfam = GatewayClient.format_adrfam(lstnr.adrfam)
                    secure = "Yes" if lstnr.secure else "No"
                    active = "Yes" if lstnr.active else "No"
                    manual = "Yes" if lstnr.manual else "No"
                    traddr = lstnr.traddr
                    if lstnr.adrfam == pb2.ipv6:
                        traddr = GatewayUtils.escape_address_if_ipv6(traddr)
                    listeners_list.append([lstnr.host_name,
                                           lstnr.trtype,
                                           adrfam,
                                           f"{traddr}:{lstnr.trsvcid}",
                                           secure,
                                           active,
                                           manual])
                if len(listeners_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    listeners_out = tabulate(listeners_list,
                                             headers=["Host",
                                                      "Transport",
                                                      "Address Family",
                                                      "Address",
                                                      "Secure",
                                                      "Active",
                                                      "Manual"],
                                             tablefmt=table_format)
                    out_func(f"Listeners for {args.subsystem}:\n{listeners_out}")
                else:
                    out_func(f"No listeners for {args.subsystem}")
            else:
                err_func(f"{listeners_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(listeners_info, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return listeners_info
        else:
            assert False

        return listeners_info.status

    listener_common_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
    ]
    listener_add_args = listener_common_args + [
        argument("--host-name",
                 "-t",
                 help="Host name",
                 required=True),
        argument("--verify-host-name",
                 "-y",
                 help="Fail in case the listener's host name is different than the gateway's",
                 action='store_true',
                 required=False),
        argument("--traddr",
                 "-a",
                 help="NVMe host IP",
                 required=True),
        argument("--trsvcid",
                 "-s",
                 help="Port number",
                 type=int,
                 required=False),
        argument("--adrfam",
                 "-f",
                 help="Address family",
                 default="",
                 type=str.lower,
                 choices=get_enum_keys_list(pb2.AddressFamily)),
        argument("--secure",
                 help="Use secure channel",
                 action='store_true',
                 required=False),
        argument("--force",
                 help="Allow contradiction in security with existing listeners",
                 action='store_true',
                 required=False),
    ]
    listener_del_args = listener_common_args + [
        argument("--host-name",
                 "-t",
                 help="Host name",
                 required=True),
        argument("--traddr",
                 "-a",
                 help="NVMe host IP",
                 required=True),
        argument("--trsvcid",
                 "-s",
                 help="Port number",
                 type=int,
                 required=True),
        argument("--adrfam",
                 "-f",
                 help="Address family",
                 default="",
                 type=str.lower,
                 choices=get_enum_keys_list(pb2.AddressFamily)),
        argument("--force",
                 help="Delete listener even if there are active connections for the address, "
                      "or the host name doesn't match",
                 action='store_true',
                 required=False),
    ]
    listener_list_args = listener_common_args + [
    ]
    listener_actions = []
    listener_actions.append({"name": "add", "args": listener_add_args, "help": "Create a listener"})
    listener_actions.append({"name": "del", "args": listener_del_args, "help": "Delete a listener"})
    listener_actions.append({"name": "list", "args": listener_list_args, "help": "List listeners"})
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
            self.cli.parser.error(f"missing action for listener command (choose "
                                  f"from {GatewayClient.listener_choices})")

    def host_add(self, args):
        """Add a host to a subsystem."""

        rc = 0
        ret_list = []
        out_func, err_func, wrn_func = self.get_output_functions(args)

        if args.psk == "":
            self.cli.parser.error("PSK key can't be empty")

        if args.dhchap_key == "":
            self.cli.parser.error("DH-HMAC-CHAP key can't be empty")

        if args.dhchap_controller_key == "":
            self.cli.parser.error("Controller's DH-HMAC-CHAP key can't be empty")

        if args.dhchap_controller_key and not args.dhchap_key:
            self.cli.parser.error("Controller's DH-HMAC-CHAP key is not allowed without "
                                  "a host DH-HMAC-CHAP key")

        if args.psk:
            if len(args.host_nqn) > 1:
                self.cli.parser.error("Can't have more than one host NQN when PSK keys are used")

        if args.dhchap_key:
            if len(args.host_nqn) > 1:
                self.cli.parser.error("Can't have more than one host NQN when "
                                      "DH-HMAC-CHAP keys are used")
        if args.dhchap_controller_key:
            if len(args.host_nqn) > 1:
                self.cli.parser.error("Can't have more than one host NQN when "
                                      "controller DH-HMAC-CHAP keys are used")

        for one_host_nqn in args.host_nqn:
            if one_host_nqn == "*" and args.psk:
                self.cli.parser.error("PSK key is only allowed for specific hosts")

            if one_host_nqn == "*":
                if args.dhchap_key:
                    self.cli.parser.error("DH-HMAC-CHAP key is only allowed for specific hosts")
                if args.dhchap_controller_key:
                    self.cli.parser.error("Controller DH-HMAC-CHAP key is only allowed "
                                          "for specific hosts")

            req = pb2.add_host_req(subsystem_nqn=args.subsystem, host_nqn=one_host_nqn,
                                   psk=args.psk, dhchap_key=args.dhchap_key,
                                   dhchap_ctrlr_key=args.dhchap_controller_key)
            try:
                ret = self.stub.add_host(req)
            except Exception as ex:
                if one_host_nqn == "*":
                    errmsg = f"Failure allowing open host access to {args.subsystem}"
                else:
                    errmsg = f"Failure adding host {one_host_nqn} to {args.subsystem}"
                ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

            if not rc:
                rc = ret.status

            if args.format == "text" or args.format == "plain":
                if ret.status == 0:
                    if one_host_nqn == "*":
                        out_func(f"Allowing open host access to {args.subsystem}: Successful")
                        wrn_func(f"Open host access to subsystem {args.subsystem} "
                                 f"might be a security breach")
                    else:
                        out_func(f"Adding host {one_host_nqn} to {args.subsystem}: Successful")
                else:
                    err_func(f"{ret.error_message}")
            elif args.format == "json" or args.format == "yaml":
                ret_str = json_format.MessageToJson(ret, indent=4,
                                                    including_default_value_fields=True,
                                                    preserving_proto_field_name=True)
                if args.format == "json":
                    out_func(ret_str)
                elif args.format == "yaml":
                    obj = json.loads(ret_str)
                    out_func(yaml.dump(obj))
            elif args.format == "python":
                ret_list.append(ret)
            else:
                assert False

        if args.format == "python":
            return ret_list

        return rc

    def host_del(self, args):
        """Delete a host from a subsystem."""

        rc = 0
        ret_list = []
        out_func, err_func, wrn_func = self.get_output_functions(args)
        for one_host_nqn in args.host_nqn:
            req = pb2.remove_host_req(subsystem_nqn=args.subsystem,
                                      host_nqn=one_host_nqn,
                                      force=args.force)

            try:
                ret = self.stub.remove_host(req)
            except Exception as ex:
                if one_host_nqn == "*":
                    errmsg = f"Failure disabling open host access to {args.subsystem}"
                else:
                    errmsg = f"Failure removing host {one_host_nqn} access to {args.subsystem}"
                ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

            # EBUSY is just a warning, so do not fail command
            if not rc and ret.status and ret.status != errno.EBUSY:
                rc = ret.status

            orig_status = ret.status
            if ret.status == errno.EBUSY:
                ret.status = 0

            if args.format == "text" or args.format == "plain":
                if ret.status == 0:
                    if one_host_nqn == "*":
                        out_func(f"Disabling open host access to {args.subsystem}: Successful")
                    else:
                        out_func(f"Removing host {one_host_nqn} access from "
                                 f"{args.subsystem}: Successful")
                    if orig_status != 0:
                        wrn_func(ret.error_message)
                else:
                    err_func(ret.error_message)
            elif args.format == "json" or args.format == "yaml":
                ret_str = json_format.MessageToJson(ret, indent=4,
                                                    including_default_value_fields=True,
                                                    preserving_proto_field_name=True)
                if args.format == "json":
                    out_func(ret_str)
                elif args.format == "yaml":
                    obj = json.loads(ret_str)
                    out_func(yaml.dump(obj))
            elif args.format == "python":
                ret_list.append(ret)
            else:
                assert False

        if args.format == "python":
            return ret_list

        return rc

    def host_del_key(self, args):
        """Delete host's inband authentication key."""

        args.dhchap_key = None
        return self.host_change_key(args)

    def host_change_key(self, args):
        """Change host's inband authentication key."""

        out_func, err_func, _ = self.get_output_functions(args)

        cmd = "delete" if args.dhchap_key is None else "change"
        cmd2 = "deleting" if args.dhchap_key is None else "changing"
        cmd3 = "Deleting" if args.dhchap_key is None else "Changing"
        if args.dhchap_key is not None and args.dhchap_key == "":
            self.cli.parser.error("DH-HMAC-CHAP key can't be empty")

        if args.host_nqn == "*":
            self.cli.parser.error(f"Can't {cmd} DH-HMAC-CHAP key for host NQN '*', "
                                  f"please use a real NQN")

        req = pb2.change_host_key_req(subsystem_nqn=args.subsystem, host_nqn=args.host_nqn,
                                      dhchap_key=args.dhchap_key,
                                      dhchap_ctrlr_key=GatewayUtilsCrypto.EXISTING_DHCHAP_KEY)
        try:
            ret = self.stub.change_host_key(req)
        except Exception as ex:
            errmsg = f"Failure {cmd2} DH-HMAC-CHAP key for host {args.host_nqn} on " \
                     f"subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"{cmd3} DH-HMAC-CHAP key for host {args.host_nqn} on subsystem "
                         f"{args.subsystem}: Successful")
            else:
                err_func(ret.error_message)
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def controller_del_key(self, args):
        """Delete controller's inband authentication key."""

        args.dhchap_controller_key = None
        return self.controller_change_key(args)

    def controller_change_key(self, args):
        """Change controller's inband authentication key."""

        out_func, err_func, _ = self.get_output_functions(args)

        cmd = "delete" if args.dhchap_controller_key is None else "change"
        cmd2 = "deleting" if args.dhchap_controller_key is None else "changing"
        cmd3 = "Deleting" if args.dhchap_controller_key is None else "Changing"
        if args.dhchap_controller_key is not None and args.dhchap_controller_key == "":
            self.cli.parser.error("Controller DH-HMAC-CHAP key can't be empty")

        if args.host_nqn == "*":
            self.cli.parser.error(f"Can't {cmd} controller DH-HMAC-CHAP key for host NQN '*', "
                                  f"please use a real NQN")

        req = pb2.change_host_key_req(subsystem_nqn=args.subsystem, host_nqn=args.host_nqn,
                                      dhchap_key=GatewayUtilsCrypto.EXISTING_DHCHAP_KEY,
                                      dhchap_ctrlr_key=args.dhchap_controller_key)
        try:
            ret = self.stub.change_host_key(req)
        except Exception as ex:
            errmsg = f"Failure {cmd2} DH-HMAC-CHAP key for controller of {args.host_nqn} on " \
                     f"subsystem {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"{cmd3} DH-HMAC-CHAP key for controller of host {args.host_nqn} "
                         f"on subsystem {args.subsystem}: Successful")
            else:
                err_func(ret.error_message)
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)

        hosts_info = None
        try:
            hosts_info = self.stub.list_hosts(
                pb2.list_hosts_req(subsystem=args.subsystem, clear_alerts=False))
        except Exception as ex:
            hosts_info = pb2.hosts_info(status=errno.EINVAL,
                                        error_message=f"Failure listing hosts:\n{ex}", hosts=[])

        if args.format == "text" or args.format == "plain":
            if hosts_info.status == 0:
                hosts_list = []
                has_timeout = False
                for h in hosts_info.hosts:
                    if h.disconnected_due_to_keepalive_timeout:
                        has_timeout = True
                        break
                if hosts_info.allow_any_host:
                    timeout_col = ["n/a"] if has_timeout else []
                    hosts_list.append(["Any host", "n/a", "n/a", "n/a"] + timeout_col)
                for h in hosts_info.hosts:
                    use_psk = "Yes" if h.use_psk else "No"
                    use_dhchap = "Yes" if h.use_dhchap else "No"
                    use_dhchap_ctrlr = GatewayClient.get_dhchap_controller_text(
                        h.dhchap_controller_origin)
                    ka_timeout = "Yes" if h.disconnected_due_to_keepalive_timeout else "No"
                    timeout_col = [ka_timeout] if has_timeout else []
                    one_host = [h.nqn, use_psk, use_dhchap, use_dhchap_ctrlr] + timeout_col
                    hosts_list.append(one_host)
                if len(hosts_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    timeout_col = ["Keepalive\nTimeout"] if has_timeout else []
                    headers_list = ["Host NQN", "Uses PSK", "Uses DHCHAP",
                                    "DHCHAP Controller"] + timeout_col
                    hosts_out = tabulate(hosts_list,
                                         headers=headers_list,
                                         tablefmt=table_format, stralign="center")
                    out_func(f"Hosts allowed to access {args.subsystem}:\n{hosts_out}")
                else:
                    out_func(f"No hosts are allowed to access {args.subsystem}")
            else:
                err_func(f"{hosts_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(hosts_info, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return hosts_info
        else:
            assert False

        return hosts_info.status

    host_common_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
    ]
    host_add_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN list",
                 nargs="+",
                 required=True),
        argument("--psk",
                 "-p",
                 help="Hosts PSK key",
                 required=False),
        argument("--dhchap-key",
                 "-k",
                 help="Host DH-HMAC-CHAP key",
                 required=False),
        argument("--dhchap-controller-key",
                 "-c",
                 help="Controller DH-HMAC-CHAP key (mutual exclusive with subsystem's key)",
                 required=False),
    ]
    host_del_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN list",
                 nargs="+",
                 required=True),
        argument("--force",
                 help="Delete the host even if it used in a namespace netmask",
                 action='store_true',
                 required=False),
    ]
    host_list_args = host_common_args + [
    ]
    host_change_key_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
        argument("--dhchap-key",
                 "-k",
                 help="Host DH-HMAC-CHAP key",
                 required=True),
    ]
    controller_change_key_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
        argument("--dhchap-controller-key",
                 "-c",
                 help="Controller DH-HMAC-CHAP key",
                 required=True),
    ]
    host_del_key_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
    ]
    controller_del_key_args = host_common_args + [
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
    ]
    host_actions = []
    host_actions.append({"name": "add",
                         "args": host_add_args,
                         "help": "Add host access to a subsystem"})
    host_actions.append({"name": "del",
                         "args": host_del_args,
                         "help": "Remove host access from a subsystem"})
    host_actions.append({"name": "list",
                         "args": host_list_args,
                         "help": "List subsystem's host access"})
    host_actions.append({"name": "change_key",
                         "args": host_change_key_args,
                         "help": "Change host's inband authentication key"})
    host_actions.append({"name": "del_key",
                         "args": host_del_key_args,
                         "help": "Delete host's inband authentication key"})
    host_actions.append({"name": "change_controller_key",
                         "args": controller_change_key_args,
                         "help": "Change controller's inband authentication key"})
    host_actions.append({"name": "del_controller_key",
                         "args": controller_del_key_args,
                         "help": "Delete controller's inband authentication key"})
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
        elif args.action == "change_key":
            return self.host_change_key(args)
        elif args.action == "del_key":
            return self.host_del_key(args)
        elif args.action == "change_controller_key":
            return self.controller_change_key(args)
        elif args.action == "del_controller_key":
            return self.controller_del_key(args)
        if not args.action:
            self.cli.parser.error(f"missing action for host command "
                                  f"(choose from {GatewayClient.host_choices})")

    def connection_list(self, args):
        """List connections for a subsystem."""

        out_func, err_func, _ = self.get_output_functions(args)
        connections_info = None
        if not args.subsystem:
            args.subsystem = GatewayUtils.ALL_SUBSYSTEMS
        try:
            list_req = pb2.list_connections_req(subsystem=args.subsystem,
                                                clear_alerts=False)
            connections_info = self.stub.list_connections(list_req)
        except Exception as ex:
            connections_info = pb2.connections_info(status=errno.EINVAL,
                                                    error_message=f"Failure listing hosts:\n{ex}",
                                                    connections=[])

        if args.format == "text" or args.format == "plain":
            if connections_info.status == 0:
                connections_list = []
                has_timeout = False
                for conn in connections_info.connections:
                    if conn.disconnected_due_to_keepalive_timeout:
                        has_timeout = True
                        break
                for conn in connections_info.connections:
                    conn_secure = "<n/a>"
                    conn_psk = "Yes" if conn.use_psk else "No"
                    conn_dhchap = "Yes" if conn.use_dhchap else "No"
                    conn_dhchap_ctrlr = GatewayClient.get_dhchap_controller_text(
                        conn.dhchap_controller_origin)
                    if conn.connected:
                        conn_secure = "Yes" if conn.secure else "No"
                    conn_addr = "<n/a>"
                    if conn.connected:
                        traddr = conn.traddr
                        if conn.adrfam == pb2.ipv6:
                            traddr = GatewayUtils.escape_address_if_ipv6(traddr)
                        conn_addr = f"{traddr}:{conn.trsvcid}"
                    subsys_col = []
                    if connections_info.subsystem_nqn == GatewayUtils.ALL_SUBSYSTEMS:
                        subsys_col = [conn.subsystem]
                    ka_timeout = "Yes" if conn.disconnected_due_to_keepalive_timeout else "No"
                    timeout_col = [ka_timeout] if has_timeout else []
                    qp_text = conn.qpairs_count if conn.connected else "<n/a>"
                    ctrl_text = conn.controller_id if conn.connected else "<n/a>"
                    connections_list.append(subsys_col + [conn.nqn,
                                                          conn_addr,
                                                          "Yes" if conn.connected else "No",
                                                          qp_text,
                                                          ctrl_text,
                                                          conn_secure,
                                                          conn_psk,
                                                          conn_dhchap,
                                                          conn_dhchap_ctrlr] + timeout_col)
                subsys_text = connections_info.subsystem_nqn
                if len(connections_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    subsys_col = []
                    if connections_info.subsystem_nqn == GatewayUtils.ALL_SUBSYSTEMS:
                        subsys_col = ["Subsystem"]
                    timeout_col = ["Keepalive\nTimeout"] if has_timeout else []
                    connections_out = tabulate(
                        connections_list,
                        headers=subsys_col + ["Host NQN",
                                              "Address",
                                              "Connected",
                                              "QPairs Count",
                                              "Controller ID",
                                              "Secure",
                                              "Uses\nPSK",
                                              "Uses\nDHCHAP",
                                              "DHCHAP\nController"] + timeout_col,
                        tablefmt=table_format)
                    if connections_info.subsystem_nqn == GatewayUtils.ALL_SUBSYSTEMS:
                        subsys_text = "all subsystems"
                    out_func(f"Connections for {subsys_text}:\n{connections_out}")
                else:
                    if connections_info.subsystem_nqn == GatewayUtils.ALL_SUBSYSTEMS:
                        subsys_text = "any subsystem"
                    out_func(f"No connections for {subsys_text}")
            else:
                err_func(f"{connections_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(connections_info, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return connections_info
        else:
            assert False

        return connections_info.status

    def connection_get_io_statistics(self, args):
        """Get connection's IO statistics."""

        def _is_latency_group_empty(grp) -> bool:
            if not grp:
                return True
            if grp.io_count:
                return False
            if grp.total.min or grp.total.max or grp.total.mean:
                return False
            if grp.bdev.min or grp.bdev.max or grp.bdev.mean:
                return False
            if grp.net.min or grp.net.max or grp.net.mean:
                return False
            if grp.qos.min or grp.qos.max or grp.qos.mean:
                return False
            return True

        def _get_latency_stats_line(ls) -> str:
            return f"{ls.min}, {ls.max}, {ls.mean}"

        out_func, err_func, _ = self.get_output_functions(args)

        if args.host_nqn == "*":
            self.cli.parser.error("Must specify a specific host NQN")

        req = pb2.get_connection_io_statistics_req(subsystem_nqn=args.subsystem,
                                                   host_nqn=args.host_nqn, reset=False)
        try:
            ret = self.stub.get_connection_io_statistics(req)
        except Exception as ex:
            ret = pb2.connection_io_statistics(status=errno.EINVAL,
                                               error_message=f"Failure getting host IO "
                                                             f"statistics:\n{ex}",
                                               subsystem_nqn=args.subsystem,
                                               host_nqn=args.host_nqn)

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if len(ret.buckets) == 0:
                    out_func(f"No IO statistics available for host {args.host_nqn} "
                             f"on {args.subsystem}")
                    if ret.total_num_ios > 0:
                        out_func(f"Total IOs count: {ret.total_num_ios}")
                    return ret.status

                table_format = "fancy_grid" if args.format == "text" else "plain"
                out_func(f"IO statistics for host {args.host_nqn} on {args.subsystem}:\n")
                stats_list = []
                for bucket in ret.buckets:
                    rd = bucket.read
                    wr = bucket.write
                    lat_groups = [("Read", rd), ("Write", wr)]
                    for lg in lat_groups:
                        if not _is_latency_group_empty(lg[1]):
                            stats_list.append([f"{bucket.size}KB",
                                               lg[0],
                                               lg[1].io_count,
                                               _get_latency_stats_line(lg[1].bdev),
                                               _get_latency_stats_line(lg[1].net),
                                               _get_latency_stats_line(lg[1].qos)])
                tbl = tabulate(stats_list,
                               headers=["Bucket\nSize",
                                        "Bucket\nType",
                                        "IOs Count",
                                        "BDEV µSec\n(Min,Max,Mean)",
                                        "Net µSec\n(Min,Max,Mean)",
                                        "QOS µSec\n(Min,Max,Mean)"],
                               tablefmt=table_format)
                out_func(f"{tbl}\n\n"
                         f"Total IOs count: {ret.total_num_ios}")
            else:
                err_func(ret.error_message)
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def connection_reset_io_statistics(self, args):
        """Reset connection's IO statistics."""

        out_func, err_func, _ = self.get_output_functions(args)

        if args.host_nqn == "*":
            self.cli.parser.error("Must specify a specific host NQN")

        req = pb2.get_connection_io_statistics_req(subsystem_nqn=args.subsystem,
                                                   host_nqn=args.host_nqn, reset=True)
        try:
            ret = self.stub.get_connection_io_statistics(req)
        except Exception as ex:
            ret = pb2.connection_io_statistics(status=errno.EINVAL,
                                               error_message=f"Failure resetting host's IO "
                                                             f"statistics:\n{ex}",
                                               subsystem_nqn=args.subsystem,
                                               host_nqn=args.host_nqn)

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Resetting host's {args.host_nqn} in {args.subsystem} "
                         f"IO statistics: Successful")
            else:
                err_func(ret.error_message)
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    connection_list_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=False),
    ]
    get_io_statistics_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
    ]
    reset_io_statistics_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
        argument("--host-nqn",
                 "-t",
                 help="Host NQN",
                 required=True),
    ]
    connection_actions = []
    connection_actions.append({"name": "list",
                               "args": connection_list_args,
                               "help": "List active connections"})
    connection_actions.append({"name": "get_io_statistics",
                               "args": get_io_statistics_args,
                               "help": "Get the IO statistics for a connection"})
    connection_actions.append({"name": "reset_io_statistics",
                               "args": reset_io_statistics_args,
                               "help": "Reset the IO statistics for a connection"})
    connection_choices = get_actions(connection_actions)

    @cli.cmd(connection_actions)
    def connection(self, args):
        """Connection commands"""
        if args.action == "list":
            return self.connection_list(args)
        elif args.action == "get_io_statistics":
            return self.connection_get_io_statistics(args)
        elif args.action == "reset_io_statistics":
            return self.connection_reset_io_statistics(args)
        if not args.action:
            self.cli.parser.error(f"missing action for connection command (choose "
                                  f"from {GatewayClient.connection_choices})")

    def ns_add(self, args):
        """Adds a namespace to a subsystem."""

        img_size = 0
        out_func, err_func, _ = self.get_output_functions(args)
        if args.block_size is None:
            args.block_size = 512
        if args.block_size <= 0:
            self.cli.parser.error("block-size value must be positive")

        if args.load_balancing_group < 0:
            self.cli.parser.error("load-balancing-group value must be positive")
        if args.nsid is not None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.rbd_create_image:
            if args.size is None:
                self.cli.parser.error("--size argument is mandatory for add command when "
                                      "RBD image creation is enabled")
            img_size = self.get_size_in_bytes(args.size)
            if img_size <= 0:
                self.cli.parser.error("size value must be positive")
            mib = 1024 * 1024
            if img_size % mib:
                self.cli.parser.error("size value must be aligned to MiBs")

            if args.encryption_format is not None and len(args.encryption_format) > 1:
                self.cli.parser.error("at most one encryption format can be specified when "
                                      "creating a new image")

        else:
            if args.size is not None:
                self.cli.parser.error("--size argument is not allowed for add command when "
                                      "RBD image creation is disabled")

            if args.encryption_algorithm is not None:
                self.cli.parser.error("--encryption-algorithm argument is not allowed for add "
                                      "command when RBD image creation is disabled")
            if args.rbd_trash_image_on_delete:
                self.cli.parser.error("Can't trash associated RBD image on delete if it wasn't "
                                      "created automatically by the gateway")

        if args.encryption_format is None:
            args.encryption_format = []

        if args.key_id is None:
            args.key_id = []

        if len(args.encryption_format) > 0:
            if not args.key_id:
                self.cli.parser.error("Must have a key ID when using encryption")
            if len(args.encryption_format) != len(args.key_id):
                self.cli.parser.error("The number of key IDs should match the "
                                      "number of encryption formats")
        else:
            if args.encryption_algorithm is not None:
                self.cli.parser.error("Encryption algorithm is only allowed when an encryption "
                                      "format is specified")
            if args.key_id:
                self.cli.parser.error("Key IDs are only valid when an encryption "
                                      "format is specified")

        enc_entries = []
        for i in range(len(args.encryption_format)):
            enc = pb2.encryption_entry(format=args.encryption_format[i],
                                       key_id=args.key_id[i])
            enc_entries.append(enc)

        req = pb2.namespace_add_req(rbd_pool_name=args.rbd_pool,
                                    rbd_image_name=args.rbd_image,
                                    subsystem_nqn=args.subsystem,
                                    nsid=args.nsid,
                                    block_size=args.block_size,
                                    uuid=args.uuid,
                                    anagrpid=args.load_balancing_group,
                                    create_image=args.rbd_create_image,
                                    size=img_size,
                                    force=args.force,
                                    no_auto_visible=args.no_auto_visible,
                                    trash_image=args.rbd_trash_image_on_delete,
                                    disable_auto_resize=args.disable_auto_resize,
                                    read_only=args.read_only,
                                    rbd_data_pool_name=args.rbd_data_pool,
                                    location=args.location,
                                    rados_namespace_name=args.rados_namespace,
                                    encryption_entries=enc_entries,
                                    encryption_algorithm=args.encryption_algorithm)
        try:
            ret = self.stub.namespace_add(req)
        except Exception as ex:
            nsid_msg = ""
            if args.nsid:
                nsid_msg = f"using NSID {args.nsid} "
            errmsg = f"Failure adding namespace {nsid_msg}to {args.subsystem}"
            ret = pb2.req_status(status=errno.EINVAL, error_message=f"{errmsg}:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Adding namespace {ret.nsid} to {args.subsystem}: Successful")
            else:
                err_func(ret.error_message)
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            ret = self.stub.namespace_delete(pb2.namespace_delete_req(
                subsystem_nqn=args.subsystem, nsid=args.nsid, i_am_sure=args.i_am_sure))
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure deleting namespace:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Deleting namespace {args.nsid} from {args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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
        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        ns_size = self.get_size_in_bytes(args.size)
        if ns_size <= 0:
            self.cli.parser.error("size value must be positive")
        mib = 1024 * 1024
        if ns_size % mib:
            self.cli.parser.error("size value must be aligned to MiBs")
        ns_size //= mib

        try:
            ret = self.stub.namespace_resize(pb2.namespace_resize_req(
                subsystem_nqn=args.subsystem, nsid=args.nsid, new_size=ns_size))
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure resizing namespace:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                sz_str = self.format_size(ns_size * mib)
                out_func(f"Resizing namespace {args.nsid} in {args.subsystem} to "
                         f"{sz_str}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def format_size(self, sz):
        units = ["Bytes"] + GatewayClient.SIZE_UNITS
        for unit_index in range(len(units)):
            if sz < 1024:
                break
            sz /= 1024.0
        unit = f"{units[unit_index]}iB" if unit_index > 0 else f"{units[unit_index]}"
        if sz == int(sz):
            return f"{int(sz)} {unit}"
        return f"{sz:2.1f} {unit}"

    def get_size_in_bytes(self, sz):
        multiply = 1
        sz = sz.strip()
        try:
            int_size = int(sz)
            sz += "MB"      # If no unit is specified assume MB
        except Exception:
            pass

        found = False
        for unit_index in range(len(GatewayClient.SIZE_UNITS)):
            if sz.endswith(GatewayClient.SIZE_UNITS[unit_index]):
                sz = sz[:-1]
                found = True
            elif sz.endswith(GatewayClient.SIZE_UNITS[unit_index] + "B"):
                sz = sz[:-2]
                found = True
            if found:
                multiply = 1024 ** (unit_index + 1)
                break

        if not found and sz.endswith("B"):
            sz = sz[:-1]

        try:
            sz = sz.strip()
            int_size = int(sz)
        except Exception:
            self.cli.parser.error(f"Size {sz} must be numeric")

        int_size *= multiply
        return int_size

    def ns_location_list(self, args, ns_info):
        """List namespace distribution per site locations"""

        out_func, _, _ = self.get_output_functions(args)
        ns_locations_count = {}
        if ns_info.namespaces:
            for ns in ns_info.namespaces:
                nqn = ns.ns_subsystem_nqn
                if not nqn:
                    self.cli.parser.error("Found a namespace without a subsystem NQN")
                lb = ns.load_balancing_group
                if not lb:
                    lb = 0
                loc = ns.location
                if not loc:
                    loc = "<default>"
                if nqn not in ns_locations_count:
                    ns_locations_count[nqn] = {}
                if lb not in ns_locations_count[nqn]:
                    ns_locations_count[nqn][lb] = {}
                if loc not in ns_locations_count[nqn][lb]:
                    ns_locations_count[nqn][lb][loc] = 0
                ns_locations_count[nqn][lb][loc] += 1

        loc_list = []
        for nqn in sorted(ns_locations_count.keys()):
            first_nqn = True
            for lb in sorted(ns_locations_count[nqn].keys()):
                for loc in sorted(ns_locations_count[nqn][lb].keys()):
                    nqnstr = nqn if first_nqn else ""
                    first_nqn = False
                    lbstr = str(lb) if lb else "<n/a>"
                    loc_list.append([nqnstr, lbstr, loc, ns_locations_count[nqn][lb][loc]])

        if len(loc_list) > 0:
            table_format = "fancy_grid" if args.format == "text" else "plain"
            headers = ["Subsystem",
                       "Load\nBalancing\nGroup",
                       "Location",
                       "Count"]
            locations_out = tabulate(loc_list,
                                     headers=headers,
                                     tablefmt=table_format)

            if args.nsid:
                prefix = f"Namespace {args.nsid} in"
            elif args.uuid:
                prefix = f"Namespace with UUID {args.uuid} in"
            else:
                prefix = "Namespaces in"
            if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                out_func(f"{prefix} all subsystems:\n{locations_out}")
            else:
                out_func(f"{prefix} subsystem {args.subsystem}:\n{locations_out}")
        else:
            if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                out_func("No namespaces in any subsystem")
            else:
                out_func(f"No namespaces in subsystem {args.subsystem}")
        return 0

    def ns_list(self, args, show_hosts=False, show_ns_locations=False):
        """Lists namespaces on a subsystem."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid is not None and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        if show_hosts and show_ns_locations:
            self.cli.parser.error("Can't list hosts and locations at once")

        if not args.subsystem:
            args.subsystem = GatewayUtils.ALL_SUBSYSTEMS

        try:
            namespaces_info = self.stub.list_namespaces(pb2.list_namespaces_req(
                subsystem=args.subsystem,
                nsid=args.nsid, uuid=args.uuid))
        except Exception as ex:
            namespaces_info = pb2.namespaces_info(
                status=errno.EINVAL,
                error_message=f"Failure listing namespaces:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if namespaces_info.status == 0:
                if args.subsystem != GatewayUtils.ALL_SUBSYSTEMS:
                    if args.nsid and len(namespaces_info.namespaces) > 1:
                        err_func(f"Got more than one namespace for namespace ID {args.nsid}")
                    if args.uuid and len(namespaces_info.namespaces) > 1:
                        err_func(f"Got more than one namespace for UUID {args.uuid}")
                    if namespaces_info.subsystem_nqn != args.subsystem:
                        err_func(f"Got namespaces in subsystem "
                                 f"{namespaces_info.subsystem_nqn} which is different than the "
                                 f"requested subsystem {args.subsystem}")
                        return errno.ENODEV
                if show_ns_locations:
                    return self.ns_location_list(args, namespaces_info)
                namespaces_list = []
                for ns in namespaces_info.namespaces:
                    ns_host_list = ns.hosts if show_hosts else []
                    if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                        if not ns.ns_subsystem_nqn:
                            err_func(f"Got namespace with ID {ns.nsid} on an unknown subsystem")
                            subsys_nqn = "<n/a>"
                        else:
                            subsys_nqn = ns.ns_subsystem_nqn
                    else:
                        if ns.ns_subsystem_nqn and ns.ns_subsystem_nqn != args.subsystem:
                            err_func(f"Got a namespace with ID {ns.nsid} in subsystem "
                                     f"{ns.ns_subsystem_nqn} which is different than the "
                                     f"requested one {args.subsystem}")
                            return errno.ENODEV
                        subsys_nqn = namespaces_info.subsystem_nqn

                    if args.nsid and args.nsid != ns.nsid:
                        err_func(f"Failure listing namespace {args.nsid}: "
                                 f"Got namespace {ns.nsid} instead")
                        return errno.ENODEV
                    if args.uuid and args.uuid != ns.uuid:
                        err_func(f"Failure listing namespace with UUID {args.uuid}: "
                                 f"Got namespace {ns.uuid} instead")
                        return errno.ENODEV
                    if not ns.load_balancing_group:
                        lb_group = "<n/a>"
                    else:
                        lb_group = str(ns.load_balancing_group)
                    if not ns.configured_load_balancing_group:
                        configured_lb_group = "<n/a>"
                    else:
                        configured_lb_group = str(ns.configured_load_balancing_group)
                        if configured_lb_group != lb_group and args.output != "stdio":
                            configured_lb_group = "\x1b[7m" + configured_lb_group + "\x1b[27m"
                    cluster_name = "<n/a>" if not ns.cluster_name else ns.cluster_name
                    if ns.auto_visible:
                        visibility = "All Hosts"
                    else:
                        if len(ns_host_list) > 0:
                            visibility = ""
                            relied_on_open = ""
                            for hst in ns_host_list:
                                if hst == "*":
                                    relied_on_open = "\nOne of the hosts relied on the " \
                                                     "subsystem being open for all hosts"
                                else:
                                    visibility += hst + "\n"
                            if relied_on_open:
                                visibility += relied_on_open
                        else:
                            visibility = "None" if show_hosts else "Restrictive"

                    encryption = ""
                    all_none = True
                    for ent in ns.encryption_entries:
                        enc_for = GatewayEnumUtils.get_key_from_value(pb2.EncryptionFormat,
                                                                      ent.format)
                        if enc_for is None:
                            enc_for = f"<unknown {ent.format}>"
                        encryption += f"{enc_for.upper()}\n"
                        if ent.format != pb2.EncryptionFormat.none:
                            all_none = False
                    if not encryption or all_none:
                        encryption = "None"
                    ro_msg = "Read-Only" if ns.read_only else "Read-Write"
                    trash_msg = "\nTrash on delete" if ns.trash_image else ""
                    auto_resize_msg = "\nDisable auto resize" if ns.disable_auto_resize else ""
                    data_pool_msg = ""
                    if ns.rbd_data_pool_name:
                        data_pool_msg = f"\nData pool {ns.rbd_data_pool_name}"
                    img_path = f"{ns.rbd_pool_name}/{ns.rados_namespace_name}/{ns.rbd_image_name}" \
                        if ns.rados_namespace_name \
                        else f"{ns.rbd_pool_name}/{ns.rbd_image_name}"
                    verbose_info = []
                    if args.verbose:
                        verbose_info = [cluster_name]
                        lb_group += f" ({configured_lb_group})"
                    location = ns.location if ns.location else "<default>"
                    qos_str = f"{self.get_qos_limit_str_value(ns.rw_mbytes_per_second)}\n" \
                              f"{self.get_qos_limit_str_value(ns.r_mbytes_per_second)}\n" \
                              f"{self.get_qos_limit_str_value(ns.w_mbytes_per_second)}"
                    if show_hosts:
                        namespaces_list.append([subsys_nqn,
                                                ns.nsid,
                                                visibility])
                    else:
                        namespaces_list.append([subsys_nqn,
                                                ns.nsid,
                                                break_string(ns.bdev_name, "-", 2),
                                                f"{img_path}{data_pool_msg}",
                                                f"{ro_msg}{trash_msg}{auto_resize_msg}",
                                                self.format_size(ns.rbd_image_size),
                                                self.format_size(ns.block_size),
                                                break_string(ns.uuid, "-", 3),
                                                lb_group,
                                                location,
                                                visibility,
                                                encryption,
                                                self.get_qos_limit_str_value(ns.rw_ios_per_second),
                                                qos_str] + verbose_info)

                if len(namespaces_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    verbose_headers = []
                    configured_txt = ""
                    if args.verbose:
                        verbose_headers = ["Cluster\nName"]
                        configured_txt = "\n(Configured)"
                    if show_hosts:
                        headers = ["NQN",
                                   "NSID",
                                   "Hosts"]
                    else:
                        headers = ["NQN",
                                   "NSID",
                                   "Bdev\nName",
                                   "RBD\nImage",
                                   "Mode",
                                   "Image\nSize",
                                   "Block\nSize",
                                   "UUID",
                                   "Load\nBalancing\nGroup" + configured_txt,
                                   "Location",
                                   "Visibility",
                                   "Encryption",
                                   "IOs per\nsecond",
                                   "R/W, R, W MBs\n"
                                   "per second"] + verbose_headers
                    namespaces_out = tabulate(namespaces_list,
                                              headers=headers,
                                              tablefmt=table_format)
                    if args.nsid:
                        prefix = f"Namespace {args.nsid} in"
                    elif args.uuid:
                        prefix = f"Namespace with UUID {args.uuid} in"
                    else:
                        prefix = "Namespaces in"
                    if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                        out_func(f"{prefix} all subsystems:\n{namespaces_out}")
                    else:
                        out_func(f"{prefix} subsystem {args.subsystem}:\n{namespaces_out}")
                else:
                    if args.nsid:
                        if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                            out_func(f"No namespace {args.nsid} in any subsystem")
                        else:
                            out_func(f"No namespace {args.nsid} in subsystem {args.subsystem}")
                    elif args.uuid:
                        if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                            out_func(f"No namespace with UUID {args.uuid} in any subsystem")
                        else:
                            out_func(f"No namespace with UUID {args.uuid} in subsystem "
                                     f"{args.subsystem}")
                    else:
                        if args.subsystem == GatewayUtils.ALL_SUBSYSTEMS:
                            out_func("No namespaces in any subsystem")
                        else:
                            out_func(f"No namespaces in subsystem {args.subsystem}")
            else:
                err_func(f"{namespaces_info.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(namespaces_info, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return namespaces_info
        else:
            assert False

        return namespaces_info.status

    def ns_get_io_stats(self, args):
        """Get namespaces IO statistics."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid and args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            ns_list_io_stats_req = pb2.list_namespaces_io_stats_req(subsystem_nqn=args.subsystem,
                                                                    nsid=args.nsid)
            ns_io_stats = self.stub.list_namespaces_io_stats(ns_list_io_stats_req)
        except Exception as ex:
            ns_io_stats = pb2.list_namespaces_io_stats_info(
                status=errno.EINVAL,
                error_message=f"Failure getting namespaces IO stats:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ns_io_stats.status == 0:
                ns_iostat_list = []
                for ns_iostat in ns_io_stats.namespaces:
                    ns_iostat_list.append([
                        ns_iostat.bdev_name,
                        ns_iostat.bytes_read,
                        ns_iostat.num_read_ops,
                        ns_iostat.bytes_written,
                        ns_iostat.num_write_ops,
                        ns_iostat.bytes_unmapped,
                        ns_iostat.num_unmap_ops,
                        ns_iostat.bytes_copied,
                        ns_iostat.num_copy_ops,
                        ns_iostat.read_latency_ticks,
                        ns_iostat.max_read_latency_ticks,
                        ns_iostat.min_read_latency_ticks,
                        ns_iostat.write_latency_ticks,
                        ns_iostat.max_write_latency_ticks,
                        ns_iostat.min_write_latency_ticks,
                        ns_iostat.unmap_latency_ticks,
                        ns_iostat.max_unmap_latency_ticks,
                        ns_iostat.min_unmap_latency_ticks,
                        ns_iostat.copy_latency_ticks,
                        ns_iostat.max_copy_latency_ticks,
                        ns_iostat.min_copy_latency_ticks,
                    ])
                if len(ns_iostat_list) > 0:
                    table_format = "fancy_grid" if args.format == "text" else "plain"
                    ns_iostat_out = tabulate(ns_iostat_list,
                                             headers=[
                                                 "Name",
                                                 "Bytes Read",
                                                 "Num Read Ops",
                                                 "Bytes Written",
                                                 "Num Write Ops",
                                                 "Bytes Unmapped",
                                                 "Num Unmap Ops",
                                                 "Bytes Copied",
                                                 "Num Copy Ops",
                                                 "Read Latency Ticks",
                                                 "Max Read Latency Ticks",
                                                 "Min Read Latency Ticks",
                                                 "Write Latency Ticks",
                                                 "Max Write Latency Ticks",
                                                 "Min Write Latency Ticks",
                                                 "Unmap Latency Ticks",
                                                 "Max Unmap Latency Ticks",
                                                 "Min Unmap Latency Ticks",
                                                 "Copy Latency Ticks",
                                                 "Max Copy Latency Ticks",
                                                 "Min Copy Latency Ticks",
                                             ],
                                             tablefmt=table_format)
                    tick_rate = ns_io_stats.tick_rate
                    ticks = ns_io_stats.ticks
                    if args.nsid and args.subsystem:
                        ns_str = f"IO statistics for namespace {args.nsid} in {args.subsystem}"
                    else:
                        ns_str = "IO statistics for all namespaces"
                    out_func(f"{ns_str}; tick rate={tick_rate} and ticks={ticks}:\n{ns_iostat_out}")
                else:
                    out_func("No namespaces found")
            else:
                err_func(f"{ns_io_stats.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ns_io_stats, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.load_balancing_group <= 0:
            self.cli.parser.error("load-balancing-group value must be positive")

        try:
            change_lb_group_req = pb2.namespace_change_load_balancing_group_req(
                subsystem_nqn=args.subsystem,
                nsid=args.nsid,
                anagrpid=args.load_balancing_group)
            ret = self.stub.namespace_change_load_balancing_group(change_lb_group_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure changing namespace load "
                                               f"balancing group:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Changing load balancing group of namespace {args.nsid} in "
                         f"{args.subsystem} to {args.load_balancing_group}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
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
            return "unset"
        else:
            return str(qos_limit)

    def ns_set_qos(self, args):
        """Set namespace QOS limits."""

        out_func, err_func, wrn_func = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")
        if args.rw_ios_per_second is None:
            if args.rw_megabytes_per_second is None:
                if args.r_megabytes_per_second is None:
                    if args.w_megabytes_per_second is None:
                        self.cli.parser.error("At least one QOS limit should be set")

        if args.format == "text" or args.format == "plain":
            if args.rw_ios_per_second and (args.rw_ios_per_second % 1000) != 0:
                rounded_rate = int((args.rw_ios_per_second + 1000) / 1000) * 1000
                wrn_func(f"IOs per second {args.rw_ios_per_second} will be "
                         f"rounded up to {rounded_rate}")
            if args.rw_megabytes_per_second:
                if args.rw_megabytes_per_second > GatewayClient.MAX_MB_PER_SECOND:
                    wrn_func(f"Read/Write megabytes per second {args.rw_megabytes_per_second} "
                             f"is too big, it will be truncated to "
                             f"{GatewayClient.MAX_MB_PER_SECOND}")
            if args.r_megabytes_per_second:
                if args.r_megabytes_per_second > GatewayClient.MAX_MB_PER_SECOND:
                    wrn_func(f"Read megabytes per second {args.r_megabytes_per_second} "
                             f"is too big, it will be truncated to "
                             f"{GatewayClient.MAX_MB_PER_SECOND}")
            if args.w_megabytes_per_second:
                if args.w_megabytes_per_second > GatewayClient.MAX_MB_PER_SECOND:
                    wrn_func(f"Write megabytes per second {args.w_megabytes_per_second} "
                             f"is too big, it will be truncated to "
                             f"{GatewayClient.MAX_MB_PER_SECOND}")

        qos_args = {}
        qos_args["subsystem_nqn"] = args.subsystem
        if args.nsid:
            qos_args["nsid"] = args.nsid
        if args.rw_ios_per_second is not None:
            qos_args["rw_ios_per_second"] = args.rw_ios_per_second
        if args.rw_megabytes_per_second is not None:
            qos_args["rw_mbytes_per_second"] = args.rw_megabytes_per_second
        if args.r_megabytes_per_second is not None:
            qos_args["r_mbytes_per_second"] = args.r_megabytes_per_second
        if args.w_megabytes_per_second is not None:
            qos_args["w_mbytes_per_second"] = args.w_megabytes_per_second
        if args.force:
            qos_args["force"] = args.force
        try:
            set_qos_req = pb2.namespace_set_qos_req(**qos_args)
            ret = self.stub.namespace_set_qos_limits(set_qos_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure setting namespaces QOS limits:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Setting QOS limits of namespace {args.nsid} in "
                         f"{args.subsystem}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_add_host(self, args):
        """Adds a host to a namespace."""

        rc = 0
        ret_list = []
        out_func, err_func, _ = self.get_output_functions(args)

        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        for one_host_nqn in args.host_nqn:
            try:
                add_host_req = pb2.namespace_add_host_req(subsystem_nqn=args.subsystem,
                                                          nsid=args.nsid,
                                                          host_nqn=one_host_nqn,
                                                          force=args.force)
                ret = self.stub.namespace_add_host(add_host_req)
            except Exception as ex:
                ret = pb2.req_status(status=errno.EINVAL,
                                     error_message=f"Failure adding host to namespace:\n{ex}")

            if not rc:
                rc = ret.status

            if args.format == "text" or args.format == "plain":
                if ret.status == 0:
                    out_func(f"Adding host {one_host_nqn} to namespace {args.nsid} on "
                             f"{args.subsystem}: Successful")
                else:
                    err_func(f"{ret.error_message}")
            elif args.format == "json" or args.format == "yaml":
                ret_str = json_format.MessageToJson(ret, indent=4,
                                                    including_default_value_fields=True,
                                                    preserving_proto_field_name=True)
                if args.format == "json":
                    out_func(ret_str)
                elif args.format == "yaml":
                    obj = json.loads(ret_str)
                    out_func(yaml.dump(obj))
            elif args.format == "python":
                ret_list.append(ret)
            else:
                assert False

        if args.format == "python":
            return ret_list

        return rc

    def ns_del_host(self, args):
        """Deletes a host from a namespace."""

        rc = 0
        ret_list = []
        out_func, err_func, _ = self.get_output_functions(args)

        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        for one_host_nqn in args.host_nqn:
            try:
                del_host_req = pb2.namespace_delete_host_req(subsystem_nqn=args.subsystem,
                                                             nsid=args.nsid,
                                                             host_nqn=one_host_nqn)
                ret = self.stub.namespace_delete_host(del_host_req)
            except Exception as ex:
                ret = pb2.req_status(status=errno.EINVAL,
                                     error_message=f"Failure deleting host from namespace:\n{ex}")

            if not rc:
                rc = ret.status

            if args.format == "text" or args.format == "plain":
                if ret.status == 0:
                    out_func(f"Deleting host {one_host_nqn} from namespace {args.nsid} "
                             f"on {args.subsystem}: Successful")
                else:
                    err_func(f"{ret.error_message}")
            elif args.format == "json" or args.format == "yaml":
                ret_str = json_format.MessageToJson(ret, indent=4,
                                                    including_default_value_fields=True,
                                                    preserving_proto_field_name=True)
                if args.format == "json":
                    out_func(ret_str)
                elif args.format == "yaml":
                    obj = json.loads(ret_str)
                    out_func(yaml.dump(obj))
            elif args.format == "python":
                ret_list.append(ret)
            else:
                assert False

        if args.format == "python":
            return ret_list

        return rc

    def ns_change_visibility(self, args):
        """Change namespace visibility."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        auto_visible = GatewayClient.parse_boolean(args.auto_visible)
        try:
            change_visibility_req = pb2.namespace_change_visibility_req(
                subsystem_nqn=args.subsystem,
                nsid=args.nsid,
                auto_visible=auto_visible,
                force=args.force)
            ret = self.stub.namespace_change_visibility(change_visibility_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure changing namespace visibility:\n{ex}")

        if auto_visible:
            vis_text = "\"visible to all hosts\""
        else:
            vis_text = "\"visible to selected hosts\""
        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Changing visibility of namespace {args.nsid} in {args.subsystem} "
                         f"to {vis_text}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_change_location(self, args):
        """Change namespace location."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            change_location_req = pb2.namespace_change_location_req(
                subsystem_nqn=args.subsystem,
                nsid=args.nsid,
                location=args.location)
            ret = self.stub.namespace_change_location(change_location_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure setting namespace location "
                                               f"to \"{args.location}\":\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                if not args.location:
                    args.location = ""
                out_func(f"Setting location for namespace {args.nsid} in {args.subsystem} "
                         f"to \"{args.location}\": Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_set_rbd_trash_image(self, args):
        """Change RBD trash image flag for a namespace."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        trash_image = GatewayClient.parse_boolean(args.rbd_trash_image_on_delete)
        try:
            set_trash_image_req = pb2.namespace_set_rbd_trash_image_req(
                subsystem_nqn=args.subsystem,
                nsid=args.nsid, trash_image=trash_image)
            ret = self.stub.namespace_set_rbd_trash_image(set_trash_image_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure setting namespace RBD trash image:\n{ex}")

        trash_text = "trash on namespace deletion\""
        if not trash_image:
            trash_text = "do not " + trash_text
        trash_text = "\"" + trash_text

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Setting RBD trash image flag for namespace {args.nsid} in "
                         f"{args.subsystem} to {trash_text}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_set_auto_resize(self, args):
        """Enable or disable namespace auto resize flag."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        auto_resize = GatewayClient.parse_boolean(args.auto_resize_enabled)
        try:
            set_auto_resize_req = pb2.namespace_set_auto_resize_req(
                subsystem_nqn=args.subsystem,
                nsid=args.nsid, auto_resize=auto_resize)
            ret = self.stub.namespace_set_auto_resize(set_auto_resize_req)
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure setting namespace auto resize flag:\n{ex}")

        auto_resize_text = "auto resize namespace\""
        if not auto_resize:
            auto_resize_text = "do not " + auto_resize_text
        auto_resize_text = "\"" + auto_resize_text

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Setting auto resize flag for namespace {args.nsid} in "
                         f"{args.subsystem} to {auto_resize_text}: Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    def ns_refresh_size(self, args):
        """Refresh namespace size to current RBD image size."""

        out_func, err_func, _ = self.get_output_functions(args)
        if args.nsid <= 0:
            self.cli.parser.error("nsid value must be positive")

        try:
            ret = self.stub.namespace_resize(pb2.namespace_resize_req(
                subsystem_nqn=args.subsystem, nsid=args.nsid, new_size=0))
        except Exception as ex:
            ret = pb2.req_status(status=errno.EINVAL,
                                 error_message=f"Failure refreshing namespace size:\n{ex}")

        if args.format == "text" or args.format == "plain":
            if ret.status == 0:
                out_func(f"Refreshing size for namespace {args.nsid} in {args.subsystem} :"
                         f" Successful")
            else:
                err_func(f"{ret.error_message}")
        elif args.format == "json" or args.format == "yaml":
            ret_str = json_format.MessageToJson(ret, indent=4,
                                                including_default_value_fields=True,
                                                preserving_proto_field_name=True)
            if args.format == "json":
                out_func(ret_str)
            elif args.format == "yaml":
                obj = json.loads(ret_str)
                out_func(yaml.dump(obj))
        elif args.format == "python":
            return ret
        else:
            assert False

        return ret.status

    ns_common_args = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=True),
    ]
    ns_add_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int),
        argument("--uuid",
                 "-u",
                 help="UUID"),
        argument("--rbd-pool",
                 "-p",
                 help="RBD pool name",
                 required=True),
        argument("--rbd-data-pool",
                 "-d",
                 help="RBD data pool name (only required for erasure pools)",
                 required=False),
        argument("--rbd-image",
                 "-i",
                 help="RBD image name",
                 required=True),
        argument("--rados-namespace",
                 "-r",
                 help="RADOS namespace name",
                 required=False),
        argument("--rbd-create-image",
                 "-c",
                 help="Create RBD image if needed",
                 action='store_true',
                 required=False),
        argument("--block-size",
                 "-s",
                 help="Block size",
                 type=int),
        argument("--load-balancing-group",
                 "-l",
                 help="Load balancing group",
                 type=int,
                 default=0),
        argument("--size",
                 help="Size in bytes or specified unit (K, KB, M, MB, G, GB, T, TB, P, PB)"),
        argument("--force",
                 help="Create a namespace even when its image is already used by another namespace",
                 action='store_true',
                 required=False),
        argument("--no-auto-visible",
                 help="Make the namespace visible only to specific hosts",
                 action='store_true',
                 required=False),
        argument("--rbd-trash-image-on-delete",
                 help="Trash associated RBD image on namespace deletion. "
                      "Only applies to images created automatically by the gateway",
                 action='store_true',
                 required=False),
        argument("--disable-auto-resize",
                 help="When the RBD image is resized, do not automatically resize the namespace",
                 action='store_true',
                 required=False),
        argument("--read-only",
                 help="Open the namespace in read-only mode",
                 action='store_true',
                 required=False),
        argument("--location",
                 help="Namespace's location",
                 required=False),
        argument("--encryption-format",
                 "-f",
                 nargs="+",
                 help="Encryption formats to use, LUKS1 or LUKS2",
                 type=str.lower,
                 choices=get_enum_keys_list(pb2.EncryptionFormat, False),
                 required=False),
        argument("--encryption-algorithm",
                 "-g",
                 help="Algorithm to use for encryption",
                 type=str.lower,
                 choices=get_enum_keys_list(pb2.EncryptionAlgorithm, False),
                 required=False),
        argument("--key-id",
                 help="Key ID(s) to use for encryption pass phrases",
                 nargs="+",
                 required=False),
    ]
    ns_del_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--i-am-sure",
                 help="Confirmation for deleting the namespace associated RBD image",
                 action='store_true',
                 required=False),
    ]
    ns_resize_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--size",
                 help="Size in bytes or specified unit (K, KB, M, MB, G, GB, T, TB, P, PB)",
                 required=True),
    ]
    ns_list_args_list = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=False),
        argument("--nsid",
                 help="Namespace ID",
                 type=int),
        argument("--uuid",
                 "-u",
                 help="UUID"),
    ]
    ns_get_io_stats_args_list = [
        argument("--subsystem",
                 "-n",
                 help="Subsystem NQN",
                 required=False),
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=False),
    ]
    ns_change_load_balancing_group_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--load-balancing-group",
                 "-l",
                 help="Load balancing group",
                 type=int,
                 required=True),
    ]
    ns_change_visibility_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--auto-visible",
                 help="Visible to all hosts if yes, otherwise visible to selected hosts only",
                 type=str.lower,
                 choices=["yes", "no", "true", "false", "1", "0"],
                 required=True),
        argument("--force",
                 help="Change visibility of namespace even if there are hosts added "
                      "to it or active connections on the subsystem",
                 action='store_true',
                 required=False),
    ]
    ns_change_location_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--location",
                 help="Namespace's location",
                 required=False),
    ]
    ns_set_auto_resize_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--auto-resize-enabled",
                 help="Enable or disable auto resize of namespace when RBD image is resized",
                 type=str.lower,
                 choices=["yes", "no", "true", "false", "1", "0"],
                 required=True),
    ]
    ns_refresh_size_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
    ]
    ns_set_qos_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--rw-ios-per-second",
                 help="R/W IOs per second limit, 0 means unlimited",
                 type=int),
        argument("--rw-megabytes-per-second",
                 help="R/W megabytes per second limit, 0 means unlimited",
                 type=int),
        argument("--r-megabytes-per-second",
                 help="Read megabytes per second limit, 0 means unlimited",
                 type=int),
        argument("--w-megabytes-per-second",
                 help="Write megabytes per second limit, 0 means unlimited",
                 type=int),
        argument("--force",
                 help="Set QOS limits even if they were changed by RBD",
                 action='store_true',
                 required=False),
    ]
    ns_add_host_args_list = ns_common_args + [
        argument("--nsid", help="Namespace ID", type=int, required=True),
        argument("--host-nqn", "-t", help="Host NQN list", nargs="+", required=True),
        argument("--force",
                 help="Allow adding the host to the namespace even if the host "
                      "has no access to the subsystem",
                 action='store_true', required=False),
    ]
    ns_list_hosts_args_list = [
        argument("--subsystem", "-n", help="Subsystem NQN"),
        argument("--nsid", help="Namespace ID", type=int),
        argument("--uuid", "-u", help="UUID"),
    ]
    ns_list_locations_args_list = [
        argument("--subsystem", "-n", help="Subsystem NQN"),
        argument("--nsid", help="Namespace ID", type=int),
        argument("--uuid", "-u", help="UUID"),
    ]
    ns_del_host_args_list = ns_common_args + [
        argument("--nsid", help="Namespace ID", type=int, required=True),
        argument("--host-nqn", "-t", help="Host NQN list", nargs="+", required=True),
    ]
    ns_set_rbd_trash_image_args_list = ns_common_args + [
        argument("--nsid",
                 help="Namespace ID",
                 type=int,
                 required=True),
        argument("--rbd-trash-image-on-delete",
                 help="When deleting the namespace, trash associated RBD image. "
                      "Only applies to images created automatically by the gateway",
                 type=str.lower,
                 choices=["yes", "no", "true", "false", "1", "0"],
                 required=True),
    ]
    ns_actions = []
    ns_actions.append({"name": "add",
                       "args": ns_add_args_list,
                       "help": "Create a namespace"})
    ns_actions.append({"name": "del",
                       "args": ns_del_args_list,
                       "help": "Delete a namespace"})
    ns_actions.append({"name": "resize",
                       "args": ns_resize_args_list,
                       "help": "Resize a namespace"})
    ns_actions.append({"name": "list",
                       "args": ns_list_args_list,
                       "help": "List namespaces"})
    ns_actions.append({"name": "get_io_stats",
                       "args": ns_get_io_stats_args_list,
                       "help": "Get I/O stats for namespaces"})
    ns_actions.append({"name": "change_load_balancing_group",
                       "args": ns_change_load_balancing_group_args_list,
                       "help": "Change load balancing group for a namespace"})
    ns_actions.append({"name": "set_qos",
                       "args": ns_set_qos_args_list,
                       "help": "Set QOS limits for a namespace"})
    ns_actions.append({"name": "add_host",
                       "args": ns_add_host_args_list,
                       "help": "Add a host to a namespace"})
    ns_actions.append({"name": "del_host",
                       "args": ns_del_host_args_list,
                       "help": "Delete a host from a namespace"})
    ns_actions.append({"name": "list_hosts",
                       "args": ns_list_hosts_args_list,
                       "help": "List namespace's hosts"})
    ns_actions.append({"name": "list_locations",
                       "args": ns_list_locations_args_list,
                       "help": "List namespace distribution by site location"})
    ns_actions.append({"name": "change_visibility",
                       "args": ns_change_visibility_args_list,
                       "help": "Change visibility for a namespace"})
    ns_actions.append({"name": "change_location",
                       "args": ns_change_location_args_list,
                       "help": "Change location for a namespace"})
    ns_actions.append({"name": "set_rbd_trash_image",
                       "args": ns_set_rbd_trash_image_args_list,
                       "help": "Set the RBD trash image on delete flag for a namespace"})
    ns_actions.append({"name": "set_auto_resize",
                       "args": ns_set_auto_resize_args_list,
                       "help": "Enable or disable namespace auto resize when RBD image is resized"})
    ns_actions.append({"name": "refresh_size",
                       "args": ns_refresh_size_args_list,
                       "help": "Refresh namespace size to the current RBD image size"})
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
            return self.ns_list(args, False, False)
        elif args.action == "get_io_stats":
            return self.ns_get_io_stats(args)
        elif args.action == "change_load_balancing_group":
            return self.ns_change_load_balancing_group(args)
        elif args.action == "set_qos":
            return self.ns_set_qos(args)
        elif args.action == "add_host":
            return self.ns_add_host(args)
        elif args.action == "del_host":
            return self.ns_del_host(args)
        elif args.action == "list_hosts":
            return self.ns_list(args, True, False)
        elif args.action == "list_locations":
            return self.ns_list(args, False, True)
        elif args.action == "change_visibility":
            return self.ns_change_visibility(args)
        elif args.action == "change_location":
            return self.ns_change_location(args)
        elif args.action == "set_rbd_trash_image":
            return self.ns_set_rbd_trash_image(args)
        elif args.action == "set_auto_resize":
            return self.ns_set_auto_resize(args)
        elif args.action == "refresh_size":
            return self.ns_refresh_size(args)
        if not args.action:
            self.cli.parser.error(f"missing action for namespace command "
                                  f"(choose from {GatewayClient.ns_choices})")

    @cli.cmd()
    def get_subsystems(self, args):
        """Get subsystems"""
        out_func, err_func, _ = self.get_output_functions(args)

        subsystems = self.stub.get_subsystems(pb2.get_subsystems_req())
        if args.format == "python":
            return subsystems
        subsystems_out = json_format.MessageToJson(subsystems,
                                                   indent=4, including_default_value_fields=True,
                                                   preserving_proto_field_name=True)
        out_func(f"Get subsystems:\n{subsystems_out}")


def main_common(client, args):
    client.logger.setLevel(GatewayEnumUtils.get_value_from_key(pb2.GwLogLevel,
                                                               args.log_level.lower()))
    server_address = args.server_address
    server_port = args.server_port
    client_key = args.client_key
    client_cert = args.client_cert
    server_cert = args.server_cert
    msg_len = args.max_message_length
    client.connect(args, server_address, server_port, client_key, client_cert, server_cert, msg_len)
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
