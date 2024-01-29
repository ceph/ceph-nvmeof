#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: congmin.yin@intel.com
#

import argparse
import grpc
import json
import logging
from .config import GatewayConfig
from .state import GatewayState, LocalGatewayState, OmapGatewayState, GatewayStateHandler
from .utils import GatewayLogger
from .proto import gateway_pb2 as pb2

import rados
from typing import Dict, Optional

import socket
import threading
import time
import enum
import uuid
import struct
import selectors
from dataclasses import dataclass, field
from ctypes import Structure, LittleEndianStructure, c_bool, c_ubyte, c_uint8, c_uint16, c_uint32, c_uint64, c_float

# NVMe tcp pdu type
class NVME_TCP_PDU(enum.IntFlag):
    ICREQ = 0x0
    ICRESP = 0x1
    H2C_TERM = 0x2
    C2H_TERM = 0x3
    CMD = 0x4
    RSP = 0x5
    H2C_DATA = 0x6
    C2H_DATA = 0x7
    TCP_R2T = 0x9

# NVMe tcp opcode
class NVME_TCP_OPC(enum.IntFlag):
    DELETE_SQ = 0x0
    CREATE_SQ = 0x1
    GET_LOG_PAGE = 0x2
    DELETE_CQ = 0x4
    CREATE_CQ = 0x5
    IDENTIFY = 0x6
    ABORT = 0x8
    SET_FEATURES = 0x9
    GET_FEATURES = 0xa
    ASYNC_EVE_REQ = 0xc
    NS_MGMT = 0xd
    FW_COMMIT = 0x10
    FW_IMG_DOWNLOAD = 0x11
    NS_ATTACH = 0x15
    KEEP_ALIVE = 0x18
    FABRIC_TYPE = 0x7F

# NVMe tcp fabric command type
class NVME_TCP_FCTYPE(enum.IntFlag):
    PROP_SET = 0x0
    CONNECT = 0x1
    PROP_GET = 0x4
    AUTH_SEND = 0x5
    AUTH_RECV = 0x6
    DISCONNECT = 0x8

# NVMe controller register space offsets
class NVME_CTL(enum.IntFlag):
    CAPABILITIES = 0x0
    VERSION = 0x08
    CONFIGURATION = 0x14
    STATUS = 0x1c


# NVM subsystem types
class NVMF_SUBTYPE(enum.IntFlag):
    # Discovery type for NVM subsystem
    DISCOVERY = 0x1
    # NVMe type for NVM subsystem
    NVME = 0x2

# NVMe over Fabrics transport types
class TRANSPORT_TYPES(enum.IntFlag):
    RDMA = 0x1
    FC = 0x2
    TCP = 0x3
    INTRA_HOST = 0xfe

# Address family types
class ADRFAM_TYPES(enum.IntFlag):
    ipv4 = 0x1
    ipv6 = 0x2
    ib = 0x3
    fc = 0x4
    intra_host = 0xfe

# Transport requirement, secure channel requirements
# Connections shall be made over a fabric secure channel
class NVMF_TREQ_SECURE_CHANNEL(enum.IntFlag):
    NOT_SPECIFIED = 0x0
    REQUIRED = 0x1
    NOT_REQUIRED = 0x2

# maximum number of connections
MAX_CONNECTION = 10240

# NVMe tcp package length, refer: MTU = 1500 bytes
NVME_TCP_PDU_UNIT = 1024

# Max SQ head pointer
SQ_HEAD_MAX = 128

@dataclass
class Connection:
    """Data used multiple times in each connection."""

    connection: socket.socket = None
    allow_listeners: list = field(default_factory=list)
    log_page: bytearray = field(default_factory=bytearray)
    recv_buffer: bytearray = field(default_factory=bytearray)
    nvmeof_connect_data_hostid: tuple = tuple((c_ubyte *16)())
    nvmeof_connect_data_cntlid: int = 0
    nvmeof_connect_data_subnqn: tuple = tuple((c_ubyte *256)())
    nvmeof_connect_data_hostnqn: tuple = tuple((c_ubyte *256)())
    sq_head_ptr: int = 0
    unsent_log_page_len: int = 0
    # NVM ExpressTM Revision 1.4, page 47
    # see Figure 78: Offset 14h: CC – Controller Configuration
    property_configuration: tuple = tuple((c_ubyte *8)())
    shutdown_now: bool = False
    controller_id: uuid = None
    gen_cnt: int = 0
    recv_async: bool = False
    async_cmd_id: int = 0
    keep_alive_time: float = 0.0
    keep_alive_timeout: int = 0

class AutoSerializableStructure(LittleEndianStructure):
    def __add__(self, other):
        if isinstance(other, LittleEndianStructure):
            return bytes(self) + bytes(other)
        elif isinstance(other, bytes):
            return bytes(self) + other
        else:
            raise ValueError("error message format.")

class Pdu(AutoSerializableStructure):
    _fields_ = [
        ("type", c_uint8),
        ("specical_flag", c_uint8),
        ("header_length", c_uint8),
        ("data_offset", c_uint8),
        ("packet_length", c_uint32),
    ]

class ICResp(AutoSerializableStructure):
    _fields_ = [
        # pdu version format
        ("version_format", c_uint16),
        # controller Pdu data alignment
        ("data_alignment", c_uint8),
        # digest types enabled
        ("digest_types", c_uint8),
        # Maximum data capsules per r2t supported
        ("maximum_data_capsules", c_uint32)
    ]

class CqeConnect(AutoSerializableStructure):
    _fields_ = [
        ("controller_id", c_uint16),
        ("authentication", c_uint16),
        ("reserved", c_uint32),
        ("sq_head_ptr", c_uint16),
        ("sq_id", c_uint16),
        ("cmd_id", c_uint16),
        ("status", c_uint16)
    ]

class CqePropertyGetSet(AutoSerializableStructure):
    _fields_ = [
        # property data for property get, reserved for property set
        ("property_data", c_ubyte * 8),
        ("sq_head_ptr", c_uint16),
        ("sq_id", c_uint16),
        ("cmd_id", c_uint16),
        ("status", c_uint16)
    ]

class NVMeTcpDataPdu(AutoSerializableStructure):
    _fields_ = [
        ("cmd_id", c_uint16),
        ("transfer_tag", c_uint16),
        ("data_offset", c_uint32),
        ("data_length", c_uint32),
        ("reserved", c_uint32)
    ]

class NVMeIdentify(AutoSerializableStructure):
    _fields_ = [
        # skip some fields, include VID, SSVID, SN, MN
        ("todo_fields1", c_ubyte * 64),
        ("firmware_revision", c_ubyte * 8),
        # RAB, IEEE, CMIC
        ("todo_fields2", c_ubyte * 5),
        # maximum data transfer size
        ("mdts", c_uint8),
        ("controller_id", c_uint16),
        ("version", c_uint8 * 4),
        # RTD3R, RTD3E
        ("todo_fields3", c_ubyte * 8),
        # optional asynchronous events supported
        ("oaes", c_ubyte * 4),
        # CTRATT, RRLS, CNTRLTYPE, FGUID, NVMe Management Interface, OACS, ACL
        ("todo_fields4", c_ubyte * 163),
        # asynchronous events request limit
        ("aerl", c_uint8),
        ("firmware_updates", c_uint8),
        # log page attributes
        ("lpa", c_uint8),
        # error log page entries(ELPE)
        ("elpe", c_uint8),
        # NPSS, AVSCC, APSTA, WCTEMP, CCTEMP, MTFA, HMPRE, HMIN, TNVMCAP...
        # TODO: keep alive support - timer value(KAS)?
        ("todo_fields5", c_ubyte * 251),
        # maximum outstanding commands
        ("max_cmd", c_uint16),
        # number of namespace, optional NVM command support
        ("todo_fields6", c_uint8 * 6),
        # fused operation support
        ("fused_operation", c_uint16),
        # FNA, VWC, AWUN, AWUPF, NVSCC, NWPC
        ("todo_fields7", c_uint8 * 8),
        # atomic compare & write unit
        ("acwu", c_uint16),
        ("reserved1", c_uint16),
        # SGL support
        ("sgls", c_uint8 * 4),
        # maxinum number of allowed namespaces
        ("mnan", c_uint32),
        ("reserved2", c_ubyte * 224),
        ("subnqn", c_ubyte * 256),
        ("reserved3", c_ubyte * 768),
        ("nvmeof_attributes", c_ubyte * 256),
        ("power_state_attributes", c_ubyte * 1024),
        ("vendor_specific", c_ubyte * 1024)
    ]

# for set feature, keep alive and async
class  CqeNVMe(AutoSerializableStructure):
    _fields_ = [
        ("dword0", c_uint32),
        ("dword1", c_uint32),
        ("sq_head_ptr", c_uint16),
        ("sq_id", c_uint16),
        ("cmd_id", c_uint16),
        ("status", c_uint16)
    ]

class NVMeGetLogPage(AutoSerializableStructure):
    _fields_ = [
        # generation counter
        ("genctr", c_uint64),
        # number of records
        ("numrec", c_uint64),
        #record format
        ("recfmt", c_uint16),
        ("reserved", c_ubyte * 1006)
    ]

class DiscoveryLogEntry(AutoSerializableStructure):
    _fields_ = [
        ("trtype", c_uint8),
        ("adrfam", c_uint8),
        ("subtype", c_uint8),
        ("treq", c_uint8),
        ("port_id", c_uint16),
        ("controller_id", c_uint16),
        # admin max SQ size
        ("asqsz", c_uint16),
        ("reserved1", c_ubyte * 22),
        ("trsvcid", c_ubyte * 32),
        ("reserved2", c_ubyte * 192),
        ("subnqn", c_ubyte * 256),
        ("traddr", c_ubyte * 256),
        # Transport specific address subtype
        ("tsas", c_ubyte * 256)
    ]

class DiscoveryService:
    """Implements discovery controller.

    Response discover request from initiator.

    Instance attributes:
        version: Discovery controller version
        config: Basic gateway parameters
        logger: Logger instance to track discovery controller events
        omap_name: OMAP object name
        ioctx: I/O context which allows OMAP access
        discovery_addr: Discovery controller addr which allows initiator send command
        discovery_port: Discovery controller's listening port
    """

    def __init__(self, config):
        self.version = 1
        self.config = config
        self.lock = threading.Lock()
        self.omap_state = OmapGatewayState(self.config)

        self.logger = GatewayLogger(config).logger

        gateway_group = self.config.get_with_default("gateway", "group", "")
        self.omap_name = f"nvmeof.{gateway_group}.state" \
            if gateway_group else "nvmeof.state"
        self.logger.info(f"log pages info from omap: {self.omap_name}")

        self.ioctx = self.omap_state.open_rados_connection(config)
        self.discovery_addr = self.config.get_with_default("discovery", "addr", "0.0.0.0")
        self.discovery_port = self.config.get_with_default("discovery", "port", "8009")
        if not self.discovery_addr or not self.discovery_port:
            self.logger.error("discovery addr/port are empty.")
            assert 0
        self.logger.info(f"discovery addr: {self.discovery_addr} port: {self.discovery_port}")

        self.conn_vals = {}
        self.connection_counter = 1
        self.selector = selectors.DefaultSelector()

    def _read_all(self) -> Dict[str, str]:
        """Reads OMAP and returns dict of all keys and values."""

        omap_dict = self.omap_state.get_state()
        return omap_dict

    def _get_vals(self, omap_dict, prefix):
        """Read values from the OMAP dict."""

        return [json.loads(val.decode('utf-8')) for (key, val) in omap_dict.items()
            if key.startswith(prefix)]

    def reply_initialize(self, conn):
        """Reply initialize request."""

        self.logger.debug("handle ICreq.")
        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.ICRESP
        pdu_reply.header_length = 128
        pdu_reply.packet_length = 128

        icresp_reply = ICResp()
        icresp_reply.maximum_data_capsules = 131072

        try:
            conn.sendall(pdu_reply + icresp_reply + bytes(112))
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply initialize connection request.")
        return 0

    def reply_fc_cmd_connect(self, conn, data, cmd_id):
        """Reply connect request."""

        self.logger.debug("handle connect request.")
        self_conn = self.conn_vals[conn.fileno()]
        hf_nvmeof_cmd_connect_rsvd1 = struct.unpack_from('<19B', data, 13)
        SIGL1 = struct.unpack_from('<QI4B', data, 32)
        address = SIGL1[0]
        length = SIGL1[1]
        reserved3 = SIGL1[2]
        descriptor_type = SIGL1[5]

        CMD2 = struct.unpack_from('<HHHBBI', data, 48)
        record_format = CMD2[0]
        queue_id = CMD2[1]
        submission_queue_size = CMD2[2]
        connect_attributes = CMD2[3]
        keep_alive_timeout = CMD2[5]

        self_conn =self.conn_vals[conn.fileno()]
        self_conn.keep_alive_time = time.time()
        if keep_alive_timeout == 0:
            keep_alive_timeout = 15000
        self.logger.debug(f"connection keep alive {keep_alive_timeout}ms.")
        self_conn.keep_alive_timeout = keep_alive_timeout

        self_conn.nvmeof_connect_data_hostid = \
            struct.unpack_from('<16B', data, 72)
        self_conn.nvmeof_connect_data_cntlid = \
            struct.unpack_from('<H', data, 88)[0]
        self_conn.nvmeof_connect_data_subnqn = \
            struct.unpack_from('<256B', data, 328)
        self_conn.nvmeof_connect_data_hostnqn = \
            struct.unpack_from('<256B', data, 584)

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

        connect_reply = CqeConnect()
        connect_reply.controller_id = int(self_conn.controller_id)
        connect_reply.sq_head_ptr = self_conn.sq_head_ptr
        connect_reply.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + connect_reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply connect request.")
        return 0

    def reply_fc_cmd_prop_get(self, conn, data, cmd_id):
        """Reply property get request."""

        self.logger.debug("handle property get request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvmeof_prop_get_set_rsvd0 = struct.unpack_from('<35B', data, 13)
        # property size = (attrib+1)x4, 0x1 means 8 bytes
        nvmeof_prop_get_set_attrib = struct.unpack_from('<1B', data, 48)[0]
        nvmeof_prop_get_set_rsvd1 = struct.unpack_from('<3B', data, 49)
        nvmeof_prop_get_set_offset = struct.unpack_from('<I', data, 52)[0]

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

        # reply different property data
        property_get = CqePropertyGetSet()
        if NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.CAPABILITIES:
            # controller capabilities
            # \x7f = maxinum queue entries support:128
            # \x01 contiguous queues required: true
            # \x1e timeout(to ready status): 1e(15000ms default in server side)
            # \x20 command sets supportd: 1 (NVM IO command set)?
            property_get.property_data = (c_ubyte * 8)(0x7f, 0x00, \
                0x01, 0x1e, 0x20, 0x00, 0x00, 0x00)
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.CONFIGURATION:
            # b'\x00\x00\x46\x00\x00\x00\x00\x00'
            # 0x46: IO Submission Queue Entry Size: 0x6 (64 bytes)
            # IO Completion Queue Entry Size: 0x4 (16 bytes)
            property_get.property_data = self_conn.property_configuration
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.STATUS:
            shutdown_notification = (self_conn.property_configuration[1] >> 6) & 0x3
            if shutdown_notification == 0:
                # check CC.EN bit
                enabled = self_conn.property_configuration[0] & 0x1
                if enabled != 0:
                    # controller status: ready
                    property_get.property_data = (c_ubyte * 8)(0x01, 0x00, \
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
                else:
                    property_get.property_data = (c_ubyte * 8)(0x00, 0x00, \
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
            else:
                # here shutdown_notification should be 0x1
                property_get.property_data = (c_ubyte * 8)(0x09, 0x00, \
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
                self_conn.shutdown_now = True
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.VERSION:
            # nvme version: 1.3
            property_get.property_data = (c_ubyte * 8)(0x00, 0x03, \
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00)
        else:
            self.logger.error("unsupported type for property getting.")
        property_get.sq_head_ptr = self_conn.sq_head_ptr
        property_get.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + property_get)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply property get request.")
        return 0

    def reply_fc_cmd_prop_set(self, conn, data, cmd_id):
        """Reply property set request."""

        self.logger.debug("handle property set request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvmeof_prop_get_set_rsvd0 = struct.unpack_from('<35B', data, 13)
        nvmeof_prop_get_set_attrib = struct.unpack_from('<1B', data, 48)[0]
        nvmeof_prop_get_set_rsvd1 = struct.unpack_from('<3B', data, 49)
        nvmeof_prop_get_set_offset = struct.unpack_from('<I', data, 52)[0]

        if NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.CAPABILITIES:
            self.logger.error("property setting of capabilities is not supported.")
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.CONFIGURATION:
            self_conn.property_configuration = struct.unpack_from('<8B', data, 56)
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.STATUS:
            self.logger.error("property setting of status is not supported.")
        elif NVME_CTL(nvmeof_prop_get_set_offset) == NVME_CTL.VERSION:
            self.logger.error("property setting of version is not supported.")
        else:
            self.logger.error("unsupported type for property settings.")

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

        property_set = CqePropertyGetSet()
        property_set.sq_head_ptr = self_conn.sq_head_ptr
        property_set.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + property_set)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply property set request.")
        return 0

    def reply_identify(self, conn, data, cmd_id):
        """Reply identify request."""

        self.logger.debug("handle identify request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvme_nsid = struct.unpack_from('<I', data, 12)[0]
        nvme_rsvd1 = struct.unpack_from('<Q', data, 16)[0]
        nvme_mptr = struct.unpack_from('<Q', data, 24)[0]
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        nvme_identify_dword10 = struct.unpack_from('<I', data, 48)[0]
        nvme_identify_dword11 = struct.unpack_from('<I', data, 52)[0]
        nvme_identify_dword12 = struct.unpack_from('<I', data, 56)[0]
        nvme_identify_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_identify_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_identify_dword15 = struct.unpack_from('<I', data, 68)[0]

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.C2H_DATA
        # 0x0c == 0b1100, means pdu data last: set, pdu data success: set
        pdu_reply.specical_flag = 0x0c
        pdu_reply.header_length = 24
        pdu_reply.data_offset = 24
        pdu_reply.packet_length = 4120

        nvme_tcp_data_pdu = NVMeTcpDataPdu()
        nvme_tcp_data_pdu.cmd_id = cmd_id
        nvme_tcp_data_pdu.data_length = 4096

        identify_reply = NVMeIdentify()
        # version: 0.01
        identify_reply.firmware_revision = (c_ubyte * 8)(0x30, 0x30, \
            0x2e, 0x30, 0x31, 0x20, 0x20, 0x20)
        # maximum data transfer size: 2^5=32 pages
        identify_reply.mdts = 0x05
        identify_reply.controller_id = int(self_conn.controller_id)
        # version: 1.3, TODO: get from hardware
        identify_reply.version = (c_uint8 * 4)(0x00, 0x30, 0x01, 0x00)
        identify_reply.oaes = (c_ubyte * 4)(0x00, 0x00, 0x00, 0x80)
        # asynchronous events request limit: 4 events(3+1)
        identify_reply.aerl = 0x03
        # log page attributes: Extended data get log page support: True
        identify_reply.lpa = 0x04
        # error log page entries:128 entries(127+1)
        identify_reply.elpe = 0x7f
        identify_reply.max_cmd = 128
        identify_reply.fused_operation = 0x0001
        identify_reply.sgls = (c_uint8 * 4)(0x05, 0x00, 0x10, 0x00)
        if len(self_conn.nvmeof_connect_data_subnqn) == 256:
            for i in range(256):
                identify_reply.subnqn[i] = self_conn.nvmeof_connect_data_subnqn[i]
        else:
            self.logger.error("error subnqn.")
            return -1

        try:
            conn.sendall(pdu_reply + nvme_tcp_data_pdu + identify_reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply identify request.")
        return 0

    def reply_set_feature(self, conn, data, cmd_id):
        """Reply set feature request."""

        self.logger.debug("handle set feature request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvme_nsid = struct.unpack_from('<I', data, 12)[0]
        nvme_rsvd1 = struct.unpack_from('<Q', data, 16)[0]
        nvme_mptr = struct.unpack_from('<Q', data, 24)[0]
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        # dword10 may include Feature Identifier:
        # Asynchronous Event Configuration (0x0b) (Not currently used)
        nvme_set_features_dword10 = struct.unpack_from('<I', data, 48)[0]
        nvme_set_features_dword11 = struct.unpack_from('<I', data, 52)[0]
        nvme_set_features_dword12 = struct.unpack_from('<I', data, 56)[0]
        nvme_set_features_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_set_features_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_set_features_dword15 = struct.unpack_from('<I', data, 68)[0]

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

        set_feature_reply =  CqeNVMe()
        set_feature_reply.sq_head_ptr = self_conn.sq_head_ptr
        set_feature_reply.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + set_feature_reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply set feature request.")
        return 0

    def reply_get_feature(self, conn, data, cmd_id):
        """Reply get feature request."""

        self.logger.debug("handle get feature request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvme_nsid = struct.unpack_from('<I', data, 12)[0]
        nvme_rsvd1 = struct.unpack_from('<Q', data, 16)[0]
        nvme_mptr = struct.unpack_from('<Q', data, 24)[0]
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        nvme_get_features_dword10 = struct.unpack_from('<I', data, 48)[0]
        nvme_get_features_dword11 = struct.unpack_from('<I', data, 52)[0]
        nvme_get_features_dword12 = struct.unpack_from('<I', data, 56)[0]
        nvme_get_features_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_get_features_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_get_features_dword15 = struct.unpack_from('<I', data, 68)[0]

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

        get_feature_reply =  CqeNVMe()
        get_feature_reply.dword0 = self_conn.keep_alive_timeout
        get_feature_reply.sq_head_ptr = self_conn.sq_head_ptr
        get_feature_reply.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + get_feature_reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply get feature request.")
        return 0

    def reply_get_log_page(self, conn, data, cmd_id):
        """Reply get log page request."""

        self.logger.debug("handle get log page request.")
        self_conn = self.conn_vals[conn.fileno()]
        my_omap_dict = self._read_all()
        listeners = self._get_vals(my_omap_dict, GatewayState.LISTENER_PREFIX)
        hosts = self._get_vals(my_omap_dict, GatewayState.HOST_PREFIX)
        if len(self_conn.nvmeof_connect_data_hostnqn) != 256:
            self.logger.error("error hostnqn.")
            return -1
        hostnqn = "".join(chr(byte) for byte \
            in self_conn.nvmeof_connect_data_hostnqn[:256]).rstrip('\x00')

        nvme_nsid = struct.unpack_from('<I', data, 12)[0]
        nvme_rsvd1 = struct.unpack_from('<Q', data, 16)[0]
        nvme_mptr = struct.unpack_from('<Q', data, 24)[0]
        # https://nvmexpress.org/wp-content/uploads/NVM-Express-1_4-2019.06.10-Ratified.pdf
        # The SGL Data Block descriptor, defined in Figure 114, describes a data block.
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        nvme_sgl_len = nvme_sgl[8] + (nvme_sgl[9] << 8) + (nvme_sgl[10] << 16) + (nvme_sgl[11] << 24)
        nvme_get_logpage_dword10 = struct.unpack_from('<I', data, 48)[0]
        # nvme_get_logpage_numd indicate the reply bytes, rule: (values+1)*4
        nvme_get_logpage_numdl = struct.unpack_from('<H', data, 50)[0]
        nvme_get_logpage_numdh = struct.unpack_from('<H', data, 52)[0]
        nvme_get_logpage_numd = (nvme_get_logpage_numdh << 16) + nvme_get_logpage_numdl
        nvme_data_len = (nvme_get_logpage_numd + 1) * 4
        nvme_get_logpage_dword11 = struct.unpack_from('<I', data, 52)[0]
        # Logpage offset overlaps with dword13
        nvme_logpage_offset = struct.unpack_from('<Q', data, 56)[0]
        nvme_get_logpage_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_get_logpage_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_get_logpage_dword15 = struct.unpack_from('<I', data, 68)[0]
        get_logpage_lid = nvme_get_logpage_dword10 & 0xFF
        get_logpage_lsp = (nvme_get_logpage_dword10 >> 8) & 0x1F
        get_logpage_lsi = nvme_get_logpage_dword11 >> 16
        get_logpage_uid_idx = nvme_get_logpage_dword14 & 0x3F

        if get_logpage_lid != 0x70:
            self.logger.error("request type error, not discovery request.")
            return -1

        if nvme_data_len != nvme_sgl_len:
            self.logger.error(f"request data len error, {nvme_data_len=} != {nvme_sgl_len=}.")
            return -1

        # Filter listeners based on host access permissions
        allow_listeners = self_conn.allow_listeners
        if len(allow_listeners) == 0:
            for host in hosts:
                a = host["host_nqn"]
                if host["host_nqn"] == '*' or host["host_nqn"] == hostnqn:
                    for listener in listeners:
                        # TODO: It is better to change nqn in the "listener"
                        # to subsystem_nqn to avoid confusion
                        if host["subsystem_nqn"] == listener["nqn"]:
                            allow_listeners += [listener,]
            self_conn.allow_listeners = allow_listeners

        # Prepare all log page data segments
        if self_conn.unsent_log_page_len == 0 and nvme_data_len > 16:
            self_conn.unsent_log_page_len = 1024 * (len(allow_listeners) + 1)
            self_conn.log_page = bytearray(self_conn.unsent_log_page_len)

            nvme_get_log_page_reply = NVMeGetLogPage()
            nvme_get_log_page_reply.genctr = self_conn.gen_cnt
            nvme_get_log_page_reply.numrec = len(allow_listeners)
            self_conn.log_page[0:1024] = nvme_get_log_page_reply

            # log entries
            log_entry_counter = 0
            while log_entry_counter < len(allow_listeners):
                log_entry = DiscoveryLogEntry()
                log_entry.trtype = TRANSPORT_TYPES.TCP
                log_adrfam = allow_listeners[log_entry_counter]["adrfam"]
                adrfam = ADRFAM_TYPES[log_adrfam.lower()]
                if adrfam is None:
                    self.logger.error(f"unsupported address family {log_adrfam}")
                else:
                    log_entry.adrfam = adrfam
                log_entry.subtype = NVMF_SUBTYPE.NVME
                log_entry.treq = NVMF_TREQ_SECURE_CHANNEL.NOT_REQUIRED
                log_entry.port_id = log_entry_counter
                log_entry.controller_id = 0xffff
                log_entry.asqsz = 128
                # transport service indentifier
                str_trsvcid = str(allow_listeners[log_entry_counter]["trsvcid"])
                log_entry.trsvcid = (c_ubyte * 32)(*[c_ubyte(x) for x \
                    in str_trsvcid.encode()])
                log_entry.trsvcid[len(str_trsvcid):] = \
                    [c_ubyte(0x20)] * (32 - len(str_trsvcid))
                # NVM subsystem qualified name
                log_entry.subnqn = (c_ubyte * 256)(*[c_ubyte(x) for x \
                    in allow_listeners[log_entry_counter]["nqn"].encode()])
                log_entry.subnqn[len(allow_listeners[log_entry_counter]["nqn"]):] = \
                    [c_ubyte(0x00)] * (256 - len(allow_listeners[log_entry_counter]["nqn"]))
                # Transport address
                log_entry.traddr = (c_ubyte * 256)(*[c_ubyte(x) for x \
                    in allow_listeners[log_entry_counter]["traddr"].encode()])
                log_entry.traddr[len(allow_listeners[log_entry_counter]["traddr"]):] = \
                    [c_ubyte(0x20)] * (256 - len(allow_listeners[log_entry_counter]["traddr"]))

                self_conn.log_page[1024*(log_entry_counter+1): \
                    1024*(log_entry_counter+2)] = log_entry
                log_entry_counter += 1
        else:
            self.logger.debug("in the process of sending log pages...")

        reply = b''
        pdu_and_nvme_pdu_len = 8 + 16
        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.C2H_DATA
        pdu_reply.specical_flag = 0x0c
        pdu_reply.header_length = pdu_and_nvme_pdu_len
        pdu_reply.data_offset = pdu_and_nvme_pdu_len
        pdu_reply.packet_length = pdu_and_nvme_pdu_len + nvme_data_len

        nvme_tcp_data_pdu = NVMeTcpDataPdu()
        nvme_tcp_data_pdu.cmd_id = cmd_id
        nvme_tcp_data_pdu.data_length = nvme_data_len

        # reply based on the received get log page request packet(length)
        if nvme_data_len <= 1024 and nvme_logpage_offset == 0:
            nvme_get_log_page_reply = NVMeGetLogPage()
            nvme_get_log_page_reply.genctr = self_conn.gen_cnt
            nvme_get_log_page_reply.numrec = len(listeners)

            reply = pdu_reply + nvme_tcp_data_pdu + bytes(nvme_get_log_page_reply)[:nvme_data_len]
        elif nvme_data_len % 1024 == 0:
            # reply log pages
            reply = pdu_reply + nvme_tcp_data_pdu + \
                self_conn.log_page[nvme_logpage_offset:nvme_logpage_offset+nvme_data_len]
            self_conn.unsent_log_page_len -= nvme_data_len
            if self_conn.unsent_log_page_len == 0:
                self_conn.log_page = b''
                self_conn.allow_listeners = []
        else:
            self.logger.error(f"request log page: invalid length error {nvme_data_len=}")
            return -1
        try:
            conn.sendall(reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply get log page request.")
        return 0

    def reply_keep_alive(self, conn, data, cmd_id):
        """Reply keep alive request."""

        self.logger.debug("handle keep alive request.")
        self_conn = self.conn_vals[conn.fileno()]
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        nvme_keep_alive_dword10 = struct.unpack_from('<I', data, 48)[0]
        nvme_keep_alive_dword11 = struct.unpack_from('<I', data, 52)[0]
        nvme_keep_alive_dword12 = struct.unpack_from('<I', data, 56)[0]
        nvme_keep_alive_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_keep_alive_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_keep_alive_dword15 = struct.unpack_from('<I', data, 68)[0]

        pdu_reply = Pdu()
        pdu_reply.type = NVME_TCP_PDU.RSP
        pdu_reply.header_length = 24
        pdu_reply.packet_length = 24

         # Cqe for keep alive
        keep_alive_reply = CqeNVMe()
        keep_alive_reply.sq_head_ptr = self_conn.sq_head_ptr
        keep_alive_reply.cmd_id = cmd_id

        try:
            conn.sendall(pdu_reply + keep_alive_reply)
        except BrokenPipeError:
            self.logger.error("client disconnected unexpectedly.")
            return -1
        self.logger.debug("reply keep alive request.")
        return 0

    def store_async(self, conn, data, cmd_id):
        """Parse and store async event."""

        self.logger.debug("parse and store async event.")
        self_conn = self.conn_vals[conn.fileno()]
        nvme_sgl = struct.unpack_from('<16B', data, 32)
        nvme_sgl_desc_type = nvme_sgl[15] & 0xF0
        nvme_sgl_desc_sub_type = nvme_sgl[15] & 0x0F
        nvme_async_dword10 = struct.unpack_from('<I', data, 48)[0]
        nvme_async_dword11 = struct.unpack_from('<I', data, 52)[0]
        nvme_async_dword12 = struct.unpack_from('<I', data, 56)[0]
        nvme_async_dword13 = struct.unpack_from('<I', data, 60)[0]
        nvme_async_dword14 = struct.unpack_from('<I', data, 64)[0]
        nvme_async_dword15 = struct.unpack_from('<I', data, 68)[0]

        self_conn.recv_async = True
        self_conn.async_cmd_id = cmd_id

    def _state_notify_update(self, update, is_add_req):
        """Notify and reply async event."""

        should_send_async_event = False
        for key in update.keys():
            if key.startswith(GatewayState.SUBSYSTEM_PREFIX) or key.startswith(GatewayState.LISTENER_PREFIX):
                should_send_async_event = True
                break

        if not should_send_async_event:
            return

        for key in list(self.conn_vals.keys()):
            if self.conn_vals[key].recv_async is True:
                pdu_reply = Pdu()
                pdu_reply.type = NVME_TCP_PDU.RSP
                pdu_reply.header_length = 24
                pdu_reply.packet_length = 24

                async_reply = CqeNVMe()
                # async_event_type:0x2 async_event_info:0xf0 log_page_identifier:0x70
                async_reply.dword0 = int.from_bytes(b'\x02\xf0\x70\x00', byteorder='little')
                async_reply.sq_head_ptr = self.conn_vals[key].sq_head_ptr
                async_reply.cmd_id = self.conn_vals[key].async_cmd_id

                try:
                    self.conn_vals[key].connection.sendall(pdu_reply + async_reply)
                except BrokenPipeError:
                    self.logger.error("client disconnected unexpectedly.")
                    return
                self.logger.debug("notify and reply async request.")
                self.conn_vals[key].recv_async = False
                return

    def handle_timeout(self):
        """Handle connection timeout."""

        while True:
            for key in list(self.conn_vals.keys()):
                if self.conn_vals[key].keep_alive_timeout != 0 and \
                  time.time() - self.conn_vals[key].keep_alive_time >= \
                  self.conn_vals[key].keep_alive_timeout / 1000:
                    # Adding locks to prevent another thread from processing sudden requests.
                    # Is there a better way?
                    with self.lock:
                        self.logger.debug(f"discover request from {self.conn_vals[key].connection} timeout.")
                        self.selector.unregister(self.conn_vals[key].connection)
                        self.conn_vals[key].connection.close()
                        del self.conn_vals[key]

            time.sleep(1)

    def reply_fabric_request(self, conn, data, cmd_id):
        """Reply fabric request."""

        fabric_type = struct.unpack_from('<B', data, 12)[0]
        handle_fabric = {
            NVME_TCP_FCTYPE.CONNECT: self.reply_fc_cmd_connect,
            NVME_TCP_FCTYPE.PROP_GET: self.reply_fc_cmd_prop_get,
            NVME_TCP_FCTYPE.PROP_SET: self.reply_fc_cmd_prop_set
        }
        class UnknownFabricType(BaseException):
            def __init__(self, fabric_type):
                super().__init__(f"unsupported opcode: {fabric_type}")
        try:
            err = handle_fabric[fabric_type](conn, data, cmd_id)
        except KeyError as e:
            raise UnknownFabricType(fabric_type) from e
        return err

    def nvmeof_tcp_connection(self, conn, mask):
        """Handle request."""

        err = 0
        with self.lock:
            if conn.fileno() not in self.conn_vals:
                self.logger.debug(f"connection {conn} timeout")
                return
            self_conn = self.conn_vals[conn.fileno()]
            self_conn.keep_alive_time = time.time()
            try:
                message = conn.recv(NVME_TCP_PDU_UNIT)
                if message:
                    self_conn.recv_buffer += message
                else:
                    return
            except BlockingIOError:
                self.logger.error("recived data failed.")

            while True:
                if len(self_conn.recv_buffer) < 8:
                    return
                pdu = struct.unpack_from('<BBBBI', self_conn.recv_buffer, 0)
                pdu_type = pdu[0]
                psh_flag = pdu[1]
                ph_len = pdu[2]
                ph_off = pdu[3]
                package_len = pdu[4]
                if len(self_conn.recv_buffer) < package_len:
                    return

                data = self_conn.recv_buffer[:package_len]
                self_conn.recv_buffer = self_conn.recv_buffer[package_len:]

                self_conn.sq_head_ptr += 1
                if self_conn.sq_head_ptr > SQ_HEAD_MAX:
                    self_conn.sq_head_ptr = 1

                if NVME_TCP_PDU(pdu_type) == NVME_TCP_PDU.ICREQ:
                    err = self.reply_initialize(conn)

                elif NVME_TCP_PDU(pdu_type) == NVME_TCP_PDU.CMD:
                    CMD1 = struct.unpack_from('<BBH', data, 8)
                    opcode = CMD1[0]
                    reserved = CMD1[1]
                    cmd_id = CMD1[2]

                    handle_opcode = {
                        NVME_TCP_OPC.FABRIC_TYPE: self.reply_fabric_request,
                        NVME_TCP_OPC.GET_LOG_PAGE: self.reply_get_log_page,
                        NVME_TCP_OPC.IDENTIFY: self.reply_identify,
                        NVME_TCP_OPC.SET_FEATURES: self.reply_set_feature,
                        NVME_TCP_OPC.GET_FEATURES: self.reply_get_feature,
                        NVME_TCP_OPC.KEEP_ALIVE: self.reply_keep_alive,
                        NVME_TCP_OPC.ASYNC_EVE_REQ: self.store_async
                    }
                    class UnknownNVMEOpcode(BaseException):
                        def __init__(self, opcode):
                            super().__init__(f"unsupported opcode: {opcode}")
                    try:
                        err = handle_opcode[opcode](conn, data, cmd_id)
                    except KeyError as e:
                        raise UnknownNVMEOpcode(opcode) from e
                else:
                    self.logger.error("unsupported pduGLOBAL_CNLID type: {pdu_type}")

                if err == -1 or self_conn.shutdown_now is True:
                    del self.conn_vals[conn.fileno()]
                    self.selector.unregister(conn)
                    self.logger.debug(f"discover request from {conn} finished.")
                    conn.close()
                    return

    def nvmeof_accept(self, sock, mask):
        """Accept connection."""

        conn, addr = sock.accept()
        self.logger.debug(f"accept connection {conn} from {addr}")
        conn.setblocking(False)
        with self.lock:
            if conn.fileno() not in self.conn_vals:
                self.conn_vals[conn.fileno()] = Connection(
                    connection=conn,
                    controller_id=uuid.uuid4(),
                    gen_cnt=self.connection_counter
                )
                self.connection_counter += 1

        self.selector.register(conn, selectors.EVENT_READ, \
                               self.nvmeof_tcp_connection)

    def start_service(self):
        """Enable listening on the server side."""

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.discovery_addr, int(self.discovery_port)))
        sock.listen(MAX_CONNECTION)
        sock.setblocking(False)
        self.selector.register(sock, selectors.EVENT_READ, self.nvmeof_accept)
        self.logger.debug("waiting for connection...")
        t = threading.Thread(target=self.handle_timeout)
        t.start()

        local_state = LocalGatewayState()
        gateway_state = GatewayStateHandler(self.config, local_state,
                                            self.omap_state, self._state_notify_update)
        gateway_state.start_update()

        try:
          while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
        except KeyboardInterrupt:
          for key in self.conn_vals:
            self.conn_vals[key].connection.close()
          self.selector.close()
          self.logger.debug("received a ctrl+C interrupt. exiting...")

def main(args=None):
    parser = argparse.ArgumentParser(prog="python3 -m control",
                                     description="Discover NVMe gateways")
    parser.add_argument(
        "-c",
        "--config",
        default="ceph-nvmeof.conf",
        type=str,
        help="Path to config file",
    )
    args = parser.parse_args()

    config = GatewayConfig(args.config)
    discovery_service = DiscoveryService(config)
    discovery_service.start_service()

if __name__ == "__main__":
    main()
