import logging
import struct

import pytest

from control.discovery import Connection, DiscoveryService, NVME_TCP_PDU

# Status Field values (bit 0 is the phase tag, the status code starts at bit 1)
INVALID_OPCODE_DNR = (0x0 << 9) | (0x01 << 1) | 0x8000
INVALID_LOG_PAGE_DNR = (0x1 << 9) | (0x09 << 1) | 0x8000


class FakeSocket:
    def __init__(self):
        self.sent = b""

    def fileno(self):
        return 42

    def sendall(self, data):
        self.sent += bytes(data)


class FakeConfig:
    def get(self, section, key):
        return {"ceph": {"pool": "rbd"}, "gateway": {"group": ""}}[section][key]


class FakeCephUtils:
    def get_gw_listeners(self, pool, group):
        return {}


@pytest.fixture
def service():
    """A discovery service without Ceph, with an empty gateway state."""
    svc = DiscoveryService.__new__(DiscoveryService)
    svc.logger = logging.getLogger("test_discovery_responses")
    svc.conn_vals = {}
    svc.config = FakeConfig()
    svc.ceph_utils = FakeCephUtils()
    svc._read_all = lambda: {"omap_version": "1"}
    return svc


def command(opcode, cmd_id, dword10=0):
    """An NVMe/TCP command capsule: 8 bytes of PDU header, then the SQE."""
    data = bytearray(72)
    data[0] = NVME_TCP_PDU.CMD
    struct.pack_into("<BBH", data, 8, opcode, 0, cmd_id)
    struct.pack_into("<I", data, 48, dword10)
    return bytes(data)


def response(sent):
    """Return (PDU type, cmd_id, status) of a response capsule."""
    assert len(sent) == 24
    pdu_type = sent[0]
    cmd_id, status = struct.unpack_from("<HH", sent, 8 + 12)
    return pdu_type, cmd_id, status


def connect(service):
    sock = FakeSocket()
    service.conn_vals[sock.fileno()] = Connection(connection=sock)
    return sock


@pytest.mark.parametrize("lid", [0x01, 0x02, 0x03, 0x0c, 0x6f, 0x71])
def test_unsupported_log_page_is_completed_with_an_error(service, lid):
    """A log page other than the discovery one gets Invalid Log Page.

    Hosts probe controllers for other log pages (udisks reads the SMART
    log). Closing the connection instead left the command without a
    completion: the host timed it out and recovered the controller.
    """
    sock = connect(service)
    # numdl = 127 dwords (512 bytes), lid in bits 7:0 of dword 10
    dword10 = (127 << 16) | lid
    ret = service.reply_get_log_page(sock, command(0x02, 0x1234, dword10), 0x1234)
    assert ret == 0, "the connection must stay up"
    pdu_type, cmd_id, status = response(sock.sent)
    assert pdu_type == NVME_TCP_PDU.RSP
    assert cmd_id == 0x1234
    assert status == INVALID_LOG_PAGE_DNR
    assert service.conn_vals, "the connection must not be dropped"


def test_unsupported_opcode_is_not_reported_as_success(service):
    """Invalid Command Opcode is status code 01h, i.e. 0x0002 with the phase bit.

    The reply used to set the status field to 1, which is only the phase tag:
    the host read it as a successful completion.
    """
    sock = connect(service)
    ret = service.reply_not_supported(sock, command(0x80, 7), 7)
    assert ret == 0
    pdu_type, cmd_id, status = response(sock.sent)
    assert pdu_type == NVME_TCP_PDU.RSP
    assert cmd_id == 7
    assert status == INVALID_OPCODE_DNR
    assert status >> 1 != 0, "status code 0 is a successful completion"


def test_discovery_log_page_still_answered(service):
    """The discovery log page itself is still returned as data."""
    sock = connect(service)
    data = bytearray(command(0x02, 9, (255 << 16) | 0x70))  # 1024 bytes
    # SGL data block descriptor length (bytes 8..11 of the SGL at offset 32)
    struct.pack_into("<I", data, 32 + 8, 1024)
    ret = service.reply_get_log_page(sock, bytes(data), 9)
    assert ret == 0
    assert sock.sent[0] == NVME_TCP_PDU.C2H_DATA
    # PDU header, data PDU header, then the 1024 byte log page
    assert len(sock.sent) == 24 + 1024
