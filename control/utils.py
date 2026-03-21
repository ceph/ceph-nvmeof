#
#  Copyright (c) 2024 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gbregman@ibm.com
#

import uuid
import errno
import os
import os.path
import socket
import logging
import logging.handlers
import gzip
import shutil
import netifaces
import ipaddress
from typing import Tuple, List
from cryptography.fernet import Fernet
import cryptography.exceptions
import base64
import json
import subprocess
from pathlib import Path
from binascii import Error, crc32


class GatewayEnumUtils:
    def get_value_from_key(e_type, keyval, ignore_case=False):
        val = None
        try:
            key_index = e_type.keys().index(keyval)
            val = e_type.values()[key_index]
        except ValueError:
            pass
        except IndexError:
            pass

        if val is not None or not ignore_case:
            return val

        if isinstance(keyval, str):
            val = GatewayEnumUtils.get_value_from_key(e_type, keyval.lower(), False)
        if val is None and isinstance(keyval, str):
            val = GatewayEnumUtils.get_value_from_key(e_type, keyval.upper(), False)

        return val

    def get_key_from_value(e_type, val):
        keyval = None
        try:
            val_index = e_type.values().index(val)
            keyval = e_type.keys()[val_index]
        except ValueError:
            pass
        except IndexError:
            pass
        return keyval


class GatewayUtils:
    DISCOVERY_NQN = "nqn.2014-08.org.nvmexpress.discovery"
    ALL_SUBSYSTEMS = "*"
    MAX_HOST_NAME_LENGTH = 253
    DOMAIN_LABEL_MAX_LEN = 63
    MAX_MESSAGE_LENGTH_DEFAULT = 4

    # We need to enclose IPv6 addresses in brackets before concatenating
    # a colon and port number to it
    @staticmethod
    def escape_address_if_ipv6(addr: str) -> str:
        ret_addr = addr
        if ":" in addr and not addr.strip().startswith("["):
            ret_addr = f"[{addr}]"
        return ret_addr

    @staticmethod
    def unescape_address(addr: str) -> str:
        ret_addr = addr.strip()
        ret_addr = ret_addr.removeprefix("[").removesuffix("]")
        return ret_addr

    @staticmethod
    def unescape_address_if_ipv6(addr: str, adrfam: str) -> str:
        ret_addr = addr.strip()
        if adrfam.lower() == "ipv6":
            ret_addr = GatewayUtils.unescape_address(addr)
        return ret_addr

    @classmethod
    def is_discovery_nqn(cls, nqn) -> bool:
        return nqn == cls.DISCOVERY_NQN

    @staticmethod
    def is_valid_host_name(hostname) -> bool:
        if not hostname:
            return False

        if not isinstance(hostname, str):
            return False

        if len(hostname) > GatewayUtils.MAX_HOST_NAME_LENGTH:
            return False

        parts = hostname.split(".")
        for one_part in parts:
            if not one_part:
                return False

            if len(one_part) > GatewayUtils.DOMAIN_LABEL_MAX_LEN:
                return False

            if one_part.startswith("-"):
                return False

            if one_part.endswith("-"):
                return False

            if not one_part.replace("-", "").isalnum():
                return False

        return True

    def get_hostname(ip_addr: str, logger) -> str:
        try:
            ret = socket.gethostbyaddr(ip_addr)
            if ret:
                return ret[0]
        except Exception as e:
            logger.error(f'error in get_hostname: {e}')
        return ''

    @staticmethod
    def is_any_host_address(addr: str, adrfam: str) -> bool:
        if adrfam.lower() == "ipv4":
            return addr == "0.0.0.0"
        elif adrfam.lower() == "ipv6":
            return addr == "::"
        return False

    @staticmethod
    def is_valid_ip_address(addr: str, adrfam: str) -> str:
        ipaddr = None
        try:
            ipaddr = ipaddress.ip_address(addr)
        except ValueError:
            ipaddr = None
        if ipaddr is None:
            return f'Invalid IP address "{addr}"'
        if not adrfam or adrfam.lower() == "ipv4":
            if ipaddr.version != 4:
                return f"IP address {addr} is not an IPv4 address"
        elif adrfam.lower() == "ipv6":
            if ipaddr.version != 6:
                return f"IP address {addr} is not an IPv6 address"
        else:
            return f'Invalid address family "{adrfam}"'
        return ""

    @staticmethod
    def is_valid_rev_domain(rev_domain):
        domain_parts = rev_domain.split(".")
        for lbl in domain_parts:
            if not lbl:
                return (errno.EINVAL, "empty domain label doesn't start with a letter")

            if len(lbl) > GatewayUtils.DOMAIN_LABEL_MAX_LEN:
                return (errno.EINVAL, f"domain label {lbl} is too long")

            if not lbl[0].isalpha():
                return (errno.EINVAL, f"domain label {lbl} doesn't start with a letter")

            if lbl.endswith("-"):
                return (errno.EINVAL,
                        f"domain label {lbl} doesn't end with an alphanumeric character")
            if not lbl.replace("-", "").isalnum():
                return (errno.EINVAL,
                        f"domain label {lbl} contains a character which is "
                        f"not [a-z,A-Z,0-9,'-','.']")

        return (0, os.strerror(0))

    @staticmethod
    def is_valid_uuid(uuid_val) -> bool:
        UUID_STRING_LENGTH = len(str(uuid.uuid4()))

        if len(uuid_val) != UUID_STRING_LENGTH:
            return False

        uuid_parts = uuid_val.split("-")
        if len(uuid_parts) != 5:
            return False
        if len(uuid_parts[0]) != 8:
            return False
        if len(uuid_parts[1]) != 4:
            return False
        if len(uuid_parts[2]) != 4:
            return False
        if len(uuid_parts[3]) != 4:
            return False
        if len(uuid_parts[4]) != 12:
            return False

        for u in uuid_parts:
            try:
                int(u, 16)
            except ValueError:
                return False

        return True

    @staticmethod
    def is_valid_nqn(nqn):
        NQN_MIN_LENGTH = 11
        NQN_MAX_LENGTH = 223
        NQN_PREFIX = "nqn."
        UUID_STRING_LENGTH = len(str(uuid.uuid4()))
        NQN_UUID_PREFIX = "nqn.2014-08.org.nvmexpress:uuid:"
        NQN_UUID_PREFIX_LENGTH = len(NQN_UUID_PREFIX)

        if not isinstance(nqn, str):
            return (errno.EINVAL, f"Invalid type {type(nqn)} for NQN, must be a string")

        try:
            nqn.encode(encoding="utf-8")
        except UnicodeEncodeError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", must have an UTF-8 encoding")

        if len(nqn) < NQN_MIN_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too short, minimal length is {NQN_MIN_LENGTH}")

        if len(nqn) > NQN_MAX_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too long, maximal length is {NQN_MAX_LENGTH}")
        if GatewayUtils.is_discovery_nqn(nqn):
            # The NQN is technically valid but we will probably reject it
            # later as being a discovery one
            return (0, os.strerror(0))

        if nqn.startswith(NQN_UUID_PREFIX):
            if len(nqn) != NQN_UUID_PREFIX_LENGTH + UUID_STRING_LENGTH:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not the correct length")
            uuid_part = nqn[NQN_UUID_PREFIX_LENGTH:]
            if not GatewayUtils.is_valid_uuid(uuid_part):
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not formatted correctly")
            return (0, os.strerror(0))

        if not nqn.startswith(NQN_PREFIX):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", doesn't start with \"{NQN_PREFIX}\"")

        nqn_no_prefix = nqn[len(NQN_PREFIX):]
        date_part = nqn_no_prefix[:8]
        rev_domain_part = nqn_no_prefix[8:]
        if not date_part.endswith("."):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
        date_part = date_part[:-1]
        try:
            year_part, month_part = date_part.split("-")
            if len(year_part) != 4 or len(month_part) != 2:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
            int(year_part)
            int(month_part)
        except ValueError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")

        try:
            rev_domain_part, user_part = rev_domain_part.split(":", 1)
        except ValueError:
            return (errno.EINVAL,
                    f"Invalid NQN \"{nqn}\": must contain a user specified name "
                    f"starting with a \":\"")

        if not user_part:
            return (errno.EINVAL,
                    f"Invalid NQN \"{nqn}\": must contain a user specified name "
                    f"starting with a \":\"")

        rc = GatewayUtils.is_valid_rev_domain(rev_domain_part)
        if rc[0] != 0:
            return (errno.EINVAL,
                    f"Invalid NQN \"{nqn}\": reverse domain is not formatted correctly: {rc[1]}")

        return (0, os.strerror(0))


class GatewayUtilsCrypto:
    KEY_SIZE = 32
    INVALID_KEY_VALUE = "<invalid>"
    EXISTING_DHCHAP_KEY = "-"
    KEY_START = "-----BEGIN PRIVATE KEY-----"
    KEY_END = "-----END PRIVATE KEY-----"

    def __init__(self, encryption_key: bytes):
        if encryption_key:
            self.__secret_box = Fernet(encryption_key)
        else:
            self.__secret_box = None

    @classmethod
    def read_encryption_key(cls, keyfile: str) -> bytes:
        keyval = ""
        encoded_key = None
        # a valid key has several lines but cephadm has an issue when exporting
        # key values and will change newlines to spaces, so handle both cases
        try:
            with open(keyfile) as f:
                for line in f:
                    keyval += line.strip()
        except FileNotFoundError:
            return None

        if not keyval:
            raise RuntimeError("Invalid encryption key, key is empty")

        if not keyval.startswith(cls.KEY_START):
            raise RuntimeError("Invalid encryption key, doesn't start with start marker")
        if not keyval.endswith(cls.KEY_END):
            raise RuntimeError("Invalid encryption key, doesn't end with end marker")
        keyval = keyval.removeprefix(cls.KEY_START).removesuffix(cls.KEY_END).replace(" ", "")

        keybytes = base64.b64decode(keyval, validate=True)
        if len(keybytes) < cls.KEY_SIZE:
            raise RuntimeError(f"Encryption key has length {len(keybytes)} which is too short. "
                               f"The minimal length is {cls.KEY_SIZE}")
        encoded_key = base64.urlsafe_b64encode(keybytes[:cls.KEY_SIZE])

        return encoded_key

    def encrypt_text(self, msg: str) -> str:
        if self.__secret_box:
            encrypted = base64.b64encode(
                self.__secret_box.encrypt(msg.encode("utf-8"))).decode("utf-8")
        else:
            encrypted = msg
        return encrypted

    def decrypt_text(self, msg: str) -> str:
        plain = None
        if self.__secret_box:
            try:
                plain = self.__secret_box.decrypt(
                    base64.b64decode(msg.encode("utf-8"))).decode("utf-8")
            except cryptography.exceptions.InvalidSignature:
                plain = None
            except cryptography.fernet.InvalidToken:
                plain = None
        else:
            plain = msg
        return plain


class GatewayKeyUtils:
    MAX_PSK_KEY_NAME_LENGTH = 200     # taken from SPDK SPDK_TLS_PSK_MAX_LEN
    PSK_CRC32_SIZE_BYTES = 4
    PSK_DELIM = ":"
    PSK_PREFIX = "NVMeTLSkey-1"
    PSK_HASH_ALGORITHMS = [0, 1, 2]
    PSK_HASH_LENGTHS = [-1, 32, 48]
    MAX_DHCHAP_KEY_NAME_LENGTH = 256
    DHCHAP_CRC32_SIZE_BYTES = 4
    DHCHAP_DELIM = ":"
    DHCHAP_PREFIX = "DHHC-1"
    DHCHAP_HASH_ALGORITHMS = [0, 1, 2, 3]
    DHCHAP_HASH_LENGTHS = [-1, 32, 48, 64]

    @classmethod
    def is_valid_psk(cls, psk: str):

        failure_prefix = "Invalid PSK key"
        if not psk:
            return (errno.ENOKEY, f"{failure_prefix}: key can't be empty")

        if not isinstance(psk, str):
            return (errno.EINVAL, f"{failure_prefix}: key must be a string")

        if not psk.startswith(cls.PSK_PREFIX + cls.PSK_DELIM):
            return (errno.EINVAL,
                    f"{failure_prefix}: key must start with \"{cls.PSK_PREFIX}{cls.PSK_DELIM}\"")

        if len(psk) >= cls.MAX_PSK_KEY_NAME_LENGTH:
            return (errno.E2BIG,
                    f"{failure_prefix}: key is too long, must be shorter than "
                    f"{cls.MAX_PSK_KEY_NAME_LENGTH} characters")

        if not psk.endswith(cls.PSK_DELIM):
            return (errno.EINVAL,
                    f"{failure_prefix}: key must end with \"{cls.PSK_DELIM}\"")

        psk_parts = psk.removeprefix(
            cls.PSK_PREFIX + cls.PSK_DELIM).removesuffix(cls.PSK_DELIM).split(cls.PSK_DELIM, 1)
        if len(psk_parts) != 2:
            return (errno.EINVAL,
                    f"{failure_prefix}: should contain a \"{cls.PSK_DELIM}\" delimiter")

        if not len(psk_parts[0]):
            return (errno.EINVAL,
                    f"{failure_prefix}: missing hash")

        try:
            key_hash = int(psk_parts[0])
        except ValueError:
            return (errno.EINVAL,
                    f"{failure_prefix}: non numeric hash \"{psk_parts[0]}\"")

        if key_hash not in cls.PSK_HASH_ALGORITHMS:
            return (errno.EINVAL,
                    f"{failure_prefix}: invalid key length")

        if not len(psk_parts[1]):
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is missing")

        try:
            decoded = base64.b64decode(psk_parts[1], validate=True)
        except Error:
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is invalid")

        if not decoded:
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is missing")

        if cls.PSK_HASH_LENGTHS[key_hash] >= 0:
            if len(decoded) != cls.PSK_HASH_LENGTHS[key_hash] + cls.PSK_CRC32_SIZE_BYTES:
                return (errno.EINVAL,
                        f"{failure_prefix}: invalid key length")

        crc32_part = decoded[-cls.PSK_CRC32_SIZE_BYTES:]
        key_part = decoded[:-cls.PSK_CRC32_SIZE_BYTES]
        computed_crc32 = crc32(key_part)
        crc32_intval = int.from_bytes(crc32_part, byteorder='little', signed=False)
        if computed_crc32 != crc32_intval:
            return (errno.EINVAL,
                    f"{failure_prefix}: CRC-32 checksums mismatch")

        return (0, os.strerror(0))

    @classmethod
    def is_valid_dhchap_key(cls, dhchap_key: str, is_ctrlr: bool = False):

        ctrlr_txt = "controller " if is_ctrlr else ""
        failure_prefix = f"Invalid DH-HMAC-CHAP {ctrlr_txt}key"
        if not dhchap_key:
            return (errno.ENOKEY, f"{failure_prefix}: key can't be empty")

        if not isinstance(dhchap_key, str):
            return (errno.EINVAL, f"{failure_prefix}: key must be a string")

        if not dhchap_key.startswith(cls.DHCHAP_PREFIX + cls.DHCHAP_DELIM):
            return (errno.EINVAL,
                    f"{failure_prefix}: key must start with \"{cls.DHCHAP_PREFIX}"
                    f"{cls.DHCHAP_DELIM}\"")

        if len(dhchap_key) >= cls.MAX_DHCHAP_KEY_NAME_LENGTH:
            return (errno.E2BIG,
                    f"{failure_prefix}: key is too long, must be shorter than "
                    f"{cls.MAX_DHCHAP_KEY_NAME_LENGTH} characters")

        if not dhchap_key.endswith(cls.DHCHAP_DELIM):
            return (errno.EINVAL,
                    f"{failure_prefix}: key must end with \"{cls.DHCHAP_DELIM}\"")

        dhchap_parts = dhchap_key.removeprefix(
            cls.DHCHAP_PREFIX + cls.DHCHAP_DELIM).removesuffix(
                cls.DHCHAP_DELIM).split(cls.DHCHAP_DELIM, 1)
        if len(dhchap_parts) != 2:
            return (errno.EINVAL,
                    f"{failure_prefix}: should contain a \"{cls.DHCHAP_DELIM}\" delimiter")

        if not len(dhchap_parts[0]):
            return (errno.EINVAL,
                    f"{failure_prefix}: missing hash")

        try:
            key_hash = int(dhchap_parts[0])
        except ValueError:
            return (errno.EINVAL,
                    f"{failure_prefix}: non numeric hash \"{dhchap_parts[0]}\"")

        if key_hash not in cls.DHCHAP_HASH_ALGORITHMS:
            return (errno.EINVAL,
                    f"{failure_prefix}: invalid key length")

        if not len(dhchap_parts[1]):
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is missing")

        try:
            decoded = base64.b64decode(dhchap_parts[1], validate=True)
        except Error:
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is invalid")

        if not decoded:
            return (errno.EINVAL,
                    f"{failure_prefix}: base64 part is missing")

        if cls.DHCHAP_HASH_LENGTHS[key_hash] >= 0:
            if len(decoded) != cls.DHCHAP_HASH_LENGTHS[key_hash] + cls.DHCHAP_CRC32_SIZE_BYTES:
                return (errno.EINVAL,
                        f"{failure_prefix}: invalid key length")

        crc32_part = decoded[-cls.DHCHAP_CRC32_SIZE_BYTES:]
        key_part = decoded[:-cls.DHCHAP_CRC32_SIZE_BYTES]
        computed_crc32 = crc32(key_part)
        crc32_intval = int.from_bytes(crc32_part, byteorder='little', signed=False)
        if computed_crc32 != crc32_intval:
            return (errno.EINVAL,
                    f"{failure_prefix}: CRC-32 checksums mismatch")

        return (0, os.strerror(0))


class GatewayLogger:
    CEPH_LOG_DIRECTORY = "/var/log/ceph/"
    MAX_LOG_FILE_SIZE_DEFAULT = 10
    MAX_LOG_FILES_COUNT_DEFAULT = 20
    MAX_LOG_DIRECTORY_BACKUPS_DEFAULT = 10
    NVME_LOG_DIR_PREFIX = "nvmeof-"
    NVME_LOG_FILE_NAME = "nvmeof-log"
    NVME_GATEWAY_LOG_LEVEL_FILE_PATH = "/tmp/nvmeof-gw-loglevel"
    logger = None
    handler = None
    init_executed = False

    def __init__(self, config=None):
        if config:
            self.log_directory = config.get_with_default(
                "gateway-logs",
                "log_directory",
                GatewayLogger.CEPH_LOG_DIRECTORY)
            gateway_name = config.get("gateway", "name")
        else:
            self.log_directory = GatewayLogger.CEPH_LOG_DIRECTORY
            gateway_name = None

        if not self.log_directory.endswith("/"):
            self.log_directory += "/"

        if not gateway_name:
            gateway_name = socket.gethostname()
        self.log_directory = self.log_directory + GatewayLogger.NVME_LOG_DIR_PREFIX + gateway_name

        if GatewayLogger.logger:
            assert self.logger == GatewayLogger.logger
            if self.handler:
                return

        logging.raiseExceptions = False
        format_string = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d " \
                        "(%(process)d): %(message)s"
        date_fmt_string = "%d-%b-%Y %H:%M:%S"
        frmtr = logging.Formatter(fmt=format_string, datefmt=date_fmt_string)

        if config:
            verbose = config.getboolean_with_default(
                "gateway-logs",
                "verbose_log_messages",
                True)
            log_files_enabled = config.getboolean_with_default(
                "gateway-logs",
                "log_files_enabled",
                True)
            log_files_rotation_enabled = config.getboolean_with_default(
                "gateway-logs",
                "log_files_rotation_enabled",
                True)
            max_log_file_size = config.getint_with_default(
                "gateway-logs",
                "max_log_file_size_in_mb",
                GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT)
            max_log_files_count = config.getint_with_default(
                "gateway-logs",
                "max_log_files_count",
                GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT)
            max_log_directory_backups = config.getint_with_default(
                "gateway-logs",
                "max_log_directory_backups",
                GatewayLogger.MAX_LOG_DIRECTORY_BACKUPS_DEFAULT)
            log_level = config.get_with_default("gateway-logs", "log_level", "INFO").upper()
        else:
            verbose = True
            log_files_enabled = False
            log_files_rotation_enabled = False
            max_log_file_size = GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT
            max_log_files_count = GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT
            max_log_directory_backups = GatewayLogger.MAX_LOG_DIRECTORY_BACKUPS_DEFAULT
            log_level = "INFO"

        self.handler = None
        logdir_ok = False
        if log_files_enabled:
            GatewayLogger.rotate_backup_directories(self.log_directory, max_log_directory_backups)
            if not log_files_rotation_enabled:
                max_log_file_size = 0
                max_log_files_count = 0
            try:
                os.makedirs(self.log_directory, 0o755, True)
                logdir_ok = True
                self.handler = logging.handlers.RotatingFileHandler(
                    self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME,
                    maxBytes=max_log_file_size * 1024 * 1024,
                    backupCount=max_log_files_count)
                self.handler.setFormatter(frmtr)
                if log_files_rotation_enabled:
                    self.handler.rotator = GatewayLogger.log_file_rotate
            except Exception:
                pass

        if not verbose:
            format_string = None
        logging.basicConfig(level=log_level, format=format_string, datefmt=date_fmt_string)
        self.logger = logging.getLogger("nvmeof")
        if self.handler:
            self.logger.addHandler(self.handler)
        self.set_log_level(log_level)
        self.logger.info(f"Initialize gateway log level to \"{log_level}\"")
        GatewayLogger.logger = self.logger
        GatewayLogger.handler = self.handler
        if not GatewayLogger.init_executed:
            if log_files_enabled:
                if not logdir_ok:
                    self.logger.error(f"Failed to create directory {self.log_directory}, "
                                      f"the log wouldn't be saved to a file")
                elif not self.handler:
                    self.logger.error("Failed to set up log file handler, the log "
                                      "wouldn't be saved to a file")
                else:
                    rot_msg = ""
                    if log_files_rotation_enabled:
                        rot_msg = ", using rotation"
                    self.logger.info(f"Log files will be saved in {self.log_directory}{rot_msg}")
            else:
                self.logger.warning("Log files are disabled, the log wouldn't be saved to a file")
            GatewayLogger.init_executed = True

    def rotate_backup_directories(dirname, count):
        try:
            shutil.rmtree(dirname + f".bak{count}", ignore_errors=True)
        except Exception:
            pass
        for i in range(count, 2, -1):
            try:
                os.rename(dirname + f".bak{i - 1}", dirname + f".bak{i}")
            except Exception:
                pass
        try:
            os.rename(dirname + ".bak", dirname + ".bak2")
        except Exception:
            pass
        try:
            os.rename(dirname, dirname + ".bak")
        except Exception:
            pass

        # Just to be on the safe side, in case the rename failed
        try:
            shutil.rmtree(dirname, ignore_errors=True)
        except Exception:
            pass

    def set_log_level(self, log_level):
        if isinstance(log_level, str):
            log_level = log_level.upper()
        self.logger.setLevel(log_level)
        logger_parent = self.logger.parent
        while logger_parent:
            logger_parent.setLevel(log_level)
            logger_parent = logger_parent.parent
        for h in self.logger.handlers:
            h.setLevel(log_level)
            h.flush()

    def log_file_rotate(src, dest):
        # Files with an extension bigger than 1 are already compressed
        if dest.endswith(".1"):
            msgs, errs = GatewayLogger.compress_file(src, dest)
            if GatewayLogger.logger:
                for m in msgs:
                    GatewayLogger.logger.info(m)
                for e in errs:
                    GatewayLogger.logger.error(e)
        else:
            os.rename(src, dest)

    def compress_file(src, dest):
        msgs = []
        errs = []
        msgs.append(f"Will compress log file {src} to {dest}")
        if src == dest:
            errs.append(f"Can't compress log file {src} into the same file name")
            return msgs, errs
        try:
            os.remove(dest)
        except Exception:
            pass
        need_to_remove_dest = False
        try:
            with open(src, 'rb') as f_in:
                with gzip.open(dest, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except FileNotFoundError:
            errs.append(f"Failure compressing file {src}: file not found")
            return msgs, errs
        except Exception as ex:
            errs.append(f"Failure compressing file {src}:\n{ex}")
            need_to_remove_dest = True

        if need_to_remove_dest:
            # We ran into a problem trying to compress so need to remove
            # destination file in case one was created
            try:
                os.remove(dest)
            except Exception as ex:
                errs.append(f"Failure deleting file {dest}, ignore:\n{ex}")
            return msgs, errs

        # If we got here the compression was successful so we can delete the source file
        try:
            os.remove(src)
        except Exception as ex:
            errs.append(f"Failure deleting file {src}, ignore:\n{ex}")

        return msgs, errs

    def compress_final_log_file(self, gw_name):
        if not self.handler:
            return

        if not self.logger:
            return

        if not gw_name:
            self.logger.error("No gateway name, can't compress the log file")
            return

        if not self.log_directory.endswith(gw_name):
            self.logger.error(f"Log directory {self.log_directory} doesn't belong to gateway "
                              f"{gw_name}, do not compress log file")
            return

        self.logger.removeHandler(self.handler)
        self.handler = None
        GatewayLogger.handler = None

        dest_name = self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".gz"
        name_0 = self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".0"
        name_1 = self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".1"
        if os.access(name_1, os.F_OK) and not os.access(name_0, os.F_OK):
            dest_name = name_0

        msgs, errs = GatewayLogger.compress_file(
            self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME,
            dest_name)
        for m in msgs:
            self.logger.info(m)
        for e in errs:
            self.logger.error(e)
        self.logger = None
        GatewayLogger.logger = None


class NICS:
    def __init__(self, logger=None, handle_all=False):
        self.logger = logger
        self.ignored_device_prefixes = ('lo')
        self.addresses = {}
        self.adapters = {}
        if handle_all:
            self.ignored_device_prefixes = ()
        self._build_adapter_info()

    def _build_adapter_info(self):
        interfaces = netifaces.interfaces()
        if self.logger:
            self.logger.debug(f"Network interfaces: {interfaces}")
        for device_name in interfaces:
            if device_name.startswith(self.ignored_device_prefixes):
                continue
            try:
                nic = NIC(device_name)
            except Exception:
                if self.logger:
                    self.logger.exception(f"Error in interface {device_name}")
                continue
            if self.logger:
                self.logger.debug(f"interface {device_name}: {nic}")
            for ipv4_addr in nic.ipv4_addresses:
                self.addresses[ipv4_addr] = device_name
            for ipv6_addr in nic.ipv6_addresses:
                self.addresses[ipv6_addr] = device_name

            self.adapters[device_name] = nic

    def verify_ip_address(self, addr: str, family: str) -> bool:
        # Allow "any host" address
        if GatewayUtils.is_any_host_address(addr, family):
            return True
        family = family.lower()
        if addr not in self.addresses:
            return False
        dev_name = self.addresses[addr]
        if dev_name not in self.adapters:
            return False
        adapter = self.adapters[dev_name]
        # The local interface has a state "unknown"
        if adapter.operstate != "up" and adapter.operstate != "unknown":
            return False
        # This should be the last condition
        if family == "ipv4":
            for v4addr in adapter.ipv4_list:
                if "addr" in v4addr and v4addr["addr"] == addr:
                    return True
        elif family == "ipv6":
            for v6addr in adapter.ipv6_list:
                if "addr" in v6addr and v6addr["addr"] == addr:
                    return True
        return False

    @staticmethod
    def is_valid_subnet(subnet: str) -> bool:
        if not subnet:
            return False
        try:
            ipaddress.ip_network(subnet, strict=False)
            return True
        except Exception:
            return False

    def get_ips_in_subnet(self, subnet):
        if not subnet:
            return []
        subnet_ = ipaddress.ip_network(subnet, strict=False)
        found_ips = []
        for dev in self.adapters:
            nic = self.adapters[dev]
            if isinstance(subnet_, ipaddress.IPv4Network):
                host_ips = nic.ipv4_addresses
            elif isinstance(subnet_, ipaddress.IPv6Network):
                host_ips = nic.ipv6_addresses
            for ip in host_ips:
                if ipaddress.ip_address(ip) in subnet_:
                    found_ips.append(ip)
        return found_ips


class NIC:

    sysfs_root = '/sys/class/net'

    def __init__(self, device_name: str) -> None:
        self.device_name = device_name

        self.mac_list = ''
        self.ipv4_list = []
        self.ipv6_list = []

        self._extract_addresses()

    def _extract_addresses(self) -> None:
        addr_info = netifaces.ifaddresses(self.device_name)
        self.mac_list = addr_info.get(netifaces.AF_LINK, [])
        self.ipv4_list = addr_info.get(netifaces.AF_INET, [])
        self.ipv6_list = addr_info.get(netifaces.AF_INET6, [])

    def _read_sysfs(self, file_name: str) -> Tuple[int, str]:
        err = 0
        try:
            with open(file_name) as f:
                content = f.read().rstrip()
        except Exception:
            # log the error and the filename
            err = 1
            content = ''

        return err, content

    @property
    def operstate(self) -> str:
        err, content = self._read_sysfs(f"{NIC.sysfs_root}/{self.device_name}/operstate")
        return content if not err else ''

    @property
    def mtu(self) -> int:
        err, content = self._read_sysfs(f"{NIC.sysfs_root}/{self.device_name}/mtu")
        return int(content) if not err else 0

    @property
    def duplex(self) -> str:
        err, content = self._read_sysfs(f"{NIC.sysfs_root}/{self.device_name}/duplex")
        return content if not err else ''

    @property
    def speed(self) -> int:
        err, content = self._read_sysfs(f"{NIC.sysfs_root}/{self.device_name}/speed")
        return int(content) if not err else 0

    @property
    def mac_address(self) -> str:
        if self.mac_list:
            return self.mac_list[0].get('addr')
        else:
            return ''

    @property
    def ipv4_addresses(self) -> List[str]:
        return [ipv4_info.get('addr') for ipv4_info in self.ipv4_list]

    @property
    def ipv6_addresses(self) -> List[str]:
        # Note. ipv6 addresses are suffixed by the adapter name
        return [ipv6_info.get('addr').split('%')[0] for ipv6_info in self.ipv6_list]

    def __str__(self):
        return (
            f"Device: {self.device_name}\n"
            f"Status: {self.operstate}\n"
            f"Speed: {self.speed}\n"
            f"MTU: {self.mtu}\n"
            f"Duplex: {self.duplex}\n"
            f"MAC: {self.mac_address}\n"
            f"ip v4: {','.join(self.ipv4_addresses)}\n"
            f"ip v6: {','.join(self.ipv6_addresses)}\n"
        )


class DsaUtils:
    def __init__(self, logger):
        self.logger = logger

    def _build_dsa_config(self):
        pci_id = ("0x8086", "0x0b25")
        base_path = Path("/sys/bus/pci/devices")
        json_list = []
        count = 0

        for dev_path in base_path.iterdir():
            vendor_file = dev_path / "vendor"
            device_file = dev_path / "device"

            if not vendor_file.exists() or not device_file.exists():
                continue

            vendor = vendor_file.read_text().strip()
            device = device_file.read_text().strip()

            if (vendor, device) == pci_id:
                dsa_dev = f"dsa{count}"
                entry = {
                    "dev": dsa_dev,
                    "read_buffer_limit": 0,
                    "groups": [
                        {
                            "dev": f"group{count}.0",
                            "grouped_workqueues": [
                                {
                                    "dev": f"wq{count}.0",
                                    "mode": "dedicated",
                                    "size": 32,
                                    "group_id": 0,
                                    "priority": 10,
                                    "block_on_fault": 1,
                                    "max_batch_size": 32,
                                    "max_transfer_size": 16384,
                                    "type": "user",
                                    "driver_name": "user",
                                    "name": "app1",
                                    "threshold": 0
                                }
                            ],
                            "grouped_engines": [
                                {
                                    "dev": f"engine{count}.0",
                                    "group_id": 0
                                }
                            ]
                        },
                        {
                            "dev": f"group{count}.1",
                            "grouped_workqueues": [
                                {
                                    "dev": f"wq{count}.1",
                                    "mode": "dedicated",
                                    "size": 32,
                                    "group_id": 1,
                                    "priority": 10,
                                    "block_on_fault": 1,
                                    "max_batch_size": 32,
                                    "max_transfer_size": 2097152,
                                    "type": "user",
                                    "driver_name": "user",
                                    "name": "app2",
                                    "threshold": 0
                                }
                            ],
                            "grouped_engines": [
                                {
                                    "dev": f"engine{count}.1",
                                    "group_id": 1
                                }
                            ]
                        }
                    ]
                }
                json_list.append(entry)
                count += 1

        return json_list, count

    def _run_command(self, cmd, label):
        try:
            output = subprocess.check_output(cmd, text=True)
            self.logger.info(f"{label} Output:\n{output}")
        except subprocess.CalledProcessError:
            self.logger.exception(f"{label} failed:")

    def config(self):
        conf_file = "/tmp/dsa_config.json"
        config_data, count = self._build_dsa_config()

        if count == 0:
            self.logger.warning("No matching DSA devices found (8086:0b25).")
            return

        self.logger.info(f"Found {count} matching DSA devices. Loading config.")
        with open(conf_file, 'w') as f:
            json.dump(config_data, f, indent=2)

        try:
            commands = [
                (["accel-config", "info"], "Before config - info"),
                (["accel-config", "list"], "Before config - list"),
                (["accel-config", "load-config", "-c", conf_file, "-e"], "Load config"),
                (["accel-config", "info"], "After config - info"),
                (["accel-config", "list"], "After config - list"),
            ]

            for cmd, label in commands:
                self._run_command(cmd, label)
        finally:
            try:
                os.remove(conf_file)
                self.logger.info(f"Removed DSA config file: {conf_file}")
            except OSError as e:
                # ENOENT is fine
                if e.errno != errno.ENOENT:
                    self.logger.exception(f"Error removing DSA config file {conf_file}")
