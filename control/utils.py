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
import socket
import logging
import logging.handlers
import gzip
import shutil

class GatewayEnumUtils:
    def get_value_from_key(e_type, keyval, ignore_case = False):
        val = None
        try:
            key_index = e_type.keys().index(keyval)
            val = e_type.values()[key_index]
        except ValueError:
            pass
        except IndexError:
            pass

        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.lower(), False)
        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.upper(), False)

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

    # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
    def escape_address_if_ipv6(addr) -> str:
        ret_addr = addr
        if ":" in addr and not addr.strip().startswith("["):
            ret_addr = f"[{addr}]"
        return ret_addr

    def is_discovery_nqn(nqn) -> bool:
        return nqn == GatewayUtils.DISCOVERY_NQN

    def is_valid_rev_domain(rev_domain):
        DOMAIN_LABEL_MAX_LEN = 63

        domain_parts = rev_domain.split(".")
        for lbl in domain_parts:
            if not lbl:
                return (errno.EINVAL, f"empty domain label doesn't start with a letter")

            if len(lbl) > DOMAIN_LABEL_MAX_LEN:
                return (errno.EINVAL, f"domain label {lbl} is too long")

            if not lbl[0].isalpha():
                return (errno.EINVAL, f"domain label {lbl} doesn't start with a letter")

            if lbl.endswith("-"):
                return (errno.EINVAL, f"domain label {lbl} doesn't end with an alphanumeric character")
            if not lbl.replace("-", "").isalnum():
                return (errno.EINVAL, f"domain label {lbl} contains a character which is not [a-z,A-Z,0-9,'-','.']")

        return (0, os.strerror(0))

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
                n = int(u, 16)
            except ValueError:
                return False

        return True

    def is_valid_nqn(nqn):
        NQN_MIN_LENGTH = 11
        NQN_MAX_LENGTH = 223
        NQN_PREFIX = "nqn."
        UUID_STRING_LENGTH = len(str(uuid.uuid4()))
        NQN_UUID_PREFIX = "nqn.2014-08.org.nvmexpress:uuid:"
        NQN_UUID_PREFIX_LENGTH = len(NQN_UUID_PREFIX)

        if type(nqn) != str:
            return (errno.EINVAL, f"Invalid type {type(nqn)} for NQN, must be a string")

        try:
            b = nqn.encode(encoding="utf-8")
        except UnicodeEncodeError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", must have an UTF-8 encoding")

        if len(nqn) < NQN_MIN_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too short, minimal length is {NQN_MIN_LENGTH}")

        if len(nqn) > NQN_MAX_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too long, maximal length is {NQN_MAX_LENGTH}")
        if GatewayUtils.is_discovery_nqn(nqn):
            # The NQN is technically valid but we will probably reject it later as being a discovery one
            return (0, os.strerror(0))

        if nqn.startswith(NQN_UUID_PREFIX):
            if len(nqn) != NQN_UUID_PREFIX_LENGTH + UUID_STRING_LENGTH:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not the correct length")
            uuid_part = nqn[NQN_UUID_PREFIX_LENGTH : ]
            if not GatewayUtils.is_valid_uuid(uuid_part):
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not formatted correctly")
            return (0, os.strerror(0))

        if not nqn.startswith(NQN_PREFIX):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", doesn't start with \"{NQN_PREFIX}\"")

        nqn_no_prefix = nqn[len(NQN_PREFIX) : ]
        date_part = nqn_no_prefix[ : 8]
        rev_domain_part = nqn_no_prefix[8 : ]
        if not date_part.endswith("."):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
        date_part = date_part[ : -1]
        try:
            year_part, month_part = date_part.split("-")
            if len(year_part) != 4 or len(month_part) != 2:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
            n = int(year_part)
            n = int(month_part)
        except ValueError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")

        try:
            rev_domain_part, user_part = rev_domain_part.split(":", 1)
        except ValueError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": must contain a user specified name starting with a \":\"")

        if not user_part:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": must contain a user specified name starting with a \":\"")

        rc = GatewayUtils.is_valid_rev_domain(rev_domain_part)
        if rc[0] != 0:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": reverse domain is not formatted correctly: {rc[1]}")

        return (0, os.strerror(0))

class GatewayLogger:
    CEPH_LOG_DIRECTORY = "/var/log/ceph/"
    MAX_LOG_FILE_SIZE_DEFAULT = 10
    MAX_LOG_FILES_COUNT_DEFAULT = 20
    NVME_LOG_DIR_PREFIX = "nvmeof-"
    NVME_LOG_FILE_NAME = "nvmeof-log"
    logger = None
    handler = None
    init_executed = False

    def __init__(self, config=None):
        if config:
            self.log_directory = config.get_with_default("gateway", "log_directory", GatewayLogger.CEPH_LOG_DIRECTORY)
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

        format_string = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d: %(message)s"
        date_fmt_string = "%d-%b-%Y %H:%M:%S"
        frmtr = logging.Formatter(fmt=format_string, datefmt=date_fmt_string)

        if config:
            verbose = config.getboolean_with_default("gateway", "verbose_log_messages", True)
            log_files_enabled = config.getboolean_with_default("gateway", "log_files_enabled", True)
            log_files_rotation_enabled = config.getboolean_with_default("gateway", "log_files_rotation_enabled", True)
            max_log_file_size = config.getint_with_default("gateway", "max_log_file_size_in_mb", GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT)
            max_log_files_count = config.getint_with_default("gateway", "max_log_files_count", GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT)
            log_level = config.get_with_default("gateway", "log_level", "info")
        else:
            verbose = True
            log_files_enabled = False
            log_files_rotation_enabled = False
            max_log_file_size = GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT
            max_log_files_count = GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT
            log_leGatewayLoggervel = "info"

        self.handler = None
        logdir_ok = False
        if log_files_enabled:
            GatewayLogger.rotate_backup_directories(self.log_directory, 5)
            if not log_files_rotation_enabled:
                max_log_file_size = 0
                max_log_files_count = 0
            try:
                os.makedirs(self.log_directory, 0o777, True)
                logdir_ok = True
                self.handler = logging.handlers.RotatingFileHandler(self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME,
                                                 maxBytes = max_log_file_size * 1024 * 1024,
                                                 backupCount = max_log_files_count)
                self.handler.setFormatter(frmtr)
                if log_files_rotation_enabled:
                    self.handler.rotator = GatewayLogger.log_file_rotate
            except Exception:
                pass

        if not verbose:
            format_string = None
        logging.basicConfig(level=GatewayLogger.get_log_level(log_level), format=format_string, datefmt=date_fmt_string)
        self.logger = logging.getLogger("nvmeof")
        if self.handler:
            self.logger.addHandler(self.handler)
        GatewayLogger.logger = self.logger
        GatewayLogger.handler = self.handler
        if not GatewayLogger.init_executed:
            if log_files_enabled:
                if not logdir_ok:
                    self.logger.error(f"Failed to create directory {self.log_directory}, the log wouldn't be saved to a file")
                elif not self.handler:
                    self.logger.error(f"Failed to set up log file handler, the log wouldn't be saved to a file")
                else:
                    rot_msg = ""
                    if log_files_rotation_enabled:
                        rot_msg = ", using rotation"
                    self.logger.info(f"Log files will be saved in {self.log_directory}{rot_msg}")
            else:
                self.logger.warning(f"Log files are disabled, the log wouldn't be saved to a file")
            GatewayLogger.init_executed = True

    def rotate_backup_directories(dirname, count):
        try:
            shutil.rmtree(dirname + f".bak{count}", ignore_errors = True)
        except Exception:
            pass
        for i in range(count, 2, -1):
            try:
                os.rename(dirname + f".bak{i - 1}", dirname + f".bak{i}")
            except Exception:
                pass
        try:
            os.rename(dirname + f".bak", dirname + f".bak2")
        except Exception:
            pass
        try:
            os.rename(dirname, dirname + f".bak")
        except Exception:
            pass

        # Just to be on the safe side, in case the rename failed
        try:
            shutil.rmtree(dirname, ignore_errors = True)
        except Exception:
            pass

    def get_log_level(log_level):
        if type(log_level) == int:
            return log_level
        assert type(log_level) == str
        if log_level.upper() == "DEBUG":
            return logging.DEBUG
        elif log_level.upper() == "INFO":
            return logging.INFO
        elif log_level.upper() == "WARNING":
            return logging.WARNING
        elif log_level.upper() == "ERROR":
            return logging.ERROR
        elif log_level.upper() == "CRITICAL":
            return logging.CRITICAL
        elif log_level.upper() == "NOTSET":
            return logging.NOTSET
        else:
            assert False

    def set_log_level(self, log_level):
        log_level = GatewayLogger.get_log_level(log_level)
        self.logger.setLevel(log_level)

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
            # We ran into a problem trying to compress so need to remove destination file in case one was created
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
            self.logger.error(f"No gateway name, can't compress the log file")
            return

        if not self.log_directory.endswith(gw_name):
            self.logger.error(f"Log directory {self.log_directory} doesn't belong to gateway {gw_name}, do not compress log file")
            return

        self.logger.removeHandler(self.handler)
        self.handler = None
        GatewayLogger.handler = None

        dest_name = self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".gz"
        if os.access(self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".1",
                     os.F_OK) and not os.access(self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".0",
                     os.F_OK):
            dest_name = self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME + ".0"

        msgs, errs = GatewayLogger.compress_file(self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME, dest_name)
        for m in msgs:
            self.logger.info(m)
        for e in errs:
            self.logger.error(e)
        self.logger = None
        GatewayLogger.logger = None
