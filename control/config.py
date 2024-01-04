#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import configparser
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

class GatewayConfig:
    """Loads and returns config file settings.

    Instance attributes:
        config: Config parser object
    """

    DISCOVERY_NQN = "nqn.2014-08.org.nvmexpress.discovery"
    CEPH_RUN_DIRECTORY = "/var/run/ceph/"

    def __init__(self, conffile):
        self.filepath = conffile
        self.conffile_logged = False
        with open(conffile) as f:
            self.config = configparser.ConfigParser()
            self.config.read_file(f)

    def get(self, section, param):
        return self.config.get(section, param)

    def getboolean(self, section, param):
        return self.config.getboolean(section, param)

    def getint(self, section, param):
        return self.config.getint(section, param)

    def getfloat(self, section, param):
        return self.config.getfloat(section, param)

    def get_with_default(self, section, param, value):
        return self.config.get(section, param, fallback=value)

    def getboolean_with_default(self, section, param, value):
        return self.config.getboolean(section, param, fallback=value)

    def getint_with_default(self, section, param, value):
        return self.config.getint(section, param, fallback=value)

    def getfloat_with_default(self, section, param, value):
        return self.config.getfloat(section, param, fallback=value)

    def dump_config_file(self, logger):
        if self.conffile_logged:
            return

        try:
            logger.info(f"Using configuration file {self.filepath}")
            with open(self.filepath) as f:
                logger.info(
                    f"====================================== Configuration file content ======================================")
                for line in f:
                    line = line.rstrip()
                    logger.info(f"{line}")
                logger.info(
                    f"========================================================================================================")
                self.conffile_logged = True
        except Exception:
            pass

    # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
    def escape_address_if_ipv6(addr) -> str:
        ret_addr = addr
        if ":" in addr and not addr.strip().startswith("["):
            ret_addr = f"[{addr}]"
        return ret_addr

class GatewayLogger:
    CEPH_LOG_DIRECTORY = "/var/log/ceph/"
    MAX_LOG_FILE_SIZE_DEFAULT = 10
    MAX_LOG_FILES_COUNT_DEFAULT = 20
    NVME_LOG_DIR_PREFIX = "nvmeof-"
    NVME_LOG_FILE_NAME = "nvmeof-log"
    logger = None
    handler = None

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

        frmtr = logging.Formatter(fmt='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d: %(message)s')
        frmtr.default_msec_format = None

        if config:
            log_files_enabled = config.getboolean_with_default("gateway", "log_files_enabled", True)
            log_files_rotation_enabled = config.getboolean_with_default("gateway", "log_files_rotation_enabled", True)
            max_log_file_size = config.getint_with_default("gateway", "max_log_file_size_in_mb", GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT)
            max_log_files_count = config.getint_with_default("gateway", "max_log_files_count", GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT)
            log_level = config.get_with_default("gateway", "log_level", "info")
        else:
            log_files_enabled = False
            log_files_rotation_enabled = False
            max_log_file_size = GatewayLogger.MAX_LOG_FILE_SIZE_DEFAULT
            max_log_files_count = GatewayLogger.MAX_LOG_FILES_COUNT_DEFAULT
            log_leGatewayLoggervel = "info"

        self.handler = None
        if log_files_enabled:
            GatewayLogger.rotate_backup_directories(self.log_directory, 5)
            if not log_files_rotation_enabled:
                max_log_file_size = 0
                max_log_files_count = 0
            try:
                os.makedirs(self.log_directory, 0o777, True)
                self.handler = logging.handlers.RotatingFileHandler(self.log_directory + "/" + GatewayLogger.NVME_LOG_FILE_NAME,
                                                 maxBytes = max_log_file_size * 1024 * 1024,
                                                 backupCount = max_log_files_count)
                self.handler.setFormatter(frmtr)
                if log_files_rotation_enabled:
                    self.handler.rotator = GatewayLogger.log_file_rotate
            except Exception:
                pass

        logging.basicConfig(level=GatewayLogger.get_log_level(log_level))
        self.logger = logging.getLogger("nvmeof")
        if self.handler:
            self.logger.addHandler(self.handler)
        GatewayLogger.logger = self.logger
        GatewayLogger.handler = self.handler

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
