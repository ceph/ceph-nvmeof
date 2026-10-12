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

CONFIG_TYPE_UNKNOWN = 0
CONFIG_TYPE_INT = 1
CONFIG_TYPE_FLOAT = 2
CONFIG_TYPE_BOOL = 3
CONFIG_TYPE_STR = 4

_MISSING = object()
_TRUE = {"1", "yes", "true", "on"}
_FALSE = {"0", "no", "false", "off"}


class GatewayConfig:
    """Loads and returns config file settings.

    Instance attributes:
        config: Config parser object
    """

    CEPH_RUN_DIRECTORY = "/var/run/ceph/"

    def __init__(self, conffile):
        self.filepath = conffile
        self.conffile_logged = False
        self.env_shown = False
        with open(conffile) as f:
            self.config = configparser.ConfigParser()
            self.config.read_file(f)
        # Values from the latest accepted snapshot. Absent means the conf file.
        self._overrides = {}

    def is_param_defined(self, section, param):
        if self.config.has_section(section):
            return self.config.has_option(section, param)
        return False

    def _lookup(self, section, param):
        return self._overrides.get((section, param), _MISSING)

    def get(self, section, param):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return str(found)
        return self.config.get(section, param)

    def getboolean(self, section, param):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return found if isinstance(found, bool) else self._parse_bool(found)
        return self.config.getboolean(section, param)

    def getint(self, section, param):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return int(found)
        return self.config.getint(section, param)

    def getfloat(self, section, param):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return float(found)
        return self.config.getfloat(section, param)

    def get_with_default(self, section, param, value):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return str(found)
        return self.config.get(section, param, fallback=value)

    def getboolean_with_default(self, section, param, value):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return found if isinstance(found, bool) else self._parse_bool(found)
        return self.config.getboolean(section, param, fallback=value)

    def getint_with_default(self, section, param, value):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return int(found)
        return self.config.getint(section, param, fallback=value)

    def getfloat_with_default(self, section, param, value):
        found = self._lookup(section, param)
        if found is not _MISSING:
            return float(found)
        return self.config.getfloat(section, param, fallback=value)

    @staticmethod
    def _parse_bool(text):
        lowered = str(text).strip().lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ValueError(f"invalid bool {text}")

    def _parse(self, kind, text):
        if kind == CONFIG_TYPE_INT:
            # int("1.0") is rejected; a float text is not an int option.
            return int(str(text).strip(), 10)
        if kind == CONFIG_TYPE_FLOAT:
            return float(str(text).strip())
        if kind == CONFIG_TYPE_BOOL:
            return self._parse_bool(text)
        if kind == CONFIG_TYPE_STR:
            return str(text)
        raise ValueError("unknown config type")

    def _apply_entry(self, entry, handler):
        section = entry.section
        key = entry.key
        if not section or not key:
            raise ValueError("missing section or key")
        if entry.type not in (CONFIG_TYPE_INT, CONFIG_TYPE_FLOAT,
                              CONFIG_TYPE_BOOL, CONFIG_TYPE_STR):
            raise ValueError("unknown config type")
        if not entry.present and not self.is_param_defined(section, key):
            self._overrides.pop((section, key), None)
            return
        if entry.present:
            text = entry.value
        else:
            text = self.config.get(section, key)
        value = self._parse(entry.type, text)
        handler(section, key, value)
        if entry.present:
            self._overrides[(section, key)] = value
        else:
            self._overrides.pop((section, key), None)

    def apply_snapshot(self, entries, handler):
        """Apply each entry. A failure rejects that entry and keeps the last value."""
        rejects = []
        for entry in entries:
            try:
                self._apply_entry(entry, handler)
            except Exception as exc:
                rejects.append((entry.section, entry.key, str(exc)))
        return rejects

    def dump_config_file(self, logger):
        if self.conffile_logged:
            return

        try:
            logger.info(f"Using configuration file {self.filepath}")
            with open(self.filepath) as f:
                logger.info(
                    "====================================== Configuration file content "
                    "======================================")
                for line in f:
                    line = line.rstrip()
                    logger.info(f"{line}")
                logger.info(
                    "========================================================="
                    "===============================================")
                self.conffile_logged = True
        except Exception:
            pass

    def display_environment_info(self, logger):
        if self.env_shown:
            return

        ver = os.getenv("NVMEOF_VERSION")
        if ver:
            logger.info(f"Using NVMeoF gateway version {ver}")
        spdk_ver = os.getenv("NVMEOF_SPDK_VERSION")
        if spdk_ver:
            logger.info(f"Configured SPDK version {spdk_ver}")
        ceph_ver = os.getenv("NVMEOF_CEPH_VERSION")
        if ceph_ver:
            logger.info(f"Using vstart cluster version based on {ceph_ver}")
        build_date = os.getenv("BUILD_DATE")
        if build_date:
            logger.info(f"NVMeoF gateway built on: {build_date}")
        git_rep = os.getenv("NVMEOF_GIT_REPO")
        if git_rep:
            logger.info(f"NVMeoF gateway Git repository: {git_rep}")
        git_branch = os.getenv("NVMEOF_GIT_BRANCH")
        if git_branch:
            logger.info(f"NVMeoF gateway Git branch: {git_branch}")
        git_commit = os.getenv("NVMEOF_GIT_COMMIT")
        if git_commit:
            logger.info(f"NVMeoF gateway Git commit: {git_commit}")
        git_modified = os.getenv("NVMEOF_GIT_MODIFIED_FILES")
        if git_modified:
            logger.info(f"NVMeoF gateway uncommitted modified files: {git_modified}")
        git_spdk_rep = os.getenv("SPDK_GIT_REPO")
        if git_spdk_rep:
            logger.info(f"SPDK Git repository: {git_spdk_rep}")
        git_spdk_branch = os.getenv("SPDK_GIT_BRANCH")
        if git_spdk_branch:
            logger.info(f"SPDK Git branch: {git_spdk_branch}")
        git_spdk_commit = os.getenv("SPDK_GIT_COMMIT")
        if git_spdk_commit:
            logger.info(f"SPDK Git commit: {git_spdk_commit}")
        self.env_shown = True
