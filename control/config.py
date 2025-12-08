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

    def is_param_defined(self, section, param):
        if self.config.has_section(section):
            return self.config.has_option(section, param)
        return False

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
