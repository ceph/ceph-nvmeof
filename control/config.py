#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import configparser

class GatewayConfig:
    """Loads and returns config file settings.

    Instance attributes:
        config: Config parser object
    """

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
