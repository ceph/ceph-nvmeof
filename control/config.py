#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import configparser


class NVMeGWConfig:
    def __init__(self, gw_config_filename):
        self.nvme_gw_config = configparser.ConfigParser()
        self.nvme_gw_config.read(gw_config_filename)

    def get(self, section, param):
        return self.nvme_gw_config.get(section, param)

    def getboolean(self, section, param):
        return self.nvme_gw_config.getboolean(section, param)

    def getint(self, section, param):
        return self.nvme_gw_config.getint(section, param)

    def getfloat(self, section, param):
        return self.nvme_gw_config.getfloat(section, param)

    def get_with_default(self, section, param, value):
        return self.nvme_gw_config.get(section, param, fallback=value)

    def getboolean_with_default(self, section, param, value):
        return self.nvme_gw_config.getboolean(section, param, fallback=value)

    def getint_with_default(self, section, param, value):
        return self.nvme_gw_config.getint(section, param, fallback=value)

    def getfloat_with_default(self, section, param, value):
        return self.nvme_gw_config.getfloat(section, param, fallback=value)
