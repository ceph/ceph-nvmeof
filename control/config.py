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
    def __init__(self, conffile):
        self.config = configparser.ConfigParser()
        self.config.read(conffile)

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
