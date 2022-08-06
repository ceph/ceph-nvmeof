#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import logging
import argparse
from .server import GatewayServer
from .config import NVMeGWConfig

if __name__ == '__main__':
    # Set up root logger
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(prog="python3 -m control",
                                     description="Manage NVMe gateways")
    parser.add_argument(
        "-c",
        "--config",
        default="ceph-nvmeof.conf",
        type=str,
        help="Path to config file",
    )
    args = parser.parse_args()
    
    config = NVMeGWConfig(args.config)
    with GatewayServer(config) as gateway:
        gateway.serve()
