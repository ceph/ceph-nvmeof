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
import configparser
from pydantic import ValidationError
from .server import GatewayServer
from .config import GatewayConfig

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
    conf = configparser.ConfigParser()
    file_list = conf.read(args.config)
    if not file_list:
        logger.error(f"Config file empty or not found: {args.config}")
        exit()

    # Validate config
    try:
        config = GatewayConfig(**conf)
    except ValidationError as e:
        logger.error(f"Invalid config file: {e}")
        exit()

    with GatewayServer(config) as gateway:
        gateway.serve()
        gateway.keep_alive()
