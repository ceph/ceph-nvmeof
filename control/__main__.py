#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import argparse
import signal
from .server import GatewayServer
from .config import GatewayConfig
from .utils import GatewayLogger

gw = None
gw_logger = None
gw_name = None

def sigterm_handler(signum, frame):
    if gw and gw.omap_state:
        gw.omap_state.cleanup_omap()
    if gw_logger and gw_name:
        gw_logger.compress_final_log_file(gw_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="python3 -m control",
                                     description="Manage NVMe gateways",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-c",
        "--config",
        default="ceph-nvmeof.conf",
        type=str,
        help="Path to config file",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, sigterm_handler)

    config = GatewayConfig(args.config)
    gw_logger = GatewayLogger(config)
    config.display_environment_info(gw_logger.logger)
    config.dump_config_file(gw_logger.logger)
    with GatewayServer(config) as gateway:
        gw = gateway
        gw_name = gateway.name
        gateway.serve()
        gateway.keep_alive()
