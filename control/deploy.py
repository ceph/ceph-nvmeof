import logging
import argparse
#from .server import GatewayServer
from .config import GatewayConfig

import time
import threading
import rados
import errno
from typing import Dict
from collections import defaultdict
from abc import ABC, abstractmethod



class RadosConn():

  def __init__(self, config):
        self.config = config
       # self.version = 1
        self.logger = logging.getLogger(__name__)
        self.watch = None

       # self.ceph_fsid = None

        try:
            self.ioctx = self.open_rados_connection(self.config)
            # Create a new gateway persistence OMAP object
            with rados.WriteOpCtx() as write_op:
                # Set exclusive parameter to fail write_op if object exists
                write_op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
                #self.ioctx.set_omap(write_op, (self.OMAP_VERSION_KEY,)     (str(self.version),))
                # self.ioctx.operate_write_op(write_op, self.omap_name)
                # self.logger.info(    f"First gateway: created object {self.omap_name}")
        except rados.ObjectExists:
            self.logger.info(f"{self.omap_name} omap object already exists.")
        except Exception as ex:
            self.logger.error(f"Unable to create omap: {ex}. Exiting!")
            raise

  def __exit__(self, exc_type, exc_value, traceback):
        if self.watch is not None:
            self.watch.close()
        self.ioctx.close()

    # def fetch_and_display_ceph_version(self, conn):
    #     try:
    #         rply = conn.mon_command('{"prefix":"mon versions"}', b'')
    #         ceph_ver = rply[1].decode().removeprefix("{").strip().split(":")[0].removeprefix('"').removesuffix('"')
    #         ceph_ver = ceph_ver.removeprefix("ceph version ")
    #         self.logger.info(f"Connected to Ceph with version \"{ceph_ver}\"")
    #     except Exception as ex:
    #         self.logger.debug(f"Got exception trying to fetch Ceph version: {ex}")
    #         pass
  def open_rados_connection(self, config):
        ceph_pool = config.get("ceph", "pool")
        ceph_conf = config.get("ceph", "config_file")
        rados_id = config.get_with_default("ceph", "id", "")
        conn = rados.Rados(conffile=ceph_conf, rados_id=rados_id)
        conn.connect()
        print ("connected!")
        self.conn = conn
       # self.fetch_and_display_ceph_version(conn)
        ioctx = conn.open_ioctx(ceph_pool)
        return ioctx




if __name__ == '__main__':
    # Set up root logger
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    print("deploy started ")
    parser = argparse.ArgumentParser(prog="python3 -m control.deploy",
                                     description="sends Gateway deployment command to monitor",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-c",
        "--config",
        default="ceph-nvmeof.conf",
        type=str,
        help="Path to config file",
    )
    parser.add_argument(
        "-D",
        "--deployment",
        type=str,
        help="Create set of Gateway names",
    )
    subparsers = parser.add_subparsers(dest="subcommand")
    valid_commands = ['create', 'delete']
    for cmd in valid_commands:
        subparsers.add_parser(cmd, help=f'{cmd} nvmeof gateway deployement')

    args = parser.parse_args()
    config = GatewayConfig(args.config)
    gw_str = args.deployment
    cmd = args.subcommand
    if cmd not in valid_commands:
        print(f'Valid commands are {valid_commands}. Use --help for usage information.')
        raise SystemExit

    print (f"{config=} {cmd=} {gw_str=}")
    rads = RadosConn(config)
    rads.conn.mon_command('{"prefix":"nvme-gw ' + cmd + '","ids":[' + gw_str + ']}', b'')
