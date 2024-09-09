#
#  Copyright (c) 2024 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gbregman@ibm.com
#

import uuid
import errno
import rbd
import rados
import time
from .utils import GatewayLogger

class CephUtils:
    """Miscellaneous functions which connect to Ceph
    """

    def __init__(self, config):
        self.logger = GatewayLogger(config).logger
        self.ceph_conf = config.get_with_default("ceph", "config_file", "/etc/ceph/ceph.conf")
        self.rados_id = config.get_with_default("ceph", "id", "")
        self.anagroup_list = []
        self.last_sent = time.time()

    def execute_ceph_monitor_command(self, cmd):
         self.logger.debug(f"Execute monitor command: {cmd}")
         with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            rply = cluster.mon_command(cmd, b'')
            self.logger.debug(f"Monitor reply: {rply}")
            return rply

    def get_number_created_gateways(self, pool, group):
        now = time.time()
        if (now - self.last_sent) < 10 and self.anagroup_list :
             self.logger.info(f"Caching response of the monitor: {self.anagroup_list}")
             return self.anagroup_list
        else :
            try:
                self.anagroup_list = []
                self.last_sent = now
                str = '{' + f'"prefix":"nvme-gw show", "pool":"{pool}", "group":"{group}"' + '}'
                self.logger.debug(f"nvme-show string: {str}")
                rply = self.execute_ceph_monitor_command(str)
                self.logger.debug(f"reply \"{rply}\"")
                conv_str = rply[1].decode()
                pos = conv_str.find("[")
                if pos != -1:
                    new_str = conv_str[pos + len("[") :]
                    pos     = new_str.find("]")
                    new_str = new_str[: pos].strip()
                    int_str_list = new_str.split(' ')
                    self.logger.debug(f"new_str : {new_str}")
                    for x in int_str_list:
                        self.anagroup_list.append(int(x))
                    self.logger.info(f"ANA group list: {self.anagroup_list}")
                else:
                    self.logger.warning("GWs not found")

            except Exception:
                self.logger.exception(f"Failure get number created gateways:")
                self.anagroup_list = []

            return self.anagroup_list

    def fetch_and_display_ceph_version(self):
        try:
            rply = self.execute_ceph_monitor_command('{"prefix":"mon versions"}')
            ceph_ver = rply[1].decode().removeprefix("{").strip().split(":")[0].removeprefix('"').removesuffix('"')
            ceph_ver = ceph_ver.removeprefix("ceph version ")
            self.logger.info(f"Connected to Ceph with version \"{ceph_ver}\"")
        except Exception:
            self.logger.exception(f"Failure fetching Ceph version:")
            pass

    def fetch_ceph_fsid(self) -> str:
        fsid = None
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                fsid = cluster.get_fsid()
        except Exception:
            self.logger.exception(f"Failure fetching Ceph fsid:")

        return fsid

    def pool_exists(self, pool) -> bool:
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                if cluster.pool_exists(pool):
                    return True
        except Exception:
            self.logger.exception(f"Can't check if pool {pool} exists, assume it does")
            return True

        return False

    def service_daemon_register(self, cluster, metadata):
        try:
            if cluster: # rados client 
                daemon_name = metadata['id']
                cluster.service_daemon_register("nvmeof", daemon_name, metadata)
                self.logger.info(f"Registered {daemon_name} to service_map!")
        except Exception:
            self.logger.exception(f"Can't register daemon to service_map!")

    def service_daemon_update(self, cluster, status_buffer):
        try:
            if cluster and status_buffer:
                cluster.service_daemon_update(status_buffer)
        except Exception:
            self.logger.exception(f"Can't update daemon status to service_map!") 

    def create_image(self, pool_name, image_name, size) -> bool:
        # Check for pool existence in advance as we don't create it if it's not there
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno = errno.ENODEV)

        image_exists = False
        try:
            image_size = self.get_image_size(pool_name, image_name)
            image_exists = True
        except rbd.ImageNotFound:
            self.logger.debug(f"Image {pool_name}/{image_name} doesn't exist, will create it using size {size}")
            pass

        if image_exists:
            if image_size != size:
                raise rbd.ImageExists(f"Image {pool_name}/{image_name} already exists with a size of {image_size} bytes which differs from the requested size of {size} bytes",
                                      errno = errno.EEXIST)
            return False    # Image exists with an idetical size, there is nothing to do here

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                try:
                    rbd_inst.create(ioctx, image_name, size)
                except rbd.ImageExists as ex:
                    self.logger.exception(f"Image {pool_name}/{image_name} was created just now")
                    raise rbd.ImageExists(f"Image {pool_name}/{image_name} was just created by someone else, please retry",
                                          errno = errno.EAGAIN)
                except Exception as ex:
                    self.logger.exception(f"Can't create image {pool_name}/{image_name}")
                    raise ex

        return True

    def get_image_size(self, pool_name, image_name) -> int:
        image_size = 0
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno = errno.ENODEV)

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        image_size = img.size()
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {pool_name}/{image_name} doesn't exist", errno = errno.ENODEV)
                except Exception as ex:
                    self.logger.exception(f"Error while trying to get the size of image {pool_name}/{image_name}")
                    raise ex

        return image_size

    def get_rbd_exception_details(self, ex):
        ex_details = (None, None)
        if rbd.OSError in type(ex).__bases__:
            msg = str(ex).strip()
            # remove the [errno] part
            if msg.startswith("["):
                pos = msg.find("]")
                if pos >= 0:
                    msg = msg[pos + 1 :].strip()
            ex_details = (ex.errno, msg)
        return ex_details
