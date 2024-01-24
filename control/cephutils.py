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
from .utils import GatewayLogger

class CephUtils:
    """Miscellaneous functions which connect to Ceph
    """

    def __init__(self, config):
        self.logger = GatewayLogger(config).logger
        self.ceph_conf = config.get_with_default("ceph", "config_file", "/etc/ceph/ceph.conf")
        self.rados_id = config.get_with_default("ceph", "id", "")

    def fetch_and_display_ceph_version(self):
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                rply = cluster.mon_command('{"prefix":"mon versions"}', b'')
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

    def create_image(self, pool_name, image_name, size) -> bool:
        rc_ex = None
        rc = False
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno = errno.ENODEV)

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                rbd_inst = rbd.RBD()
                try:
                    image_size = 0
                    images = rbd_inst.list(ioctx)
                    if image_name in images:
                        try:
                            with rbd.Image(ioctx, image_name) as img:
                                img_stat = img.stat()
                                image_size = img_stat["size"]
                        except Exception as ex:
                            self.logger.exception(f"Can't get image object for {image_name}")
                            rc_ex = ex
                        if image_size != size:
                            rc_ex = rbd.ImageExists(f"Image {pool_name}/{image_name} already exists with a size of {image_size} bytes which differs from the requested size of {size} bytes",
                                                  errno = errno.EEXIST)
                    else:
                        rbd_inst.create(ioctx, image_name, size)
                        rc = True
                except rbd.ImageExists:
                    return False
                except Exception as ex:
                    self.logger.exception(f"Can't create image {image_name} in pool {pool_name}")
                    rc_ex = ex

        if rc_ex != None:
            raise rc_ex

        return rc

