#
#  Copyright (c) 2024 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gbregman@ibm.com
#
import errno
import os
import rbd
import rados
import time
import json
from enum import Enum
from .utils import GatewayLogger
from .proto import gateway_pb2 as pb2


class CephUtils:
    """Miscellaneous functions which connect to Ceph
    """

    class CephPoolType(Enum):
        INVALID = -1
        REPLICATED = 1
        RAID4 = 2
        ERASURE = 3

    RBD_QOS_PREFIX = "rbd_qos_"
    RBD_QOS_SUFFIX = "_limit"
    # if these values are changed we need to update SPDK as well
    METADATA_KEY_AUTO_RESIZE = "NVME_GATEWAY_AUTO_RESIZE"
    METADATA_VALUE_NO_AUTO_RESIZE = "no"
    METADATA_KEY_IMAGE_ID = "NVME_IMAGE_IDENTIFICATION"

    def __init__(self, config):
        self.logger = GatewayLogger(config).logger
        self.ceph_conf = config.get_with_default("ceph", "config_file", "/etc/ceph/ceph.conf")
        self.rados_id = config.get_with_default("ceph", "id", "")
        self.anagroup_list = []
        self.rebalance_supported = False
        self.rebalance_ana_group = 0
        self.num_gws = 0
        self.last_sent = time.time()
        self.ana_group_to_location: dict = {}
        self.gw_id_to_location: dict = {}

    def execute_ceph_monitor_command(self, cmd):
        self.logger.debug(f"Execute monitor command: {cmd}")
        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            rply = cluster.mon_command(cmd, b'')
        self.logger.debug(f"Monitor reply: {rply}")
        if not rply or len(rply) != 3:
            self.logger.error(f"Invalid Ceph reply: {rply} for command \"{cmd}\"")
            return (-errno.EINVAL, b'', '')
        if rply[0] != 0:
            self.logger.warning(f"Got error {-rply[0]} ({os.strerror(-rply[0])}) from Ceph: "
                                f"{rply[2]}\nCommand was \"{cmd}\"")
        return rply

    def get_gw_listeners(self, pool, group) -> list:
        try:
            str = '{' + f'"prefix":"nvme-gw listeners", "pool":"{pool}", "group":"{group}"' + '}'
            self.logger.debug(f"nvme-listeners string: {str}")
            rply = self.execute_ceph_monitor_command(str)
            self.logger.debug(f"reply \"{rply}\"")
            if rply and rply[0] != 0:
                self.logger.warning("'nvme-gw listeners' mon command failed. \
                                    It might not be supported in current ceph version.")
                return []
            conv_str = rply[1].decode()
            data = json.loads(conv_str)
            return data["Created listeners"]
        except Exception as e:
            self.logger.error(f"nvme-gw listeners command failed: {e}")
            return []

    def get_gw_id_owner_ana_group(self, pool, group, anagrp):
        str = '{' + f'"prefix":"nvme-gw show", "pool":"{pool}", "group":"{group}"' + '}'
        self.logger.debug(f"nvme-show string: {str}")
        rply = self.execute_ceph_monitor_command(str)
        self.logger.debug(f"reply \"{rply}\"")
        conv_str = rply[1].decode()
        data = json.loads(conv_str)

        # Find the gw-id that contains "2: ACTIVE" in "ana states"
        gw_id = None
        comp_str = f"{anagrp}: ACTIVE"
        for gateway in data["Created Gateways:"]:
            if comp_str in gateway["ana states"]:
                gw_id = gateway["gw-id"]
                self.logger.debug(f"found gw owner of anagrp {anagrp}: gw {gw_id}")
                break
        return gw_id

    def is_rebalance_supported(self):
        return self.rebalance_supported

    def get_rebalance_ana_group(self):
        return self.rebalance_ana_group

    def get_num_gws(self):
        return self.num_gws

    def get_ana_grp_location(self):
        return self.ana_group_to_location

    def get_ana_grp_list_per_location(self, location):
        ana_group_locaton_list = []
        # anagroup_list does not contain  deleting GW
        # ana_group_to_location contains all existed GWs
        for ana_grp in self.anagroup_list:
            self.logger.debug(f"ANA Group: '{ana_grp}', location: "
                              f"{self.ana_group_to_location[ana_grp]}, target: {location}")
            if self.ana_group_to_location[ana_grp] == location:
                ana_group_locaton_list.append(ana_grp)
        return ana_group_locaton_list

    def get_gateway_location(self, pool, group, gateway_id):
        if not gateway_id:
            return ""
        self.get_number_created_gateways(pool, group, True)
        location = self.gw_id_to_location.get(gateway_id, "")
        return location

    def get_number_created_gateways(self, pool, group, caching=True):
        now = time.time()
        if caching and ((now - self.last_sent) < 10) and self.anagroup_list:
            self.logger.info(f"Caching response of the monitor: {self.anagroup_list}")
            return self.anagroup_list
        else:
            try:
                self.anagroup_list = []
                self.last_sent = now
                cmd = '{' + f'"prefix":"nvme-gw show", "pool":"{pool}", "group":"{group}"' + '}'
                self.logger.debug(f"nvme-show string: {cmd}")
                rply = self.execute_ceph_monitor_command(cmd)
                self.logger.debug(f"reply \"{rply}\"")
                conv_str = rply[1].decode()
                pos = conv_str.find('"LB"')
                if pos != -1:
                    data = json.loads(conv_str)
                    self.rebalance_supported = True
                    self.rebalance_ana_group = data.get("rebalance_ana_group", 0)
                    if self.rebalance_ana_group == 0:
                        self.logger.info("illegal rebalance ana group  0")
                        self.rebalance_supported = False
                    self.num_gws = data.get("num gws", None)
                    self.logger.debug(f"Rebalance ana_group: {self.rebalance_ana_group}, "
                                      f"num-gws: {self.num_gws}")

                    gateways = data.get("Created Gateways:", [])
                    # self.logger.info(f"gateways: {gateways}")
                    self.ana_group_to_location: dict = {}
                    self.gw_id_to_location: dict = {}
                    for gw in gateways:
                        try:
                            ana_id = int(gw["anagrp-id"])
                            location = gw.get("location", "")
                            gw_id = gw.get("gw-id", "")
                            self.ana_group_to_location[ana_id] = location
                            if gw_id and location:
                                self.gw_id_to_location[gw_id] = location
                        except (KeyError, ValueError, TypeError) as e:
                            self.logger.info(f"ana-location error: gw ,{gw},reason {repr(e)}")
                            continue
                    self.logger.debug(f"ana-location dict:  {self.ana_group_to_location}")
                    self.logger.debug(f"gw_id-location dict:  {self.gw_id_to_location}")
                else:
                    self.rebalance_supported = False
                pos = conv_str.find("[")
                if pos != -1:
                    new_str = conv_str[pos + len("["):]
                    pos = new_str.find("]")
                    new_str = new_str[: pos].strip()
                    int_str_list = new_str.split(' ')
                    self.logger.debug(f"new_str : {new_str}")
                    for x in int_str_list:
                        self.anagroup_list.append(int(x))
                    self.logger.debug(f"ANA group list: {self.anagroup_list}")
                else:
                    self.logger.warning("GWs not found")

            except Exception:
                self.logger.exception("Failure get number created gateways")
                self.anagroup_list = []
                self.rebalance_supported = False

            return self.anagroup_list

    def fetch_and_display_ceph_version(self):
        try:
            rply = self.execute_ceph_monitor_command('{"prefix":"mon versions"}')
            ceph_ver = rply[1].decode().removeprefix("{").strip().split(":")[0]
            ceph_ver = ceph_ver.removeprefix('"').removesuffix('"')
            ceph_ver = ceph_ver.removeprefix("ceph version ")
            self.logger.info(f"Connected to Ceph with version \"{ceph_ver}\"")
            ceph_fsid = self.fetch_ceph_fsid()
            if ceph_fsid:
                self.logger.info(f"Cluster ID is {ceph_fsid}")
        except Exception:
            self.logger.exception("Failure fetching Ceph version")
            pass

    def fetch_ceph_fsid(self) -> str:
        fsid = None
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                fsid = cluster.get_fsid()
        except Exception:
            self.logger.exception("Failure fetching Ceph FSID")

        return fsid

    def pool_exists(self, pool) -> bool:
        if not pool:
            return False
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                if cluster.pool_exists(pool):
                    return True
        except Exception:
            self.logger.exception(f"Can't check if pool {pool} exists, assume it does")
            return True

        return False

    def get_pool_type(self, pool) -> CephPoolType:
        cmd = '{' + '"prefix": "osd pool ls", "detail": "detail", "format": "json"' + '}'
        rply = self.execute_ceph_monitor_command(cmd)
        assert len(rply) == 3, f"Invalid Ceph reply: {rply}"
        if rply[0] != 0:
            return CephUtils.CephPoolType.INVALID
        rply_str = rply[1].decode()
        pools_data = None
        try:
            pools_data = json.loads(rply_str)
        except Exception:
            self.logger.exception(f"JSON loads {rply_str}")
        if not pools_data:
            self.logger.error(f"Can't load pool {pool} data")
            return CephUtils.CephPoolType.INVALID
        pool_found = None
        for one_pool in pools_data:
            try:
                if one_pool["pool_name"] == pool:
                    pool_found = one_pool
                    break
            except Exception:
                self.logger.exception(f"Error parsing pool {one_pool}")
        if not pool_found:
            self.logger.error(f"Can't find pool {pool} data")
            return CephUtils.CephPoolType.INVALID

        try:
            pool_type = pool_found["type"]
        except Exception:
            self.logger.exception(f"Can't get type from pool {pool_found}")
            return CephUtils.CephPoolType.INVALID

        try:
            return CephUtils.CephPoolType(pool_type)
        except ValueError:
            self.logger.exception(f"Unknown pool type {pool_type} in pool {pool_found}")

        return CephUtils.CephPoolType.INVALID

    def allow_ec_overwrites_is_set(self, pool) -> bool:
        cmd = '{' + f'"prefix": "osd pool get", "pool": "{pool}", ' \
                    f'"var": "allow_ec_overwrites", "format": "json"' + '}'
        rply = self.execute_ceph_monitor_command(cmd)
        assert len(rply) == 3, f"Invalid Ceph reply: {rply}"
        if rply[0] != 0:
            return False
        rply_str = rply[1].decode()
        ec_overwrites = False
        try:
            ec_overwrites = json.loads(rply_str)
        except Exception:
            self.logger.exception(f"JSON loads {rply_str}")
            return False

        try:
            return ec_overwrites["allow_ec_overwrites"]
        except Exception:
            self.logger.exception(f"Can't get \"allow_ec_overwrites\" attribute "
                                  f"from pool {pool}: {ec_overwrites}")

        return False

    def rados_namespace_exists(self, pool, namespace) -> bool:
        """Check if RADOS namespace exists in pool."""
        found = False
        if not pool or not namespace:
            return False
        try:
            with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
                with cluster.open_ioctx(pool) as ioctx:
                    rbd_inst = rbd.RBD()
                    found = rbd_inst.namespace_exists(ioctx, namespace)
                    if found:
                        self.logger.debug(f"RADOS namespace {namespace} exists in pool {pool}")
                    else:
                        self.logger.error(f"RADOS namespace {namespace} "
                                          f"does NOT exist in pool {pool}")
        except Exception:
            self.logger.exception(f"Can't check if RADOS namespace {namespace} "
                                  f"exists in pool {pool}")

        return found

    def service_daemon_register(self, cluster, metadata):
        try:
            if cluster:              # rados client
                daemon_name = metadata['id']
                cluster.service_daemon_register("nvmeof", daemon_name, metadata)
                self.logger.info(f"Registered {daemon_name} to service_map!")
        except Exception:
            self.logger.exception("Can't register daemon to service_map!")

    def service_daemon_update(self, cluster, status_buffer):
        try:
            if cluster and status_buffer:
                cluster.service_daemon_update(status_buffer)
        except Exception:
            self.logger.exception("Can't update daemon status to service_map!")

    @staticmethod
    def gateway_encryption_format_to_rbd(gw_format: pb2.EncryptionFormat) -> int:
        if gw_format is None or gw_format == pb2.EncryptionFormat.none:
            return None
        elif gw_format == pb2.EncryptionFormat.luks1:
            return rbd.RBD_ENCRYPTION_FORMAT_LUKS1
        elif gw_format == pb2.EncryptionFormat.luks2:
            return rbd.RBD_ENCRYPTION_FORMAT_LUKS2
        return -1

    @staticmethod
    def gateway_encryption_algorithm_to_rbd(gw_algo: pb2.EncryptionAlgorithm) -> int:
        if gw_algo is None or gw_algo == pb2.EncryptionAlgorithm.no_algorithm:
            return None
        elif gw_algo == pb2.EncryptionAlgorithm.aes128:
            return rbd.RBD_ENCRYPTION_ALGORITHM_AES128
        elif gw_algo == pb2.EncryptionAlgorithm.aes256:
            return rbd.RBD_ENCRYPTION_ALGORITHM_AES256
        return -1

    def create_image(self, pool_name, data_pool_name, rados_namespace_name, image_name,
                     size, encryption_format=None, encryption_algorithm=None,
                     passphrase=None) -> bool:
        image_path = f"{pool_name}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool_name}/{image_name}"
        # Check for pool existence in advance as we don't create it if it's not there
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno=errno.ENODEV)

        if data_pool_name and not self.pool_exists(data_pool_name):
            raise rbd.ImageNotFound(f"Data pool {data_pool_name} doesn't exist",
                                    errno=errno.ENODEV)

        if rados_namespace_name:
            if not self.rados_namespace_exists(pool_name, rados_namespace_name):
                raise rbd.ImageNotFound(f"Namespace {rados_namespace_name} doesn't exist in pool "
                                        f"{pool_name}", errno=errno.ENODEV)
        image_exists = False
        try:
            image_size = self.get_image_size(pool_name, image_name, rados_namespace_name)
            image_exists = True
        except rbd.ImageNotFound:
            self.logger.debug(f"Image {image_path} doesn't exist, will "
                              f"create it using size {size}")
            pass

        if image_exists:
            if image_size != size:
                raise rbd.ImageExists(f"Image {image_path} already exists with "
                                      f"a size of {image_size} bytes which differs from the "
                                      f"requested size of {size} bytes",
                                      errno=errno.EEXIST)
            return False    # Image exists with an identical size, there is nothing to do here

        encryption_format = CephUtils.gateway_encryption_format_to_rbd(encryption_format)
        if encryption_format is not None and encryption_format < 0:
            raise ValueError("Invalid encryption format")

        encryption_algorithm = CephUtils.gateway_encryption_algorithm_to_rbd(encryption_algorithm)
        if encryption_algorithm is not None and encryption_algorithm < 0:
            raise ValueError("Invalid encryption algorithm")

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                rbd_inst = rbd.RBD()
                try:
                    rbd_inst.create(ioctx, image_name, size, data_pool=data_pool_name)
                except rbd.ImageExists:
                    self.logger.exception(f"Image {image_path} was created just now")
                    raise rbd.ImageExists(f"Image {image_path} was just created by "
                                          f"someone else, please retry",
                                          errno=errno.EAGAIN)
                except Exception:
                    self.logger.exception(f"Can't create image {image_path}")
                    raise

                if encryption_format is not None and passphrase is not None:
                    try:
                        with rbd.Image(ioctx, image_name) as img:
                            if encryption_algorithm is not None:
                                img.encryption_format(encryption_format,
                                                      passphrase,
                                                      encryption_algorithm)
                            else:
                                img.encryption_format(encryption_format, passphrase)
                    except Exception:
                        self.logger.exception(f"Can't encrypt image {image_path}")
                        try:
                            self.logger.info(f"Will delete the created image {image_path}")
                            self.delete_image(pool_name, image_name, rados_namespace_name)
                        except Exception:
                            self.logger.exception(f"Error deleting image {image_path}")
                        raise

        return True

    def delete_image(self, pool_name, image_name, rados_namespace_name=None) -> bool:
        image_path = f"{pool_name}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool_name}/{image_name}"
        if not pool_name and not image_name:
            return True

        if not self.pool_exists(pool_name):
            self.logger.warning(f"Pool {pool_name} doesn't exist, can't delete RBD image")
            return True

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                rbd_inst = rbd.RBD()
                try:
                    rbd_inst.remove(ioctx, image_name)
                except rbd.ImageNotFound:
                    self.logger.warning(f"Image {image_path} is not found")
                    return True
                except (rbd.ImageBusy, rbd.ImageHasSnapshots):
                    self.logger.exception(f"Can't delete image {image_path}")
                    return False

        return True

    def get_image_size(self, pool_name: str, image_name: str,
                       rados_namespace_name: str = None) -> int:
        image_size = 0
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno=errno.ENODEV)

        image_path = f"{pool_name}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool_name}/{image_name}"

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                # Set namespace if provided
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        image_size = img.size()
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {image_path} doesn't exist",
                                            errno=errno.ENODEV)
                except Exception as ex:
                    self.logger.exception(f"Error while trying to get the size of image "
                                          f"{image_path}")
                    raise ex

        return image_size

    def were_image_qos_limits_changed(self, pool_name: str, image_name: str,
                                      rados_namespace_name: str) -> bool:
        if not self.pool_exists(pool_name):
            raise rbd.ImageNotFound(f"Pool {pool_name} doesn't exist", errno=errno.ENODEV)
        image_path = f"{pool_name}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool_name}/{image_name}"

        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool_name) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        attributes = img.config_list()
                        self.logger.debug(f"Config for image {image_path}:")
                        self.logger.debug("==========================================")
                        for one_img_attr in attributes:
                            self.logger.debug(f"{one_img_attr}")
                            try:
                                if not one_img_attr["name"].startswith(CephUtils.RBD_QOS_PREFIX):
                                    continue
                                if not one_img_attr["name"].endswith(CephUtils.RBD_QOS_SUFFIX):
                                    continue
                                if one_img_attr["value"] != "0":
                                    self.logger.warning(f'RBD QOS attribute '
                                                        f'{one_img_attr["name"]} was changed '
                                                        f'to {one_img_attr["value"]}')
                                    return True
                            except Exception:
                                self.logger.exception(f"error parsing {one_img_attr}")
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {image_path} doesn't exist",
                                            errno=errno.ENODEV)
                except Exception:
                    self.logger.exception(f"Error while trying to get the config of image "
                                          f"{image_path}")
                    raise

        return False

    def does_image_exist(self, pool_name: str, image_name: str,
                         rados_namespace_name: str = None) -> bool:
        if not pool_name:
            return False
        if not image_name:
            return False
        try:
            self.get_image_size(pool_name, image_name, rados_namespace_name)
            return True
        except rbd.ImageNotFound:
            return False
        except Exception:
            self.logger.exception("Failure getting image size")
        return False

    def set_image_metadata(self, pool: str, image_name: str,
                           rados_namespace_name: str, key: str, value: str) -> None:
        if not self.pool_exists(pool):
            raise rbd.ImageNotFound(f"Pool {pool} doesn't exist", errno=errno.ENOENT)
        if not self.does_image_exist(pool, image_name, rados_namespace_name):
            raise rbd.ImageNotFound(f"Image {pool}/{image_name} doesn't exist", errno=errno.ENOENT)

        img_path = f"{pool}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool}/{image_name}"
        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        img.metadata_set(key, value)
                        self.logger.debug(f"Set metadata {key} of image "
                                          f"{img_path} to {value}")
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {img_path} doesn't exist",
                                            errno=errno.ENODEV)
                except Exception:
                    self.logger.exception(f"Error while trying to set metadata {key} of image "
                                          f"{img_path} to {value}")
                    raise

    def get_image_metadata(self, pool: str, image_name: str,
                           rados_namespace_name: str, key: str) -> str:
        if not self.pool_exists(pool):
            raise rbd.ImageNotFound(f"Pool {pool} doesn't exist", errno=errno.ENODEV)

        value = None
        img_path = f"{pool}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool}/{image_name}"
        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        value = img.metadata_get(key)
                        self.logger.debug(f"Metadata {key} of image {img_path} "
                                          f"is {value}")
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {img_path} doesn't exist",
                                            errno=errno.ENODEV)
                except KeyError:
                    self.logger.debug(f"No metadata {key} for image {img_path}")
                    return None
                except Exception:
                    self.logger.exception(f"Error while trying to get metadata {key} of image "
                                          f"{img_path}")
                    raise
        return value

    def remove_image_metadata(self, pool: str, image_name: str,
                              rados_namespace_name: str, key: str) -> None:
        if not self.pool_exists(pool):
            raise rbd.ImageNotFound(f"Pool {pool} doesn't exist", errno=errno.ENODEV)

        img_path = f"{pool}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool}/{image_name}"
        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        img.metadata_remove(key)
                        self.logger.debug(f"Removed metadata {key} of image {img_path}")
                except rbd.ImageNotFound:
                    raise rbd.ImageNotFound(f"Image {img_path} doesn't exist",
                                            errno=errno.ENODEV)
                except KeyError:
                    self.logger.info(f"No metadata {key} for image {img_path}, no need to remove")
                    pass
                except Exception:
                    self.logger.exception(f"Error while trying to remove metadata {key} of image "
                                          f"{img_path}")
                    raise

    def get_image_id(self, pool: str, image_name: str, rados_namespace_name: str) -> str:
        value = None
        if not self.pool_exists(pool):
            self.logger.warning(f"Pool {pool} doesn't exist")
            return None

        img_path = f"{pool}/{rados_namespace_name}/{image_name}" if rados_namespace_name \
            else f"{pool}/{image_name}"
        with rados.Rados(conffile=self.ceph_conf, rados_id=self.rados_id) as cluster:
            with cluster.open_ioctx(pool) as ioctx:
                if rados_namespace_name:
                    ioctx.set_namespace(rados_namespace_name)
                try:
                    with rbd.Image(ioctx, image_name) as img:
                        value = img.id()
                        self.logger.debug(f"The ID of image {img_path} is {value}")
                except rbd.ImageNotFound:
                    self.logger.warning(f"Image {img_path} doesn't exist")
                except Exception:
                    self.logger.exception(f"Error while trying to get the id of image "
                                          f"{img_path}")
        return value

    def get_rbd_exception_details(self, ex):
        ex_details = (None, None)
        if rbd.OSError in type(ex).__bases__:
            msg = str(ex).strip()
            # remove the [errno] part
            if msg.startswith("["):
                pos = msg.find("]")
                if pos >= 0:
                    msg = msg[pos + 1:].strip()
            ex_details = (ex.errno, msg)
        return ex_details
