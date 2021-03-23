import importlib
import logging
import os
import pkgutil


logger = logging.getLogger(__name__)


def load():
    schemas_dir = os.path.dirname(os.path.realpath(__file__))
    logger.debug("schemas_dir=%s", schemas_dir)

    mods = [mod for _, mod, _ in pkgutil.iter_modules([schemas_dir])]
    logger.debug("mods=%s", mods)
    for mod_name in mods:
        importlib.import_module('.schemas.{}'.format(mod_name),
                                package='ceph_nvmeof_gateway')
