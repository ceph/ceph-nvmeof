import importlib
import logging
import os
import pkgutil
from sqlalchemy.ext.declarative import declarative_base


logger = logging.getLogger(__name__)


def load():
    models_dir = os.path.dirname(os.path.realpath(__file__))
    logger.debug("models_dir=%s", models_dir)

    mods = [mod for _, mod, _ in pkgutil.iter_modules([models_dir])]
    logger.debug("mods=%s", mods)
    for mod_name in mods:
        importlib.import_module('.models.{}'.format(mod_name),
                                package='ceph_nvmeof_gateway')


class ModelBase:
    pass


Model = declarative_base(cls=ModelBase, name="Model")
