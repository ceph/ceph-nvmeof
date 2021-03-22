import cherrypy
import logging
import rados
import sqlalchemy
import sqlalchemy.orm
import sqlite3

import ceph_nvmeof_gateway.models
import ceph_nvmeof_gateway.settings

GATEWAY_DB_OBJECT_NAME = "nvmeof_gateway"


logger = logging.getLogger(__name__)


def create(settings):
    # TODO incorporate alembic for migrations
    engine = init_engine(settings)

    # create the initial DB tables
    ceph_nvmeof_gateway.models.load()
    ceph_nvmeof_gateway.models.Model.metadata.create_all(engine)


def init_engine(settings=None):
    if settings is None:
        settings = ceph_nvmeof_gateway.settings.Settings()

    initialize_ceph_vfs()
    engine = sqlalchemy.create_engine(get_connect_uri(settings), echo=True)
    initialize_session(engine)
    return engine


def init_session(settings=None):
    engine = init_engine(settings)
    session_maker = sqlalchemy.orm.sessionmaker(bind=engine, class_=Session)
    return session_maker()


def initialize_ceph_vfs():
    # instruct sqlite to load the libcephsqlite extension
    db = sqlite3.connect(':memory:')
    db.enable_load_extension(True)
    db.load_extension('libcephsqlite')


def initialize_session(session):
    # configure per-thread sessions
    session.execute("PRAGMA FOREIGN_KEYS = 1")
    session.execute('PRAGMA JOURNAL_MODE = PERSIST')
    session.execute('PRAGMA PAGE_SIZE = 65536')
    session.execute('PRAGMA CACHE_SIZE = 256')


def get_connect_uri(settings):
    with rados.Rados(conffile=settings.config.ceph_config,
                     name=settings.config.client_name) as cluster:
        with cluster.open_ioctx(settings.config.pool_name) as ioctx:
            pool_id = ioctx.get_pool_id()

    uri = "sqlite:///file:*{}:/{}.db?vfs=ceph&uri=true".format(
        pool_id, settings.config.db_name)
    logger.info("URI: {}".format(uri))
    return uri


class Query(sqlalchemy.orm.Query):
    def get_or_404(self, *entities, **kwargs):
        value = super(Query, self).get(*entities, **kwargs)
        if not value:
            raise cherrypy.HTTPError(404)
        return value

    def first_or_404(self):
        value = super(Query, self).first()
        if not value:
            raise cherrypy.HTTPError(404)
        return value


class Session(sqlalchemy.orm.Session):
    def __init__(self, **kwargs):
        kwargs['query_cls'] = Query
        super(Session, self).__init__(**kwargs)
