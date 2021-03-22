import cherrypy
import logging
import os
import threading
import time
from cp_sqlalchemy import SQLAlchemyPlugin, SQLAlchemyTool

from ceph_nvmeof_gateway import controllers, db, models

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, settings):
        self.settings = settings
        self.shutdown_event = threading.Event()

    def shut_down(self):
        logger.info("shutting down")
        self.shutdown_event.set()

    def serve(self):
        self._configure()

        models.load()
        mapper, parent_urls = controllers.generate_routes()

        current_dir = os.path.dirname(os.path.abspath(__file__))
        config = {
            '/static': {
                'tools.db.on': False,
                'tools.staticdir.on': True,
                'tools.staticdir.dir': os.path.join(current_dir, 'static'),
                'tools.staticdir.content_types': {'css': 'text/css',
                                                  'js': 'application/javascript'},
            }
        }
        for purl in parent_urls:
            config[purl] = {
                'request.dispatch': mapper
            }

        sqlalchemy_plugin = DbPlugin(cherrypy.engine, models.Model,
                                     db.get_connect_uri(self.settings))
        sqlalchemy_plugin.subscribe()
        sqlalchemy_plugin.create()

        cherrypy.tree.mount(None, config=config)
        cherrypy.engine.start()

        self.shutdown_event.wait()
        self.shutdown_event.clear()

        cherrypy.engine.stop()
        logger.info("engine stopped")

    def _configure(self):
        db.initialize_ceph_vfs()

        server_addr = self.settings.config.api_host
        server_port = self.settings.config.api_port
        logger.info('server: host=%s port=%d', server_addr, server_port)

        cherrypy.tools.db = SQLAlchemyTool(class_=db.Session)

        cherrypy.tools.request_logging = RequestLoggingTool()
        cherrypy.log.access_log.propagate = False
        cherrypy.log.error_log.propagate = False

        # Apply the 'global' CherryPy configuration.
        config = {
            'engine.autoreload.on': False,
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'tools.gzip.on': True,
            'tools.gzip.mime_types': [
                'application/json',
                'application/*+json',
            ],
            'tools.json_in.on': True,
            'tools.json_in.force': True,
            'tools.db.on': True,
            'tools.request_logging.on': True,
            'log.access_file': '',
            'log.error_file': '',
            'log.screen': False,
            'error_page.default': controllers.json_error_page,
        }
        cherrypy.config.update(config)


class DbPlugin(SQLAlchemyPlugin):
    def __init__(self, bus, orm_base, db_uri, **kwargs):
        super(DbPlugin, self).__init__(bus, orm_base, db_uri, **kwargs)

    def start(self):
        super(DbPlugin, self).start()
        db.initialize_session(self.sa_engine)


class RequestLoggingTool(cherrypy.Tool):
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_handler', self.request_begin,
                               priority=10)
        self.logger = logging.getLogger('{}.request'.format(__name__))

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('on_end_request', self.request_end,
                                      priority=5)
        cherrypy.request.hooks.attach('after_error_response', self.request_error,
                                      priority=5)

    def request_begin(self):
        req = cherrypy.request
        # Log the request.
        self.logger.debug('[%s:%s] [%s] %s', req.remote.ip, req.remote.port,
                          req.method, req.path_info)

    def request_error(self):
        self._request_log(self.logger.error)
        self.logger.error(cherrypy.response.body)

    def request_end(self):
        status = cherrypy.response.status[:3]
        if status in ["401", "403"]:
            # log unauthorized accesses
            self._request_log(self.logger.warning)
        else:
            self._request_log(self.logger.info)

    def _format_bytes(self, num):
        units = ['B', 'K', 'M', 'G']

        if isinstance(num, str):
            try:
                num = int(num)
            except ValueError:
                return "n/a"

        format_str = "{:.0f}{}"
        for i, unit in enumerate(units):
            div = 2**(10*i)
            if num < 2**(10*(i+1)):
                if num % div == 0:
                    format_str = "{}{}"
                else:
                    div = float(div)
                    format_str = "{:.1f}{}"
                return format_str.format(num/div, unit[0])

        # content-length bigger than 1T!! return value in bytes
        return "{}B".format(num)

    def _request_log(self, logger_fn):
        req = cherrypy.request
        res = cherrypy.response
        lat = time.time() - res.time
        status = res.status[:3] if isinstance(res.status, str) else res.status
        if 'Content-Length' in res.headers:
            length = self._format_bytes(res.headers['Content-Length'])
        else:
            length = self._format_bytes(0)
        logger_fn("[%s:%s] [%s] [%s] [%s] [%s] [%s] %s", req.remote.ip,
                  req.remote.port, req.method, status,
                  "{0:.3f}s".format(lat), length, getattr(req, 'unique_id', '-'), req.path_info)
