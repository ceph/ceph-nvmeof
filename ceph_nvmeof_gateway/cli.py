import argparse
import logging
import logging.handlers
from logging.handlers import RotatingFileHandler
import signal
import sys

from . import api, db
from .settings import Settings


def _initialize_logging(settings):
    # setup syslog handler to help diagnostics
    logger_level = logging.getLevelName(settings.config.logger_level)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # syslog (systemctl/journalctl messages)
    if settings.config.log_to_stderr:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logger_level)

        stderr_format = logging.Formatter("%(name)s: %(message)s")
        if settings.config.log_to_stderr_prefix:
            stderr_format = logging.Formatter("{} {}".format(
                settings.config.log_to_stderr_prefix, stderr_format))
        stderr_handler.setFormatter(stderr_format)

        logger.addHandler(stderr_handler)

    if settings.config.log_to_file:
        # file target - more verbose logging for diagnostics
        file_handler = RotatingFileHandler('/var/log/ceph-nvmeof-gw/ceph-nvmeof-gw.log',
                                           maxBytes=5242880,
                                           backupCount=7)
        file_handler.setLevel(logger_level)
        file_format = logging.Formatter(
            "%(asctime)s %(levelname)8s [%(filename)s:%(lineno)s:%(funcName)s()] "
            "- %(message)s")
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def _parse_argments():
    parser = argparse.ArgumentParser(description='Ceph NVMeOF Gateway.')
    parser.add_argument('db-init', nargs='?', help="initialize the backing DB")
    parser.add_argument('--conf', '-c', default='/etc/ceph/nvmeof-gateway.cfg',
                        help='path to configuration')

    return vars(parser.parse_args())


class SignalHandler:
    def __init__(self, logger, api_server):
        self.logger = logger
        self.api_server = api_server

    def register(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGHUP, self.reload)

    def shutdown(self, signum, frame):
        self.logger.info("terminate signal")
        self.api_server.shut_down()

    def reload(self, signum, frame):
        self.logger.info("reload signal")


def main():
    arguments = _parse_argments()
    settings = Settings(arguments)
    settings.load()

    logger = _initialize_logging(settings)

    if arguments['db-init']:
        db.create(settings)
        return

    api_server = api.Server(settings)

    signal_handler = SignalHandler(logger, api_server)
    signal_handler.register()

    api_server.serve()
