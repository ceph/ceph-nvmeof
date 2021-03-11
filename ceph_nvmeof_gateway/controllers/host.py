import logging

from . import BackendControllerRoute, FrontendControllerRoute, RESTController


logger = logging.getLogger(__name__)


@FrontendControllerRoute('/hosts')
class Hosts(RESTController):
    RESOURCE_ID = 'host_uuid'

    def list(self):
        # TODO
        return ['host_uuid1']

    def get(self, host_uuid):
        # TODO
        return {}

    def create(self, host_spec):
        # TODO
        return 'host_uuid'

    def delete(self, host_uuid):
        # TODO
        pass


@BackendControllerRoute('/hosts')
class HostsBackend(RESTController):
    RESOURCE_ID = 'host_uuid'

    def create(self, host_spec):
        # TODO
        pass

    def delete(self, host_uuid):
        # TODO
        pass
