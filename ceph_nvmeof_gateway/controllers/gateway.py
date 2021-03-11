import logging

from . import BackendControllerRoute, FrontendControllerRoute, RESTController


logger = logging.getLogger(__name__)


@FrontendControllerRoute('/gateways')
class Gateways(RESTController):
    RESOURCE_ID = 'gateway_uuid'

    def list(self):
        # TODO
        return ['gateway_uuid1']

    def get(self, gateway_uuid):
        # TODO
        return {}

    def create(self, gateway_spec):
        # TODO
        return 'gateway_uuid'

    def delete(self, gateway_uuid):
        # TODO
        pass


@BackendControllerRoute('/gateways')
class GatewaysBackend(RESTController):
    RESOURCE_ID = 'gateway_uuid'

    def create(self, gateway_spec):
        # TODO
        pass

    def delete(self, gateway_uuid):
        # TODO
        pass
