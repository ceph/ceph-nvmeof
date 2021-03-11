import logging

from . import BackendControllerRoute, ControllerDoc, EndpointDoc, \
              FrontendControllerRoute, RESTController


logger = logging.getLogger(__name__)


@ControllerDoc('Manage RBD images exposed via the gateway')
@FrontendControllerRoute('/images')
class Images(RESTController):
    RESOURCE_ID = 'image_uuid'

    @EndpointDoc("List managed RBD images")
    def list(self):
        # TODO
        return ['image_uuid1']

    def get(self, image_uuid):
        # TODO
        return {}

    def create(self, image_spec):
        # TODO
        return 'image_uuid'

    def delete(self, image_uuid):
        # TODO
        pass


@ControllerDoc('Gateway-to-gateway RBD image management')
@BackendControllerRoute('/images')
class ImagesBackend(RESTController):
    RESOURCE_ID = 'image_uuid'

    def create(self, image_spec):
        # TODO
        pass

    def delete(self, image_uuid):
        # TODO
        pass
