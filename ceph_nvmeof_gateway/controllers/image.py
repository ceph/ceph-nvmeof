import cherrypy
import logging
import sqlalchemy.exc

from . import BackendControllerRoute, ControllerDoc, EndpointDoc, \
              FrontendControllerRoute, RESTController
from ceph_nvmeof_gateway.models.image import Image
from ceph_nvmeof_gateway.schemas.image import Image as ImageSchema


logger = logging.getLogger(__name__)
image_schema = ImageSchema()
image_schemas = ImageSchema(many=True)


@ControllerDoc('Manage RBD images exposed via the gateway')
@FrontendControllerRoute('/images')
class Images(RESTController):
    RESOURCE_ID = 'image_id'

    @EndpointDoc("List managed RBD images",
                 responses={200: image_schemas})
    def list(self):
        return self.db.query(Image).all()

    @EndpointDoc(responses={200: image_schema})
    def get(self, image_id):
        return self.db.query(Image).get_or_404(image_id)

    @EndpointDoc(parameters=image_schema,
                 responses={201: image_schema})
    def create(self, image):
        logger.debug("create: image={}".format(image_schema.dumps(image)))

        self.db.add(image)
        try:
            self.db.commit()
        except sqlalchemy.exc.IntegrityError:
            self.db.rollback()
            raise cherrypy.HTTPError(422, message='duplicate image')

        return image

    def delete(self, image_id):
        image = self.db.query(Image).get_or_404(image_id)
        self.db.delete(image)


@ControllerDoc('Gateway-to-gateway RBD image management')
@BackendControllerRoute('/images')
class ImagesBackend(RESTController):
    RESOURCE_ID = 'image_id'

    def create(self, image_spec):
        # TODO
        pass

    def delete(self, image_id):
        # TODO
        pass
