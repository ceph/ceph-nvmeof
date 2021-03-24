import cherrypy
import logging
import sqlalchemy.exc

from . import BackendControllerRoute, EndpointDoc, FrontendControllerRoute, RESTController
from ceph_nvmeof_gateway.models.host import Host
from ceph_nvmeof_gateway.models.image import Image
from ceph_nvmeof_gateway.schemas.host import Host as HostSchema


logger = logging.getLogger(__name__)
host_schema = HostSchema()
host_schemas = HostSchema(many=True)


@FrontendControllerRoute('/hosts')
class Hosts(RESTController):
    RESOURCE_ID = 'host_id'

    @EndpointDoc(responses={200: host_schemas})
    def list(self):
        return self.db.query(Host).all()

    @EndpointDoc(responses={200: host_schema})
    def get(self, host_id):
        return self.db.query(Host).get_or_404(host_id)

    @EndpointDoc(parameters=host_schema,
                 responses={201: host_schema})
    def create(self, host):
        logger.debug("create: host={}".format(host_schema.dumps(host)))

        self.db.add(host)
        try:
            self.db.commit()
        except sqlalchemy.exc.IntegrityError:
            self.db.rollback()
            raise cherrypy.HTTPError(422, message='duplicate host')

        return host

    def delete(self, host_id):
        host = self.db.query(Host).get_or_404(host_id)
        self.db.delete(host)


@FrontendControllerRoute('/hosts/{host_id}/images')
class HostImages(RESTController):
    RESOURCE_ID = 'image_id'

    def list(self, host_id):
        return [x.id for x in self.db.query(Host).get_or_404(host_id).images]

    def get(self, host_id, image_id):
        pass

    def create(self, host_id, image_id):
        host = self.db.query(Host).get_or_404(host_id)
        image = self.db.query(Image).get_or_404(image_id)
        host.images.append(image)

        self.db.add(host)

    def delete(self, host_id, image_id):
        host = self.db.query(Host).get_or_404(host_id)
        image = self.db.query(Image).get_or_404(image_id)

        if image not in host.images:
            raise cherrypy.HTTPError(404)

        host.images.remove(image)
        self.db.add(host)


@BackendControllerRoute('/hosts')
class HostsBackend(RESTController):
    RESOURCE_ID = 'host_id'

    def create(self, host_spec):
        # TODO
        pass

    def delete(self, host_id):
        # TODO
        pass
