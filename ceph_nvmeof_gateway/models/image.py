from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import validates
import uuid

from ceph_nvmeof_gateway.models import Model


class Image(Model):
    __tablename__ = 'images'

    id = Column(Integer, primary_key=True)
    image_spec = Column(String, nullable=False, unique=True)

    namespace_uuid = Column(String)

    @validates('namespace_uuid')
    def validate_namespace_uuid(self, key, value):
        try:
            uuid.UUID(str(value))
            return True
        except ValueError:
            return False
