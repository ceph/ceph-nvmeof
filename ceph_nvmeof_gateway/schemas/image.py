from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field

from ceph_nvmeof_gateway.models import image


class Image(SQLAlchemyAutoSchema):
    class Meta:
        model = image.Image
        load_instance = True

    id = auto_field(dump_only=True)
