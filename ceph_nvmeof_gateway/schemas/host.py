from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field

from ceph_nvmeof_gateway.models import host


class Host(SQLAlchemyAutoSchema):
    class Meta:
        model = host.Host
        load_instance = True

    id = auto_field(dump_only=True)


class HostGroup(SQLAlchemyAutoSchema):
    class Meta:
        model = host.HostGroup
        load_instance = True

    id = auto_field(dump_only=True)
