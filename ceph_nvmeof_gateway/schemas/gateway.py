from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field

from ceph_nvmeof_gateway.models import gateway


class GatewayPortal(SQLAlchemyAutoSchema):
    class Meta:
        model = gateway.GatewayPortal
        load_instance = True

    id = auto_field(dump_only=True)


class Gateway(SQLAlchemyAutoSchema):
    class Meta:
        model = gateway.Gateway
        load_instance = True

    id = auto_field(dump_only=True)


class GatewayGroup(SQLAlchemyAutoSchema):
    class Meta:
        model = gateway.GatewayGroup
        load_instance = True

    id = auto_field(dump_only=True)
