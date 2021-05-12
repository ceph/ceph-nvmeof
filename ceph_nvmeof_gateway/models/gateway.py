from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from ceph_nvmeof_gateway.models import Model


class Gateway(Model):
    __tablename__ = 'gateways'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    gateway_group_id = Column(Integer, ForeignKey('gateway_groups.id'))
    gateway_group = relationship("GatewayGroup", backref="gateways")


class GatewayPortal(Model):
    __tablename__ = 'gateway_portals'

    id = Column(Integer, primary_key=True)

    gateway_id = Column(Integer, ForeignKey('gateways.id'))
    gateway = relationship("Gateway", backref="gateway_portals")


class GatewayGroup(Model):
    __tablename__ = 'gateway_groups'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
