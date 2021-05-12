from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint
from sqlalchemy.orm import relationship
from ceph_nvmeof_gateway.models import image, Model  # noqa: F401


HostImages = Table('host_images', Model.metadata,
                   Column('host_id', Integer, ForeignKey('hosts.id')),
                   Column('image_id', Integer, ForeignKey('images.id')),
                   UniqueConstraint('host_id', 'image_id'))


class Host(Model):
    __tablename__ = 'hosts'

    id = Column(Integer, primary_key=True)
    nqn = Column(String, nullable=False, unique=True)

    host_group_id = Column(Integer, ForeignKey('host_groups.id'))
    host_group = relationship("HostGroup", backref="hosts")

    images = relationship("Image", secondary=HostImages,
                          backref="hosts")


class HostGroup(Model):
    __tablename__ = 'host_groups'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
