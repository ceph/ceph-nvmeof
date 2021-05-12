#!/usr/bin/python

from setuptools import setup
import os

if os.path.exists('README.md'):
    with open('README.md') as readme_file:
        long_description = readme_file.read().strip()
else:
    long_description = ''


setup(
    name="ceph_nvmeof",
    version="0.1",
    description="Common classes/functions and tools used to configure NVMeOF "
                "gateways backed by Ceph RBD",
    long_description=long_description,
    author="Jason Dillaman",
    author_email="dillaman@redhat.com",
    url="http://github.com/ceph/ceph-nvmeof",
    license="LGPLv3",
    packages=[
        "ceph_nvmeof_gateway",
        "ceph_nvmeof_gateway.controllers",
        "ceph_nvmeof_gateway.models",
        "ceph_nvmeof_gateway.schemas",
    ],
    package_data={
        "ceph_nvmeof_gateway": ["static/*"],
    },
    entry_points={
        'console_scripts': [
            "ceph-nvmeof-gw=ceph_nvmeof_gateway.cli:main"
        ]
    },
    data_files=[("var/log/ceph-nvmeof-gw", [])],
    install_requires=[
        'apispec',
        'CherryPy',
        'CherryPy-SQLAlchemy',
        'marshmallow',
        'marshmallow_sqlalchemy',
        'rados',
        'rbd',
        'routes',
        'sqlalchemy',
    ]
)
