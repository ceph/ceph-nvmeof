#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com, guptsanj@us.ibm.com
#
SHELL := /bin/bash
PROJDIR := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
MODULE := control
CONFIG ?= ceph-nvmeof.conf
curr_dir := $(shell pwd)
DOCKER_NO_CACHE :=
CONT_NAME := ceph-nvmeof
CONT_VERS := latest
REMOTE_REPO ?= ""
SPDK_VERSION := $(shell cd spdk;git -C . describe --tags --abbrev=0 | sed -r 's/-/\./g')
DOCKER_VERSION := $(shell docker image ls | grep spdk | tr -s ' ' | cut -d ' ' -f2)
PYVENV := $(PROJDIR)/venv
CEPH_VERSION := 17.2.5
UBI_VERSION ?= ubi8
PYTHON_VERSION := 36

# Switch the python version to 39 if this is a ubi9 build as 
# python3-rados-2 needs > python36
ifeq ($(UBI_VERSION), ubi9))
	PYTHON_VERSION = 39
endif

# Utility Function to activate the Python Virtual Environment
define callpyvenv =
	(source ${PYVENV}/bin/activate; export PATH=$(PYVENV)/bin; $(1))
endef

## setup: setup add requirements
.PHONY: setup
setup:
	@echo "dir: $(curr_dir)/spdk"
	git submodule update --init --recursive

## grpc: Compile grpc code
.PHONY: grpc
grpc: $(PYVENV)
	@mkdir -p $(MODULE)/generated
	$(call callpyvenv, python3 -m grpc_tools.protoc \
			--proto_path=./proto \
			--python_out=./$(MODULE)/generated \
			--grpc_python_out=./$(MODULE)/generated \
			./proto/*.proto)
	@sed -i 's/^import.*_pb2/from . \0/' ./$(MODULE)/generated/*.py

## run: Run the gateway server
.PHONY: run
run: $(PYVENV)
	@echo "Executing PEP8 on python files..."
	$(call callpyvenv,python3 -m $(MODULE) -c $(CONFIG))

## test: Run tests
.PHONY: test
test: $(PYVENV)
	$(call callpyvenv,pytest)

## spdk-image: Build spdk image if it does not exist
.PHONY: spdk-image
spdk-image:
ifneq ($(DOCKER_VERSION), $(SPDK_VERSION))
ifeq ($(UBI_VERSION), ubi8)
		cp docker/centos8.repo docker/centos.repo
		cp docker/ceph8.repo docker/ceph.repo
else
		cp docker/centos9.repo docker/centos.repo
		cp docker/ceph9.repo docker/ceph.repo
endif
	docker buildx build \
	--network=host \
	--build-arg spdk_version=$(SPDK_VERSION) \
	--build-arg CEPH_VERSION=$(CEPH_VERSION) \
	--build-arg spdk_branch=ceph-nvmeof \
	--build-arg UBI_VERSION=$(UBI_VERSION) \
	--progress=plain \
	${DOCKER_NO_CACHE} \
	-t spdk:$(SPDK_VERSION) -f docker/Dockerfile.spdk .
	rm docker/centos.repo
	rm docker/ceph.repo
	@touch .spdk-image
else
 	@echo "Docker image for version: $(SPDK_VERSION) exists"
endif

## spdk-rpms: Copy the rpms from spdk container in output directory
.PHONY: spdk-rpms
spdk_rpms:
	docker run --rm -v $(curr_dir)/output:/output spdk:$(SPDK_VERSION) \
	bash -c "cp -f /tmp/rpms/*.rpm /output/"


## gateway-image: Build the ceph-nvme gateway image. The spdk image needs to be built first.
.PHONY: gateway-image
gateway-image: spdk-image
ifeq ($(UBI_VERSION), ubi8)
		cp docker/centos8.repo docker/centos.repo
		cp docker/ceph8.repo docker/ceph.repo
else
		cp docker/centos9.repo docker/centos.repo
		cp docker/ceph9.repo docker/ceph.repo
endif
	docker buildx build \
	--network=host \
	--build-arg spdk_version=$(SPDK_VERSION) \
	--build-arg CEPH_VERSION=${CEPH_VERSION} \
	--build-arg UBI_VERSION=$(UBI_VERSION) \
	--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
	${DOCKER_NO_CACHE} \
	--progress=plain \
	-t ${CONT_NAME}:${CONT_VERS} -f docker/Dockerfile.gateway .
	rm docker/centos.repo
	rm docker/ceph.repo

## push-gateway-image: Publish container into the docker registry for devs
.PHONY: push-gateway-image
push-gateway-image: gateway-image
	docker tag ${CONT_NAME}:${CONT_VERS} ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}
	docker push ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}

## clean: Clean local images and rpms
.PHONY: clean
clean:
	find control -name __pycache__ -type d -exec rm -rf "{}" \;
	rm -rf output
	rm -rf $(PYVENV)

# Setup a Python Virtual Environment to use for running the static analysis and unit tests
$(PYVENV):
	[ -d $(PYVENV) ] || (mkdir -p $(PYVENV); python3 -m venv $(PYVENV))
	$(PYVENV)/bin/pip3 install --upgrade pip==20.3.3
	$(PYVENV)/bin/pip3 install -r $(PROJDIR)/requirements.txt
	$(PYVENV)/bin/pip3 install -e $(PROJDIR)

## help: Describes the help
.PHONY: help
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

