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
DOCKER_NO_CACHE := --no-cache
CONT_NAME := ceph-nvmeof
CONT_VERS := latest
REMOTE_REPO ?= ""
SPDK_VERSION := $(shell cd spdk;git -C . describe --tags --abbrev=0 | sed -r 's/-/\./g')
DOCKER_VERSION := $(shell docker image ls | grep spdk | tr -s ' ' | cut -d ' ' -f2)
SPDK_IMAGE ?= fedora:36
PYVENV := $(PROJDIR)/venv

# Utility Function to activate the Python Virtual Environment
define callpyvenv =
	(source ${PYVENV}/bin/activate; export PATH=$(PYVENV)/bin; $(1))
endef

## setup: setup add requirements
.PHONY: setup
setup:
	@echo "dir: $(curr_dir)/spdk"
	cd $(curr_dir)/spdk && \
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
	ln -sf docker/.dockerignore.spdk .dockerignore
	docker build \
	--network=host \
	--build-arg SPDK_IMAGE=$(SPDK_IMAGE) \
	--build-arg spdk_version=$(SPDK_VERSION) \
	--build-arg spdk_branch=ceph-nvmeof \
	${DOCKER_NO_CACHE} \
	-t spdk:$(SPDK_VERSION) -f docker/Dockerfile.spdk .
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
	ln -sf  docker/.dockerignore.gateway .dockerignore
	docker build \
	--network=host \
	${DOCKER_NO_CACHE} \
	--build-arg spdk_version=$(SPDK_VERSION) \
	-t ${CONT_NAME}:${CONT_VERS} -f docker/Dockerfile.gateway .

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

.PHONY: test1
test1: $(PYVENV)
	@echo "This is a test"


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

