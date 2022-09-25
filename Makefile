#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

# MThis is the Makefile for acadia-platform. Based on from genctl/Acadia-service-workspace

CONFIG ?= ceph-nvmeof.conf
TEST_IMAGE := $(shell docker image ls | grep test_image | tr -s ' ' | cut -d ' ' -f1 )
SHELL := /bin/bash
PROJDIR := $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
MODULE := control
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

# Utility Function to activate the Python Virtual Environment
define callpyvenv =
        (source ${PYVENV}/bin/activate; export PATH=$(PYVENV)/bin; $(1))
endef

## setup: setup add requirements
.PHONY: setup
setup:
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
run:
	@python3 -m $(MODULE) -c $(CONFIG)

## test: Run tests
.PHONY: test
test: $(PYVENV)
	@pytest

## test_image: Build a test container for unit tests
.PHONY: test_image
test_image:
ifeq ($(TEST_IMAGE),test_image)
	@echo "test_image: Reusing existing test_image:latest"
else
	@echo mode is development
	docker build --network=host -t test_image:latest -f docker/Dockerfile.test .
endif

## unittests: Run unit tests for gateway
.PHONY: unittests
unittests: $(PYVENV) test_image
	@docker run -it -v $${PWD}:/src -w /src -e HOME=/src \
		test_image:latest ./test.sh


## image: Build spdk image if it does not exist
.PHONY: spdk
spdk:
ifneq ($(DOCKER_VERSION), $(SPDK_VERSION))
	docker build \
	--network=host \
	--build-arg spdk_version=$(SPDK_VERSION) \
	--build-arg spdk_branch=ceph-nvmeof \
	${DOCKER_NO_CACHE} \
	-t spdk:$(SPDK_VERSION) -f docker/Dockerfile.spdk .
else
	@echo "Docker image for version: $(SPDK_VERSION) exists"
endif

## spdk_rpms: Copy the rpms from spdk container in output directory
.PHONY: spdk_rpms
spdk_rpms:
	docker run --rm -v $(curr_dir)/output:/output spdk:$(SPDK_VERSION) \
	bash -c "cp -f /tmp/rpms/*.rpm /output/"


## gateway: Build the nvme gateway image. The spdk image needs to be built first.
.PHONY: gateway
gateway: spdk grpc
	docker build \
	--network=host \
	${DOCKER_NO_CACHE} \
	--build-arg spdk_version=$(SPDK_VERSION) \
	-t ${CONT_NAME}:${CONT_VERS} -f docker/Dockerfile.gateway .

## push_gateway: Publish container into the docker registry for devs
.PHONY: push_gateway
push_gateway: gateway
	docker tag ${CONT_NAME}:${CONT_VERS} ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}
	docker push ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}

## clean: Clean local images and rpms
.PHONY: clean
clean:
	find control -name __pycache__ -type d -delete
	rm -rf output

## help: Describes the help
.PHONY: help
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

# Setup a Python Virtual Environment to use for running the static analysis and unit tests
$(PYVENV):
	[ -d $(PYVENV) ] || (mkdir -p $(PYVENV); python3 -m venv $(PYVENV))
	$(PYVENV)/bin/pip3 install --upgrade pip==20.3.3
	$(PYVENV)/bin/pip3 install -r $(PROJDIR)/requirements.txt
	$(PYVENV)/bin/pip3 install -e $(PROJDIR)
