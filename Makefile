#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

# MThis is the Makefile for acadia-platform. Based on from genctl/Acadia-service-workspace

MODULE := control
CONFIG ?= ceph-nvmeof.conf
curr_dir := $(shell pwd)
DOCKER_NO_CACHE := --no-cache
CONT_NAME := nvme
CONT_VERS := latest
REMOTE_REPO := ""
SPDK_VERSION := $(shell cd spdk;git -C . describe --tags --abbrev=0 | sed -r 's/-/\./g')
DOCKER_VERSION := $(shell docker image ls | grep spdk | tr -s ' ' | cut -d ' ' -f2)
TEST_IMAGE := $(shell docker image ls | grep test_image | tr -s ' ' | cut -d ' ' -f1 )


## setup: setup add requirements
.PHONY: setup
setup: requirements.txt
	pip3 install -r requirements.txt
	@echo "dir: $(curr_dir)/spdk"
	cd $(curr_dir)/spdk && \
	git submodule update --init --recursive

## grpc: Compile grpc code
.PHONY: grpc
grpc:
	@python3 -m grpc_tools.protoc \
		--proto_path=./$(MODULE)/proto \
		--python_out=./$(MODULE)/proto \
		--grpc_python_out=./$(MODULE)/proto \
		./$(MODULE)/proto/*.proto
	@sed -E -i 's/^import.*_pb2/from . \0/' ./$(MODULE)/proto/*.py

## run: Run the gateway server
.PHONY: run
run:
	@python3 -m $(MODULE) -c $(CONFIG)

## test: Run tests
.PHONY: test
test:
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
unittests: test_image
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
	find . -name __pycache__ -type d -delete
	rm -rf output

## help: Describes the help
.PHONY: help
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

