curr_dir := $(shell pwd)


DOCKER_NO_CACHE := --no-cache
CONT_NAME := nvme
CONT_VERS := latest
REMOTE_REPO :=
SPDK_VERSION := $(shell cd spdk;git -C . describe --tags --abbrev=0 | sed -r 's/-/\./g')
DOCKER_VERSION := $(shell docker image ls | grep spdk | tr -s ' ' | cut -d ' ' -f2)

## init: Initialize the spdk submodule
.PHONY: init
init:
	@echo "dir: $(curr_dir)/spdk"
	cd $(curr_dir)/spdk && \
	git submodule update --init --recursive

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

grpc:
	@python3 -m grpc_tools.protoc \
		--proto_path=./$(MODULE)/proto \
		--python_out=./$(MODULE)/proto \
		--grpc_python_out=./$(MODULE)/proto \
		./$(MODULE)/proto/*.proto
	@sed -E -i 's/^import.*_pb2/from . \0/' ./$(MODULE)/proto/*.py

## image: Build the nvme gateway image. The spdk image needs to be built first.
.PHONY: image
image: spdk grpc
	docker build \
	--network=host \
	${DOCKER_NO_CACHE} \
	--build-arg spdk_version=$(SPDK_VERSION) \
	-t ${CONT_NAME}:${CONT_VERS} -f docker/Dockerfile.gateway .

## push_image: Publish container into the docker registry for devs
.PHONY: push_image
push_image: image
	docker tag ${CONT_NAME}:${CONT_VERS} ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}
	docker push ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}

## clean: Clean local images and rpms
.PHONY: clean
clean: 
	find . -name __pycache__ -type d -delete
	rm -rf output
	
.PHONY: help
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'
