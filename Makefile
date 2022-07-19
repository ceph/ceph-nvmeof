curr_dir := $(shell pwd)


DOCKER_NO_CACHE ?=
CONT_NAME = nvme
CONT_VERS = latest
REMOTE_REPO =

## init: Initialize the spdk submodule
.PHONY: init
init:
	@echo "dir: $(curr_dir)/spdk"
	cd $(curr_dir)/spdk && \
	git submodule update --init --recursive && \
	git -C . describe --tags --abbrev=0 | sed -r 's/-/\./g' > VERSION

## image: Build spdk rpms
.PHONY: spdk
spdk:
	tar -czf docker/spdk.tar.gz -C . spdk
	docker build \
	--network=host \
	${DOCKER_NO_CACHE} \
	-t spdk:$(shell cat spdk/VERSION) -f docker/Dockerfile.spdk .

.PHONY: spdk_rpms
spdk_rpms:
	docker run --rm -v $(curr_dir)/output:/output spdk:$(shell cat spdk/VERSION) \
	bash -c "cp -f /tmp/rpms/*.rpm /output/"

## image: Build the nvme gateway image
.PHONY: image
image:
	docker build \
	--network=host \
	${DOCKER_NO_CACHE} \
	-t ${CONT_NAME}:${CONT_VERS} -f docker/Dockerfile.gateway .

## push_image: Publish container into the docker registry for devs
.PHONY: push_image
push_image: image
	docker tag ${CONT_NAME}:${CONT_VERS} ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}
	docker push ${REMOTE_REPO}/${CONT_NAME}:${CONT_VERS}

.PHONY: help
help:
	@echo "Usage: \n"
