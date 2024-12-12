# Make config
MAKEFLAGS += --no-builtin-rules --no-builtin-variables
TARGET_ARCH := $(shell uname -m)
.SUFFIXES:

# Assign default CPU arch related parameters
include .env
ifneq (, $(filter $(TARGET_ARCH), arm64 aarch64))
ceph_repo_arch = arm64
TARGET_PLATFORM = linux/arm64
SPDK_TARGET_ARCH = armv8-a+crypto
SPDK_MAKEFLAGS = $(shell echo "DPDKBUILD_FLAGS=-Dplatform=generic -j $$(nproc)")
else ifneq (, $(filter $(TARGET_ARCH), amd64 x86_64))
ceph_repo_arch = x86_64
TARGET_PLATFORM = linux/amd64
SPDK_TARGET_ARCH = x86-64-v2
else
$(error Unspported CPU arch '$(TARGET_ARCH)' !! Set TARGET_ARCH to x86_64, amd64 or arm64, aarch64 arches)
endif

# Includes
include mk/containerized.mk
include mk/demo.mk
include mk/demosecurepsk.mk
include mk/demosecuredhchap.mk
include mk/misc.mk
include mk/autohelp.mk

## Basic targets:
.DEFAULT_GOAL := all
all: setup $(ALL)

setup: ## Configure huge-pages (requires sudo/root password)

	@echo Setup core dump pattern as /tmp/coredump/core.*
	mkdir -p /tmp/coredump
	sudo mkdir -p /var/log/ceph
	sudo chmod 0755 /var/log/ceph
	sudo bash -c 'echo "|/usr/bin/env tee /tmp/coredump/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern'
	sudo bash -c 'echo $(HUGEPAGES) > $(HUGEPAGES_DIR)'
	@echo Actual Hugepages allocation: $$(cat $(HUGEPAGES_DIR))
	@[ $$(cat $(HUGEPAGES_DIR)) -eq $(HUGEPAGES) ]

build pull logs down: SVC ?= ceph spdk bdevperf nvmeof nvmeof-devel nvmeof-cli discovery

build: export NVMEOF_GIT_REPO != git remote get-url origin
build: export NVMEOF_GIT_BRANCH != git name-rev --name-only HEAD
build: export NVMEOF_GIT_COMMIT != git rev-parse HEAD
build: export SPDK_GIT_REPO != git -C spdk remote get-url origin
build: export SPDK_GIT_BRANCH != git -C spdk name-rev --name-only HEAD
build: export SPDK_GIT_COMMIT != git rev-parse HEAD:spdk
build: export BUILD_DATE != date -u +"%Y-%m-%d %H:%M:%S %Z"
build: export NVMEOF_GIT_MODIFIED_FILES != git status -s | grep -e "^ *M" | sed 's/^ *M //' | xargs

# Variables
SHAMAN_FETCH_ATTEMPTS := 3

# Fetch and export CEPH_CLUSTER_CEPH_REPO_BASEURL with retries
build: export CEPH_CLUSTER_CEPH_REPO_BASEURL != \
	for i in $$(seq 1 $(SHAMAN_FETCH_ATTEMPTS)); do \
		>&2 echo "Attempt ($$i): Fetching URL for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$(CEPH_SHA)..."; \
		url=$$(curl -s https://shaman.ceph.com/api/repos/ceph/$(CEPH_BRANCH)/$(CEPH_SHA)/centos/9/ | jq -r '.[] | select(.status == "ready" and .archs[] == "$(ceph_repo_arch)") | .url'); \
		if [ -n "$$url" ]; then \
			>&2 echo "Success: Retrieved URL for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$(CEPH_SHA): $$url"; \
			echo "$$url"; \
			break; \
		fi; \
		>&2 echo "Retrying... Failed attempt ($$i) for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$(CEPH_SHA)"; \
		sleep 2; \
	done; \
	if [ -z "$$url" ]; then \
		>&2 echo "Failure: Unable to retrieve a valid URL for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$(CEPH_SHA) after $(SHAMAN_FETCH_ATTEMPTS) attempts"; \
		exit 1; \
	fi

build: export TARGET_PLATFORM := $(TARGET_PLATFORM)
build: export SPDK_TARGET_ARCH := $(SPDK_TARGET_ARCH)
build: export SPDK_MAKEFLAGS := $(SPDK_MAKEFLAGS)
up: ## Launch services
up: SCALE?= 1 ## Number of gateways
up:
	@$(CURDIR)/tests/ha/start_up.sh $(SCALE)

clean: $(CLEAN) setup  ## Clean-up environment
clean: override HUGEPAGES = 0
clean:
	/usr/bin/rm -f control/proto/gateway_pb2_grpc.py control/proto/gateway_pb2.py control/proto/gateway_pb2.pyi control/proto/monitor_pb2_grpc.py control/proto/monitor_pb2.py control/proto/monitor_pb2.pyi

update-lockfile: run ## Update dependencies in lockfile (pdm.lock)
update-lockfile: SVC=nvmeof-builder-base
update-lockfile: override OPTS+=--entrypoint=pdm
update-lockfile: CMD=update --no-sync --no-isolation --no-self --no-editable

protoc: run ## Generate gRPC protocol files
protoc: SVC=nvmeof-builder
protoc: override OPTS+=--entrypoint=pdm
protoc: CMD=run protoc

EXPORT_DIR ?= /tmp ## Directory to export packages (RPM and Python wheel)
export-rpms: SVC=spdk-rpm-export
export-rpms: OPTS=--entrypoint=cp -v $(strip $(EXPORT_DIR)):/tmp
export-rpms: CMD=-r /rpm /tmp
export-rpms: run ## Build SPDK RPMs and copy them to $(EXPORT_DIR)/rpm
	@echo RPMs exported to:
	@find $(strip $(EXPORT_DIR))/rpm -type f

export-python: SVC=nvmeof-python-export
export-python: OPTS=--entrypoint=pdm -v $(strip $(EXPORT_DIR)):/tmp
export-python: CMD=build --no-sdist --no-clean -d /tmp
export-python: run ## Build Ceph NVMe-oF Gateway Python package and copy it to /tmp
	@echo Python wheel exported to:
	@find $(strip $(EXPORT_DIR))/ceph_nvmeof-*.whl

help: AUTOHELP_SUMMARY = Makefile to build and deploy the Ceph NVMe-oF Gateway
help: autohelp

.PHONY: all setup clean help update-lockfile protoc export-rpms export-python
