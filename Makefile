# Make config
MAKEFLAGS += --no-builtin-rules --no-builtin-variables
.SUFFIXES:

# Includes
include .env
include mk/containerized.mk
include mk/demo.mk
include mk/misc.mk
include mk/autohelp.mk

## Basic targets:
.DEFAULT_GOAL := all
all: setup $(ALL)

setup: ## Configure huge-pages (requires sudo/root password)

	@echo Setup core dump pattern as /tmp/coredump/core.*
	mkdir -p /tmp/coredump
	sudo mkdir -p /var/log/ceph
	sudo bash -c 'echo "|/usr/bin/env tee /tmp/coredump/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern'
	sudo bash -c 'echo $(HUGEPAGES) > $(HUGEPAGES_DIR)'
	@echo Actual Hugepages allocation: $$(cat $(HUGEPAGES_DIR))
	@[ $$(cat $(HUGEPAGES_DIR)) -eq $(HUGEPAGES) ]

build pull logs: SVC ?= ceph spdk bdevperf nvmeof nvmeof-devel nvmeof-cli discovery

build: export NVMEOF_GIT_REPO != git remote get-url origin
build: export NVMEOF_GIT_BRANCH != git name-rev --name-only HEAD
build: export NVMEOF_GIT_COMMIT != git rev-parse HEAD
build: export SPDK_GIT_REPO != git -C spdk remote get-url origin
build: export SPDK_GIT_BRANCH != git -C spdk name-rev --name-only HEAD
build: export SPDK_GIT_COMMIT != git rev-parse HEAD:spdk
build: export BUILD_DATE != date -u +"%Y-%m-%d %H:%M:%S %Z"
build: export NVMEOF_GIT_MODIFIED_FILES != git status -s | grep -e "^ *M" | sed 's/^ *M //' | xargs
build: export CEPH_CLUSTER_CEPH_REPO_BASEURL != curl -s https://shaman.ceph.com/api/repos/ceph/$(CEPH_BRANCH)/$(CEPH_SHA)/centos/9/ | jq -r '.[0].url'

up: ## Launch services
up: SVC ?= ceph nvmeof ## Services
up: OPTS ?= --abort-on-container-exit --exit-code-from $(SVC) --remove-orphans
#up: override OPTS += --scale nvmeof=$(SCALE)

clean: $(CLEAN) setup  ## Clean-up environment
clean: override HUGEPAGES = 0

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
