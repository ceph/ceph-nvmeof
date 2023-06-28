HUGEPAGES = 2048 # 4 GB
HUGEPAGES_DIR = /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

# Includes
include mk/containerized.mk
include mk/demo.mk
include mk/misc.mk
include mk/autohelp.mk

## Basic targets:
.DEFAULT_GOAL := all
all: setup $(ALL)

setup: ## Configure huge-pages (requires sudo/root password)
	sudo bash -c 'echo $(HUGEPAGES) > $(HUGEPAGES_DIR)'
	@echo Actual Hugepages allocation: $$(cat $(HUGEPAGES_DIR))
	@[ $$(cat $(HUGEPAGES_DIR)) -eq $(HUGEPAGES) ]

build push pull: SVC = spdk nvmeof nvmeof-cli ceph

build: export NVMEOF_GIT_BRANCH != git name-rev --name-only HEAD
build: export NVMEOF_GIT_COMMIT != git rev-parse HEAD
build: export SPDK_GIT_REPO != git -C spdk remote get-url origin
build: export SPDK_GIT_BRANCH != git -C spdk name-rev --name-only HEAD
build: export SPDK_GIT_COMMIT != git rev-parse HEAD:spdk
build: export BUILD_DATE != date -u +"%Y-%m-%dT%H:%M:%SZ"


up: SVC = nvmeof ## Services
up: OPTS ?= --abort-on-container-exit
up: override OPTS += --no-build --remove-orphans --scale nvmeof=$(SCALE)

clean: $(CLEAN)  ## Clean-up environment

help: AUTOHELP_SUMMARY = Makefile to build and deploy the Ceph NVMe-oF Gateway
help: autohelp

.PHONY: all setup clean help
