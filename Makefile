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
SPDK_CONFIGURE_DSA=
SPDK_MAKEFLAGS = $(shell echo "DPDKBUILD_FLAGS=-Dplatform=generic -j $$(nproc)")
else ifneq (, $(filter $(TARGET_ARCH), amd64 x86_64))
ceph_repo_arch = x86_64
TARGET_PLATFORM = linux/amd64
SPDK_TARGET_ARCH = x86-64-v2
SPDK_CONFIGURE_DSA=--with-idxd
else
$(error Unsupported CPU arch '$(TARGET_ARCH)' !! Set TARGET_ARCH to x86_64, amd64 or arm64, aarch64 arches)
endif

# Includes
include mk/containerized.mk
include mk/demo.mk
include mk/demosecurepsk.mk
include mk/demosecuredhchap.mk
include mk/demosecuredhchap_ctrlr.mk
include mk/misc.mk
include mk/autohelp.mk

## Basic targets:
.DEFAULT_GOAL := all
all: setup $(ALL)

verify: ## Run Python source files through flake8
	@echo Verifying Python source files
	flake8 control/*.py tests/*.py tests/kmip/*.py

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
build: export SPDK_CONFIGURE_DSA := $(SPDK_CONFIGURE_DSA)

# Variables
SHAMAN_FETCH_ATTEMPTS := 3

# Only perform the Ceph Shaman repo lookup when the build path is an explicit
# goal, so non-build targets (help, clean, verify, ...) don't hit the network
# on every Makefile parse
ifneq (,$(filter build check-ceph-repo-url,$(MAKECMDGOALS)))
# Fetch shaman repo URL and the git SHA it corresponds to (same attempt).
# Output: <url> <sha>
CEPH_SHAMAN_FETCH := $(shell \
	for i in $$(seq 1 $(SHAMAN_FETCH_ATTEMPTS)); do \
		ceph_commit_sha="$(CEPH_SHA)"; \
		resolved_sha=; \
		tag_sha=; \
		tag_type=; \
		if [ "$$ceph_commit_sha" = "latest" ]; then \
			idx=$$((i - 1)); \
			ceph_commit_sha=$$(curl -s \
				"https://shaman.ceph.com/api/search/?status=ready&project=ceph&ref=$(CEPH_BRANCH)&flavor=default&distros=rocky/10/$(ceph_repo_arch)" \
				| jq -r "sort_by(.modified) | reverse | .[$$idx].sha1"); \
			>&2 echo "Attempt ($$i): Using 'latest' commit SHA for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH): $$ceph_commit_sha"; \
		elif [ "$${ceph_commit_sha#v}" != "$$ceph_commit_sha" ]; then \
			>&2 echo "Attempt ($$i): Resolving release tag '$$ceph_commit_sha' to git commit SHA..."; \
			tag_response=$$(curl -s "https://api.github.com/repos/ceph/ceph/git/ref/tags/$$ceph_commit_sha"); \
			tag_sha=$$(echo "$$tag_response" | jq -r '.object.sha'); \
			tag_type=$$(echo "$$tag_response" | jq -r '.object.type'); \
			if [ "$$tag_type" = "tag" ]; then \
				>&2 echo "Attempt ($$i): Dereferencing annotated tag..."; \
				resolved_sha=$$(curl -s "https://api.github.com/repos/ceph/ceph/git/tags/$$tag_sha" | jq -r '.object.sha'); \
			elif [ "$$tag_type" = "commit" ]; then \
				resolved_sha="$$tag_sha"; \
			fi; \
			if [ -n "$$resolved_sha" ] && [ "$$resolved_sha" != "null" ]; then \
				>&2 echo "Attempt ($$i): Resolved tag '$$ceph_commit_sha' to commit SHA: $$resolved_sha"; \
				ceph_commit_sha="$$resolved_sha"; \
			else \
				>&2 echo "Attempt ($$i): Failed to resolve tag '$$ceph_commit_sha' to a commit SHA"; \
				sleep 2; \
				continue; \
			fi; \
		fi; \
		if ! printf '%s' "$$ceph_commit_sha" | grep -qE '^[0-9a-fA-F]{40}$$'; then \
			>&2 echo "Attempt ($$i): Refused non-SHA value '$$ceph_commit_sha' (expected a 40-char commit SHA)"; \
			sleep 2; \
			continue; \
		fi; \
		ceph_commit_sha=$$(printf '%s' "$$ceph_commit_sha" | tr 'A-F' 'a-f'); \
		>&2 echo "Attempt ($$i): Fetching URL for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$$ceph_commit_sha..."; \
		url=$$(curl -s "https://shaman.ceph.com/api/repos/ceph/$(CEPH_BRANCH)/$$ceph_commit_sha/rocky/10/" | jq -r '[.[] | select(.status == "ready" and .archs[] == "$(ceph_repo_arch)")] | first | .url // empty'); \
		if [ -n "$$url" ] && [ "$$url" != "null" ]; then \
			if printf '%s' "$$url" | grep -q '[[:space:]]'; then \
				>&2 echo "Attempt ($$i): Refused shaman URL with whitespace: $$url"; \
				sleep 2; \
				continue; \
			fi; \
			>&2 echo "Success: Retrieved URL for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$$ceph_commit_sha: $$url"; \
			printf "%s %s" "$$url" "$$ceph_commit_sha"; \
			break; \
		fi; \
		>&2 echo "Retrying... Failed attempt ($$i) for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$$ceph_commit_sha"; \
		sleep 2; \
	done)
CEPH_CLUSTER_CEPH_REPO_BASEURL := $(firstword $(CEPH_SHAMAN_FETCH))
CEPH_CLUSTER_CEPH_SHA := $(lastword $(CEPH_SHAMAN_FETCH))
endif

build: export CEPH_CLUSTER_CEPH_REPO_BASEURL := $(CEPH_CLUSTER_CEPH_REPO_BASEURL)
build: export CEPH_CLUSTER_CEPH_SHA := $(CEPH_CLUSTER_CEPH_SHA)
build: check-ceph-repo-url

check-ceph-repo-url:
	@test -n "$(CEPH_CLUSTER_CEPH_REPO_BASEURL)" && test "$(CEPH_CLUSTER_CEPH_REPO_BASEURL)" != "null" || \
		(>&2 echo "Failure: No ready Ceph Shaman repo for arch=$(ceph_repo_arch), branch=$(CEPH_BRANCH), sha=$(CEPH_CLUSTER_CEPH_SHA) (CEPH_SHA=$(CEPH_SHA)) on rocky/10"; \
		 >&2 echo "Shaman currently provides rocky/10 repos only for x86_64 in this branch/ref combination."; \
		 exit 1)
	@printf '%s' "$(CEPH_CLUSTER_CEPH_SHA)" | grep -qE '^[0-9a-fA-F]{40}$$' || \
		(>&2 echo "Failure: CEPH_CLUSTER_CEPH_SHA is not a 40-char commit SHA: '$(CEPH_CLUSTER_CEPH_SHA)'"; \
		 exit 1)

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

# Re-resolve for CI/Docker: Rocky-style manylinux_2_39 x86_64 + aarch64, any Python 3.12.x.
# Run after `make update-lockfile` so pins stay in sync with dual targets (see README).
regenerate-lockfile: ## Re-resolve pdm.lock for manylinux x86_64 + aarch64 (Python 3.12.x)
regenerate-lockfile: DOCKER_COMPOSE_ENV = DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1
regenerate-lockfile:
	$(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) run --rm --entrypoint pdm $(OPTS) nvmeof-builder-base lock -S cross_platform,inherit_metadata --no-isolation -v --platform manylinux_2_39_x86_64 --python ">=3.12,<3.13"
	$(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) run --rm --entrypoint pdm $(OPTS) nvmeof-builder-base lock --no-isolation -v --platform manylinux_2_39_aarch64 --python ">=3.12,<3.13" --append

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

.PHONY: all setup clean help update-lockfile regenerate-lockfile protoc export-rpms export-python check-ceph-repo-url
