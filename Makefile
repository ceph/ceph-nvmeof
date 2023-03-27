HUGEPAGES_2MB = 2048 # 4 GB

# Includes
include mk/containerized.mk
include mk/demo.mk
include mk/misc.mk
include mk/autohelp.mk

## Basic targets:
.DEFAULT_GOAL := all
all: setup $(ALL)

setup: ## Configure huge-pages (requires sudo/root password)
	sudo bash -c 'echo $(HUGEPAGES_2MB) > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'
	cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

clean: $(CLEAN)  ## Clean-up environment

help: AUTOHELP_SUMMARY = Makefile to build and deploy the Ceph NVMe-oF Gateway
help: autohelp

.PHONY: all setup clean help
