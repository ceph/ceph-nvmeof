## Demo:

# rbd
rbd: exec
rbd: SVC = ceph
rbd: CMD = bash -c "rbd -p $(RBD_POOL) info $(RBD_IMAGE_NAME) || rbd -p $(RBD_POOL) create $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE)"

# demo
# the first gateway in docker environment, hostname defaults to container id
demo: export NVMEOF_HOSTNAME != docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)
demo: rbd ## Expose RBD_IMAGE_NAME as NVMe-oF target
	$(NVMEOF_CLI) subsystem add --subsystem $(NQN)
	$(NVMEOF_CLI) namespace add --subsystem $(NQN) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME)
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --gateway-name $(NVMEOF_HOSTNAME) --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT)
	$(NVMEOF_CLI_IPV6) listener add --subsystem $(NQN) --gateway-name $(NVMEOF_HOSTNAME) --traddr $(NVMEOF_IPV6_ADDRESS) --trsvcid $(NVMEOF_IO_PORT) --adrfam IPV6
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host "*"

.PHONY: demo rbd
