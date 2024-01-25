## Demo:

# demo
demo:
	$(NVMEOF_CLI) subsystem add --subsystem $(NQN)
	$(NVMEOF_CLI) namespace add --subsystem $(NQN) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE) --rbd-create-image
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --gateway-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT)
	$(NVMEOF_CLI_IPV6) listener add --subsystem $(NQN) --gateway-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IPV6_ADDRESS) --trsvcid $(NVMEOF_IO_PORT) --adrfam IPV6
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host "*"

.PHONY: demo
