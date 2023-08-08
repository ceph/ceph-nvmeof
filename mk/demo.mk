## Demo:

# rbd
rbd: exec
rbd: SVC = ceph
rbd: CMD = bash -c "rbd -p $(RBD_POOL) info $(RBD_IMAGE_NAME) || rbd -p $(RBD_POOL) create $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE)"

# demo
demo: rbd ## Expose RBD_IMAGE_NAME as NVMe-oF target
	$(NVMEOF_CLI) create_bdev --pool $(RBD_POOL) --image $(RBD_IMAGE_NAME) --bdev $(BDEV_NAME)
	$(NVMEOF_CLI) create_subsystem --subnqn $(NQN) --serial $(SERIAL)
	$(NVMEOF_CLI) add_namespace --subnqn $(NQN) --bdev $(BDEV_NAME)
	$(NVMEOF_CLI) create_listener --subnqn $(NQN) --gateway-name $(NVMEOF_HOSTNAME) --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT)
	$(NVMEOF_CLI) add_host --subnqn $(NQN) --host "*"

.PHONY: demo rbd
