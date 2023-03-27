## Demo:

# rbd
RBD_IMAGE_NAME = demo_image ## Name of the RBD image
RBD_IMAGE_SIZE = 10M ## Size of the RBD image

rbd: exec
rbd: SVC = ceph
rbd: CMD = bash -c "rbd info $(RBD_IMAGE_NAME) || rbd create $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE)"

# demo
BDEV_NAME = demo_bdev ## Name of the bdev
NQN = nqn.2016-06.io.spdk:cnode1 ## NVMe Qualified Name address
SERIAL = SPDK00000000000001 ## Serial number
LISTENER_PORT = 4420 ## Listener port

demo: rbd ## Expose RBD_IMAGE_NAME as NVMe-oF target
	$(NVMEOF_CLI) create_bdev --pool rbd --image $(RBD_IMAGE_NAME) --bdev $(BDEV_NAME)
	$(NVMEOF_CLI) create_subsystem --subnqn $(NQN) --serial $(SERIAL)
	$(NVMEOF_CLI) add_namespace --subnqn $(NQN) --bdev $(BDEV_NAME)
	$(NVMEOF_CLI) create_listener --subnqn $(NQN) -s $(LISTENER_PORT)
	$(NVMEOF_CLI) add_host --subnqn $(NQN) --host "*"

.PHONY: demo rbd
