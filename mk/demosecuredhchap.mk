## Demo secure DHCHAP:

HOSTNQN=`cat /etc/nvme/hostnqn`
HOSTNQN2=`cat /etc/nvme/hostnqn | sed 's/......$$/ffffff/'`
HOSTNQN3=`cat /etc/nvme/hostnqn | sed 's/......$$/fffffe/'`
NVMEOF_IO_PORT2=`expr $(NVMEOF_IO_PORT) + 1`
NVMEOF_IO_PORT3=`expr $(NVMEOF_IO_PORT) + 2`
DHCHAPKEY1=$(DHCHAP_KEY1)
DHCHAPKEY2=$(DHCHAP_KEY2)
DHCHAPKEY3=$(DHCHAP_KEY3)
# demosecuredhchap
demosecuredhchap:
	$(NVMEOF_CLI) subsystem add --subsystem $(NQN) --no-group-append
	$(NVMEOF_CLI) namespace add --subsystem $(NQN) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE) --rbd-create-image
	$(NVMEOF_CLI) namespace add --subsystem $(NQN) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME)2 --size $(RBD_IMAGE_SIZE) --rbd-create-image --no-auto-visible
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --host-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT)
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --host-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT2)
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --host-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT3)
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host-nqn $(HOSTNQN) --dhchap-key $(DHCHAPKEY1)
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host-nqn $(HOSTNQN2) --dhchap-key $(DHCHAPKEY2) --dhchap-ctrlr-key $(DHCHAPKEY3)
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host-nqn $(HOSTNQN3)
	$(NVMEOF_CLI) namespace add_host --subsystem $(NQN) --nsid 2 --host-nqn $(HOSTNQN)

.PHONY: demosecuredhchap
