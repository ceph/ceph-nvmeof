## Demo secure:

HOSTNQN=`cat /etc/nvme/hostnqn`
HOSTNQN2=`cat /etc/nvme/hostnqn | sed 's/......$$/ffffff/'`
NVMEOF_IO_PORT2=`expr $(NVMEOF_IO_PORT) + 1`
# demosecure
demosecure:
	$(NVMEOF_CLI) subsystem add --subsystem $(NQN) --no-group-append
	$(NVMEOF_CLI) namespace add --subsystem $(NQN) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE) --rbd-create-image
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --host-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT) --secure
	$(NVMEOF_CLI) listener add --subsystem $(NQN) --host-name `docker ps -q -f name=$(NVMEOF_CONTAINER_NAME)` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT2)
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host-nqn "$(HOSTNQN)" --psk "NVMeTLSkey-1:01:YzrPElk4OYy1uUERriPwiiyEJE/+J5ckYpLB+5NHMsR2iBuT:"
	$(NVMEOF_CLI) host add --subsystem $(NQN) --host-nqn "$(HOSTNQN2)"

.PHONY: demosecure
