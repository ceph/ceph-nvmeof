## Demo secure DHCHAP controller:

SUBNQN1=$(NQN)
HOSTNQN=`cat /etc/nvme/hostnqn`
DHCHAPKEY1=$(DHCHAP_KEY5)
DHCHAPKEY2=$(DHCHAP_KEY6)
# demosecuredhchap_ctrlr
demosecuredhchap_ctrlr:
	$(NVMEOF_CLI) subsystem add --subsystem $(SUBNQN1) --no-group-append
	$(NVMEOF_CLI) namespace add --subsystem $(SUBNQN1) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE) --rbd-create-image
	$(NVMEOF_CLI) listener add --subsystem $(SUBNQN1) --host-name `$(NVMEOF_CLI) --output stdio gw info | grep "Gateway's host name:" | cut -d: -f2 | sed 's/ //g'` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT) --verify-host-name
	$(NVMEOF_CLI) host add --subsystem $(SUBNQN1) --host-nqn $(HOSTNQN) --dhchap-key $(DHCHAPKEY1) --dhchap-controller-key $(DHCHAPKEY2)

.PHONY: demosecuredhchap_ctrlr
