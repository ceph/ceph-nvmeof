## Miscellaneous:

# nvmeof_cli
NVMEOF_CLI = $(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) run --rm nvmeof-cli --server-address $(NVMEOF_IP_ADDRESS) --server-port $(NVMEOF_GW_PORT)

alias: ## Print bash alias command for the nvmeof-cli. Usage: "eval $(make alias)"
	@echo alias nvmeof-cli=\"$(NVMEOF_CLI)\"

.PHONY: alias
