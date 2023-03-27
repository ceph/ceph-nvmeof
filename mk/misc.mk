## Miscellaneous:

# nvmeof_cli
SERVER_ADDRESS = nvmeof ## Address of the nvmeof gateway
SERVER_PORT = 5500 ## Port of the nvmeof gateway
NVMEOF_CLI = $(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) run --rm nvmeof-cli --server-address $(SERVER_ADDRESS) --server-port $(SERVER_PORT)

alias: ## Print bash alias command for the nvmeof-cli. Usage: "eval $(make alias)"
	@echo alias nvmeof-cli=\"$(NVMEOF_CLI)\"

.PHONY: alias
