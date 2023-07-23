## Deployment commands (docker-compose):

# Docker and docker-compose specific commands
DOCKER = docker
DOCKER_COMPOSE = docker-compose ## Docker-compose command
DOCKER_COMPOSE_COMMANDS = pull build push up run exec ps top images logs port \
	pause unpause stop restart down events

OPTS ?= ## Docker-compose subcommand options
SCALE ?= 1 ## Number of instances
CMD ?= ## Command to run with run/exec targets

.PHONY: $(DOCKER_COMPOSE_COMMANDS) shell
$(DOCKER_COMPOSE_COMMANDS):
	$(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) $@ $(OPTS) $(SVC) $(CMD)

pull: ## Download SVC images

build:  ## Build SVC images
build: DOCKER_COMPOSE_ENV = DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1

push: ## Push SVC container images to a registry. Requires previous "docker login"

run: ## Run command CMD inside SVC containers
run: override OPTS += --rm
run: DOCKER_COMPOSE_ENV = DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1

shell: ## Exec shell inside running SVC containers
shell: CMD = bash
shell: exec

exec: ## Run command inside an existing container

ps: ## Display status of SVC containers

top: ## Display running processes in SVC containers

port: ## Print public port for a port binding

logs: ## View SVC logs
logs: MAX_LOGS = 40
logs: OPTS ?= --follow --tail=$(MAX_LOGS)

images: ## List images

pause: ## Pause running deployment
unpause: ## Resume paused deployment

stop: ## Stop SVC

restart: ## Restart SVC

down: ## Shut down deployment
down: override SVC =
down: override OPTS += --volumes --remove-orphans

events: ## Receive real-time events from containers

docker_compose_clean: down
	$(DOCKER) system prune --all --force --volumes --filter label="io.ceph.nvmeof"

CLEAN += docker_compose_clean
ALL += pull up ps
