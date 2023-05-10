## Deployment commands (docker-compose):

# Docker and docker-compose specific commands
DOCKER = docker

PYTHON_INTERPRETER = python3
VERSION != $(PYTHON_INTERPRETER) -c "print(__import__('control').__version__)" ## Version of the NVMe-oF gateway
VERSIONS_ENV = VERSION=$(VERSION)
DOCKER_COMPOSE_ENV = $(VERSIONS_ENV) DOCKER_BUILDKIT=1
DOCKER_COMPOSE = docker-compose ## Docker-compose command
DOCKER_COMPOSE_COMMANDS = pull build push up run exec ps top images logs port \
	pause unpause stop restart down events


#OPTS = ## Docker-compose subcommand options
SCALE = 1 ## Number of nvmeof instances
CMD ?= ## Command to run with run/exec targets

.PHONY: $(DOCKER_COMPOSE_COMMANDS) shell
$(DOCKER_COMPOSE_COMMANDS):
	$(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) $@ $(OPTS) $(SVC) $(CMD)

pull: ## Download SVC images

build: ## Build SVC images
build: GIT_BRANCH != git rev-parse --abbrev-ref HEAD
build: GIT_COMMIT != git rev-parse HEAD
build: ARGS = VERSION GIT_BRANCH GIT_COMMIT
build: OPTS ?= --no-rm $(foreach arg, $(ARGS), --build-arg $(arg)="$(strip $($(arg)))")

push: ## Push SVC container images to a registry. Requires previous "docker login"

up: ## Launch Ceph cluster and SCALE instances of nvmeof containers
up: SVC ?= nvmeof
up: OPTS ?= --abort-on-container-exit --exit-code-from $(SVC) --remove-orphans --scale nvmeof=$(SCALE)


run: ## Run command CMD inside SVC containers
run: SVC ?= nvmeof-cli
run: OPTS ?= --rm

shell: ## Run shell inside SVC containers
shell: override OPTS += --entrypoint bash
shell: run

exec: ## Run command inside an existing container

ps: ## Display status of SVC containers

top: ## Display running processes in SVC containers

port: ## Print public port for a port binding

logs: ## View SVC logs
logs: MAX_LOGS = 40
logs: OPTS ?= --follow --tail=$(MAX_LOGS)
logs: SVC ?= spdk ceph nvmeof nvmeof-cli ## Services

images: ## List images

pause: ## Pause running deployment
unpause: ## Resume paused deployment

stop: ## Stop SVC

restart: ## Restart SVC

down: ## Shut down deployment
down: override SVC =
down: OPTS ?= --volumes --remove-orphans

events: ## Receive real-time events from containers

docker_compose_clean: down
	$(DOCKER) system prune --all --force --volumes --filter label="io.ceph.nvmeof"

CLEAN += docker_compose_clean
ALL += pull up ps
