## Deployment commands (docker-compose):

# Docker and docker-compose specific commands
DOCKER = docker
# Require docker-compose v2 to support multi-platform build option 'services.xxx.build.platforms'
DOCKER_COMPOSE != DOCKER=$$(command -v docker) && $$DOCKER compose version > /dev/null && printf "%s compose\n" $$DOCKER
ifndef DOCKER_COMPOSE
$(error DOCKER_COMPOSE command not found. Please install from: https://docs.docker.com/compose/install/)
endif
DOCKER_COMPOSE_COMMANDS = pull build run exec ps top images logs port \
	pause unpause stop restart down events

OPTS ?= ## Docker-compose subcommand options
CMD ?=  ## Command to run with run/exec targets

.PHONY: $(DOCKER_COMPOSE_COMMANDS) shell
$(DOCKER_COMPOSE_COMMANDS):
	$(DOCKER_COMPOSE_ENV) $(DOCKER_COMPOSE) $@ $(OPTS) $(SVC) $(CMD)

pull: ## Download SVC images

build:  ## Build SVC images
build: DOCKER_COMPOSE_ENV = DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1

push: QUAY := $(CONTAINER_REGISTRY)
push: IMAGES := nvmeof nvmeof-cli
push: TAG_SUFFIX :=  # e.g. "-aarch64" for multi-arch image push
push: ## Push nvmeof and nvmeof-cli containers images to quay.io registries
	@echo "Push images $(IMAGES) to registry $(QUAY)";  \
	short_version=$(shell echo $(VERSION) | cut -d. -f1-2); \
	versions="$(VERSION) $${short_version} latest"; \
	for image in $(IMAGES); do \
		for version in $$versions; do \
			if [ -n "$(TAG_SUFFIX)" ] && [ "$$version" = "$(VERSION)" ] || \
			   [ -z "$(TAG_SUFFIX)" ]; then \
				echo "Pushing image $(QUAY)/$${image}:$${version}$(TAG_SUFFIX) ...";  \
				docker tag $(CONTAINER_REGISTRY)/$${image}:$(VERSION)$(TAG_SUFFIX) $(QUAY)/$${image}:$${version}$(TAG_SUFFIX) && \
				docker push $(QUAY)/$${image}:$${version}$(TAG_SUFFIX); \
			fi \
		done \
	done

push-manifest-list: QUAY := $(CONTAINER_REGISTRY)
push-manifest-list: IMAGES := nvmeof nvmeof-cli
push-manifest-list:
	@echo "Push images $(IMAGES) manifestlists to $(QUAY)"; \
	short_version=$(shell echo $(VERSION) | cut -d. -f1-2); \
	versions="$(VERSION) $${short_version} latest"; \
	for image in $(IMAGES); do \
		source_list=$$(docker image list --filter reference="$(QUAY)/$${image}:$(VERSION)*" --format "{{.Repository}}:{{.Tag}}"); \
		for version in $$versions; do \
			echo "Pushing image manifestlist $(QUAY)/$${image}:$${version} ..."; \
			docker buildx imagetools create  --tag $(QUAY)/$${image}:$${version} $$source_list; \
		done \
	done

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
down: override OPTS += --volumes --remove-orphans

events: ## Receive real-time events from containers

.PHONY:
image_name:
	@$(DOCKER_COMPOSE) config --format=json | jq '.services."$(SVC)".image'

.PHONY:
docker_compose_clean: down
	$(DOCKER) system prune --all --force --volumes --filter label="io.ceph.nvmeof"

.PHONY:
clean_cache: ## Clean the Docker build cache
	$(DOCKER) builder prune --force --all

CLEAN += docker_compose_clean
ALL += pull up ps
