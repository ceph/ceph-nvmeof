# Makefile for KMIP Mock Server Docker deployment

# Python script for testing connection
define TEST_CONNECTION
import socket
import sys

s = socket.socket()
s.settimeout(2)
try:
    s.connect(('127.0.0.1', 5696))
    print('✓ Server is accepting connections on port 5696')
    s.close()
except Exception as e:
    print(f'✗ Connection failed: {e}')
    sys.exit(1)
endef
export TEST_CONNECTION

# Docker Compose command (can be overridden, e.g., DOCKER_COMPOSE=docker-compose make build)
DOCKER_COMPOSE ?= docker compose

.PHONY: help build run stop logs shell clean test clean-all

help:
	@echo "KMIP Mock Server - Docker Commands"
	@echo "==================================="
	@echo "build      - Build the Docker image"
	@echo "run        - Run single KMIP server (port 5696)"
	@echo "stop       - Stop all running containers"
	@echo "logs       - Show server logs"
	@echo "shell      - Access container shell"
	@echo "test       - Run connection test"
	@echo "clean      - Stop containers and remove volumes"
	@echo "clean-all  - Remove everything including images"

build:
	@echo "Building KMIP server image..."
	$(DOCKER_COMPOSE) build kmip-server
	@echo "Building KMIP CLI image (depends on server)..."
	$(DOCKER_COMPOSE) build kmip-cli

run:
	@echo "Starting KMIP server on port 5696..."
	$(DOCKER_COMPOSE) up -d kmip-server
	@echo "Waiting for server to be ready..."
	@sleep 3
	@$(DOCKER_COMPOSE) logs kmip-server
	@echo ""
	@echo "KMIP server is running!"
	@echo "  Port: 5696"
	@echo "  Logs: make logs"

stop:
	@echo "Stopping KMIP server..."
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

shell:
	$(DOCKER_COMPOSE) exec kmip-server /bin/sh
test:
	@echo "Testing KMIP server connection..."
	@$(DOCKER_COMPOSE) exec -T kmip-server python3 -c "$$TEST_CONNECTION"

clean:
	@echo "Stopping all containers and removing volumes..."
	$(DOCKER_COMPOSE) down -v
	@echo "Cleanup complete"

clean-all: clean
	@echo "Removing Docker images..."
	docker rmi kmip-server:latest 2>/dev/null || true
	docker rmi kmip-cli:latest 2>/dev/null || true
	@echo "Full cleanup complete"
