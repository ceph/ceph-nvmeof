# KMIP Server Docker - Quick Start Guide

## Overview

This Docker setup provides a ready-to-use KMIP mock server with auto-generated TLS certificates for testing purposes. Both server and CLI images are **self-contained** with certificates baked in.

## Prerequisites

- Docker
- Docker Compose
- Make (optional, for convenience commands)

## Quick Start

### Option 1: Simple Docker Run (Recommended for Testing)

```bash
# 1. Build the images (server + CLI)
cd tests/kmip/
make build

# 2. Start server
docker run --network host -d --name kmip-server kmip-server:latest

# 3. Use CLI
docker run --network host --rm kmip-cli:latest create-passphrase --name mykey --value secret123
docker run --network host --rm kmip-cli:latest list
```

### Option 2: Using Docker Compose

```bash
# 1. Build the images
make build

# 2. Start the server
make run

# 3. Use CLI
docker compose run --rm kmip-cli --hostname kmip-mock-server create-passphrase --name mykey --value secret123
```

That's it! Your KMIP server is running on `localhost:5696` with TLS certificates ready.

## Manual Commands (without Make)

```bash
# Build
docker compose build

# Run
docker compose up -d kmip-server

# Stop
docker compose down
```

## Verify Server is Running

```bash
# Check status
docker ps | grep kmip

# View logs
# If you used Option 1 (Simple Docker Run):
docker logs kmip-server
# If you used Option 2 (Docker Compose):
docker logs kmip-mock-server

# Test connection
make test
```

## Using the CLI Container

### With Docker Run (Simplest)

```bash
# Create alias for convenience
alias kmip='docker run --network host --rm kmip-cli:latest'

# Use it
kmip create-passphrase --name "test" --value "secret123"
kmip list
kmip get --uuid 1
kmip destroy --uuid 1
```

### With Docker Compose

```bash
# Create alias
alias kmip_cli='docker compose run --rm kmip-cli --hostname kmip-mock-server'

# Use it
kmip_cli create-passphrase --name "test" --value "secret123"
kmip_cli get --uuid 1
```

See [CLI_CONTAINER_USAGE.md](CLI_CONTAINER_USAGE.md) for complete CLI documentation.

## Available Make Commands

| Command | Description |
|---------|-------------|
| `make build` | Build the Docker images (server + CLI) |
| `make run` | Start single server (port 5696) |
| `make stop` | Stop all containers |
| `make logs` | View server logs (follow mode) |
| `make shell` | Access container shell |
| `make test` | Test server connection |
| `make clean` | Stop and remove volumes |
| `make clean-all` | Remove everything including images |

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 5696
sudo lsof -i :5696

# Or change the port in docker-compose.yml
ports:
  - "6000:5696"  # Map to different host port
```

### Connection Refused

```bash
# Check if server is running
docker ps | grep kmip

# Check logs for errors
docker logs kmip-mock-server

# Verify health
docker inspect --format='{{.State.Health.Status}}' kmip-mock-server
```

### Certificate Issues

```bash
# Rebuild container (regenerates certs)
make clean-all
make build
make run
```

## Architecture

```
┌─────────────────────────────────────┐
│  KMIP Server Container              │
│  ┌───────────────────────────────┐  │
│  │ KMIP Server (Python)          │  │
│  │ - Port: 5696                  │  │
│  │ - TLS enabled                 │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │ Certificates (baked in)       │  │
│  │ /kmip/certs/                  │  │
│  │ ├── ca_cert.pem               │  │
│  │ ├── server_cert.pem           │  │
│  │ ├── server_key.pem            │  │
│  │ ├── client_cert.pem           │  │
│  │ └── client_key.pem            │  │
│  └───────────────────────────────┘  │
└────────────┬────────────────────────┘
             │ --network host
             ▼
      localhost:5696

┌─────────────────────────────────────┐
│  CLI Container (on-demand)          │
│  ┌───────────────────────────────┐  │
│  │ kmip_cli.py                   │  │
│  │ - Passphrase management       │  │
│  │ - Certs copied from server    │  │
│  │   during build (multi-stage)  │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

## Integration with Tests

To use with the test suite, see [test_kmip_client.py](test_kmip_client.py).

