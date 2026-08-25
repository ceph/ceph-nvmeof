# KMIP CLI Container - Quick Guide

## Build (builds both server and CLI)

```bash
cd tests/kmip/
make build
```

## Simple Usage (Docker Run - Recommended)

### Start Server

```bash
docker run --network host -d --name kmip-server kmip-server:latest
```

### Setup Bash Alias

```bash
# Add to your ~/.bashrc or run in your terminal:
alias kmip='docker run --network host --rm kmip-cli:latest'

# Now you can use it simply:
kmip create-passphrase --name "test" --value "secret123"
kmip list
kmip get --uuid 1
kmip destroy --uuid 1
kmip info --uuid 1
```

### Without Alias

```bash
docker run --network host --rm kmip-cli:latest create-passphrase --name "test" --value "secret123"
```

## Alternative: Docker Compose Usage

### Start Server

```bash
make run
```

### Setup Alias

```bash
alias kmip_cli='docker compose run --rm kmip-cli --hostname kmip-mock-server'

# Use it
kmip_cli create-passphrase --name "test" --value "secret123"
```

## Certificate Requirement

**YES, the CLI requires certificates!** KMIP uses TLS client certificate authentication.

The certificates are automatically:
- Generated when the server image is built
- Copied into the CLI image during build (multi-stage build)
- Located at `/kmip/certs/` in both containers

**Both images are self-contained** You don't need to do anything - it's all automatic.

## Quick Example (Docker Run)

```bash
cd tests/kmip/

# Build
make build

# Start server
docker run --network host -d --name kmip-server kmip-server:latest

# Create alias
alias kmip='docker run --network host --rm kmip-cli:latest'

# Create passphrase
kmip create-passphrase --name "rbd-key-1" --value "mysecret123"
# Output: uuid: 1

# Retrieve it
kmip get --uuid 1

# List all
kmip list

# Done!
```

## Quick Example (Docker Compose)

```bash
cd tests/kmip/

# Build
make build

# Start server  
make run

# Create alias
alias kmip_cli='docker compose run --rm kmip-cli --hostname kmip-mock-server'

# Create passphrase
kmip_cli create-passphrase --name "rbd-key-1" --value "mysecret123"
# Output: uuid: 1

# Retrieve it
kmip_cli get --uuid 1

# Done!
```

## JSON Output

Use each subcommand's `-o/--output` option to select JSON output:
```bash
kmip_cli create-passphrase --name "test" --value "secret" -o json
# or
kmip_cli get --uuid 1 --output json
```

## Summary

✅ `make build` - Builds both server AND CLI containers (server first, then CLI)  
✅ Certificates are automatic (baked into both images)  
✅ Use `--network host` for simple networking (defaults work!)  
✅ No volumes needed - fully self-contained images!  
✅ Use alias for simple commands
