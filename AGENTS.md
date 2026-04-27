# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project-Specific Build/Test Commands

**Running a single test file:**
```bash
# Tests must run inside docker container with proper Ceph/SPDK environment
docker compose run --rm nvmeof-devel pytest tests/test_grpc.py -v
```

**Running tests with specific config:**
```bash
pytest --config=tests/ceph-nvmeof.tls.conf tests/test_cli.py
```

**Verify Python code (flake8):**
```bash
make verify  # Runs: flake8 control/*.py tests/*.py tests/kmip/*.py
```

**Update Python dependencies lockfile:**
```bash
make update-lockfile  # Uses PDM inside container
```

**Regenerate gRPC protobuf files:**
```bash
make protoc  # Must run after modifying .proto files in control/proto/
```

## Non-Obvious Architecture Patterns

**OMAP State Management:**
- Gateway state is persisted to Ceph OMAP (not local files)
- State keys use underscore delimiter: `GatewayState.OMAP_KEY_DELIMITER = "_"`
- Key prefixes defined in `control/state.py` (e.g., `NAMESPACE_PREFIX`, `SUBSYSTEM_PREFIX`)
- OMAP keys cannot contain underscores in their element components (validated by `is_key_element_valid()`)

**SPDK Integration:**
- SPDK runs as subprocess, not library
- RPC communication via Unix socket (default: `/var/tmp/spdk.sock`)
- Gateway must handle SIGCHLD when SPDK subprocess terminates
- SPDK target path: `/usr/local/bin/nvmf_tgt` (configurable in ceph-nvmeof.conf)

**Cluster Allocation Strategies (Non-Standard):**
Three mutually exclusive strategies for mapping BDEVs to Ceph cluster contexts:
1. `bdevs_per_cluster` - Legacy ANA group-based (per ANA group)
2. `flat_bdevs_per_cluster` - Ignores ANA groups, flat distribution
3. `cluster_connections` - Pre-defined pool, assigns to least-loaded context

Only ONE should be set in config. See `ceph-nvmeof.conf` [spdk] section.

**Discovery Service:**
- Separate service from main gateway: `python3 -m control.discovery`
- Runs on port 8009 (configurable)
- Sources target info from Ceph OMAP, not live gateways
- Can be started independently: `docker compose up --detach discovery`

## Code Style (Non-Obvious)

**Flake8 config in tox.ini:**
- Max line length: 100 characters
- No ignored errors by default (empty ignore list)
- Use `# noqa: E501` for specific line exceptions

**Import patterns:**
- Relative imports within control package: `from .proto import gateway_pb2 as pb2`
- SPDK RPC client: `import spdk.rpc.client as rpc_client`
- Protobuf JSON: `from google.protobuf import json_format`

**Logging:**
- Use `GatewayLogger` singleton, not direct logging
- Logger instance: `GatewayLogger().logger`
- Config-driven log levels in `[gateway-logs]` section

## Testing Specifics

**Test fixtures in conftest.py:**
- `config` fixture: Returns `GatewayConfig` object from `--config` CLI arg
- `conffile` fixture: Returns path to config file
- `image` fixture: Returns RBD image name from `--image` CLI arg

**Test execution context:**
- Tests MUST run inside containers (require Ceph cluster + SPDK)
- Use `docker compose run --rm nvmeof-devel pytest ...`
- Tests use pytest caplog for output verification
- Filter warnings: `@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")`

**Test patterns:**
- CLI invoked via `from control.cli import main as cli` then `cli(["subsystem", "add", ...])`
- Gateway server context manager: `with GatewayServer(config) as gateway:`
- Wait for async operations: `wait_for_string(caplog, "expected_text", timeout_seconds)`

## Critical Gotchas

**Huge pages required:**
- SPDK requires huge pages allocated: `make setup` (requires sudo)
- Default: 2048 pages (4GB), configurable: `make setup HUGEPAGES=512`
- Alternative: Set `mem_size=4096` in [spdk] section to avoid huge pages

**Container-only development:**
- Cannot run gateway directly on host (needs SPDK + Ceph cluster)
- Use `nvmeof-devel` service for development with mounted source
- Production uses `nvmeof` service with baked-in code

**PDM package manager (not pip):**
- Dependencies in `pyproject.toml`, lockfile is `pdm.lock`
- Use `make update-lockfile` to update dependencies
- PDM configured with `use_venv = false` in pdm.toml

**Git submodules:**
- SPDK is a submodule: `git submodule update --init --recursive`
- Must initialize before building

**Config file sections:**
- `[gateway]` - Main gateway settings
- `[spdk]` - SPDK-specific (cluster allocation, RPC socket, etc.)
- `[ceph]` - Ceph connection (pool, config file)
- `[mtls]` - Mutual TLS certificates
- `[kmip]` - KMIP server for encryption keys
- `[discovery]` - Discovery service settings
- `[gateway-logs]` - Logging configuration
- `[monitor]` - Monitor service settings