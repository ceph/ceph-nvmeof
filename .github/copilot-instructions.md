# Ceph NVMe-oF Gateway - Coding Agent Instructions

## Repository Overview

**Purpose**: Provides block storage on top of Ceph for platforms without native Ceph RBD support (e.g., VMware) using the NVMe over Fabrics (NVMe-oF) protocol. Exports existing RBD images as NVMe-oF namespaces.

**Project Type**: Python-based containerized service with gRPC API
- **Size**: ~4.2MB of source code, 192 files total (51 Python files)
- **Languages**: Python 3.9+, Protocol Buffers, Shell scripts
- **Key Dependencies**: 
  - SPDK (v25.09) - Storage Performance Development Kit with DPDK
  - Ceph cluster (v20.2.0) for RBD backend
  - gRPC (v1.53.0) for communication
  - Docker and docker-compose (v2.11.0+) for containerization

**Main Components**:
- `control/` - Python gateway service (server, CLI, gRPC, state management)
- `tests/` - Pytest-based integration tests
- `spdk/` - Git submodule for SPDK library
- Container images: nvmeof (gateway), nvmeof-cli (CLI), spdk (base), ceph (test cluster)

## Build and Development Workflow

### Initial Setup (Required Once)

1. **Clone with submodules** (ALWAYS required):
   ```bash
   git clone https://github.com/ceph/ceph-nvmeof.git
   cd ceph-nvmeof
   git submodule update --init --recursive
   ```

2. **Install build dependencies**:
   ```bash
   # Required for verification
   pip install flake8
   
   # Required for containerized builds
   sudo dnf install -y make moby-engine docker-compose-plugin
   ```

3. **Configure huge-pages** (required for SPDK):
   ```bash
   make setup  # Allocates 2048 huge-pages (4GB) by default
   # Or: make setup HUGEPAGES=512  # For smaller allocations
   ```
   Note: This requires sudo and must be run before starting containers.

### Verification and Linting

**ALWAYS run before making changes** to understand existing issues:

```bash
make verify  # Runs flake8 on control/*.py tests/*.py
```

- Configuration in `tox.ini`: max-line-length=100
- Ignore specific errors with `# noqa: ERROR_CODE` comments
- Global ignores can be added to `tox.ini` under `[flake8]` section
- Exit code 0 means success

### Building Container Images

**Build time**: 10-20 minutes depending on services and network

```bash
# Build all services (takes longest - builds spdk, ceph, nvmeof, nvmeof-cli)
make build

# Build specific service (faster for development)
make build SVC=nvmeof        # Gateway service only
make build SVC=nvmeof-cli    # CLI tool only
make build SVC=spdk          # SPDK base image only
make build SVC=ceph          # Test Ceph cluster only

# For ARM64 builds, specify target architecture
make build SPDK_TARGET_ARCH="armv8.2-a+crypto" SPDK_MAKEFLAGS="DPDKBUILD_FLAGS=-Dplatform=kunpeng920"
```

**Note**: Builds may fail during `CEPH_CLUSTER_CEPH_REPO_BASEURL` fetch with "Unable to retrieve a valid URL" - this is a transient network issue with shaman.ceph.com, not a code issue.

### Running Tests

**Prerequisites**: 
1. Run `make setup` to allocate huge-pages
2. Build or pull container images

**Run integration tests**:
```bash
# Start test environment
make up  # Starts ceph and nvmeof containers, takes 2-3 minutes

# Run specific test (recommended during development)
make run SVC="nvmeof" OPTS="--volume=$(pwd)/tests:/src/tests --entrypoint=python3" CMD="-m pytest -s -vv tests/test_cli.py"

# Common test modules:
# - test_cli.py - CLI functionality (large, 144KB)
# - test_state.py - State management
# - test_grpc.py - gRPC service tests
# - test_server.py - Server functionality
# - test_multi_gateway.py - Multi-gateway scenarios

# Teardown after testing
make down  # Stop and remove containers
make clean # Clean up and reset huge-pages to 0
```

**Test execution time**: Individual tests range from 30 seconds to 5 minutes.

### Generate Protocol Buffer Files

**Required** after modifying `.proto` files in `control/proto/`:

```bash
make protoc  # Generates gateway_pb2.py, gateway_pb2_grpc.py, monitor_pb2.py, etc.
```

This uses PDM (Python Dependency Manager) to run `grpc_tools.command:build_package_protos`.

### Updating Python Dependencies

After modifying `pyproject.toml` dependencies:

```bash
make update-lockfile  # Updates pdm.lock
git add pdm.lock
```

### Docker Compose Commands

All docker-compose operations are wrapped via Makefile:

```bash
make ps        # Show running containers
make logs      # View logs (default: last 40 lines, following)
make shell SVC=nvmeof  # Exec bash in running container
make exec SVC=ceph CMD="ceph -s"  # Run command in container
make down      # Stop all containers
make pull      # Download pre-built images (faster than building)
```

## CI/CD Workflows

Located in `.github/workflows/`:

### build-container.yml (Main CI)
**Triggers**: Push to any branch, PRs to devel, daily at 21:00 UTC, manual dispatch

**Steps**:
1. **Linting**: `make verify` with flake8 (must pass)
2. **Build**: Builds spdk, bdevperf, nvmeof, nvmeof-cli, ceph containers
3. **Pytest**: Matrix of 30+ test modules run in parallel
   - Each test runs in isolated environment with huge-pages (512)
   - Requires healthy Ceph cluster (3-minute timeout)
   - Creates RBD pools and images before tests
4. **Demo tests**: Tests unsecured, PSK, DH-CHAP security protocols
5. **Performance tests**: bdevperf I/O testing

**Common CI failures**:
- Huge-pages not allocated properly
- Ceph cluster health check timeout
- SPDK target startup issues
- Network/shaman.ceph.com transient errors

### codeql.yml (Security Scanning)
Analyzes Python and GitHub Actions for security issues.

### check-deps.yml (Dependency Checks)
Checks for outdated dependencies.

## Repository Structure

### Key Directories

**`control/`** - Main Python package (gateway service):
- `server.py` - Main gateway server with SPDK integration (entry: `python3 -m control`)
- `cli.py` - Command-line interface tool (entry: `python3 -m control.cli` or `ceph-nvmeof`)
- `grpc.py` - gRPC service implementations
- `state.py` - State management (OmapGatewayState, LocalGatewayState)
- `config.py` - Configuration parser for ceph-nvmeof.conf
- `discovery.py` - NVMe discovery service (entry: `python3 -m control.discovery`)
- `prometheus.py` - Metrics exporter (port 10008)
- `proto/` - Protocol buffer definitions (gateway.proto, monitor.proto)

**`tests/`** - Pytest integration tests:
- `conftest.py` - Pytest fixtures and configuration
- `test_*.py` - Individual test modules (30+ modules)
- `ha/` - High availability and demo test scripts

**Root Configuration Files**:
- `ceph-nvmeof.conf` - Main gateway configuration (default config)
- `docker-compose.yaml` - Container orchestration
- `.env` - Environment variables (versions, registry, ports)
- `Makefile` - Primary build interface
- `pyproject.toml` - Python package metadata and dependencies
- `tox.ini` - Flake8 configuration

**`mk/`** - Makefile includes:
- `containerized.mk` - Docker/docker-compose commands
- `demo.mk` - Demo scenario targets
- `misc.mk` - Helper targets (alias, protoc)

### Important Files

**Entry Points**:
- Gateway service: `control/__main__.py` → `control/server.py`
- CLI tool: `control/cli.py:main()` (installed as `ceph-nvmeof` command)
- Discovery service: `control/discovery.py:main()`

**Configuration**:
- Gateway config: `ceph-nvmeof.conf` sections: [gateway], [ceph], [spdk], [mtls], [discovery]
- Test configs in `tests/`: alternative configurations for different scenarios

**Container Build**:
- `Dockerfile` - Multi-stage build for gateway and CLI
- `Dockerfile.spdk` - SPDK base image with RBD support
- `Dockerfile.ceph` - Sandboxed Ceph cluster for testing

## Architecture and Key Concepts

### NVMe-oF Gateway Architecture

1. **SPDK Integration**: Gateway runs SPDK `nvmf_tgt` as subprocess, communicates via JSON-RPC
2. **Ceph RBD Backend**: SPDK BDEVs map to Ceph RBD images (block devices)
3. **State Management**: Gateway state stored in Ceph OMAP (persistent key-value store)
4. **Multi-Gateway**: Multiple gateway instances share state via OMAP with locking
5. **gRPC API**: Management API on port 5500 for CLI/external tools
6. **Discovery Service**: Optional NVMe discovery controller on port 8009

### Key Subsystems

**Subsystems**: NVMe-oF namespace containers (nqn.2016-06.io.spdk:cnode1)
- Each subsystem has namespaces (RBD images), listeners (IP:port), and allowed hosts

**Namespaces**: Individual RBD images exposed as NVMe namespaces
- Create with `--rbd-pool`, `--rbd-image`, `--size` parameters

**Listeners**: Network endpoints where initiators connect
- Requires host-name verification in multi-gateway setups

**Hosts**: NQN-based access control (can use "*" for open access)

### SPDK BDEV-to-Cluster Mapping Strategies

Three strategies for mapping SPDK BDEVs to Ceph cluster contexts:

1. **Legacy (default)**: Per ANA group, `bdevs_per_cluster = 32` in [spdk] config
2. **Flat**: Ignore ANA groups, `flat_bdevs_per_cluster = 32`
3. **Cluster Pool**: Pre-defined pool, `cluster_connections = 32`

## Development Tips

### Making Code Changes

1. **For Python code**: Edit files in `control/` directory
2. **For protocol changes**: Edit `control/proto/*.proto`, then run `make protoc`
3. **For test changes**: Edit files in `tests/` directory
4. **Always run** `make verify` before committing

### Testing Changes

**Development containers** (faster iteration, no rebuild):
```bash
docker compose up nvmeof-devel  # Mounts source at runtime
```

**Debugging**:
- Gateway logs: `make logs SVC=nvmeof`
- Ceph logs: `make exec SVC=ceph CMD="ceph -s"`
- Container shell: `make shell SVC=nvmeof`

### Common Issues and Solutions

1. **"command not found: make"**: Install with `yum groupinstall "Development Tools"`
2. **Huge-pages errors**: Always run `make setup` before `make up`
3. **Container build hangs on CEPH_CLUSTER_CEPH_REPO_BASEURL**: Transient network issue, retry
4. **SELinux issues**: Set to permissive: `sudo setenforce 0`
5. **Protocol buffer errors**: Run `make protoc` to regenerate
6. **Test failures after dependency changes**: Run `make update-lockfile`

### Code Style Guidelines

- Follow PEP-8 (max line length: 100)
- Use `# noqa: ERROR_CODE` sparingly for legitimate exceptions
- Sign commits with `-s` flag (DCO required)
- Follow Conventional Commit syntax (type: description)
- Use gRPC and Protocol Buffers for service communication

### Performance Considerations

- Gateway requires 16GB+ RAM
- Huge-pages allocation: default 4GB (2048 × 2MB pages)
- For multi-gateway tests: ~256 huge-pages per gateway instance
- NVMEOF_NOFILE limit: 20,480 open files (depends on connected hosts)

## File Reference

**Root directory** (selected files):
```
.env                    - Environment variables (VERSIONS, CONTAINER_REGISTRY)
.gitmodules             - Git submodule configuration (spdk)
Dockerfile              - Multi-stage build (gateway + CLI)
Dockerfile.ceph         - Test Ceph cluster image
Dockerfile.spdk         - SPDK base image with RBD support
Makefile                - Primary build interface
README.md               - User documentation (installation, usage, configuration)
CONTRIBUTING.md         - Contribution guidelines (DCO, commit format)
ceph-nvmeof.conf        - Default gateway configuration
docker-compose.yaml     - Container orchestration
pdm.lock                - Locked Python dependencies
pyproject.toml          - Python package configuration
tox.ini                 - Flake8 linting configuration
```

**Second-level directories**:
```
control/proto/          - gRPC protocol definitions (gateway.proto, monitor.proto)
tests/ha/               - High availability test scripts
tests/kmip/             - KMIP integration tests
lib/go/                 - Go language bindings
mk/                     - Makefile fragments
monitoring/             - Prometheus/Grafana dashboard examples
spdk/                   - SPDK submodule (external dependency)
```

This repository requires container-based development. Most operations go through the Makefile. Always start with `make setup` and `make verify` when working with this codebase.
