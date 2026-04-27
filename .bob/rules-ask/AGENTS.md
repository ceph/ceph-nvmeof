# Ask Mode Rules (Non-Obvious Only)

## Project Documentation Context

**Container-based architecture (counterintuitive):**
- Gateway CANNOT run directly on host (requires SPDK + Ceph cluster)
- Development uses `nvmeof-devel` service with mounted source code
- Production uses `nvmeof` service with baked-in code
- All tests must run inside containers: `docker compose run --rm nvmeof-devel pytest ...`

**Discovery service is separate:**
- Discovery service runs independently: `python3 -m control.discovery`
- Sources target info from Ceph OMAP, not from live gateways
- Default port 8009 (not the gateway port 5500)
- Can be started separately: `docker compose up --detach discovery`

**State storage location (hidden):**
- Gateway state is NOT in local files or database
- All state persisted to Ceph OMAP (object map)
- State keys use underscore delimiter with strict validation
- Key element components cannot contain underscores

**Config file structure (non-standard):**
- Multiple config sections with specific purposes
- `[spdk]` section has THREE mutually exclusive cluster allocation strategies
- Only ONE strategy can be set at a time (not documented in config comments)
- Setting multiple strategies causes undefined behavior

**Testing environment requirements:**
- Tests require full Ceph cluster + SPDK environment
- Cannot run tests on host machine
- Must use docker compose with specific services
- Test fixtures in `conftest.py` provide config/image parameters

**PDM package manager (not pip):**
- Project uses PDM, not pip or poetry
- Dependencies in `pyproject.toml`, lockfile is `pdm.lock`
- PDM configured with `use_venv = false` (no virtual environment)
- Update lockfile: `make update-lockfile` (runs PDM in container)

**SPDK as git submodule:**
- SPDK is included as git submodule, not external dependency
- Must initialize: `git submodule update --init --recursive`
- SPDK has its own nested submodules
- Required before building

**Huge pages requirement (hidden dependency):**
- SPDK requires huge pages allocated on host
- Setup command: `make setup` (requires sudo)
- Default: 2048 pages (4GB)
- Alternative: Set `mem_size=4096` in [spdk] section to avoid huge pages

**CLI invocation patterns:**
- CLI can be invoked as module: `python3 -m control.cli`
- In tests, imported as function: `from control.cli import main as cli`
- Environment variables: `CEPH_NVMEOF_SERVER_ADDRESS`, `CEPH_NVMEOF_SERVER_PORT`
- NOT invoked via subprocess in tests

**Protobuf file generation:**
- Protobuf files ARE committed to repo (not gitignored)
- After modifying `.proto` files, must run: `make protoc`
- Generates `*_pb2.py`, `*_pb2_grpc.py`, `*_pb2.pyi` files
- Build will fail if protobuf files are out of sync