# Plan Mode Rules (Non-Obvious Only)

## Project Architecture Constraints

**SPDK subprocess architecture (not library):**
- SPDK runs as separate subprocess, not in-process library
- Gateway must handle SIGCHLD signal when SPDK terminates
- Communication via RPC over Unix socket (not function calls)
- SPDK crash causes gateway to exit (by design)

**State persistence via OMAP (architectural decision):**
- All gateway state stored in Ceph OMAP, not local storage
- Enables multi-gateway coordination without shared filesystem
- State keys have strict validation (no underscores in components)
- OMAP provides atomic operations for state updates

**Cluster allocation strategies (architectural choice):**
- Three mutually exclusive strategies for BDEV-to-cluster mapping
- Choice affects performance, scalability, and resource usage
- Only ONE can be active (enforced by configuration validation)
- Strategies: legacy ANA-based, flat distribution, pool-based

**Container-only execution (deployment constraint):**
- Gateway cannot run on bare metal (requires SPDK + Ceph cluster)
- Development uses mounted source code in containers
- Production uses baked-in code in containers
- Tests must run inside containers with full environment

**Discovery service separation (architectural pattern):**
- Discovery service is independent from gateway service
- Sources data from OMAP, not from live gateways
- Allows discovery even when gateways are down
- Separate lifecycle and scaling from gateway instances

**GatewayLogger singleton (design pattern):**
- Singleton pattern ensures consistent logging across modules
- Configured once from config file `[gateway-logs]` section
- All modules must use `GatewayLogger().logger`, not `logging` directly
- Prevents logging configuration conflicts

**Test execution architecture:**
- Tests require full Ceph cluster + SPDK environment
- Cannot mock SPDK or Ceph (integration tests by design)
- CLI invoked as Python function in tests (not subprocess)
- Gateway server uses context manager pattern for lifecycle

**PDM package management (tooling choice):**
- PDM chosen over pip/poetry for dependency management
- Configured with `use_venv = false` (no virtual environments)
- Lockfile updates must run inside container (not on host)
- Ensures consistent dependencies across environments

**Protobuf code generation (build process):**
- Protobuf files committed to repo (not generated at build time)
- Must manually regenerate after `.proto` changes: `make protoc`
- Generates Python, gRPC stubs, and type hints
- Build fails if protobuf files are out of sync

**Huge pages requirement (system dependency):**
- SPDK requires huge pages allocated on host system
- Setup requires sudo: `make setup HUGEPAGES=2048`
- Alternative: Use `mem_size` config to avoid huge pages
- Affects performance and memory allocation strategy

**Config file architecture:**
- Multiple sections for different subsystems
- Some settings are mutually exclusive (not validated at parse time)
- Config accessed via `GatewayConfig` wrapper (not raw configparser)
- Settings have fallback defaults in code, not in config file