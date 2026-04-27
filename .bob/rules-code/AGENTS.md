# Code Mode Rules (Non-Obvious Only)

## Project-Specific Coding Patterns

**State persistence via OMAP (not files):**
- All gateway state stored in Ceph OMAP, not local filesystem
- State keys built using `GatewayState.build_*_key()` methods in `control/state.py`
- Key components CANNOT contain underscores (validated by `is_key_element_valid()`)
- Example: `namespace_nqn.2016-06.io.spdk:cnode1_1` (underscore separates prefix from NQN from NSID)

**SPDK subprocess management:**
- SPDK runs as subprocess, not in-process library
- Must register SIGCHLD handler to detect SPDK crashes
- RPC calls via `spdk.rpc.client` over Unix socket
- Socket path: `rpc_socket_dir + rpc_socket_name` from config (default: `/var/tmp/spdk.sock`)

**GatewayLogger singleton pattern:**
- NEVER use `import logging` directly
- Always use: `from .utils import GatewayLogger` then `GatewayLogger().logger`
- Logger configured from `[gateway-logs]` section in config file
- Singleton ensures consistent logging across all modules

**CLI invocation in tests:**
- Import: `from control.cli import main as cli`
- Call: `cli(["subsystem", "add", "--subsystem", "nqn.example"])`
- NOT: subprocess or shell execution
- Captures output via pytest caplog fixture

**Config file access pattern:**
- Use `GatewayConfig` class, not raw configparser
- Methods: `get_with_default()`, `getboolean_with_default()`, etc.
- Config sections are hardcoded strings (no constants): `"gateway"`, `"spdk"`, `"ceph"`

**Protobuf imports:**
- Gateway protobuf: `from .proto import gateway_pb2 as pb2`
- gRPC stubs: `from .proto import gateway_pb2_grpc as pb2_grpc`
- JSON conversion: `from google.protobuf import json_format`
- Monitor protobuf: `from .proto import monitor_pb2_grpc`

**Test context managers:**
- Gateway server: `with GatewayServer(config) as gateway:`
- Must call `gateway.serve()` after entering context
- Tests run inside containers only (cannot run on host)

**Cluster allocation strategy (critical):**
- Only ONE of these can be set in `[spdk]` section:
  - `bdevs_per_cluster` (legacy, per ANA group)
  - `flat_bdevs_per_cluster` (ignores ANA groups)
  - `cluster_connections` (pool-based, recommended)
- Setting multiple will cause undefined behavior

**Error handling in tests:**
- Use `@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")`
- Check caplog for "failed" and "Failure" strings (case-sensitive)
- Exclude known warnings: `.replace("failed to notify", "")`

**PDM not pip:**
- Dependencies in `pyproject.toml`, lockfile is `pdm.lock`
- Update deps: `make update-lockfile` (runs PDM in container)
- PDM configured with `use_venv = false` in pdm.toml
- NEVER use `pip install` for dependencies

**Protobuf regeneration:**
- After modifying `.proto` files in `control/proto/`
- MUST run: `make protoc`
- Generates: `*_pb2.py`, `*_pb2_grpc.py`, `*_pb2.pyi` files
- These are NOT in .gitignore (committed to repo)