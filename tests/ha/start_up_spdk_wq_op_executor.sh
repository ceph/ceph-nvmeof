#!/bin/sh
# Bring vstart Ceph up, enable objecter_dispatch_to_op_executor via
# `ceph config set` before gateways connect, then start GWs with SpdkContextWQ.
set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
    repo_root="$GITHUB_WORKSPACE"
else
    test_dir=$(dirname "$0")
    repo_root="$(cd "$test_dir/../.." && pwd)"
fi

export NVMEOF_CONFIG=./tests/ceph-nvmeof.spdk_wq.conf

# Compose needs CEPH_* version exports (same as start_up.sh).
eval "$(make -C "$repo_root" -s ceph-env)"

echo "ℹ️  Starting ceph (before gateways) for objecter_dispatch_to_op_executor"
docker compose --progress quiet up --no-build --detach ceph

while true; do
  container_status=$(docker inspect --format='{{.State.Health.Status}}' ceph)
  if [ "$container_status" = "healthy" ]; then
    break
  fi
  sleep 1
  printf .
done
echo "✅ ceph is healthy"

echo "ℹ️  Enable objecter_dispatch_to_op_executor (startup; before GW connect)"
docker compose exec -T ceph \
  ceph config set client objecter_dispatch_to_op_executor true
val=$(docker compose exec -T ceph \
  ceph config get client objecter_dispatch_to_op_executor | tr -d '\r\n')
echo "objecter_dispatch_to_op_executor=$val"
[ "$val" = "true" ]

# Objecter enable line is ldout(cct, 1); keep objecter logging visible in GW logs.
docker compose exec -T ceph ceph config set client debug_objecter 1/5

# start_up.sh re-checks ceph health, then starts gateways + nvme-gw create.
"$test_dir/start_up.sh"
