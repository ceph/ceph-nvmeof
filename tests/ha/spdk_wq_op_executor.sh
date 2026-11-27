#!/bin/sh
# SpdkContextWQ + objecter_dispatch_to_op_executor HA check.
# Expects start_up_spdk_wq_op_executor.sh (ceph config set before GWs) and setup.sh.
set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname "$0")
fi

NQN="nqn.2016-06.io.spdk:cnode1"
rpc=/usr/libexec/spdk/scripts/rpc.py
SPDK_SOCKET=/var/tmp/spdk.sock

echo "ℹ️  T1: mon config objecter_dispatch_to_op_executor"
op_exec=$(docker compose exec -T ceph \
  ceph config get client objecter_dispatch_to_op_executor | tr -d '\r\n')
echo "objecter_dispatch_to_op_executor=$op_exec"
[ "$op_exec" = "true" ]
strand=$(docker compose exec -T ceph \
  ceph config get client objecter_dispatch_to_session_strand 2>/dev/null | tr -d '\r\n' || true)
echo "objecter_dispatch_to_session_strand=${strand:-<default>}"
# Default is false; accept false or empty.
if [ -n "$strand" ] && [ "$strand" != "false" ]; then
  echo "FAIL: objecter_dispatch_to_session_strand must be false, got $strand" >&2
  exit 1
fi

echo "ℹ️  T2/T4/T5: sanity I/O + SpdkContextWQ asserts"
. "$test_dir/spdk_wq.sh"

echo "ℹ️  T3: Objecter took op-executor path (enable log)"
# Control-plane and/or RBD client logs land in compose nvmeof / container logs.
if ! docker compose logs nvmeof 2>&1 | grep -q 'objecter_dispatch_to_op_executor=true'; then
  echo "FAIL: missing Objecter_dispatch_to_op_executor=true enable log in nvmeof logs" >&2
  docker compose logs nvmeof 2>&1 | tail -200 >&2 || true
  exit 1
fi
echo "✅ Objecter op-executor enable log present"

echo "ℹ️  T6: namespace delete under light load"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
cli() {
  docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 "$@"
}
# Delete nsid 2 (demo_image2 from setup.sh); keep nsid 1 for residual path.
cli namespace del --subsystem "$NQN" --nsid 2
ns_list=$(cli --output stdio --format json namespace list --subsystem "$NQN")
echo "$ns_list"
[ "$(echo "$ns_list" | jq -r '.status')" = "0" ]
[ "$(echo "$ns_list" | jq -r '.namespaces[0].nsid')" = "1" ]
[ "$(echo "$ns_list" | jq -r '.namespaces[1]')" = "null" ]

if docker compose logs nvmeof 2>&1 | grep -q 'operations still pending during destruction'; then
  echo "FAIL: operations still pending during destruction after namespace del" >&2
  exit 1
fi

echo "✅ spdk_wq_op_executor: all checks passed"
