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

echo "ℹ️  T1: validate ceph config set objecter_dispatch_to_op_executor"
op_exec=$(docker compose exec -T ceph \
  ceph config get client objecter_dispatch_to_op_executor | tr -d '\r\n')
echo "ceph config get client objecter_dispatch_to_op_executor=$op_exec"
[ "$op_exec" = "true" ]

# Confirm the option is present in the mon config DB (set by start_up_*, not default).
dump=$(docker compose exec -T ceph ceph config dump -f json)
echo "$dump" | jq -e '
  map(select(
    .name == "objecter_dispatch_to_op_executor"
    and (.value == "true" or .value == true)
  )) | length > 0
' >/dev/null
echo "✅ mon config DB has objecter_dispatch_to_op_executor=true"

strand=$(docker compose exec -T ceph \
  ceph config get client objecter_dispatch_to_session_strand 2>/dev/null | tr -d '\r\n' || true)
echo "objecter_dispatch_to_session_strand=${strand:-<default>}"
if [ -n "$strand" ] && [ "$strand" != "false" ]; then
  echo "FAIL: objecter_dispatch_to_session_strand must be false, got $strand" >&2
  exit 1
fi

echo "ℹ️  T3: Objecter applied op-executor from mon config (ctor enable log)"
# Objecter.cc logs this only when dispatch_to_op_executor was true at construction
# (after get_monmap_and_config). Proves ceph config set was honored before GW connect.
# librados dout lands in gateway/SPDK log files under /var/log/ceph (bind-mounted),
# not always in `docker compose logs nvmeof` (Python logger only).
enable_re='objecter_dispatch_to_op_executor=true: OSD op replies'
if ! grep -Rsq -- "$enable_re" /var/log/ceph/ 2>/dev/null && \
   ! docker compose logs nvmeof 2>&1 | grep -q -- "$enable_re"; then
  echo "FAIL: missing Objecter enable log matching: $enable_re" >&2
  echo "---- /var/log/ceph (grep) ----" >&2
  grep -Rs -- "$enable_re" /var/log/ceph/ 2>&1 | head -50 >&2 || true
  echo "---- nvmeof compose logs (tail) ----" >&2
  docker compose logs nvmeof 2>&1 | tail -200 >&2 || true
  exit 1
fi
echo "✅ Objecter ctor applied objecter_dispatch_to_op_executor=true"

echo "ℹ️  T2/T4/T5: sanity I/O + SpdkContextWQ asserts"
. "$test_dir/spdk_wq.sh"

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
