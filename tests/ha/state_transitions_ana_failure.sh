#!/bin/bash
#
# Fault-injection variant of tests/ha/state_transitions.sh.
#
# state_transitions.sh only exercises the *happy* failover path, in which PHASE 2
# of GatewayService.set_ana_state_safe() - the bdev_rbd_wait_for_latest_osdmap
# wait - always succeeds. That wait exists precisely because it can fail during
# the OSD map settling window, which is the failover window. This script forces
# it to fail there and asserts that the gateway:
#
#   1. reports the failure, instead of dying inside its own exception handler
#      while formatting ana_grpids/ana_states (which are bound only at the end of
#      the try block), and
#   2. does not record an ANA ownership that it never managed to push to SPDK.
#
# Assertion 2 is observable from outside: show_gateway_listeners_info_safe()
# compares self.ana_grp_state against what SPDK reports for each listener and
# logs an error when they disagree, so running "gw listener_info" makes the
# divergence show up in the gateway log.
#
# Expected results
#   unfixed gateway: gw1 logs an UnboundLocalError for 'ana_grpids', never logs
#                    "Failure set_ana_states_all to", and reports
#                    'is "OPTIMIZED" but is "inaccessible" in SPDK'  -> test FAILS
#   fixed gateway:   gw1 logs "Failure set_ana_states_all to  ana_grpids=[],
#                    ana_states=[]", no UnboundLocalError, no divergence
#                    -> test PASSES
#
# Prerequisites - identical to state_transitions.sh: a running two-gateway stack
#   make up
#   tests/ha/wait_gateways.sh
#   tests/ha/setup.sh          # subsystem nqn.2016-06.io.spdk:cnode1 + listeners
# plus docker, docker compose and jq on the host.
#
# The fault is injected by patching /src/control/grpc.py inside the gw1 container
# only. It is guarded by a flag file so it can be armed and disarmed without a
# further restart, and the original file is restored on exit.
#
set -xe

rpc=/usr/libexec/spdk/scripts/rpc.py
cmd=nvmf_subsystem_get_listeners
nqn=nqn.2016-06.io.spdk:cnode1

GRPC_PY=/src/control/grpc.py
GRPC_BAK=/src/control/grpc.py.faultbak
FAULT_FLAG=/tmp/nvmeof_fail_osdmap_wait

# How long to wait for an ANA state to converge, and how long to wait while
# asserting that it does *not* converge.
CONVERGE_TIMEOUT=${CONVERGE_TIMEOUT:-120}
NEGATIVE_TIMEOUT=${NEGATIVE_TIMEOUT:-45}

# GW name by index - same helper as state_transitions.sh
gw_name() {
  i=$1
  docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
}

gw_socket() {
  docker exec "$1" find /var/tmp -name spdk.sock
}

# Number of ANA groups currently "optimized" on this gateway's listener, as SPDK
# itself reports it. Prints 0 if the gateway cannot be reached.
count_optimized() {
  GW_NAME=$1
  socket=$(gw_socket "$GW_NAME") || { echo 0; return 0; }
  response=$(docker exec "$GW_NAME" "$rpc" "-s" "$socket" "$cmd" "$nqn") || { echo 0; return 0; }
  echo "$response" | jq -r '[.[0].ana_states[] | select(.ana_state == "optimized")] | length'
}

# The ANA group ids that are "optimized", one per line.
list_optimized() {
  GW_NAME=$1
  socket=$(gw_socket "$GW_NAME")
  docker exec "$GW_NAME" "$rpc" "-s" "$socket" "$cmd" "$nqn" |
    jq -r '.[0].ana_states[] | select(.ana_state == "optimized") | .ana_group'
}

# Wait until the gateway reports exactly $2 optimized groups. Returns 1 on timeout.
wait_optimized() {
  GW_NAME=$1
  EXPECTED=$2
  TIMEOUT=$3
  deadline=$(( $(date +%s) + TIMEOUT ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(count_optimized "$GW_NAME")" -eq "$EXPECTED" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# verify that given numbers must be either 1 and 2 or 2 and 1
verify_ana_groups() {
    nr1=$1
    nr2=$2
    if [ "$nr1" -eq 1 ] && [ "$nr2" -eq 2 ]; then
        echo "Verified: first is 1 and second is 2"
    elif [ "$nr1" -eq 2 ] && [ "$nr2" -eq 1 ]; then
        echo "Verified: first is 2 and second is 1"
    else
        echo "Invalid numbers: first and second must be either 1 and 2 or 2 and 1"
        exit 1
    fi
}

install_fault() {
  GW_NAME=$1
  docker exec "$GW_NAME" rm -f "$FAULT_FLAG"
  docker exec "$GW_NAME" cp "$GRPC_PY" "$GRPC_BAK"
  docker exec -i "$GW_NAME" env FLAG="$FAULT_FLAG" TARGET="$GRPC_PY" python3 - <<'PY'
import os
import sys

path = os.environ["TARGET"]
flag = os.environ["FLAG"]
src = open(path, encoding="utf-8").read()

anchor = (
    '                            self.logger.debug('
    'f"bdev_rbd_wait_for_latest_osdmap for {cluster=}")\n'
    '                            if not self.spdk_rpc_client.'
    'bdev_rbd_wait_for_latest_osdmap(\n'
)
if anchor not in src:
    sys.exit("FAULT-INJECTION: anchor not found in %s, "
             "PHASE 2 of set_ana_state_safe() has changed shape" % path)
if src.count(anchor) != 1:
    sys.exit("FAULT-INJECTION: anchor is not unique in %s" % path)

injected = (
    '                            self.logger.debug('
    'f"bdev_rbd_wait_for_latest_osdmap for {cluster=}")\n'
    '                            if os.path.exists("%s"):\n'
    '                                set_ana_status = errno.EBUSY\n'
    '                                raise Exception("FAULT-INJECTION: forced "\n'
    '                                                "bdev_rbd_wait_for_latest_osdmap '
    'failure")\n'
    '                            if not self.spdk_rpc_client.'
    'bdev_rbd_wait_for_latest_osdmap(\n'
) % flag

open(path, "w", encoding="utf-8").write(src.replace(anchor, injected))
print("FAULT-INJECTION: installed into", path)
PY
  # drop any stale bytecode so the edit is picked up
  docker exec "$GW_NAME" sh -c 'rm -rf /src/control/__pycache__' || true
  docker exec "$GW_NAME" python3 -c "import ast,sys; ast.parse(open('$GRPC_PY').read())"
  docker exec "$GW_NAME" grep -q "FAULT-INJECTION: forced" "$GRPC_PY"
}

remove_fault() {
  GW_NAME=$1
  docker exec "$GW_NAME" rm -f "$FAULT_FLAG" || true
  docker exec "$GW_NAME" sh -c "test -f $GRPC_BAK && mv $GRPC_BAK $GRPC_PY" || true
  docker exec "$GW_NAME" sh -c 'rm -rf /src/control/__pycache__' || true
}

#
# MAIN
#
GW1_NAME=$(gw_name 1)
GW2_NAME=$(gw_name 2)
[ -n "$GW1_NAME" ] && [ -n "$GW2_NAME" ] || {
  echo "Could not find both gateway containers - is the stack up?"
  exit 1
}

# Always leave the stack the way we found it.
cleanup() {
  set +e
  echo "Cleaning up: removing fault injection from $GW1_NAME"
  remove_fault "$GW1_NAME"
  docker restart "$GW1_NAME"
  docker start "$GW2_NAME"
}
trap cleanup EXIT

#
# Step 1: baseline - each gateway optimized for exactly one of ANA groups 1 and 2
#
wait_optimized "$GW1_NAME" 1 "$CONVERGE_TIMEOUT" || { echo "gw1 never reached 1 optimized group"; exit 1; }
wait_optimized "$GW2_NAME" 1 "$CONVERGE_TIMEOUT" || { echo "gw2 never reached 1 optimized group"; exit 1; }
gw1_ana=$(list_optimized "$GW1_NAME")
gw2_ana=$(list_optimized "$GW2_NAME")
verify_ana_groups "$gw1_ana" "$gw2_ana"

#
# Step 2: install the fault into gw1 and restart it, with the fault DISARMED, so
# that gw1 comes back up and reaches its normal baseline first.
#
install_fault "$GW1_NAME"
docker restart "$GW1_NAME"
wait_optimized "$GW1_NAME" 1 "$CONVERGE_TIMEOUT" || {
  echo "gw1 did not come back to its baseline after the fault was installed"
  exit 1
}

#
# Step 3: arm the fault, then fail gw2 over to gw1.
#
MARK=$(date -u +%Y-%m-%dT%H:%M:%S)
docker exec "$GW1_NAME" touch "$FAULT_FLAG"
echo "Fault armed on $GW1_NAME, stopping $GW2_NAME"
docker stop "$GW2_NAME"

#
# Step 4: PHASE 2 now raises on every set_ana_state, so PHASE 4 never runs and
# gw1 must NOT pick up the second ANA group. This holds both with and without the
# fix - it is the precondition for the assertions that follow.
#
if wait_optimized "$GW1_NAME" 2 "$NEGATIVE_TIMEOUT"; then
  echo "gw1 became optimized for 2 groups although the OSD map wait was forced to fail;"
  echo "the fault did not take effect - check that install_fault() patched the right code"
  exit 1
fi
echo "As expected, gw1 did not take over group 2 while the fault was armed"

LOGS=$(docker logs --since "$MARK" "$GW1_NAME" 2>&1)

# Sanity: the fault really fired.
echo "$LOGS" | grep -q "Error during set_ana_state_safe execution" || {
  echo "set_ana_state_safe() never failed - the monitor may not have pushed an ANA update yet"
  exit 1
}

#
# Step 5: the gateway must report the failure rather than crash in its own
# exception handler.
#
if echo "$LOGS" | grep -q "UnboundLocalError"; then
  echo "FAIL: set_ana_state_safe() raised UnboundLocalError while building its error message."
  echo "      ana_grpids/ana_states are bound only at the end of the try block, so every"
  echo "      PHASE 2 failure destroys the error report and loses set_ana_status."
  echo "$LOGS" | grep -A5 "UnboundLocalError" | head -20
  exit 1
fi
echo "$LOGS" | grep -q "Failure set_ana_states_all to" || {
  echo "FAIL: the ANA failure was never reported - expected a"
  echo "      \"Failure set_ana_states_all to  ana_grpids=[], ana_states=[]\" line"
  exit 1
}
echo "OK: the ANA failure was reported instead of crashing the handler"

#
# Step 6: the gateway must not claim an ANA ownership it never pushed to SPDK.
# show_gateway_listeners_info_safe() cross-checks self.ana_grp_state against SPDK
# and logs an error on a mismatch, so this call surfaces the divergence.
#
MARK2=$(date -u +%Y-%m-%dT%H:%M:%S)
gw1_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")
docker compose run --rm nvmeof-cli --server-address "$gw1_ip" --server-port 5500 \
  --output stdio --format json gw listener_info -n "$nqn" || true

DIVERGENCE=$(docker logs --since "$MARK2" "$GW1_NAME" 2>&1 | grep 'in SPDK' || true)
if [ -n "$DIVERGENCE" ]; then
  echo "FAIL: the gateway recorded an ANA state that SPDK never received:"
  echo "$DIVERGENCE"
  echo "      PHASE 1 wrote self.ana_map/self.ana_grp_state before the SPDK batch call,"
  echo "      so rebalance_logic() and create_listener_safe() now act on an ownership"
  echo "      that no initiator can see."
  exit 1
fi
echo "OK: self.ana_grp_state still agrees with SPDK"

#
# Step 7 (informational): clear the fault and see whether the gateway takes over
# on its own. Whether this converges before the failback depends on how often the
# Ceph monitor client republishes the ANA map, so a timeout here is reported but
# is NOT treated as a failure - step 8 is the deterministic recovery check.
#
docker exec "$GW1_NAME" rm -f "$FAULT_FLAG"
if wait_optimized "$GW1_NAME" 2 "$NEGATIVE_TIMEOUT"; then
  echo "INFO: gw1 took over group 2 on its own once the fault cleared"
else
  echo "INFO: gw1 had not taken over group 2 within ${NEGATIVE_TIMEOUT}s of the fault clearing;"
  echo "      this depends on the monitor's ANA republish cadence, continuing to failback"
fi

#
# Step 8: failback - both gateways must return to one optimized group each. This
# is the deterministic proof that the failed set_ana_state did not wedge gw1.
#
echo "Start gw $GW2_NAME"
docker start "$GW2_NAME"

wait_optimized "$GW1_NAME" 1 "$CONVERGE_TIMEOUT" || { echo "gw1 did not fail back to 1 optimized group"; exit 1; }
wait_optimized "$GW2_NAME" 1 "$CONVERGE_TIMEOUT" || { echo "gw2 did not come back to 1 optimized group"; exit 1; }
gw1_ana=$(list_optimized "$GW1_NAME")
gw2_ana=$(list_optimized "$GW2_NAME")
verify_ana_groups "$gw1_ana" "$gw2_ana"

echo "PASS: set_ana_state_safe() reported the ANA failure and kept its local state"
echo "      consistent with SPDK throughout"
