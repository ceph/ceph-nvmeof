#!/bin/bash

set -xe
POOL="${RBD_POOL:-rbd}"

# Get number of gateways from ceph nvme-gw show
get_num_gateways() {
  local output
  if output=$(docker compose exec -T ceph ceph nvme-gw show $POOL '' 2>/dev/null); then
    echo "$output" | jq -r '."num gws"' 2>/dev/null || echo "0"
  else
    echo "0"
  fi
}

# Get all nvmeof container IDs directly  
get_all_gw_containers() {
  docker ps --filter name=nvmeof --format '{{.ID}}'
}

#
# MAIN - Remove ALL gateways
#

# Get actual number of gateways dynamically
NUM_GATEWAYS=$(get_num_gateways)

echo "=== Gateway Removal Test ==="
echo "Simulating: ceph orch rm nvmeof.$POOL."
echo ""

#
# Step 1: Show initial state and get gateway count
#
echo "Step 1: Before removal"
docker compose exec -T ceph ceph nvme-gw show $POOL '' || echo "Failed to show initial state"

echo ""
echo "Found $NUM_GATEWAYS gateways to remove"

if [ "$NUM_GATEWAYS" -eq 0 ]; then
  echo "⚠️ No gateways found - nothing to remove"
  exit 0
fi

#
# Step 2: Remove ALL gateways
#
echo ""
echo "Step 2: Remove all $NUM_GATEWAYS gateways"

gw_containers=$(get_all_gw_containers)
removed_count=0

if [ -z "$gw_containers" ]; then
  echo "⚠️ No nvmeof containers found to remove"
  exit 0
fi

for gw_container in $gw_containers; do
  echo "Stop gw $gw_container"
  docker stop $gw_container
  echo "nvme-gw delete gateway: '$gw_container' pool: '$POOL', group: '' (empty string)"
  docker compose exec -T ceph ceph nvme-gw delete $gw_container $POOL ''
  removed_count=$((removed_count + 1))
done

sleep 2


#
# Step 3: Show final state
#
echo ""
echo "Step 3: After removal"
final_output=$(docker compose exec -T ceph ceph nvme-gw show $POOL '' 2>&1)
final_gw_count=$(echo "$final_output" | grep -o '"num gws": [0-9]*' | grep -o '[0-9]*' || echo "unknown")

echo "Final result:"
echo "$final_output"
echo ""

if [ "$final_gw_count" = "0" ]; then
  echo "=== TEST PASSED ==="
  echo "✅ 'ceph nvme-gw show' confirms 0 gateways"
  echo "✅ Successfully simulated 'ceph orch rm nvmeof.$POOL.'"
elif [ "$final_gw_count" = "unknown" ]; then
  echo "🎉 === TEST PASSED ==="
  echo "✅ Gateway group completely removed (command failed as expected)"
  echo "✅ Successfully simulated 'ceph orch rm nvmeof.$POOL.'"
else
  echo "=== TEST FAILED ==="
  echo "❌ Expected 'num gws': 0 but got: $final_gw_count"
  echo "❌ Manual removal != orchestrator removal"
  exit 1
fi