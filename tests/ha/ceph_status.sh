set -xe

POOL="${RBD_POOL:-rbd}"
CEPH_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v nvme | grep ceph | awk '{print $1}')
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')

docker compose exec -T ceph ceph service dump
docker compose exec -T ceph ceph status

echo "ℹ️  Step 1: verify 2 gateways"

docker compose exec -T ceph ceph status | grep "2 gateways active"

echo "ℹ️  Step 2: stop a gateway"

docker stop $GW1_NAME
wait

echo "ℹ️  Step 3: verify 1 gateway"

# The "N gateways active" line is the manager service map. A gateway
# that does not unregister is dropped after mgr_service_beacon_grace
# and published on the next mgr tick.
BEACON_GRACE=$(docker compose exec -T ceph ceph config get mgr mgr_service_beacon_grace | tr -d '\r[:space:]')
BEACON_GRACE_SEC=${BEACON_GRACE%%[^0-9]*}
MGR_TICK=$(docker compose exec -T ceph ceph config get mgr mgr_tick_period | tr -d '\r[:space:]')
MGR_TICK_SEC=${MGR_TICK%%[^0-9]*}
deadline=$((SECONDS + BEACON_GRACE_SEC + MGR_TICK_SEC))
while true; do
  status=$(docker compose exec -T ceph ceph status)
  if grep "1 gateway active" <<<"$status"; then
    exit 0
  fi
  if (( SECONDS >= deadline )); then
    printf '%s\n' "$status"
    exit 1
  fi
  sleep 1
done
