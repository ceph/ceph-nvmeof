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

# The monitor marks the gateway down after mon_nvmeofgw_beacon_grace,
# then publishes that on the next mon tick.
BEACON_GRACE=$(docker compose exec -T ceph ceph config get mon mon_nvmeofgw_beacon_grace | tr -d '\r[:space:]')
BEACON_GRACE_SEC=${BEACON_GRACE%%[^0-9]*}
MON_TICK=$(docker compose exec -T ceph ceph config get mon mon_tick_interval | tr -d '\r[:space:]')
MON_TICK_SEC=${MON_TICK%%[^0-9]*}
deadline=$((SECONDS + BEACON_GRACE_SEC + MON_TICK_SEC))
while (( SECONDS < deadline )); do
  if docker compose exec -T ceph ceph status | grep "1 gateway active"; then
    exit 0
  fi
  sleep 1
done
docker compose exec -T ceph ceph status
exit 1
