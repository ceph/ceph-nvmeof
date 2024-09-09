set -xe

POOL="${RBD_POOL:-rbd}"
CEPH_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v nvme | grep ceph | awk '{print $1}')
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')

docker compose exec -T ceph ceph service dump
docker compose exec -T ceph ceph status

echo "ℹ️  Step 1: verify 2 gateways"

docker compose exec -T ceph ceph status | grep "nvmeof: 2 gateways active"

echo "ℹ️  Step 2: stop a gateway"

docker stop $GW1_NAME
wait
sleep 5

echo "ℹ️  Step 3: verify 1 gateway"

docker compose exec -T ceph ceph status | grep "nvmeof: 1 gateway active"
