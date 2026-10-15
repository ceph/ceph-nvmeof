set -xe

POOL="${RBD_POOL:-rbd}"
CEPH_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v nvme | grep ceph | awk '{print $1}')
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')

docker compose exec -T ceph ceph service dump
docker compose exec -T ceph ceph status

wait_for_status() {
    for i in $(seq 20); do
        if docker compose exec -T ceph ceph status | grep -q "$1"; then
            break
        fi
        sleep 5
    done
}

echo "ℹ️  Step 1: verify 2 gateways"

wait_for_status "2 gateways active"
docker compose exec -T ceph ceph status | grep "2 gateways active"

echo "ℹ️  Step 2: stop a gateway"

docker stop $GW1_NAME
wait

echo "ℹ️  Step 3: verify 1 gateway"

wait_for_status "1 gateway active"
docker compose exec -T ceph ceph status | grep "1 gateway active"
