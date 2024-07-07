set -xe

POOL="${RBD_POOL:-rbd}"
CEPH_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v nvme | grep ceph | awk '{print $1}')
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')

echo "ℹ️  Step 1: verify 4 watchers"

docker exec $CEPH_NAME rados listwatchers -p $POOL nvmeof.state | grep "watcher=" | wc -l | grep 4

echo "ℹ️  Step 2: stop a gateway"

docker stop $GW1_NAME
wait
sleep 5

echo "ℹ️  Step 3: verify 2 watchers"

docker exec $CEPH_NAME rados listwatchers -p $POOL nvmeof.state | grep "watcher=" | wc -l | grep 2
