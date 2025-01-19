#!/bin/bash

set -xe
rpc="/usr/libexec/spdk/scripts/rpc.py"
NQN="nqn.2016-06.io.spdk:cnode1"

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
GW2_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"

echo -n "ℹ️  Starting bdevperf container"
docker compose up -d bdevperf
sleep 10
echo "ℹ️  bdevperf start up logs"
make logs SVC=bdevperf
eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r' )

echo "ℹ️  bdevperf bdev_nvme_set_options"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"

echo "ℹ️  bdevperf tcp connect ip: $GW2_IP port: 4420 nqn: $NQN"
devs=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $GW2_IP -s 4420 -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"`
[[ "$devs" == "Nvme0n1" ]]

grep "Received request to create $GW2_NAME TCP ipv4 listener for $NQN at ${GW2_IP}:4420, secure: False, verify host name: False, context: <grpc._server" /var/log/ceph/nvmeof-$GW1_NAME/nvmeof-log
grep "Listener will be stashed to be used later by the right gateway." /var/log/ceph/nvmeof-$GW1_NAME/nvmeof-log
[[ `grep "create_listener: " /var/log/ceph/nvmeof-$GW1_NAME/nvmeof-log | wc -l` == 0 ]]

grep "Received request to create $GW2_NAME TCP ipv4 listener for $NQN at ${GW2_IP}:4420, secure: False, verify host name: True, context: None" /var/log/ceph/nvmeof-$GW2_NAME/nvmeof-log
grep "create_listener: True" /var/log/ceph/nvmeof-$GW2_NAME/nvmeof-log

echo "ℹ️  bdevperf tcp connect ip: $GW1_IP port: 4420 nqn: $NQN, should fail"
set +e
make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $GW1_IP -s 4420 -f ipv4 -n $NQN -q ${NQN}host2 -l -1 -o 10"
if [[ $? -eq 0 ]]; then
    echo "Connect to $GW1_IP should fail"
    exit 1
fi
set -e
exit 0
