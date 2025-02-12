#!/bin/sh -xe

NS_COUNT=300
NQN="nqn.2016-06.io.spdk:cnode1"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
cephnvmf="docker compose run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500"

$cephnvmf subsystem add --subsystem $NQN --max-namespaces 1024 --no-group-append
for i in `seq 1 $NS_COUNT`
do
    $cephnvmf namespace add -n $NQN --rbd-pool rbd --rbd-image image${i} --rbd-create-image --size 10MB
    $cephnvmf namespace set_qos -n $NQN --nsid $i --rw-ios-per-second 1000 --rw-megabytes-per-second 19 --r-megabytes-per-second 19 --w-megabytes-per-second 19
done
