#!/bin/bash

set -xe
NQN="nqn.2016-06.io.spdk:cnode1-ns-reopen"
HOSTNQN="${NQN}:host"
GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW_NAME")"
IMG_NAME="image_ns_reopen"
rpc="/usr/libexec/spdk/scripts/rpc.py"

function cephnvmf()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${GW_IP} --server-port 5500 $@
}

calc_written_bytes_in_sec()
{
  num_bytes=$(docker compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 --output stdio --format json namespace get_io_stats -n $NQN --nsid 1 | jq '.bytes_written'| sed 's/[^0-9]*//g')

  sleep 1
  num_bytes1=$(docker compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 --output stdio --format json namespace get_io_stats -n $NQN --nsid 1 | jq '.bytes_written'| sed 's/[^0-9]*//g')

  res=$(expr $num_bytes1 - $num_bytes)
  echo $res
}


echo "ℹ️  get initial NVMf statistics"
cephnvmf gateway get_stats

cephnvmf subsystem add --subsystem $NQN --no-group-append
cephnvmf namespace add -n $NQN --rbd-pool rbd --rbd-image ${IMG_NAME} --rbd-create-image --size 10MB --read-only
cephnvmf listener add --subsystem $NQN --host-name $GW_NAME --traddr $GW_IP --trsvcid 4420 --verify-host-name
cephnvmf host add --subsystem $NQN --host-nqn ${HOSTNQN}

ns_list=$(cephnvmf --output stdio --format json namespace list --subsystem $NQN --nsid 1)
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].read_only'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${IMG_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo -n "ℹ️  Starting bdevperf container"
docker compose up -d bdevperf
sleep 10
echo "ℹ️  bdevperf start up logs"
make logs SVC=bdevperf
BDEVPERF_SOCKET=/tmp/bdevperf.sock
SPDK_SOCKET=/var/tmp/spdk.sock

echo "ℹ️  bdevperf bdev_nvme_set_options"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"

echo "ℹ️  get bdevs from SPDK"
bdevs=`make -s exec SVC=nvmeof OPTS=-T CMD="$rpc -v -s $SPDK_SOCKET bdev_get_bdevs"`
[[ `echo $bdevs | jq -r '.[0].product_name'` == "Ceph Rbd Disk" ]]
bdev_uuid=$(echo $bdevs | jq -r '.[0].uuid')
[[ `echo $bdevs | jq -r '.[0].name'` == "bdev_${bdev_uuid}" ]]
[[ `echo $bdevs | jq -r '.[0].driver_specific.rbd.pool_name'` == "rbd" ]]
[[ `echo $bdevs | jq -r '.[0].driver_specific.rbd.rbd_name'` == "${IMG_NAME}" ]]
[[ `echo $bdevs | jq -r '.[0].supported_io_types.read'` == "true" ]]
[[ `echo $bdevs | jq -r '.[0].supported_io_types.write'` == "false" ]]
[[ `echo $bdevs | jq -r '.[0].supported_io_types.write_zeroes'` == "false" ]]
[[ `echo $bdevs | jq -r '.[0].supported_io_types.compare_and_write'` == "false" ]]

echo "ℹ️  bdevperf perform_tests"
timeout=30
bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
echo "run io test"
set +e
make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests" &

res1=$(calc_written_bytes_in_sec)
if [[ $res1 -ne 0 ]]; then
    echo "Shouldn't write any bytes on a read only namespace"
    exit 1
fi
res1=$(calc_written_bytes_in_sec)
if [[ $res1 -ne 0 ]]; then
    echo "Shouldn't write any bytes on a read only namespace"
    exit 1
fi

sleep 60
set -e

echo "ℹ️  get final NVMf statistics"
cephnvmf gateway get_stats

echo "ℹ️  delete subsystem"
cephnvmf subsystem del --subsystem $NQN --force

exit 0
