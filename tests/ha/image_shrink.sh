#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env

set -e
set -x

echo "ℹ️  create a 20MB big namespace"
cephnvmf_func subsystem add --subsystem "${NQN}" --no-group-append
cephnvmf_func namespace add --subsystem "${NQN}" --rbd-pool rbd --rbd-image shrink_image --size 20MB --rbd-create-image

nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "false" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  shrink image to 19MB"
make -s exec SVC=ceph OPTS=-T CMD="rbd --pool rbd resize shrink_image --size 19MB --allow-shrink"
sleep 120
nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "true" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  resize namespace using CLI"
cephnvmf_func namespace resize --subsystem "${NQN}" --nsid 1 --size 25MB
nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "false" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  shrink image to 15MB"
make -s exec SVC=ceph OPTS=-T CMD="rbd --pool rbd resize shrink_image --size 15MB --allow-shrink"
sleep 120
nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "true" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  refresh size"
cephnvmf_func namespace refresh_size --subsystem "${NQN}" --nsid 1

echo "ℹ️  disable SPDK notification handling"
sed -i 's/^.*notifications_interval.*$/notifications_interval=0/' ceph-nvmeof.conf
container_id=$(docker ps -q -f name=nvmeof)
docker restart ${container_id}
sleep 20
nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "false" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  shrink image to 10MB"
make -s exec SVC=ceph OPTS=-T CMD="rbd --pool rbd resize shrink_image --size 10MB --allow-shrink"
sleep 120
nslist=$(cephnvmf_func --output stdio --format json namespace list --subsystem $NQN)
[[ `echo $nslist | jq -r '.status'` == "0" ]]
[[ `echo $nslist | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $nslist | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $nslist | jq -r '.namespaces[0].image_was_shrunk'` == "false" ]]
[[ `echo $nslist | jq -r '.namespaces[1]'` == "null" ]]
