#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env
UUID="398c0838-8963-4673-a92e-55a65ca7847a"
UUID2="398c0838-8963-4673-a92e-55a65ca7847b"
UUID3="398c0838-8963-4673-a92e-55a65ca7847c"

set -e
set -x

GROUP_NAME=`cephnvmf_func --output stdio --format json gw info | jq -r '.group' | tr _ -`
GROUP_NAME2=${GROUP_NAME}2

echo "ℹ️  get FSID"
FSID=`make -s exec SVC=ceph OPTS=-T CMD="ceph fsid"`

echo "ℹ️  create resources, group ${GROUP_NAME}"
cephnvmf_func subsystem add --subsystem ${NQN} --no-group-append
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --size ${RBD_IMAGE_SIZE} --rbd-create-image --uuid ${UUID}

echo "ℹ️  list namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].uuid'` == "${UUID}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  get RBD id"
rbd_id=`make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}" 2> /dev/null | grep "	id: " | sed 's/\tid: //'`

echo "ℹ️  check RBD metadata"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID}_${FSID}_${rbd_id}" ]]

echo "ℹ️  change group in RBD image metadata"
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta set -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION ${GROUP_NAME2}_${NQN}_${UUID}_${FSID}_${rbd_id}"

echo "ℹ️  try to reuse RBD image, with a different group and UUID"
set +e
cephnvmf_func --output stdio namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --uuid ${UUID2} > /tmp/ns_add1.txt 2>&1
if [[ $? -eq 0 ]]; then
    echo "Shouldn't be able to reuse RBD image from another group"
    exit 1
fi
set -e
grep "RBD image ${RBD_POOL}/${RBD_IMAGE_NAME} is already used by a namespace in subsystem ${NQN}, group ${GROUP_NAME2}" /tmp/ns_add1.txt
rm -f /tmp/ns_add1.txt

echo "ℹ️  change group in RBD image metadata to an empty value"
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta set -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION _${NQN}_${UUID}_${FSID}_${rbd_id}"

echo "ℹ️  try to reuse RBD image, with a different group and UUID"
set +e
cephnvmf_func --output stdio namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --uuid ${UUID3} > /tmp/ns_add2.txt 2>&1
if [[ $? -eq 0 ]]; then
    echo "Shouldn't be able to reuse RBD image from another group"
    exit 1
fi
set -e
grep "RBD image ${RBD_POOL}/${RBD_IMAGE_NAME} is already used by a namespace in subsystem ${NQN}" /tmp/ns_add2.txt
grep -q -v ", group " /tmp/ns_add2.txt
rm -f /tmp/ns_add2.txt
