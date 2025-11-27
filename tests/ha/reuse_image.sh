#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env
UUID="398c0838-8963-4673-a92e-55a65ca7847a"
UUID2="398c0838-8963-4673-a92e-55a65ca7847b"
UUID3="398c0838-8963-4673-a92e-55a65ca7847c"
UUID4="398c0838-8963-4673-a92e-55a65ca7847d"
UUID5="398c0838-8963-4673-a92e-55a65ca7847e"
UUID6="398c0838-8963-4673-a92e-55a65ca7847f"
UUID7="398c0838-8963-4673-a92e-55a65ca78480"
UUID8="398c0838-8963-4673-a92e-55a65ca78481"

set -e
set -x

GROUP_NAME=`cephnvmf_func --output stdio --format json gw info | jq -r '.group'`
GROUP_NAME2=${GROUP_NAME}2

echo "ℹ️  get FSID"
FSID=`make -s exec SVC=ceph OPTS=-T CMD="ceph fsid"`
FSID2=1-${FSID}

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

echo "ℹ️  change group back in RBD image metadata"
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta set -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION ${GROUP_NAME}_${NQN}_${UUID}_${FSID}_${rbd_id}"

echo "ℹ️  try to reuse RBD image, with same group and different UUID"
set +e
cephnvmf_func --output stdio namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --uuid ${UUID3} > /tmp/ns_add2.txt 2>&1
if [[ $? -eq 0 ]]; then
    echo "Shouldn't be able to reuse RBD image with a different UUID"
    exit 1
fi
set -e
grep "RBD image ${RBD_POOL}/${RBD_IMAGE_NAME} is already used by a namespace with UUID ${UUID} in subsystem ${NQN}, group ${GROUP_NAME}" /tmp/ns_add2.txt
rm -f /tmp/ns_add2.txt

echo "ℹ️  try to reuse RBD image, with different UUID using --force"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --uuid ${UUID3} --force

echo "ℹ️  list namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].uuid'` == "${UUID}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].uuid'` == "${UUID3}" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  check RBD metadata for two namespaces"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID}_${FSID}_${rbd_id}___${GROUP_NAME}_${NQN}_${UUID3}_${FSID}_${rbd_id}" ]]

echo "ℹ️  delete first namespace"
cephnvmf_func namespace del --subsystem ${NQN} --nsid 1

echo "ℹ️  check RBD metadata after first namespace delete"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID3}_${FSID}_${rbd_id}" ]]

echo "ℹ️  delete second namespace"
cephnvmf_func namespace del --subsystem ${NQN} --nsid 2
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  check RBD metadata is gone"
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_IMAGE_IDENTIFICATION" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have image id in metadata after namespace delete"
    exit 1
fi
set -e

echo "ℹ️  create a second RBD image"
make -s exec SVC=ceph OPTS=-T CMD="rbd create -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 --size 10M" 2> /dev/null
make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}2" 2> /dev/null
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_IMAGE_IDENTIFICATION" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have image id in metadata after image creation"
    exit 1
fi
set -e

echo "ℹ️  get RBD id"
rbd_id2=`make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}2" 2> /dev/null | grep "	id: " | sed 's/\tid: //'`

echo "ℹ️  create a namespace using the RBD image"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}2 --uuid ${UUID4}

echo "ℹ️  list namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].uuid'` == "${UUID4}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  check RBD metadata is back"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID4}_${FSID}_${rbd_id2}" ]]
cephnvmf_func namespace del --subsystem ${NQN} --nsid 1
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_IMAGE_IDENTIFICATION" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have image id in metadata after namespace delete"
    exit 1
fi
set -e

echo "ℹ️  create a namespace"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}3 --size ${RBD_IMAGE_SIZE} --rbd-create-image --uuid ${UUID5}

echo "ℹ️  get RBD id"
rbd_id3=`make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}3" 2> /dev/null | grep "	id: " | sed 's/\tid: //'`

echo "ℹ️  check RBD metadata"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}3 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID5}_${FSID}_${rbd_id3}" ]]

echo "ℹ️  change FSID in RBD metadata"
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta set -p ${RBD_POOL} ${RBD_IMAGE_NAME}3 NVME_IMAGE_IDENTIFICATION ${GROUP_NAME}_${NQN}_${UUID6}_${FSID2}_${rbd_id3}"

echo "ℹ️  check RBD metadata"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}3 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID6}_${FSID2}_${rbd_id3}" ]]

echo "ℹ️  create a namespace using the same image"
set +e
cephnvmf_func --output stdio namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}3 --uuid ${UUID6}
if [[ $? -eq 0 ]]; then
    echo "Shouldn't succeed to create a namespace using the same image, even with changed FSID"
    exit 1
fi
set -e

echo "ℹ️  check RBD metadata"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}3 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID6}_${FSID2}_${rbd_id3}" ]]

echo "ℹ️  create a fourth RBD image"
make -s exec SVC=ceph OPTS=-T CMD="rbd create -p ${RBD_POOL} ${RBD_IMAGE_NAME}4 --size 10M" 2> /dev/null
make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}4" 2> /dev/null
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}4 NVME_IMAGE_IDENTIFICATION" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have image id in metadata after image creation"
    exit 1
fi
set -e

echo "ℹ️  get RBD id"
rbd_id4=`make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}4" 2> /dev/null | grep "	id: " | sed 's/\tid: //'`

echo "ℹ️  create a namespace using the RBD image"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}4 --uuid ${UUID7}

echo "ℹ️  list namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}3" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].uuid'` == "${UUID5}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}4" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].uuid'` == "${UUID7}" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  check RBD metadata is back"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}4 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID7}_${FSID}_${rbd_id4}" ]]

echo "ℹ️  copy RBD image to a different image"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd copy ${RBD_POOL}/${RBD_IMAGE_NAME}4 ${RBD_POOL}/${RBD_IMAGE_NAME}5" 2> /dev/null`

echo "ℹ️  get RBD id"
rbd_id5=`make -s exec SVC=ceph OPTS=-T CMD="rbd info -p ${RBD_POOL} ${RBD_IMAGE_NAME}5" 2> /dev/null | grep "	id: " | sed 's/\tid: //'`

set +e
if [[ "${rbd_id4}" == "${rbd_id5}" ]]; then
    echo "Shouldn't get an identical RBD id ${rbd_id4} for ${RBD_POOL}/${RBD_IMAGE_NAME}4 and ${RBD_POOL}/${RBD_IMAGE_NAME}5"
    exit 1
fi
set -e

echo "ℹ️  check copied RBD metadata is the same"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}5 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID7}_${FSID}_${rbd_id4}" ]]

echo "ℹ️  create a namespace using the copied RBD image"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}5 --uuid ${UUID8}

echo "ℹ️  check RBD metadata of copied image after namespace creation"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}5 NVME_IMAGE_IDENTIFICATION" 2> /dev/null`
[[ "$rbd_meta" == "${GROUP_NAME}_${NQN}_${UUID7}_${FSID}_${rbd_id4}___${GROUP_NAME}_${NQN}_${UUID8}_${FSID}_${rbd_id5}" ]]

echo "ℹ️  list namespaces with copied image"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}3" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].uuid'` == "${UUID5}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}4" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].uuid'` == "${UUID7}" ]]
[[ `echo $ns_list | jq -r '.namespaces[2].nsid'` == "3" ]]
[[ `echo $ns_list | jq -r '.namespaces[2].rbd_image_name'` == "${RBD_IMAGE_NAME}5" ]]
[[ `echo $ns_list | jq -r '.namespaces[2].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[2].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[2].uuid'` == "${UUID8}" ]]
[[ `echo $ns_list | jq -r '.namespaces[3]'` == "null" ]]
