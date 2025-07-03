#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env

set -e
set -x

echo "ℹ️  create resources"
cephnvmf_func subsystem add --subsystem ${NQN} --no-group-append
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --size ${RBD_IMAGE_SIZE} --rbd-create-image --disable-auto-resize
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}2 --size ${RBD_IMAGE_SIZE} --rbd-create-image

echo "ℹ️  list namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  check RBD metadata"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null`
[[ "$rbd_meta" == "no" ]]
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have NVME_GATEWAY_AUTO_RESIZE in metadata for ${RBD_POOL}/${RBD_IMAGE_NAME}2"
    exit 1
fi
set -e

echo "ℹ️  set namespaces auto-resize flag to same value"
cephnvmf_func namespace set_auto_resize --subsystem ${NQN} --nsid 1 --auto-resize-enabled no
cephnvmf_func namespace set_auto_resize --subsystem ${NQN} --nsid 2 --auto-resize-enabled yes

echo "ℹ️  namespaces list should be identical"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  RBD metadata should be identical"
rbd_meta=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null`
[[ "$rbd_meta" == "no" ]]
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have NVME_GATEWAY_AUTO_RESIZE in metadata for ${RBD_POOL}/${RBD_IMAGE_NAME}2"
    exit 1
fi
set -e

echo "ℹ️  resize RBD images"
make -s exec SVC=ceph OPTS=-T CMD="rbd resize -p ${RBD_POOL} ${RBD_IMAGE_NAME} --size 20M"
make -s exec SVC=ceph OPTS=-T CMD="rbd resize -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 --size 20M"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "10485760" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "20971520" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  refresh namespace size"
cephnvmf_func namespace refresh_size --subsystem ${NQN} --nsid 1
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "20971520" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "20971520" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  resize namespaces using CLI"
cephnvmf_func namespace resize --subsystem ${NQN} --nsid 1 --size 30MB
cephnvmf_func namespace resize --subsystem ${NQN} --nsid 2 --size 30MB
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "31457280" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "31457280" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  set auto-resize flag for namespaces"
cephnvmf_func namespace set_auto_resize --subsystem ${NQN} --nsid 1 --auto-resize-enabled yes
cephnvmf_func namespace set_auto_resize --subsystem ${NQN} --nsid 2 --auto-resize-enabled no
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "31457280" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "31457280" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]

echo "ℹ️  check RBD metadata"
set +e
make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME} NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null
if [[ $? -eq 0 ]]; then
    echo "Shouldn't have NVME_GATEWAY_AUTO_RESIZE in metadata for ${RBD_POOL}/${RBD_IMAGE_NAME}"
    exit 1
fi
set -e
rbd_meta2=`make -s exec SVC=ceph OPTS=-T CMD="rbd image-meta get -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 NVME_GATEWAY_AUTO_RESIZE" 2> /dev/null`
[[ "$rbd_meta2" == "no" ]]

echo "ℹ️  resize RBD images again"
make -s exec SVC=ceph OPTS=-T CMD="rbd resize -p ${RBD_POOL} ${RBD_IMAGE_NAME} --size 40M"
make -s exec SVC=ceph OPTS=-T CMD="rbd resize -p ${RBD_POOL} ${RBD_IMAGE_NAME}2 --size 40M"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_name'` == "${RBD_IMAGE_NAME}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rbd_image_size'` == "41943040" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].disable_auto_resize'` == "false" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].nsid'` == "2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_name'` == "${RBD_IMAGE_NAME}2" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_pool_name'` == "${RBD_POOL}" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].rbd_image_size'` == "31457280" ]]
[[ `echo $ns_list | jq -r '.namespaces[1].disable_auto_resize'` == "true" ]]
[[ `echo $ns_list | jq -r '.namespaces[2]'` == "null" ]]
