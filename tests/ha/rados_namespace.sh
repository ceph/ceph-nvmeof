#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env
RADOS_NS1="test_ns1"
RADOS_NS2="test_ns2"
IMAGE1="ns_image1"
IMAGE2="ns_image2"
IMAGE3="ns_image3"

set -e
set -x

echo "ℹ️ Create RBD namespaces"
docker exec ceph rbd namespace create ${RBD_POOL}/${RADOS_NS1}
docker exec ceph rbd namespace create ${RBD_POOL}/${RADOS_NS2}

echo "ℹ️ Create subsystem"
cephnvmf_func subsystem add --subsystem ${NQN} --no-group-append

echo "ℹ️ Test: Create namespace in rbd/${RADOS_NS1}"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rados-namespace ${RADOS_NS1} --rbd-image ${IMAGE1} --size 10MB --rbd-create-image

echo "ℹ️ Test: Same image name in rbd/${RADOS_NS2} , different RADOS namespace should succeed"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rados-namespace ${RADOS_NS2} --rbd-image ${IMAGE1} --size 10MB --rbd-create-image

echo "ℹ️ Test: Duplicate namespace in same RADOS namespace should fail"
set +e
cephnvmf_func --output stdio namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rados-namespace ${RADOS_NS1} --rbd-image ${IMAGE1} --size 10MB --rbd-create-image > /tmp/ns_dup.txt 2>&1
if [[ $? -eq 0 ]]; then
    echo "ERROR: Should not allow duplicate image in same namespace"
    exit 1
fi
set -e
grep -q "is already used" /tmp/ns_dup.txt
rm -f /tmp/ns_dup.txt

echo "ℹ️ Test: Create namespace using the same image name, no RADOS namespace, should succeed"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rbd-image ${IMAGE1} --size 10MB --rbd-create-image

echo "ℹ️ Test: Namespace without namespace vs with RADOS namespace"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rbd-image ${IMAGE2} --size 10MB --rbd-create-image

cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} \
    --rados-namespace ${RADOS_NS1} --rbd-image ${IMAGE2} --size 10MB --rbd-create-image

echo "ℹ️ List namespaces"
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
echo "$ns_list" | jq .

echo "✅ All tests passed"
