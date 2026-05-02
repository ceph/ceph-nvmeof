#!/bin/bash

. .env
set -e
set -x

rpc=/usr/libexec/spdk/scripts/rpc.py
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
ip1="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
ip2="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"

function cephnvmf_func1()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${ip1} --server-port ${NVMEOF_GW_PORT} "$@"
}

function cephnvmf_func2()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${ip2} --server-port ${NVMEOF_GW_PORT} "$@"
}

echo "ℹ️  Starting bdevperf container"
docker compose up -d bdevperf
sleep 10
make logs SVC=bdevperf
BDEVPERF_SOCKET_LINE="$(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r')"
BDEVPERF_SOCKET="${BDEVPERF_SOCKET_LINE#BDEVPERF_SOCKET=}"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"

echo "ℹ️  Create resources"
cephnvmf_func1 listener add --subsystem ${NQN} --host-name ${GW1_NAME} --traddr $ip1 --trsvcid 4430
cephnvmf_func2 listener add --subsystem ${NQN} --host-name ${GW2_NAME} --traddr $ip2 --trsvcid 4430
/usr/bin/docker compose run --rm nvmeof-cli --server-address ${ip1} --server-port ${NVMEOF_GW_PORT} host del --subsystem ${NQN} --host-nqn "*"
cephnvmf_func1 namespace del --subsystem ${NQN} --nsid 2
cephnvmf_func1 host add --subsystem ${NQN} --host-nqn ${NQN}host1
sleep 20

echo "ℹ️  Connect"
devs=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $ip1 -s 4420 -f ipv4 -n $NQN -q ${NQN}host1 -l -1 -o 10"`
[[ "$devs" == "Nvme0n1" ]]

echo "ℹ️  Verify connection"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host1" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4420" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

echo "ℹ️  Verify OMAP before host delete"
set +e
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "connected-del-host"
    if [[ $? -eq 0 ]]; then
        echo "Shouldn't have connected host entry in OMAP before we delete the host"
        exit 1
    fi
set -e

echo "ℹ️  Delete host and keep connection"
cephnvmf_func1 host del --subsystem ${NQN} --host-nqn ${NQN}host1 --keep-connections
sleep 25
grep "Received request to remove host ${NQN}host1 access from ${NQN}, force: False, keep connections: True, context: <grpc._server" /var/log/ceph/nvmeof-$GW1_NAME/nvmeof-log
grep "Received request to remove host ${NQN}host1 access from ${NQN}, force: False, keep connections: True, context: None" /var/log/ceph/nvmeof-$GW2_NAME/nvmeof-log

echo "ℹ️  Verify connection after host delete"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host1" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4420" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "true" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

echo "ℹ️  Verify OMAP after host delete"
make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "connected-del-host"

echo "ℹ️  Verify host list is empty after host delete"
hosts=$(cephnvmf_func1 --output stdio --format json host list --subsystem $NQN)
[[ `echo $hosts | jq -r '.status'` == "0" ]]
[[ `echo $hosts | jq -r '.hosts[0]'` == "null" ]]

echo "ℹ️  Connect after host delete"
set +e
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $ip1 -s 4430 -f ipv4 -n $NQN -q ${NQN}host1 -l -1 -o 10"
    if [[ $? -eq 0 ]]; then
        echo "Connection should fail after host delete"
        exit 1
    fi
set -e

echo "ℹ️  Verify connection was unsuccessful"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host1" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4420" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "true" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

echo "ℹ️  Re-add host"
cephnvmf_func1 host add --subsystem ${NQN} --host-nqn ${NQN}host1
sleep 20

echo "ℹ️  Verify OMAP after host re-add"
set +e
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "connected-del-host"
    if [[ $? -eq 0 ]]; then
        echo "Shouldn't have connected host entry in OMAP after we re-add the host"
        exit 1
    fi
set -e

echo "ℹ️  Verify host list after host re-add"
hosts=$(cephnvmf_func1 --output stdio --format json host list --subsystem $NQN)
[[ `echo $hosts | jq -r '.status'` == "0" ]]
[[ `echo $hosts | jq -r '.subsystem_nqn'` == "$NQN" ]]
[[ `echo $hosts | jq -r '.hosts[0].nqn'` == "${NQN}host1" ]]
[[ `echo $hosts | jq -r '.hosts[1]'` == "null" ]]

echo "ℹ️  Connect after host re-add"
make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $ip1 -s 4430 -f ipv4 -n $NQN -q ${NQN}host1 -l -1 -o 10"

echo "ℹ️  Verify connection list after re-add and second connect"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host1" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4420" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host1" ]]
[[ `echo $conns | jq -r '.connections[1].trsvcid'` == "4430" ]]
[[ `echo $conns | jq -r '.connections[1].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[1].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[1].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[1].secure'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[1].host_deleted'` == "false" ]]
[[ `echo $conns | jq -r '.connections[2]'` == "null" ]]

echo "ℹ️  Delete host and do not keep connection"
cephnvmf_func1 host del --subsystem ${NQN} --host-nqn ${NQN}host1
sleep 25
grep "Received request to remove host ${NQN}host1 access from ${NQN}, force: False, keep connections: False, context: <grpc._server" /var/log/ceph/nvmeof-$GW1_NAME/nvmeof-log
grep "Received request to remove host ${NQN}host1 access from ${NQN}, force: False, keep connections: False, context: None" /var/log/ceph/nvmeof-$GW2_NAME/nvmeof-log

echo "ℹ️  Verify OMAP after second host delete"
set +e
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "connected-del-host"
    if [[ $? -eq 0 ]]; then
        echo "Shouldn't have connected host entry in OMAP after we deleted the host without keep-connections"
        exit 1
    fi
set -e

echo "ℹ️  Verify connection list is empty after second host delete"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.connections[0]'` == "null" ]]

make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"

echo "ℹ️  Test a host with PSK key"
rm -rf /tmp/temp-psk
mkdir -p /tmp/temp-psk/psk/${NQN}
echo -n ${PSK_KEY1} > /tmp/temp-psk/psk/${NQN}/${NQN}host2
echo -n ${PSK_KEY2} > /tmp/temp-psk/psk/${NQN}/${NQN}host2_2
chmod 0600 /tmp/temp-psk/psk/${NQN}/${NQN}host2
chmod 0600 /tmp/temp-psk/psk/${NQN}/${NQN}host2_2
cephnvmf_func1 listener add --subsystem ${NQN} --host-name ${GW1_NAME} --traddr $ip1 --trsvcid 4440 --secure
cephnvmf_func2 listener add --subsystem ${NQN} --host-name ${GW2_NAME} --traddr $ip2 --trsvcid 4440 --secure
cephnvmf_func1 listener add --subsystem ${NQN} --host-name ${GW1_NAME} --traddr $ip1 --trsvcid 4450 --secure
cephnvmf_func2 listener add --subsystem ${NQN} --host-name ${GW2_NAME} --traddr $ip2 --trsvcid 4450 --secure
cephnvmf_func1 host add --subsystem ${NQN} --host-nqn ${NQN}host2 --psk ${PSK_KEY1}
docker cp /tmp/temp-psk/psk ${BDEVPERF_CONTAINER_NAME}:/tmp/
make exec SVC=bdevperf OPTS=-T CMD="chown -R root:root /tmp/psk/"
rm -rf /tmp/temp-psk

echo "ℹ️  bdevperf add PSK keys to keyring"
make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key1 /tmp/psk/${NQN}/${NQN}host2"
make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key2 /tmp/psk/${NQN}/${NQN}host2_2"

echo "ℹ️  Connect host using first PSK key"
devs=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme2 -t tcp -a $ip1 -s 4440 -f ipv4 -n $NQN -q ${NQN}host2 -l -1 -o 10 --psk key1"`
[[ "$devs" == "Nvme2n1" ]]

echo "ℹ️  Verify connection list"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host2" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4440" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].controller_id'` == "3" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

echo "ℹ️  Delete host, keeping connection"
cephnvmf_func1 host del --subsystem ${NQN} --host-nqn ${NQN}host2 --keep-connections
sleep 25

echo "ℹ️  Verify connection list after host delete"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host2" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4440" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].controller_id'` == "3" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "true" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

echo "ℹ️  Re-add host, using second PSK key"
cephnvmf_func1 host add --subsystem ${NQN} --host-nqn ${NQN}host2 --psk ${PSK_KEY2}

echo "ℹ️  Try to connect using old PSK key"
set +e
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme3 -t tcp -a $ip1 -s 4450 -f ipv4 -n $NQN -q ${NQN}host2 -l -1 -o 10 --psk key1"
    if [[ $? -eq 0 ]]; then
        echo "Connection should fail using old PSK key"
        exit 1
    fi
set -e

echo "ℹ️  Try to connect using new PSK key"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme3 -t tcp -a $ip1 -s 4450 -f ipv4 -n $NQN -q ${NQN}host2 -l -1 -o 10 --psk key2"

echo "ℹ️  Verify connection list after connect using new PSK key"
conns=$(cephnvmf_func1 --output stdio --format json connection list --subsystem $NQN)
[[ `echo $conns | jq -r '.status'` == "0" ]]
[[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host2" ]]
[[ `echo $conns | jq -r '.connections[0].trsvcid'` == "4450" ]]
[[ `echo $conns | jq -r '.connections[0].traddr'` == "${ip1}" ]]
[[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
[[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
[[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
[[ `echo $conns | jq -r '.connections[0].controller_id'` == "4" ]]
[[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
[[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
[[ `echo $conns | jq -r '.connections[0].dhchap_controller_origin'` == "no_key" ]]
[[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
[[ `echo $conns | jq -r '.connections[0].host_deleted'` == "false" ]]
[[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme2"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme3"
