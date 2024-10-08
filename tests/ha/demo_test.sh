#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

function demo_test_unsecured()
{
    make demo OPTS=-T
    return $?
}

function demo_test_psk()
{
    psk_path_prefix="/tmp/psk/"
    psk_path="${psk_path_prefix}${NQN}"
    rm -rf /tmp/temp-psk
    mkdir -p /tmp/temp-psk/psk/${NQN}
    echo -n ${PSK_KEY1} > /tmp/temp-psk/psk/${NQN}/${NQN}host
    echo -n ${PSK_KEY3} > /tmp/temp-psk/psk/${NQN}/${NQN}host3
    chmod 0600 /tmp/temp-psk/psk/${NQN}/${NQN}host
    chmod 0600 /tmp/temp-psk/psk/${NQN}/${NQN}host3
    make demosecurepsk OPTS=-T HOSTNQN="${NQN}host" HOSTNQN2="${NQN}host2" HOSTNQN3="${NQN}host3" NVMEOF_IO_PORT2=${port2} PSKKEY2=${PSK_KEY3}

    echo "ℹ️  verify PSK key files removal"
    psk_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk_rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    [[ `echo $psk_key_list | jq -r '.[0].removed'` == "true" ]]
    [[ `echo $psk_key_list | jq -r '.[1].removed'` == "true" ]]
    [[ `echo $psk_key_list | jq -r '.[2].removed'` == "null" ]]
    set +e
    make -s exec SVC=nvmeof OPTS=-T CMD="ls -lR /var/tmp/psk_${NQN}_*"
    if [[ $? -eq 0 ]]; then
        echo "PSK key files should be deleted"
        exit 1
    fi
    set -e
    return 0
}

function demo_test_dhchap()
{
    rm -rf /tmp/temp-dhchap
    mkdir -p /tmp/temp-dhchap/dhchap/${NQN}
    echo -n "${DHCHAP_KEY4}" > /tmp/temp-dhchap/dhchap/${NQN}/key1
    echo -n "${DHCHAP_KEY5}" > /tmp/temp-dhchap/dhchap/${NQN}/key2
    echo -n "${DHCHAP_KEY6}" > /tmp/temp-dhchap/dhchap/${NQN}/key3
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key1
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key2
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key3

    make demosecuredhchap OPTS=-T HOSTNQN="${NQN}host" HOSTNQN2="${NQN}host2" HOSTNQN3="${NQN}host3" NVMEOF_IO_PORT2=${port2} NVMEOF_IO_PORT3=${port3} DHCHAPKEY1="${DHCHAP_KEY4}" DHCHAPKEY2="${DHCHAP_KEY5}" DHCHAPKEY3="${DHCHAP_KEY6}"
}

function demo_bdevperf_unsecured()
{
    echo -n "ℹ️  Starting bdevperf container"
    docker compose up -d bdevperf
    sleep 10
    echo "ℹ️  bdevperf start up logs"
    make logs SVC=bdevperf
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r' )

    echo "ℹ️  bdevperf bdev_nvme_set_options"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"
    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"

    echo "ℹ️  verify connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  bdevperf perform_tests"
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
    timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
    bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
    make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"
    return $?
}

function demo_bdevperf_psk()
{
    cephnvmf_func spdk_log_level set --level debug --print debug
    echo -n "ℹ️  Starting bdevperf container"
    docker compose up -d bdevperf
    sleep 10
    echo "ℹ️  bdevperf start up logs"
    make logs SVC=bdevperf
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r' )
    psk_path_prefix="/tmp/psk/"
    psk_path="${psk_path_prefix}${NQN}"
    docker cp /tmp/temp-psk/psk ${BDEVPERF_CONTAINER_NAME}:`dirname ${psk_path_prefix}`
    make exec SVC=bdevperf OPTS=-T CMD="chown -R root:root ${psk_path_prefix}"
    rm -rf /tmp/temp-psk

    echo "ℹ️  bdevperf bdev_nvme_set_options"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"

    echo "ℹ️  bdevperf add PSK key name key1 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key1 ${psk_path}/${NQN}host"
    echo "ℹ️  bdevperf add PSK key name key2 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key2 ${psk_path}/${NQN}host3"
    echo "ℹ️  bdevperf list keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_get_keys"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN using PSK"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10 --psk key1"

    echo "ℹ️  verify connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].controller_id'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == "0" ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[2].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns | jq -r '.connections[2].trsvcid'` == "0" ]]
    [[ `echo $conns | jq -r '.connections[2].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[2].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[2].qpairs_count'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[2].controller_id'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[2].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[3]'` == "null" ]]

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 no PSK"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n $NQN -q "${NQN}host2" -l -1 -o 10"

    echo "ℹ️  verify connection list again"
    conns=`cephnvmf_func --output stdio --format json connection list --subsystem $NQN`

    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].controller_id'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == "${port2}" ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == "2" ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[1].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[2].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns | jq -r '.connections[2].trsvcid'` == "0" ]]
    [[ `echo $conns | jq -r '.connections[2].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[2].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[2].qpairs_count'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[2].controller_id'` == "-1" ]]
    [[ `echo $conns | jq -r '.connections[2].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[2].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[3]'` == "null" ]]

    echo "ℹ️  get controllers list"
    controllers=$(make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_controllers")

    echo "ℹ️  bdevperf perform_tests"
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
    timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
    bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
    make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"

    echo "ℹ️  bdevperf detach controllers"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host using PSK, unsecure listener"
    set +e
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10 --psk key1"
    if [[ $? -eq 0 ]]; then
        echo "Using PSK keys on an unsecure listener should fail"
        exit 1
    fi
    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using PSK, wrong key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10 --psk key2"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the wrong PSK key should fail"
        exit 1
    fi
    set -e
    return 0
}

function demo_bdevperf_dhchap()
{
    echo -n "ℹ️  Starting bdevperf container"
    docker compose up -d bdevperf
    sleep 10
    echo "ℹ️  bdevperf start up logs"
    make logs SVC=bdevperf
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r' )

    echo "ℹ️  bdevperf bdev_nvme_set_options"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"

    dhchap_path_prefix="/tmp/dhchap/"
    dhchap_path="${dhchap_path_prefix}${NQN}"
    docker cp /tmp/temp-dhchap/dhchap ${BDEVPERF_CONTAINER_NAME}:`dirname ${dhchap_path_prefix}`
    make exec SVC=bdevperf OPTS=-T CMD="chown -R root:root ${dhchap_path_prefix}"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key1"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key2"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key3"
    rm -rf /tmp/temp-dhchap

    echo "ℹ️  bdevperf add DHCHAP key name key1 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key1 ${dhchap_path}/key1"
    echo "ℹ️  bdevperf add DHCHAP key name key2 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key2 ${dhchap_path}/key2"
    echo "ℹ️  bdevperf add DHCHAP controller key name key3 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key3 ${dhchap_path}/key3"

    echo "ℹ️  bdevperf list keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -s $BDEVPERF_SOCKET keyring_get_keys"

    set +e
    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using DHCHAP, wrong key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n ${NQN} -q ${NQN}host -l -1 -o 10 --dhchap-key key3"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the wrong DHCAP key should fail"
        exit 1
    fi

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using DHCHAP, no key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n ${NQN} -q ${NQN}host -l -1 -o 10"
    if [[ $? -eq 0 ]]; then
        echo "Connecting without a DHCAP key should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using DHCHAP"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n ${NQN} -q ${NQN}host -l -1 -o 10 --dhchap-key key1"

    set +e
    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 using DHCHAP controller, wrong key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN} -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key1"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the wrong DHCAP controller key should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 using DHCHAP controller"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN} -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key3"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port3} nqn: ${NQN}host3 not using DHCHAP"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme2 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port3} -f ipv4 -n ${NQN} -q ${NQN}host3 -l -1 -o 10"

    echo "ℹ️  get controllers list"
    controllers=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_controllers"`

    echo "ℹ️  verify connection list"
    conns=`cephnvmf_func --output stdio --format json connection list --subsystem $NQN`

    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].controller_id'` == "3" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "true" ]]

    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == "${port2}" ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == "5" ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[1].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "true" ]]

    [[ `echo $conns | jq -r '.connections[2].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns | jq -r '.connections[2].trsvcid'` == "${port3}" ]]
    [[ `echo $conns | jq -r '.connections[2].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[2].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[2].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[2].controller_id'` == "6" ]]
    [[ `echo $conns | jq -r '.connections[2].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[2].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_dhchap'` == "false" ]]

    [[ `echo $conns | jq -r '.connections[3]'` == "null" ]]

    echo "ℹ️  bdevperf perform_tests"
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
    timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
    bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
    make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"

    echo "ℹ️  verify DHCHAP key files removal"
    dhchap_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk_rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    path1=`echo ${dhchap_key_list} | jq -r '.[0].path'`
    path2=`echo ${dhchap_key_list} | jq -r '.[1].path'`
    path3=`echo ${dhchap_key_list} | jq -r '.[2].path'`
    subsys_dir=`dirname ${path1}`
    [[ `echo $dhchap_key_list | jq -r '.[0].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[1].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[2].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[3].removed'` == "null" ]]
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path2}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path3}"
    make exec SVC=nvmeof OPTS=-T CMD="test -d ${subsys_dir}"
    cephnvmf_func host del --subsystem $NQN --host-nqn ${NQN}host2
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path2}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path3}"
    cephnvmf_func subsystem del --subsystem $NQN --force
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -d ${subsys_dir}"
    dhchap_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk_rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    [[ `echo $dhchap_key_list | jq -r '.[0]'` == "null" ]]

    echo "ℹ️  bdevperf detach controllers"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme2"

    echo "ℹ️  get controllers list again"
    controllers=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_controllers"`
    [[ "${controllers}" == "[]" ]]
    return 0
}

. .env

set -e
set -x
rpc="/usr/libexec/spdk/scripts/rpc.py"
port2=`expr ${NVMEOF_IO_PORT} + 10`
port3=`expr ${NVMEOF_IO_PORT} + 20`
case "$1" in
    test_unsecured)
        demo_test_unsecured
    ;;
    test_psk)
        demo_test_psk
    ;;
    test_dhchap)
        demo_test_dhchap
    ;;
    bdevperf_unsecured)
        demo_bdevperf_unsecured
    ;;
    bdevperf_psk)
        demo_bdevperf_psk
    ;;
    bdevperf_dhchap)
        demo_bdevperf_dhchap
    ;;
    *)
        echo "Invalid argument $1"
        exit 1
    ;;
esac

exit $?
