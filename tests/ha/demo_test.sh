#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

function demo_test_unsecured()
{
    make demo OPTS=-T NVMEOF_IO_PORT2=${port2}
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
    psk_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk-rpc -s /var/tmp/spdk.sock keyring_get_keys"`
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
    echo -n "${DHCHAP_KEY7}" > /tmp/temp-dhchap/dhchap/${NQN}/key4
    echo -n "${DHCHAP_KEY8}" > /tmp/temp-dhchap/dhchap/${NQN}/key5
    echo -n "${PSK_KEY1}" > /tmp/temp-dhchap/dhchap/${NQN}/key6
    echo -n "${DHCHAP_KEY9}" > /tmp/temp-dhchap/dhchap/${NQN}/key7
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key1
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key2
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key3
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key4
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key5
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key6
    chmod 0600 /tmp/temp-dhchap/dhchap/${NQN}/key7

    make demosecuredhchap OPTS=-T SUBNQN1="${NQN}" SUBNQN2="${NQN}2" HOSTNQN="${NQN}host" HOSTNQN2="${NQN}host2" HOSTNQN3="${NQN}host3" HOSTNQN4="${NQN}host4" NVMEOF_IO_PORT2=${port2} NVMEOF_IO_PORT3=${port3} NVMEOF_IO_PORT4=${port4} DHCHAPKEY1="${DHCHAP_KEY4}" DHCHAPKEY2="${DHCHAP_KEY5}" DHCHAPKEY3="${DHCHAP_KEY6}" DHCHAPKEY4="${DHCHAP_KEY8}" PSKKEY1="${PSK_KEY1}"
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
    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN, host not in namespace netmask"
    devs=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1" ]]

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
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  verify connection list no subsystem"
    conns=$(cephnvmf_func --output stdio --format json connection list)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "*" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  bdevperf perform_tests"
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
    timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
    bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
    make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"

    echo "ℹ️  bdevperf detach controller"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"

    echo "ℹ️  verify empty connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0]'` == "null" ]]

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN, host in namespace netmask"
    localhostnqn=`cat /etc/nvme/hostnqn`
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q $localhostnqn -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1 Nvme0n2" ]]

    echo "ℹ️  verify connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${localhostnqn}" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: $NQN, using any host listener"
    localhostnqn=`cat /etc/nvme/hostnqn`
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n $NQN -q $localhostnqn -l -1 -o 10"`

    echo "ℹ️  verify connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${localhostnqn}" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${localhostnqn}" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == "${port2}" ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[1].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[2]'` == "null" ]]

    echo "ℹ️  bdevperf detach controllers"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IPV6_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN, using IPv6"
    make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IPV6_ADDRESS -s $NVMEOF_IO_PORT -f ipv6 -n $NQN -q $localhostnqn -l -1 -o 10"

    echo "ℹ️  verify connection list with IPv6"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    echo $conns
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${localhostnqn}" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IPV6_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv6" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  bdevperf detach controller"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"

    echo "ℹ️  verify empty connection list"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0]'` == "null" ]]

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN, host not in namespace netmask"
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1" ]]

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
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  change namespace visibility"
    set +e
    cephnvmf_func namespace change_visibility --subsystem $NQN --nsid 2 --auto-visible yes
    if [[ $? -eq 0 ]]; then
        echo "Changing namespace visibility with active connections should fail"
        exit 1
    fi
    set -e

    cephnvmf_func namespace change_visibility --subsystem $NQN --nsid 2 --auto-visible yes --force
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
    set +e
    cephnvmf_func namespace change_visibility --subsystem $NQN --nsid 5 --no-auto-visible
    if [[ $? -eq 0 ]]; then
        echo "Changing visibility of a non-existing namespace should fail"
        exit 1
    fi
    set -e

    cephnvmf_func namespace change_visibility --subsystem $NQN --nsid 2 --auto-visible no

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: $NQN, after changing visibility"
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1" ]]
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"

    echo "ℹ️  add host to namespace again, wrong NSID"
    set +e
    cephnvmf_func namespace add_host --subsystem $NQN --nsid 5 --host-nqn ${NQN}host
    if [[ $? -eq 0 ]]; then
        echo "Adding host to a non-existing namespace should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  add host to namespace again"
    cephnvmf_func namespace add_host --subsystem $NQN --nsid 2 --host-nqn ${NQN}host
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1 Nvme0n2" ]]
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"

    echo "ℹ️  verify listeners info, both gateway and SPDK"
    rm -f /tmp/gw_listeners.txt
    rm -f /tmp/listeners2.txt
    cephnvmf_func --output stdio --format json gw listener_info --subsystem $NQN > /tmp/gw_listeners.txt
    cephnvmf_func --output stdio --format json listener list --subsystem $NQN > /tmp/listeners.txt
    cat /tmp/gw_listeners.txt
    [[ `cat /tmp/gw_listeners.txt | jq -r '.status'` == "0" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.trtype'` == "TCP" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.adrfam'` == "ipv6" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.traddr'` == "2001:db8::3" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.trsvcid'` == "4420" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.secure'` == "false" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.active'` == "true" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.trtype'` == "TCP" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.adrfam'` == "ipv4" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.traddr'` == "0.0.0.0" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.trsvcid'` == "4430" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.secure'` == "false" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.active'` == "true" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.trtype'` == "TCP" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.adrfam'` == "ipv4" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.traddr'` == "192.168.13.3" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.trsvcid'` == "4420" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.secure'` == "false" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.active'` == "true" ]]
    [[ `cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[3]'` == "null" ]]
    hostname0=`cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[0].listener.host_name'`
    hostname1=`cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[1].listener.host_name'`
    hostname2=`cat /tmp/gw_listeners.txt | jq -r '.gw_listeners[2].listener.host_name'`
    [[ "$hostname0" == "$hostname1" ]]
    [[ "$hostname0" == "$hostname2" ]]
    for lsnr in 0 1 2
    do
        [[ `cat /tmp/gw_listeners.txt | jq -r ".gw_listeners[${lsnr}].lb_states[0].grp_id"` == "1" ]]
        [[ `cat /tmp/gw_listeners.txt | jq -r ".gw_listeners[${lsnr}].lb_states[0].state"` == "OPTIMIZED" ]]
        for i in `seq 1 15`
        do
            grp=`expr ${i} + 1`
            [[ `cat /tmp/gw_listeners.txt | jq -r ".gw_listeners[${lsnr}].lb_states[${i}].grp_id"` == "${grp}" ]]
            [[ `cat /tmp/gw_listeners.txt | jq -r ".gw_listeners[${lsnr}].lb_states[${i}].state"` == "INACCESSIBLE" ]]
        done
        [[ `cat /tmp/gw_listeners.txt | jq -r ".gw_listeners[${lsnr}].lb_states[16]"` == "null" ]]
    done

    cat /tmp/listeners.txt
    [[ `cat /tmp/listeners.txt | jq -r '.status'` == "0" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].trtype'` == "TCP" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].adrfam'` == "ipv4" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].traddr'` == "192.168.13.3" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].trsvcid'` == "4420" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].secure'` == "false" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[0].active'` == "true" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].trtype'` == "TCP" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].adrfam'` == "ipv4" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].traddr'` == "0.0.0.0" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].trsvcid'` == "4430" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].secure'` == "false" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[1].active'` == "true" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].trtype'` == "TCP" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].adrfam'` == "ipv6" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].traddr'` == "[2001:db8::3]" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].trsvcid'` == "4420" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].secure'` == "false" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[2].active'` == "true" ]]
    [[ `cat /tmp/listeners.txt | jq -r '.listeners[3]'` == "null" ]]
    hostname20=`cat /tmp/listeners.txt | jq -r '.listeners[0].host_name'`
    hostname21=`cat /tmp/listeners.txt | jq -r '.listeners[1].host_name'`
    hostname22=`cat /tmp/listeners.txt | jq -r '.listeners[2].host_name'`
    [[ "$hostname20" == "$hostname21" ]]
    [[ "$hostname20" == "$hostname22" ]]
    [[ "$hostname20" == "$hostname0" ]]

    rm -f /tmp/gw_listeners.txt
    rm -f /tmp/listeners.txt
    cephnvmf_func --output stdio --format plain gw listener_info --subsystem $NQN > /tmp/gw_listeners.txt
    cat /tmp/gw_listeners.txt
    grep "TCP          IPv6              2001:db8::3:4420   No        Yes       1: Optimized" /tmp/gw_listeners.txt
    grep "TCP          IPv4              0.0.0.0:4430       No        Yes       1: Optimized" /tmp/gw_listeners.txt
    grep "TCP          IPv4              192.168.13.3:4420  No        Yes       1: Optimized" /tmp/gw_listeners.txt

    set +e
    tail -n +4 /tmp/gw_listeners.txt | grep -v "Optimized"
    if [[ $? -eq 0 ]]; then
        echo "Should only get optimized load balancing states"
        exit 1
    fi
    set -e

    echo "ℹ️  add hosts for deletion test"
    cephnvmf_func host add --subsystem ${NQN} --host-nqn ${NQN}host31
    cephnvmf_func host add --subsystem ${NQN} --host-nqn ${NQN}host32

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: $NVMEOF_IO_PORT nqn: ${NQN}host31"
    devs=`make exec -s SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme0 -t tcp -a $NVMEOF_IP_ADDRESS -s $NVMEOF_IO_PORT -f ipv4 -n $NQN -q ${NQN}host31 -l -1 -o 10"`
    [[ "$devs" == "Nvme0n1" ]]

    echo "ℹ️  verify connection list for deletion test"
    conns=$(cephnvmf_func --output stdio --format json connection list --subsystem $NQN)
    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host31" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${NVMEOF_IO_PORT}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host32" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == 0 ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]
    [[ `echo $conns | jq -r '.connections[2]'` == "null" ]]

    echo "ℹ️  test deleting connected host"
    rm -f /tmp/hostdel.err
    cephnvmf_func --output stdio host del --subsystem ${NQN} --host-nqn ${NQN}host31 ${NQN}host32 > /dev/null 2> /tmp/hostdel.err
    cat /tmp/hostdel.err
    grep -q "Host ${NQN}host31 is still connected to ${NQN}." /tmp/hostdel.err
    grep -q "Notice that re-connecting the host would fail unless it's re-added to the subsystem" /tmp/hostdel.err
    grep "is still connected" /tmp/hostdel.err | grep -q -v "Host ${NQN}host32"
    rm -f /tmp/hostdel.err

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
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]

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
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]

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
    [[ `echo $conns | jq -r '.connections[2].subsystem'` == "${NQN}" ]]

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
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]

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
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]

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
    [[ `echo $conns | jq -r '.connections[2].subsystem'` == "${NQN}" ]]

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

    echo "ℹ️  use encryption key like it was exported by cephadm"
    docker exec ${NVMEOF_CONTAINER_NAME} rm -f /var/log/ceph/ex_encryption.key /tmp/create_enckey.sh
    rm -f /tmp/create_enckey.sh
    echo "#!/bin/bash" > /tmp/create_enckey.sh
    echo 'echo -n "-----BEGIN PRIVATE KEY----- MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAqg+wrkvj9D47BRVi A4tMOv4aBL6RLBbLEwYuhJSLTG6FagZFNknjRj0y9s5C+J0fktl3XMu9UmyUR1LR 3ojPlwIDAQABAkA2F9ONPVp+4CSJ02lf0zkmMpk4FR28NmvV20uEpHNClggqmjmW zFjGV+KHJ//r17gQD3yh+NvJzX9FlncseluBAiEA3MjrizLw6wjsk80IaGL8oQNd cUlD2wYTW6Gk7JLlFmECIQDFL6Chljk3rBoPl0jASBFHq1FT/Zqgg/z060OWBns4 9wIhAKkd3g7J/nCKbWzpaL9M02YiRbk4/ZkPllRiBQqRmpkBAiAgCx9VYu4lZ+hM RE9kP9HfDa4HshygnRJMUrcG+EKp/QIgR5uDteq1fToI5ZbYOf+KJsVoJOpPrN3b vPKX3JuIds8= -----END PRIVATE KEY-----" > /var/log/ceph/ex_encryption.key' >> /tmp/create_enckey.sh
    chmod 755 /tmp/create_enckey.sh
    docker cp /tmp/create_enckey.sh ${NVMEOF_CONTAINER_NAME}:/tmp/
    docker exec ${NVMEOF_CONTAINER_NAME} /tmp/create_enckey.sh
    rm -f /tmp/create_enckey.sh
    sed -i 's#encryption_key = /etc/ceph/encryption.key#encryption_key = /var/log/ceph/ex_encryption.key#' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}7 --no-group-append
    cephnvmf_func host add --subsystem ${NQN}7 --host-nqn ${NQN}host21 --psk "${PSK_KEY1}"
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "host21"
    sed -i 's#encryption_key = /var/log/ceph/ex_encryption.key#encryption_key = /etc/ceph/encryption.key#' ceph-nvmeof.conf
    docker exec ${NVMEOF_CONTAINER_NAME} rm -f /var/log/ceph/ex_encryption.key /tmp/create_enckey.sh

    echo "ℹ️  use invalid encryption key"
    docker exec ${NVMEOF_CONTAINER_NAME} rm -f /var/log/ceph/bad_encryption.key /tmp/create_enckey.sh
    rm -f /tmp/create_enckey.sh
    echo "#!/bin/bash" > /tmp/create_enckey.sh
    echo 'echo -n "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAqg+wrkvj9D47BRVi A4tMOv4aBL6RLBbLEwYuhJSLTG6FagZFNknjRj0y9s5C+J0fktl3XMu9UmyUR1LR 3ojPlwIDAQABAkA2F9ONPVp+4CSJ02lf0zkmMpk4FR28NmvV20uEpHNClggqmjmW zFjGV+KHJ//r17gQD3yh+NvJzX9FlncseluBAiEA3MjrizLw6wjsk80IaGL8oQNd cUlD2wYTW6Gk7JLlFmECIQDFL6Chljk3rBoPl0jASBFHq1FT/Zqgg/z060OWBns4 9wIhAKkd3g7J/nCKbWzpaL9M02YiRbk4/ZkPllRiBQqRmpkBAiAgCx9VYu4lZ+hM RE9kP9HfDa4HshygnRJMUrcG+EKp/QIgR5uDteq1fToI5ZbYOf+KJsVoJOpPrN3b vPKX3JuIds8= -----END PRIVATE KEY-----" > /var/log/ceph/bad_encryption.key' >> /tmp/create_enckey.sh
    chmod 755 /tmp/create_enckey.sh
    docker cp /tmp/create_enckey.sh ${NVMEOF_CONTAINER_NAME}:/tmp/
    docker exec ${NVMEOF_CONTAINER_NAME} /tmp/create_enckey.sh
    rm -f /tmp/create_enckey.sh
    sed -i 's#encryption_key = /etc/ceph/encryption.key#encryption_key = /var/log/ceph/bad_encryption.key#' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}8 --no-group-append
    set +e
        cephnvmf_func host add --subsystem ${NQN}8 --host-nqn ${NQN}host22 --psk "${PSK_KEY1}"
        if [[ $? -eq 0 ]]; then
            echo "Add host with PSK key should fail without valid encryption key"
            exit 1
        fi
    set -e
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep -q -v "host22"
    sed -i 's#encryption_key = /var/log/ceph/bad_encryption.key#encryption_key = /etc/ceph/encryption.key#' ceph-nvmeof.conf
    docker exec ${NVMEOF_CONTAINER_NAME} rm -f /var/log/ceph/baad_encryption.key /tmp/create_enckey.sh

    echo "ℹ️  use missing encryption key"
    sed -i 's#encryption_key = /etc/ceph/encryption.key#encryption_key = /etc/ceph/XXXencryption.key#' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}6 --no-group-append
    set +e
        cephnvmf_func host add --subsystem ${NQN}6 --host-nqn ${NQN}host20 --psk "${PSK_KEY1}"
        if [[ $? -eq 0 ]]; then
            echo "Add host with PSK key should fail without valid encryption key"
            exit 1
        fi
        make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "NVMeTLSkey"
        if [[ $? -eq 0 ]]; then
            echo "Shouldn't have unencrypted PSK keys in OMAP"
            exit 1
        fi
    set -e
    sed -i 's#encryption_key = /etc/ceph/XXXencryption.key#encryption_key = /etc/ceph/encryption.key#' ceph-nvmeof.conf

    echo "ℹ️  disable key encryption"
    sed -i '/enable_key_encryption/d' ceph-nvmeof.conf
    sed -i '/encryption_key/i enable_key_encryption = False' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}5 --no-group-append
    cephnvmf_func host add --subsystem ${NQN}5 --host-nqn ${NQN}host17 --psk "${PSK_KEY1}"
    cephnvmf_func host add --subsystem ${NQN}5 --host-nqn ${NQN}host18 --psk "${PSK_KEY2}"
    cephnvmf_func host add --subsystem ${NQN}5 --host-nqn ${NQN}host19 --psk "${PSK_KEY3}"
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "NVMeTLSkey"
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
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key4"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key5"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key6"
    make exec SVC=bdevperf OPTS=-T CMD="chmod 0600 ${dhchap_path}/key7"
    rm -rf /tmp/temp-dhchap

    echo "ℹ️  bdevperf add DHCHAP key name key1 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key1 ${dhchap_path}/key1"
    echo "ℹ️  bdevperf add DHCHAP key name key2 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key2 ${dhchap_path}/key2"
    echo "ℹ️  bdevperf add DHCHAP controller key name key3 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key3 ${dhchap_path}/key3"
    echo "ℹ️  bdevperf add DHCHAP key name key4 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key4 ${dhchap_path}/key4"
    echo "ℹ️  bdevperf add DHCHAP key name key5 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key5 ${dhchap_path}/key5"
    echo "ℹ️  bdevperf add DHCHAP key name key6 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key6 ${dhchap_path}/key6"
    echo "ℹ️  bdevperf add DHCHAP key name key7 to keyring"
    make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET keyring_file_add_key key7 ${dhchap_path}/key7"

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
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN}2 -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key1"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the wrong DHCAP controller key should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 using DHCHAP controller"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme1 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN}2 -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key3"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port3} nqn: ${NQN}host3 not using DHCHAP"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme2 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port3} -f ipv4 -n ${NQN} -q ${NQN}host3 -l -1 -o 10"

    echo "ℹ️  get controllers list"
    controllers=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_controllers"`

    echo "ℹ️  verify connection list"
    conns=`cephnvmf_func --output stdio --format json connection list --subsystem $NQN`
    conns2=`cephnvmf_func --output stdio --format json connection list --subsystem ${NQN}2`

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
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == "${port3}" ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == "4" ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[1].secure'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[2].nqn'` == "${NQN}host4" ]]
    [[ `echo $conns | jq -r '.connections[2].trsvcid'` == 0 ]]
    [[ `echo $conns | jq -r '.connections[2].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[2].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[2].qpairs_count'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[2].controller_id'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[2].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[2].use_dhchap'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[2].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[3]'` == "null" ]]

    [[ `echo $conns2 | jq -r '.status'` == "0" ]]
    [[ `echo $conns2 | jq -r '.subsystem_nqn'` == "${NQN}2" ]]

    [[ `echo $conns2 | jq -r '.connections[0].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns2 | jq -r '.connections[0].trsvcid'` == "${port2}" ]]
    [[ `echo $conns2 | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns2 | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns2 | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns2 | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns2 | jq -r '.connections[0].controller_id'` == "2" ]]
    [[ `echo $conns2 | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns2 | jq -r '.connections[0].secure'` == "false" ]]
    [[ `echo $conns2 | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns2 | jq -r '.connections[0].use_dhchap'` == "true" ]]

    [[ `echo $conns2 | jq -r '.connections[1]'` == "null" ]]

    echo "ℹ️  bdevperf perform_tests"
    eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
    timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
    bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
    make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"

    echo "ℹ️  bdevperf detach controllers"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme0"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme1"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme2"

    echo "ℹ️  get controllers list again"
    controllers=`make -s exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_controllers"`
    [[ "${controllers}" == "[]" ]]

    echo "ℹ️  keep keys before change"
    dhchap_key_list_pre_change=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk-rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    path1_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[0].path'`
    path2_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[1].path'`
    path3_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[2].path'`
    path4_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[3].path'`
    name1_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[0].name'`
    name2_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[1].name'`
    name3_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[2].name'`
    name4_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[3].name'`
    name5_pre=`echo ${dhchap_key_list_pre_change} | jq -r '.[4].name'`
    [[ `echo $dhchap_key_list_pre_change | jq -r '.[4].removed'` == "true" ]]
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path1_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path2_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path3_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path4_pre}"

    echo "ℹ️  change the key for host ${NQN}host"
    cephnvmf_func host change_key --subsystem $NQN --host-nqn ${NQN}host --dhchap-key "${DHCHAP_KEY7}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path1_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path2_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path3_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path4_pre}"

    echo "ℹ️  change the key for subsystem ${NQN}2"
    cephnvmf_func subsystem change_key --subsystem ${NQN}2 --dhchap-key "${DHCHAP_KEY9}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path2_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path3_pre}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path4_pre}"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using previous DHCHAP key"
    set +e
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme4 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n ${NQN} -q ${NQN}host -l -1 -o 10 --dhchap-key key1"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the previous DHCAP key should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${NVMEOF_IO_PORT} nqn: ${NQN}host using the new DHCHAP key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme5 -t tcp -a $NVMEOF_IP_ADDRESS -s ${NVMEOF_IO_PORT} -f ipv4 -n ${NQN} -q ${NQN}host -l -1 -o 10 --dhchap-key key4"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme5"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 using previous DHCHAP controller key"
    set +e
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme7 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN}2 -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key1"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using the previous DHCAP controller key should fail"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port2} nqn: ${NQN}host2 using the new DHCHAP controller key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme8 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port2} -f ipv4 -n ${NQN}2 -q ${NQN}host2 -l -1 -o 10 --dhchap-key key2 --dhchap-ctrlr-key key7"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_detach_controller Nvme8"

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port4} nqn: ${NQN}host4 using no PSK key"
    set +e
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme6 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port4} -f ipv4 -n ${NQN} -q ${NQN}host4 -l -1 -o 10 --dhchap-key key5"
    if [[ $? -eq 0 ]]; then
        echo "Connecting using no PSK key should fail"
        exit 1
    fi

    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "DHHC"
    if [[ $? -eq 0 ]]; then
        echo "DHCHAP keys should be encrypted in OMAP"
        exit 1
    fi
    set -e

    echo "ℹ️  bdevperf tcp connect ip: $NVMEOF_IP_ADDRESS port: ${port4} nqn: ${NQN}host4 using PSK key"
    make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_attach_controller -b Nvme6 -t tcp -a $NVMEOF_IP_ADDRESS -s ${port4} -f ipv4 -n ${NQN} -q ${NQN}host4 -l -1 -o 10 --dhchap-key key5 --psk key6"

    echo "ℹ️  verify connection list with PSK"
    conns=`cephnvmf_func --output stdio --format json connection list --subsystem $NQN`
    conns2=`cephnvmf_func --output stdio --format json connection list --subsystem ${NQN}2`
    conns3=`cephnvmf_func --output stdio --format json connection list`

    [[ `echo $conns | jq -r '.status'` == "0" ]]
    [[ `echo $conns | jq -r '.subsystem_nqn'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[0].nqn'` == "${NQN}host4" ]]
    [[ `echo $conns | jq -r '.connections[0].trsvcid'` == "${port4}" ]]
    [[ `echo $conns | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns | jq -r '.connections[0].controller_id'` == "7" ]]
    [[ `echo $conns | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].secure'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_psk'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].use_dhchap'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[0].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[1].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns | jq -r '.connections[1].trsvcid'` == 0 ]]
    [[ `echo $conns | jq -r '.connections[1].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[1].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[1].qpairs_count'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[1].controller_id'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[1].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].use_dhchap'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[1].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[2].nqn'` == "${NQN}host" ]]
    [[ `echo $conns | jq -r '.connections[2].trsvcid'` == 0 ]]
    [[ `echo $conns | jq -r '.connections[2].traddr'` == "<n/a>" ]]
    [[ `echo $conns | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns | jq -r '.connections[2].trtype'` == "" ]]
    [[ `echo $conns | jq -r '.connections[2].qpairs_count'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[2].controller_id'` == -1 ]]
    [[ `echo $conns | jq -r '.connections[2].connected'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_psk'` == "false" ]]
    [[ `echo $conns | jq -r '.connections[2].use_dhchap'` == "true" ]]
    [[ `echo $conns | jq -r '.connections[2].subsystem'` == "${NQN}" ]]

    [[ `echo $conns | jq -r '.connections[3]'` == "null" ]]

    [[ `echo $conns2 | jq -r '.status'` == "0" ]]
    [[ `echo $conns2 | jq -r '.subsystem_nqn'` == "${NQN}2" ]]

    [[ `echo $conns2 | jq -r '.connections[0].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns2 | jq -r '.connections[0].trsvcid'` == 0 ]]
    [[ `echo $conns2 | jq -r '.connections[0].traddr'` == "<n/a>" ]]
    [[ `echo $conns2 | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns2 | jq -r '.connections[0].trtype'` == "" ]]
    [[ `echo $conns2 | jq -r '.connections[0].qpairs_count'` == -1 ]]
    [[ `echo $conns2 | jq -r '.connections[0].controller_id'` == -1 ]]
    [[ `echo $conns2 | jq -r '.connections[0].connected'` == "false" ]]
    [[ `echo $conns2 | jq -r '.connections[0].use_psk'` == "false" ]]
    [[ `echo $conns2 | jq -r '.connections[0].use_dhchap'` == "true" ]]
    [[ `echo $conns2 | jq -r '.connections[0].subsystem'` == "${NQN}2" ]]

    [[ `echo $conns2 | jq -r '.connections[1]'` == "null" ]]

    [[ `echo $conns3 | jq -r '.status'` == "0" ]]
    [[ `echo $conns3 | jq -r '.subsystem_nqn'` == "*" ]]

    [[ `echo $conns3 | jq -r '.connections[0].nqn'` == "${NQN}host4" ]]
    [[ `echo $conns3 | jq -r '.connections[0].trsvcid'` == "${port4}" ]]
    [[ `echo $conns3 | jq -r '.connections[0].traddr'` == "${NVMEOF_IP_ADDRESS}" ]]
    [[ `echo $conns3 | jq -r '.connections[0].adrfam'` == "ipv4" ]]
    [[ `echo $conns3 | jq -r '.connections[0].trtype'` == "TCP" ]]
    [[ `echo $conns3 | jq -r '.connections[0].qpairs_count'` == "1" ]]
    [[ `echo $conns3 | jq -r '.connections[0].controller_id'` == "7" ]]
    [[ `echo $conns3 | jq -r '.connections[0].connected'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[0].secure'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[0].use_psk'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[0].use_dhchap'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[0].subsystem'` == "${NQN}" ]]

    [[ `echo $conns3 | jq -r '.connections[1].nqn'` == "${NQN}host3" ]]
    [[ `echo $conns3 | jq -r '.connections[1].trsvcid'` == 0 ]]
    [[ `echo $conns3 | jq -r '.connections[1].traddr'` == "<n/a>" ]]
    [[ `echo $conns3 | jq -r '.connections[1].adrfam'` == "ipv4" ]]
    [[ `echo $conns3 | jq -r '.connections[1].trtype'` == "" ]]
    [[ `echo $conns3 | jq -r '.connections[1].qpairs_count'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[1].controller_id'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[1].connected'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[1].use_psk'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[1].use_dhchap'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[1].subsystem'` == "${NQN}" ]]

    [[ `echo $conns3 | jq -r '.connections[2].nqn'` == "${NQN}host" ]]
    [[ `echo $conns3 | jq -r '.connections[2].trsvcid'` == 0 ]]
    [[ `echo $conns3 | jq -r '.connections[2].traddr'` == "<n/a>" ]]
    [[ `echo $conns3 | jq -r '.connections[2].adrfam'` == "ipv4" ]]
    [[ `echo $conns3 | jq -r '.connections[2].trtype'` == "" ]]
    [[ `echo $conns3 | jq -r '.connections[2].qpairs_count'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[2].controller_id'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[2].connected'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[2].use_psk'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[2].use_dhchap'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[2].subsystem'` == "${NQN}" ]]

    [[ `echo $conns3 | jq -r '.connections[3].nqn'` == "${NQN}host2" ]]
    [[ `echo $conns3 | jq -r '.connections[3].trsvcid'` == 0 ]]
    [[ `echo $conns3 | jq -r '.connections[3].traddr'` == "<n/a>" ]]
    [[ `echo $conns3 | jq -r '.connections[3].adrfam'` == "ipv4" ]]
    [[ `echo $conns3 | jq -r '.connections[3].trtype'` == "" ]]
    [[ `echo $conns3 | jq -r '.connections[3].qpairs_count'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[3].controller_id'` == -1 ]]
    [[ `echo $conns3 | jq -r '.connections[3].connected'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[3].use_psk'` == "false" ]]
    [[ `echo $conns3 | jq -r '.connections[3].use_dhchap'` == "true" ]]
    [[ `echo $conns3 | jq -r '.connections[3].subsystem'` == "${NQN}2" ]]

    [[ `echo $conns3 | jq -r '.connections[4]'` == "null" ]]

    echo "ℹ️  verify DHCHAP key files removal"
    dhchap_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk-rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    path1=`echo ${dhchap_key_list} | jq -r '.[0].path'`
    path2=`echo ${dhchap_key_list} | jq -r '.[1].path'`
    path3=`echo ${dhchap_key_list} | jq -r '.[2].path'`
    path4=`echo ${dhchap_key_list} | jq -r '.[3].path'`
    path5=`echo ${dhchap_key_list} | jq -r '.[4].path'`
    name1=`echo ${dhchap_key_list} | jq -r '.[0].name'`
    name2=`echo ${dhchap_key_list} | jq -r '.[1].name'`
    name3=`echo ${dhchap_key_list} | jq -r '.[2].name'`
    name4=`echo ${dhchap_key_list} | jq -r '.[3].name'`
    name5=`echo ${dhchap_key_list} | jq -r '.[4].name'`
    [[ "$path1_pre" != "$path1" ]]
    [[ "$path1_pre" != "$path2" ]]
    [[ "$path1_pre" != "$path3" ]]
    [[ "$path1_pre" != "$path4" ]]
    [[ "$path1_pre" != "$path5" ]]
    [[ "$name1_pre" != "$name1" ]]
    [[ "$name1_pre" == "$name2" ]]
    [[ "$name1_pre" != "$name3" ]]
    [[ "$name1_pre" != "$name4" ]]
    [[ "$name1_pre" != "$name5" ]]
    [[ "$path2_pre" != "$path1" ]]
    [[ "$path2_pre" != "$path2" ]]
    [[ "$path2_pre" != "$path3" ]]
    [[ "$path2_pre" != "$path4" ]]
    [[ "$path2_pre" != "$path5" ]]
    [[ "$name2_pre" != "$name1" ]]
    [[ "$name2_pre" != "$name2" ]]
    [[ "$name2_pre" == "$name3" ]]
    [[ "$name2_pre" != "$name4" ]]
    [[ "$name2_pre" != "$name5" ]]
    [[ "$path3_pre" != "$path1" ]]
    [[ "$path3_pre" != "$path2" ]]
    [[ "$path3_pre" != "$path3" ]]
    [[ "$path3_pre" != "$path4" ]]
    [[ "$path3_pre" != "$path5" ]]
    [[ "$name3_pre" != "$name1" ]]
    [[ "$name3_pre" != "$name2" ]]
    [[ "$name3_pre" != "$name3" ]]
    [[ "$name3_pre" == "$name4" ]]
    [[ "$name3_pre" != "$name5" ]]
    [[ "$path4_pre" == "$path1" ]]
    [[ "$path4_pre" != "$path2" ]]
    [[ "$path4_pre" != "$path3" ]]
    [[ "$path4_pre" != "$path4" ]]
    [[ "$path4_pre" != "$path5" ]]
    [[ "$name4_pre" == "$name1" ]]
    [[ "$name4_pre" != "$name2" ]]
    [[ "$name4_pre" != "$name3" ]]
    [[ "$name4_pre" != "$name4" ]]
    [[ "$name4_pre" != "$name5" ]]
    [[ "$name5_pre" != "$name1" ]]
    [[ "$name5_pre" != "$name2" ]]
    [[ "$name5_pre" != "$name3" ]]
    [[ "$name5_pre" != "$name4" ]]
    [[ "$name5_pre" == "$name5" ]]

    subsys_dir=`dirname ${path1}`
    subsys2_dir=`dirname ${path3}`
    [[ `echo $dhchap_key_list | jq -r '.[0].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[1].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[2].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[3].removed'` == "false" ]]
    [[ `echo $dhchap_key_list | jq -r '.[4].removed'` == "true" ]]
    [[ `echo $dhchap_key_list | jq -r '.[5].removed'` == "null" ]]
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path2}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path3}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path4}"
    make exec SVC=nvmeof OPTS=-T CMD="test -d ${subsys_dir}"
    cephnvmf_func host del --subsystem ${NQN}2 --host-nqn ${NQN}host2
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path3}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path4}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test -f ${path2}"
    make exec SVC=nvmeof OPTS=-T CMD="test -d ${subsys2_dir}"
    cephnvmf_func subsystem del --subsystem $NQN --force
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path1}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -f ${path2}"
    make exec SVC=nvmeof OPTS=-T CMD="test ! -d ${subsys_dir}"
    cephnvmf_func subsystem del --subsystem ${NQN}2 --force
    make exec SVC=nvmeof OPTS=-T CMD="test ! -d ${subsys2_dir}"
    dhchap_key_list=`make -s exec SVC=nvmeof OPTS=-T CMD="/usr/local/bin/spdk-rpc -s /var/tmp/spdk.sock keyring_get_keys"`
    [[ `echo $dhchap_key_list | jq -r '.[0]'` == "null" ]]

    echo "ℹ️  use invalid encryption key"
    sed -i '/enable_key_encryption/d' ceph-nvmeof.conf
    sed -i 's#encryption_key = /etc/ceph/encryption.key#encryption_key = /etc/ceph/XXXencryption.key#' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}4 --dhchap-key "${DHCHAP_KEY10}" --no-group-append
    set +e
        cephnvmf_func host add --subsystem ${NQN}4 --host-nqn ${NQN}hosta --dhchap-key "${DHCHAP_KEY11}"
        if [[ $? -eq 0 ]]; then
            echo "Add host with DHCHAP key should fail without valid encryption key"
            exit 1
        fi
        make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "DHHC"
        if [[ $? -eq 0 ]]; then
            echo "Shouldn't have unencrypted DHCHAP keys in OMAP"
            exit 1
        fi
    set -e

    echo "ℹ️  disable key encryption"
    sed -i '/enable_key_encryption/d' ceph-nvmeof.conf
    sed -i '/encryption_key/i enable_key_encryption = False' ceph-nvmeof.conf
    sed -i '/encryption_key/d' ceph-nvmeof.conf
    sed -i '#encryption_key#i #encryption_key = /etc/ceph/encryption.key#' ceph-nvmeof.conf
    docker restart ${NVMEOF_CONTAINER_NAME}
    sleep 20
    cephnvmf_func subsystem add --subsystem ${NQN}3 --dhchap-key "${DHCHAP_KEY10}" --no-group-append
    cephnvmf_func host add --subsystem ${NQN}3 --host-nqn ${NQN}host7 --dhchap-key "${DHCHAP_KEY11}"
    cephnvmf_func host add --subsystem ${NQN}3 --host-nqn ${NQN}host8 --dhchap-key "${DHCHAP_KEY5}"
    cephnvmf_func host add --subsystem ${NQN}3 --host-nqn ${NQN}host9 --dhchap-key "${DHCHAP_KEY6}"
    make -s exec SVC=ceph OPTS=-T CMD="rados --pool rbd listomapvals nvmeof.state" | grep "DHHC"

    return 0
}

. .env

set -e
set -x
rpc="/usr/libexec/spdk/scripts/rpc.py"
port2=`expr ${NVMEOF_IO_PORT} + 10`
port3=`expr ${NVMEOF_IO_PORT} + 20`
port4=`expr ${NVMEOF_IO_PORT} + 30`
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
