#!/bin/bash
set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')

ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
ip2="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"

NQN="nqn.2016-06.io.spdk:cnode1"

 verify_gw_exists_and_no_subs()
 {
     IP=$1
     subs=$(docker compose run -T --rm nvmeof-cli --server-address $IP --server-port 5500 --output stdio --format json get_subsystems)
     echo "show subsystems after del : $subs"
     if echo "$subs" | grep -q '"subsystems": \[\]'; then
       echo "The string contains 'subsystems:[]' on GW  ip $IP"
     else
       echo "The string does not contain 'subsystems:[]'on GW  ip $IP "
       exit 1
     fi
 }


  echo "ℹ️ ℹ️ Start test:  Delete the last subsystem:"

  for i in $(seq 2); do

     docker compose run -T --rm nvmeof-cli --server-address $ip --server-port 5500 subsystem del -n $NQN --force
     sleep 2
     verify_gw_exists_and_no_subs  $ip
     verify_gw_exists_and_no_subs  $ip2

     echo "ℹ️ ℹ️ next : Create  subsystem:"

     docker compose run -T --rm nvmeof-cli --server-address $ip --server-port 5500 subsystem add -n $NQN
     docker compose  run --rm nvmeof-cli --server-address $ip  --server-port 5500 listener add  --subsystem $NQN --host-name $GW1_NAME --traddr $ip --trsvcid 4420
     docker compose  run --rm nvmeof-cli --server-address $ip2 --server-port 5500 listener add  --subsystem $NQN --host-name $GW2_NAME --traddr $ip2 --trsvcid 4420

     sleep 5
     subs=$(docker compose run -T --rm nvmeof-cli --server-address $ip --server-port 5500 --output stdio --format json get_subsystems)

     echo "subsystems $subs"
     #test that ana group is Active
     json=$(docker compose exec -T ceph ceph nvme-gw show  rbd '')

     states=$(echo "$json" | jq -r '.["Created Gateways:"][] | ."ana states"')
     echo "$states"

     if echo "$states" | grep -q '1: ACTIVE'; then
        echo "state found ACTIVE in group 1"
     else
	echo "ACTIVE state not found for group 1"
	exit 1
     fi
     if echo "$states" | grep -q '2: ACTIVE'; then
       echo "state found ACTIVE in group 2"
     else
       echo "ACTIVE state not found for group 2"
       exit 1
     fi

  done
  echo "ℹ️ ℹ️  test passed"
