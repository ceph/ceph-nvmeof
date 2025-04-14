#!/bin/bash
set -xe

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
#GW3_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /3/ {print $1}')


  verify_number_active_groups()
  {
     NUM="$1"
     #test that ana group is Active
     json=$(docker compose exec -T ceph ceph nvme-gw show  rbd '')
     states=$(echo "$json" | jq -r '.["Created Gateways:"][] | ."ana states"')
     echo "$states"
     rc=$(echo "$states" | grep  ' ACTIVE' | wc -l)
     echo $rc
     if [ "$rc" -ne "$NUM" ]; then
       echo "Error!: wrong number of Active ANA groups found $rc"
       exit 1
     else
       echo "Correct number of Active ANA groups found $rc"
     fi
     echo "$json" | jq -r '."GW-epoch"'

  }

echo "ℹ️ ℹ️ Start test:  Redeploy test  - simulate fast reboot and verify that no failovers during 12 sec"

sleep 10
epoch0=$(verify_number_active_groups 2)

#simulate fast-reboot
# get nvme-gw show 
docker stop $GW1_NAME
docker start $GW1_NAME
sleep 16
epoch1=$(verify_number_active_groups 2)
#verify only one Ana group is Active - means no failover 

sleep 8
epoch2=$(verify_number_active_groups 2)

if (( epoch1 - epoch0 > 3 )); then
	exit 1
fi


# now redeploy all  all Gws
docker stop $GW1_NAME
docker start $GW1_NAME
sleep 1
docker stop $GW2_NAME
docker start $GW2_NAME

sleep 16
epoch3=$(verify_number_active_groups 2)

if (( epoch3 - epoch2 > 5 )); then
        exit 1
fi


sleep 8
verify_number_active_groups 2

echo "ℹ️ ℹ️  test passed"
