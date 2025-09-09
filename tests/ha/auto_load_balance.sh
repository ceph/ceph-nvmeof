#!/bin/bash
set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
GW3_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /3/ {print $1}')


ip1="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
ip2="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"
#ip3="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW3_NAME")"

NQN1="nqn.2016-06.io.spdk:cnode1"
NQN2="nqn.2016-06.io.spdk:cnode2"
NQN3="nqn.2016-06.io.spdk:cnode3"

NUM_SUBSYSTEMS=3
MAX_NAMESPACE=58

 test_ns_distribution()
 {  
    num_grps=$1
    prev_cnt=0
    first_cnt=1
    declare -A ana_load

    for group in $(seq "$num_grps"); do
        ana_load[$group]=0
    done

    for i in $(seq $NUM_SUBSYSTEMS); do
      NQN="nqn.2016-06.io.spdk:cnode$i"
      for group in $(seq $num_grps); do
        ns_list=$(docker compose run -T --rm nvmeof-cli --server-address $ip2 --server-port 5500 --output stdio --format json namespace list -n $NQN)
        #count=$(echo "$json"|jq '[.namespaces[] | select(.load_balancing_group == 1)]|length')
        count=$(echo "$ns_list" | jq --argjson group "$group" '[.namespaces[] | select(.load_balancing_group == $group)] | length')
        echo "namespaces of subsystem "  $NQN " Ana-group " $group " = "  $count
        ana_load[$group]=$(( ana_load[$group] + count ))
        if ((first_cnt == 1)); then
          first_cnt=0
          prev_cnt=$count
        else 
           #compare count with prev_count
           diff=$(( count - prev_cnt ))
           diff=${diff#-}
           if (( diff > 2 )); then
              echo "ℹ️ ℹ️ Namespace Distribution issue"
              exit 1
           else 
              echo "ℹ️ ℹ️ Compared OK!"
           fi 
        fi
      done
    done
    min_val=10000
    max_val=0
    for group in "${!ana_load[@]}"; do
       val=${ana_load[$group]}
       if (( val < min_val )); then
          min_val=$val
       fi
       if (( val > max_val )); then
          max_val=$val
       fi
    done
    if (( max_val - min_val > 2 )); then
       echo "ℹ️ ℹ️ Namespace ANA group Distribution issue"
       exit 1
    else
       echo "ℹ️ ℹ️ ANA Compared OK!"
    fi
}


  echo "ℹ️ ℹ️ Start test:  create additional 3 subsystems and 3 listeners:"

  docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN2 --no-group-append 
  docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN3 --no-group-append
  #docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN3 --no-group-append
  sleep 2
  docker compose  run --rm nvmeof-cli --server-address $ip1  --server-port 5500 listener add  --subsystem $NQN2 --host-name $GW1_NAME --traddr $ip1 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip1  --server-port 5500 listener add  --subsystem $NQN3 --host-name $GW1_NAME --traddr $ip1 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip2  --server-port 5500 listener add  --subsystem $NQN2 --host-name $GW2_NAME --traddr $ip2 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip2  --server-port 5500 listener add  --subsystem $NQN3 --host-name $GW2_NAME --traddr $ip2 --trsvcid 4420

  echo "ℹ️ ℹ️  Create namespaces with explicit LB = 1"


 for i in $(seq $NUM_SUBSYSTEMS); do
    NQN="nqn.2016-06.io.spdk:cnode$i"
    for num in $(seq $MAX_NAMESPACE);
    do
      image_name="demo_image$(expr \( $num + 5 \) \* $i)"
      echo $image_name
      docker compose  run --rm nvmeof-cli --server-address $ip2 --server-port 5500 namespace add --subsystem $NQN --rbd-pool rbd --rbd-image $image_name  --size 10M --rbd-create-image -l 1 --force
    done
 done

#auto load balance is working , check distribution now and then after 2 minutes (300 ns rebalance takes ~ 4 mins)  # subs=$(docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 --output stdio --format json get_subsystems 2>&1 | sed 's/Get subsystems://')
echo "ℹ️ ℹ️  Wait for rebalance "
sleep 250

test_ns_distribution 2

docker compose exec -T ceph ceph nvme-gw delete $GW1_NAME rbd ''
echo "ℹ️ ℹ️  Wait for scale-down rebalance "
sleep 110
test_ns_distribution 1
docker compose exec -T ceph ceph nvme-gw create $GW1_NAME rbd ''
echo "ℹ️ ℹ️  Wait for rebalance after create GW"
sleep 200
test_ns_distribution 2

############################################################################################

echo "ℹ️ ℹ️  test passed"
exit 0
