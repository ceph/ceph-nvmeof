set -xe
rpc=/usr/libexec/spdk/scripts/rpc.py
cmd=nvmf_subsystem_get_listeners
POOL="${RBD_POOL:-rbd}"

expect_optimized() {
  GW_NAME=$1
  EXPECTED_OPTIMIZED=$2
  NQN=$3

  socket_retries=0
  socket=""
  while [ $socket_retries -lt 10 ] ; do
      socket=$(docker exec "$GW_NAME" find /var/run/ceph -name spdk.sock)
      if [ -n "$socket" ]; then
          break
      fi
      socket_retries=$(expr $socket_retries + 1)
      sleep 1
  done
  if [ -z "$socket" ]; then
      exit 1 # failed
  fi

  # Verify expected number of "optimized"
  for i in $(seq 50); do
    response=$(docker exec "$GW_NAME" "$rpc" "-s" "$socket" "$cmd" "$NQN")
    ana_states=$(echo "$response" | jq -r '.[0].ana_states')

    # Count the number of "optimized" groups
    optimized_count=$(jq -nr --argjson ana_states "$ana_states" '$ana_states | map(select(.ana_state == "optimized")) | length')

    # Check if there is expected number of "optimized" group
    if [ "$optimized_count" -eq "$EXPECTED_OPTIMIZED" ]; then
      # Iterate through JSON array
      for item in $(echo "$ana_states" | jq -c '.[]'); do
        ana_group=$(echo "$item" | jq -r '.ana_group')
        ana_state=$(echo "$item" | jq -r '.ana_state')

        # Check if ana_state is "optimized"
        if [ "$ana_state" = "optimized" ]; then
          echo "$ana_group"
        fi
      done
      return
    else
      sleep 5
      continue
    fi
  done
  echo "‼️  expect_optimized timeout GW_NAME=$1 EXPECTED_OPTIMIZED=$2 NQN=$3"
  exit 1 # failed
}

# GW name by index
gw_name() {
  i=$1
  docker ps --format '{{.ID}}\t{{.Names}}' --filter status=running --filter status=exited | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
}

# Function to access numbers by index
access_number_by_index() {
    numbers=$1
    index=$(expr $2 + 1)
    number=$(echo "$numbers" | awk -v idx="$index" 'NR == idx {print}')
    echo "$number"
}

# verify that given numbers must be either 1 and 2 or 2 and 1
verify_ana_groups() {
    nr1=$1
    nr2=$2

    if [ "$nr1" -eq 1 ] && [ "$nr2" -eq 2 ]; then
        echo "Verified: first is 1 and second is 2"
    elif [ "$nr1" -eq 2 ] && [ "$nr2" -eq 1 ]; then
        echo "Verified: first is 2 and second is 1"
    else
        echo "Invalid numbers: first and second must be either 1 and 2 or 2 and 1"
        exit 1
    fi
}

# Function to choose n random number at 1..m range
choose_n_m() {
    n=$1
    m=$2
    count=0
    numbers=""

    # Ensure m is greater than 1 to avoid division by zero errors
    if [ "$m" -le 1 ]; then
        echo "Upper limit m must be greater than 1."
        exit 1
    fi

    while [ "$count" -lt "$n" ]; do
        # Generate a random number between 1 and m
        random_number=$(expr $RANDOM % $m + 1)

        # Check if the number is unique
        is_unique=$(echo "$numbers" | grep -c "\<$random_number\>")
        if [ "$is_unique" -eq 0 ]; then
            # Add the unique number to the list
            numbers="$numbers $random_number"
            echo $random_number
            count=$(expr $count + 1)
        fi
    done
}

validate_all_active() {
  for s in $(seq $NUM_SUBSYSTEMS); do
    all_ana_states=$(for g in $(seq $NUM_GATEWAYS); do
                       NQN="nqn.2016-06.io.spdk:cnode$s"
                       GW_OPTIMIZED=$(expect_optimized "$(gw_name $g)" 1 "$NQN")
                       gw_ana=$(access_number_by_index "$GW_OPTIMIZED" 0)
                       echo $gw_ana
                     done)

    if [ "$(echo "$all_ana_states" | sort -n)" != "$(seq $NUM_GATEWAYS)" ]; then
      echo "all active state failure"
      exit 1
    fi
  done
}


#
# MAIN
#

NUM_SUBSYSTEMS=2
NUM_GATEWAYS=4
FAILING_GATEWAYS=2
#
# Step 1 validate all gateways are optimized for one of ANA group
# and all groups are unique
#

echo "ℹ️ Step 1"
validate_all_active

#
# Step 2 failover
#

echo "ℹ️ Step 2"
gws_to_stop=$(choose_n_m $FAILING_GATEWAYS $NUM_GATEWAYS)
for i in $(seq 0 $(expr $FAILING_GATEWAYS - 1)); do
  gw=$(access_number_by_index "$gws_to_stop" $i)
  gw_name=$(gw_name $gw)
  echo "ℹ️ Stop gw $gw_name i=$i gw=$gw"
  docker stop $gw_name
  echo  📫 nvme-gw delete gateway: \'$gw_name\' pool: \'$POOL\', group: \'\' \(empty string\)
  docker-compose exec -T ceph ceph nvme-gw delete $gw_name $POOL ''
done

docker ps

# expect remaining gws to have two optimized groups each
for i in $(seq 4); do
  found=0
  for j in $(seq 0 $(expr $FAILING_GATEWAYS - 1)); do
    stopped_gw=$(access_number_by_index "$gws_to_stop" $j)
    if [ "$i" -eq "$stopped_gw" ]; then
      found=1
      break
    fi
  done

  # if gw is a healthy one
  if [ "$found" -eq "0" ]; then
     echo "ℹ️ Check healthy gw gw=$i"
     for s in $(seq $NUM_SUBSYSTEMS); do
       NQN="nqn.2016-06.io.spdk:cnode$s"
       GW_OPTIMIZED=$(expect_optimized "$(gw_name $i)" 1 "$NQN")
     done
  fi
done
  
#
# Step 3 failback
#
echo "ℹ️ Step 3"
for i in $(seq 0 $(expr $FAILING_GATEWAYS - 1)); do
  gw=$(access_number_by_index "$gws_to_stop" $i)
  gw_name=$(gw_name $gw)
  echo "ℹ️ Start gw $gw_name i=$i gw=$gw"
  docker start $gw_name
  echo  📫 nvme-gw create gateway: \'$gw_name\' pool: \'$POOL\', group: \'\' \(empty string\)
  docker-compose exec -T ceph ceph nvme-gw create $gw_name $POOL ''
done

docker ps

validate_all_active
