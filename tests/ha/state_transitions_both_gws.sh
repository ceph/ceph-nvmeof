set -xe
rpc=/usr/libexec/spdk/scripts/rpc.py
cmd=nvmf_subsystem_get_listeners
nqn=nqn.2016-06.io.spdk:cnode1

expect_optimized() {
  GW_NAME=$1
  EXPECTED_OPTIMIZED=$2

  socket=$(docker exec "$GW_NAME" find /var/run/ceph -name spdk.sock)
  # Verify expected number of "optimized"
  while true; do
    response=$(docker exec "$GW_NAME" "$rpc" "-s" "$socket" "$cmd" "$nqn")
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
      break
    else
      sleep 1
      continue
    fi
  done
}

# GW name by index
gw_name() {
  i=$1
  docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
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

#
# MAIN
#
GW1_NAME=$(gw_name 1)
GW2_NAME=$(gw_name 2)

#
# Step 1 validate both gataeways are optimized for one of ANA groups 1 and 2
#
GW1_OPTIMIZED=$(expect_optimized $GW1_NAME 1)
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

GW2_OPTIMIZED=$(expect_optimized $GW2_NAME 1)
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"

#
# Step 2 failover
#
echo "Stop GW2 $GW2_NAME"
docker stop $GW2_NAME

docker ps

GW1_FAILOVER_OPTIMIZED=$(expect_optimized $GW1_NAME 2)
gw1_ana1=$(access_number_by_index "$GW1_FAILOVER_OPTIMIZED" 0)
gw1_ana2=$(access_number_by_index "$GW1_FAILOVER_OPTIMIZED" 1)
verify_ana_groups "$gw1_ana1" "$gw1_ana2"

#
# Step 3 failback
#
echo "Start GW2 $GW2_NAME"
docker start $GW2_NAME

docker ps

GW1_OPTIMIZED=$(expect_optimized $GW1_NAME 1)
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

GW2_OPTIMIZED=$(expect_optimized $GW2_NAME 1)
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"

#
# Step 4 failover
#
echo "Stop GW1 $GW1_NAME"
docker stop $GW1_NAME

docker ps

GW2_FAILOVER_OPTIMIZED=$(expect_optimized $GW2_NAME 2)
gw2_ana1=$(access_number_by_index "$GW2_FAILOVER_OPTIMIZED" 0)
gw2_ana2=$(access_number_by_index "$GW2_FAILOVER_OPTIMIZED" 1)
verify_ana_groups "$gw2_ana1" "$gw2_ana2"

#
# Step 5 failback
#
echo "Start GW1 $GW1_NAME"
docker start $GW1_NAME

docker ps

GW2_OPTIMIZED=$(expect_optimized $GW2_NAME 1)
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

GW1_OPTIMIZED=$(expect_optimized $GW1_NAME 1)
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"


