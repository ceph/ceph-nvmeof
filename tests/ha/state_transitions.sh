set -xe
rpc=/usr/libexec/spdk/scripts/rpc.py
cmd=nvmf_subsystem_get_listeners
nqn=nqn.2016-06.io.spdk:cnode1

expect_optimized() {
  local gateway=$1 expected=$2 peer=${3:-}
  local deadline=$((SECONDS + ${CONVERGE_TIMEOUT:-120}))
  local socket groups remaining state node
  local -a optimized=()

  socket=$(timeout 5 docker exec "$gateway" find /var/tmp -name spdk.sock) || return 1
  while (( SECONDS < deadline )); do
    for node in "$gateway" ${peer:+"$peer"}; do
      state=$(timeout 5 docker inspect "$node" \
        --format '{{.State.Status}} (exit {{.State.ExitCode}})') || return 1
      if [[ $state != running* ]]; then
        echo "Gateway $node is $state while waiting for $gateway to own $expected ANA groups" >&2
        return 1
      fi
    done
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || break
    if groups=$(set -o pipefail
      timeout "$remaining" docker exec "$gateway" "$rpc" -s "$socket" "$cmd" "$nqn" |
        jq -r '.[0].ana_states[]? | select(.ana_state == "optimized") | .ana_group'
    ); then
      optimized=()
      if [[ -n $groups ]]; then
        mapfile -t optimized <<< "$groups"
      fi
      if (( ${#optimized[@]} == expected )); then
        printf '%s\n' "${optimized[@]}"
        return 0
      fi
    fi
    sleep 1
  done
  echo "Timed out waiting for $gateway to own $expected optimized ANA groups" \
    "(last: ${optimized[*]:-none})" >&2
  return 1
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

verify_blocklist() {
  NODE_IP=$1
  BLOCKLIST=$(docker compose exec -T ceph ceph osd blocklist ls)

  echo "verifying there is at least 1 entry in the blocklist related to the stopped gateway"
  if echo "$BLOCKLIST" | grep -q "$NODE_IP"; then
    echo "ip $NODE_IP for the stopped gateway was found the blocklist."
  else
      echo "ip $NODE_IP for node the stopped gateway was not found in blocklist."
      exit 1
  fi

  echo "verifying there are no entries in the blocklist which are not related to the stopped gateway"
  if echo "$BLOCKLIST" | grep -qv "$NODE_IP"; then
    echo "found at least 1 entry in blocklist which is not related to gateway in the stopped gateway. failing"
    exit 1
  else
      echo "didn't find unexpected entries which are not relaetd to the stopped gateway."
  fi
  echo "blocklist verification successful"
}

#
# MAIN
#
GW1_NAME=$(gw_name 1)
GW2_NAME=$(gw_name 2)

#
# Step 1 validate both gave are optimized for one of ANA groups 1 and 2
#
GW1_OPTIMIZED=$(expect_optimized "$GW1_NAME" 1 "$GW2_NAME")
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

GW2_OPTIMIZED=$(expect_optimized "$GW2_NAME" 1 "$GW1_NAME")
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"

#
# Step 2 failover
#
gw2_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $GW2_NAME)
gw1_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $GW1_NAME)


docker compose exec -T ceph ceph osd blocklist clear

BL=$(docker compose exec -T ceph ceph osd blocklist ls -f json)
echo "blocklist should be empty at this point  $BL"

echo "Stop gw $GW2_NAME ip: $gw2_ip"
docker stop $GW2_NAME

docker ps

GW1_FAILOVER_OPTIMIZED=$(expect_optimized $GW1_NAME 2)
gw1_ana1=$(access_number_by_index "$GW1_FAILOVER_OPTIMIZED" 0)
gw1_ana2=$(access_number_by_index "$GW1_FAILOVER_OPTIMIZED" 1)
verify_ana_groups "$gw1_ana1" "$gw1_ana2"
verify_blocklist "$gw2_ip"

#
# Step 3 failback
#
echo "Start gw $GW2_NAME"
docker start $GW2_NAME

docker ps

GW1_OPTIMIZED=$(expect_optimized "$GW1_NAME" 1 "$GW2_NAME")
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

GW2_OPTIMIZED=$(expect_optimized "$GW2_NAME" 1 "$GW1_NAME")
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"
