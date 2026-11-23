set -xe
. "${GITHUB_WORKSPACE:-.}/tests/ha/ana_polling.sh"
nqn=nqn.2016-06.io.spdk:cnode1

# GW name by index
gw_name() {
  i=$1
  docker ps --all --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
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

random_sleep() {
    # Generate a random number between 0 and 25 seconds
    # This ensures the sleep is safely below typical monitor connect timeout (30+ seconds)
    # while still providing enough randomness for late registration testing
    seconds=$(( RANDOM % 26 ))

    # Sleep for the random number of seconds
    echo "Sleeping for $seconds secs (random delay capped below monitor connect timeout)"
    sleep "$seconds"
}

#
# MAIN
#

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

# Step 1 Stop the existing deployement
make down

# Step 2 Start a new deployment
docker compose up -d --scale nvmeof=2 nvmeof

# Step 3 Wait for the Ceph container to become healthy
ceph_deadline=$(($(date +%s) + ${CONVERGE_TIMEOUT:-120}))
while :; do
  container_status=$(timeout 5 docker inspect --format '{{.State.Health.Status}}' ceph)
  [ "$container_status" = healthy ] && break
  if [ "$(date +%s)" -ge "$ceph_deadline" ]; then
    echo "Timed out waiting for the Ceph container to become healthy (last: $container_status)" >&2
    exit 1
  fi
  sleep 1
done
docker ps

# Step 4 random sleep
random_sleep

# Step 5 Send nvme-gw create for both gateways
for i in $(seq 2); do
  GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
  docker compose exec -T ceph ceph nvme-gw create $GW_NAME rbd ''
done

# Step 6 Wait for both gateways to be ready
if ! CLI_TLS_ARGS="${CLI_TLS_ARGS:-}" timeout "${CONVERGE_TIMEOUT:-120}" \
    "$test_dir/wait_gateways.sh" 2; then
  echo "Gateways did not become ready after late registration" >&2
  exit 1
fi

# Step 7 Setup host
. "$test_dir/setup.sh"

#
# Step 8 validate both gateways are optimized for one of ANA groups 1 and 2
#
GW1_NAME=$(gw_name 1)
GW2_NAME=$(gw_name 2)
GW1_OPTIMIZED=$(expect_optimized "$GW1_NAME" 1 "$nqn" "$GW2_NAME")
gw1_ana=$(access_number_by_index "$GW1_OPTIMIZED" 0)

GW2_OPTIMIZED=$(expect_optimized "$GW2_NAME" 1 "$nqn" "$GW1_NAME")
gw2_ana=$(access_number_by_index "$GW2_OPTIMIZED" 0)

verify_ana_groups "$gw1_ana" "$gw2_ana"
