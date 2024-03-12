set -e
SCALE=2
POOL="${RBD_POOL:-rbd}"
# Check if argument is provided
if [ $# -ge 1 ]; then
    # Check if argument is an integer larger or equal than 1
    if [ "$1" -eq "$1" ] 2>/dev/null && [ "$1" -ge 1 ]; then
        # Set variable to the provided argument
        SCALE="$1"
    else
        echo "Error: Argument must be an integer larger than 1." >&2
        exit 1
    fi
fi
echo ℹ️  Starting $SCALE nvmeof gateways
docker-compose up -d --remove-orphans --scale nvmeof=$SCALE nvmeof

# Waiting for the ceph container to become healthy
while true; do
  container_status=$(docker inspect --format='{{.State.Health.Status}}' ceph)
  if [ "$container_status" = "healthy" ]; then
    # success
    break
  else
    # Wait for a specific time before checking again
    sleep 1
    printf .
  fi
done
echo ✅ ceph is healthy
echo ℹ️  Running processes of services
docker-compose top

echo ℹ️  Send nvme-gw create for all gateways
for i in $(seq $SCALE); do
  GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
  echo  📫 nvme-gw create gateway: \'$GW_NAME\' pool: \'$POOL\', group: \'\' \(empty string\)
  docker-compose exec -T ceph ceph nvme-gw create $GW_NAME $POOL ''
done
