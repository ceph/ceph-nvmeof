set -ex
docker-compose up -d --scale nvmeof=2 nvmeof

echo "Wait for ceph container to become healthy"
while true; do
  container_status=$(docker inspect --format='{{.State.Health.Status}}' ceph)
  if [[ $container_status == "healthy" ]]; then
    # success
    break
  else
    # Wait for a specific time before checking again
    sleep 1
    echo -n .
  fi
done
docker ps

# Send nvme-gw create for both gateways
for i in $(seq 2); do
  GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
  docker-compose exec -T ceph ceph nvme-gw create $GW_NAME rbd ''
done
