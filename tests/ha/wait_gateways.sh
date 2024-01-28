for i in $(seq 2); do
  while true; do
    sleep 1  # Adjust the sleep duration as needed
    GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
    container_status=$(docker inspect -f '{{.State.Status}}' "$GW_NAME")
    if [ "$container_status" == "running" ]; then
      echo "Container $i $GW_NAME is now running."
    else
      echo "Container $i $GW_NAME is still not running. Waiting..."
      continue
    fi
    GW_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW_NAME")"
    if docker-compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems 2>&1 | grep -i failed; then
      echo "Container $i $GW_NAME $GW_IP no subsystems. Waiting..."
      continue
    fi
    echo "Container $i $GW_NAME $GW_IP subsystems:"
    docker-compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems
    break;
  done
done
