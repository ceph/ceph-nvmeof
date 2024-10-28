#!/bin/sh
set -ex
SCALE=2
echo CLI_TLS_ARGS $CLI_TLS_ARGS
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
for i in $(seq $SCALE); do
  while true; do
    GW_NAME=''
    while [ ! -n "$GW_NAME" ]; do
      sleep 1  # Adjust the sleep duration as needed
      GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
    done
    container_status=$(docker inspect -f '{{.State.Status}}' "$GW_NAME")
    if [ "$container_status" = "running" ]; then
      echo "Container $i $GW_NAME is now running."
    else
      echo "Container $i $GW_NAME is still not running. Waiting..."
      continue
    fi
    GW_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW_NAME")"
    if ! docker compose run --rm nvmeof-cli $CLI_TLS_ARGS --server-address $GW_IP --server-port 5500 get_subsystems; then
      echo "Container $i $GW_NAME $GW_IP no subsystems. Waiting..."
      continue
    fi
    break;
  done
done
