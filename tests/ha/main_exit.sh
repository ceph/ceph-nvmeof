#!/bin/sh
set -xe
SCALE=1
POOL="${RBD_POOL:-rbd}"

background_task() {

  # Give gateway some time
  sleep 5

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
  docker compose top

  echo ℹ️  Send nvme-gw create for all gateways
  GW_NAME=''
  GW_GROUP=''
  i=1 # a single gw index
  while [ ! -n "$GW_NAME" ]; do
    sleep 1
    GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
  done
  echo  📫 nvme-gw create gateway: \'$GW_NAME\' pool: \'$POOL\', group: \'$GW_GROUP\'
  docker compose exec -T ceph ceph nvme-gw create $GW_NAME $POOL "$GW_GROUP"
  docker compose exec -T ceph ceph nvme-gw show $POOL "$GW_GROUP"

  echo ℹ️  Wait for gateway to be ready
  while true; do
    sleep 1  # Adjust the sleep duration as needed
    container_status=$(docker inspect -f '{{.State.Status}}' "$GW_NAME")
    if [ "$container_status" == "running" ]; then
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
    break
  done

  # Signal to send (e.g., SIGTERM or SIGKILL)
  SIGNAL="SIGABRT"

  # Get the PID of monitor_client inside the container
  PID=$(docker exec "$GW_NAME" sh -c "for pid in /proc/*; do
        if [ -f \"\$pid/comm\" ] && grep -q 'ceph-nvmeof-mon' \"\$pid/comm\"; then
            echo \$(basename \$pid)
            break
        fi
    done")

  if [ -n "$PID" ]; then
    echo "ℹ️  Sending $SIGNAL to monitor_client (PID: $PID) in $GW_NAME..."
    docker exec "$GW_NAME" kill -s "$SIGNAL" "$PID"
  else
    echo "❌ monitor_client process not found in $GW_NAME."
    exit 1
  fi

}

##
## MAIN
##

background_task &
TASK_PID=$!  # Capture the PID of the background task

echo ℹ️  Starting $SCALE nvmeof gateways
docker compose up --remove-orphans --scale nvmeof=$SCALE nvmeof
GW_NAME=$(docker ps -a --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
docker inspect "$GW_NAME"
exit_code=$(docker inspect --format='{{.State.ExitCode}}' "$GW_NAME")

# expect exit code 1
if [ $exit_code -eq 1 ]; then
    echo ✅  gateway returned exit code 1, exiting with success.
else
    echo ❌  gateway returned exit code $exit_code, exiting with failure.
    exit 1  # Failure exit code
fi

# Wait for the background task to finish
wait $TASK_PID  # Wait for the specific PID to complete
background_task_exit_code=$?    # Capture the exit code of the background task

# Check the exit code and print the result
if [ $background_task_exit_code -eq 0 ]; then
    echo ✅ background task completed successfully
else
    echo ❌ background task failed with exit code: $background_task_exit_code
fi

# Exit with the same code as the background task
exit $background_task_exit_code


