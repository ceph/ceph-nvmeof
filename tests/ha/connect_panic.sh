set -xe

# GW name by index
gw_name() {
  i=$1
  docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
}

# Extended sleep to exceed monitor connect timeout
extended_sleep() {
    # Sleep for 35 seconds to exceed typical monitor connect timeout (30 seconds)
    # This ensures the test triggers the connect panic behavior
    seconds=35
    echo "Sleeping for $seconds secs (extended delay to exceed monitor connect timeout)"
    sleep "$seconds"
}

# Check if a gateway has panicked by looking for the specific log message
check_gateway_panic_log() {
    GW_NAME=$1
    # Look for the specific panic message in the gateway logs
    if docker logs "$GW_NAME" 2>&1 | grep -q "what():  Did not receive initial map from monitor (connect panic)."; then
        echo "✅ Found connect panic log message in gateway $GW_NAME"
        return 0
    else
        echo "❌ Connect panic log message not found in gateway $GW_NAME"
        return 1
    fi
}

# Check if a gateway container has exited
check_gateway_exited() {
    GW_NAME=$1
    if ! docker ps --format '{{.Names}}' | grep -q "$GW_NAME"; then
        echo "✅ Gateway $GW_NAME has exited as expected"
        return 0
    else
        echo "❌ Gateway $GW_NAME is still running"
        return 1
    fi
}

#
# MAIN
#

echo "🧪 Testing connect panic behavior when monitor client timeout is exceeded"

# Step 1 Stop the existing deployment
make down

# Step 2 Start a new deployment with a single gateway
echo "Starting deployment with single gateway..."
docker compose up -d --scale nvmeof=1 nvmeof

# Step 3 Wait for the gateway to be running
echo "Waiting for gateway to be running..."
timeout_seconds=60
elapsed=0
while [ $elapsed -lt $timeout_seconds ]; do
  GW_NAME=$(gw_name 1)
  if [ -n "$GW_NAME" ]; then
    container_status=$(docker inspect -f '{{.State.Status}}' "$GW_NAME" 2>/dev/null || echo "unknown")
    if [ "$container_status" = "running" ]; then
      echo "✅ Gateway $GW_NAME is running after ${elapsed}s"
      break
    fi
  fi
  sleep 1
  elapsed=$((elapsed + 1))
  echo -n "."
  if [ $elapsed -eq $timeout_seconds ]; then
    echo "❌ Timeout waiting for gateway to be running after ${timeout_seconds}s"
    docker ps -a
    exit 1
  fi
done

# Step 4 Extended sleep to exceed monitor connect timeout
echo "Sleeping to exceed monitor connect timeout..."
extended_sleep

# Step 5 Verify the gateway has panicked and exited
echo "Checking gateway status after extended sleep..."
if check_gateway_panic_log "$GW_NAME"; then
    echo "✅ Gateway panic log verified"
else
    echo "❌ Gateway panic log not found"
    exit 1
fi

if check_gateway_exited "$GW_NAME"; then
    echo "✅ Gateway exit verified"
else
    echo "❌ Gateway exit verification failed"
    exit 1
fi

echo "🎉 Connect panic test completed successfully!"
echo "The test demonstrates that the monitor client timeout mechanism works correctly:"
echo "- Gateways that cannot connect to the monitor within the timeout will panic and exit"
echo "- This is the expected safety behavior to prevent gateways from running without monitor connectivity"
echo "- The test verifies that the panic mechanism is working as designed"
