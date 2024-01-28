set -xe
echo "ℹ️  HA failover/failback test"
eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
failover_step=$(expr $BDEVPERF_TEST_DURATION / 4)
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
wreak_havoc() {
  echo "Waiting $failover_step secs before failover..."
  sleep $failover_step
  echo "Stop gateway $GW2_NAME"
  docker stop $GW2_NAME
  echo "Waiting  $failover_step secs before failback..."
  sleep $failover_step
  echo "Restart gateway $GW2_NAME"
  docker start $GW2_NAME
  echo "wreak_havoc() function completed."
}

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi
wreak_havoc &
source $test_dir/sanity.sh
wait

