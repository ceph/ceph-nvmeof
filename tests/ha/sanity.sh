set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html
. .env
container_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

echo -n "ℹ️  Starting bdevperf container"
make up  SVC=bdevperf OPTS="--detach"
sleep 10
echo "ℹ️  bdevperf start up logs"
make logs SVC=bdevperf
eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_SOCKET | tr -d '\n\r' )


ip=$(container_ip $GW1)
echo "ℹ️  Using discovery service in gateway $GW1 ip  $ip"
rpc="/usr/libexec/spdk/scripts/rpc.py"
echo "ℹ️  bdevperf bdev_nvme_set_options"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_set_options -r -1"
echo "ℹ️  bdevperf start discovery ip: $ip port: $NVMEOF_DISC_PORT"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_start_discovery -b Nvme0 -t tcp -a $ip -s $NVMEOF_DISC_PORT -f ipv4 -w"
echo "ℹ️  bdevperf bdev_nvme_get_discovery_info"
make exec SVC=bdevperf OPTS=-T CMD="$rpc -v -s $BDEVPERF_SOCKET bdev_nvme_get_discovery_info"
echo "ℹ️  bdevperf perform_tests"
eval $(make run SVC=bdevperf OPTS="--entrypoint=env" | grep BDEVPERF_TEST_DURATION | tr -d '\n\r' )
timeout=$(expr $BDEVPERF_TEST_DURATION \* 2)
bdevperf="/usr/libexec/spdk/scripts/bdevperf.py"
make exec SVC=bdevperf OPTS=-T CMD="$bdevperf -v -t $timeout -s $BDEVPERF_SOCKET perform_tests"
