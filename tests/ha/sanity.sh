set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
echo -n "ℹ️  Starting bdevperf container"
docker-compose up -d bdevperf
sleep 10
echo "ℹ️  bdevperf start up logs"
make logs SVC=bdevperf
BDEVPERF_SOCKET=/tmp/bdevperf.sock
NVMEOF_DISC_PORT=8009

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
