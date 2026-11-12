set -eu

# Create the subsystem and namespaces once. Restart timing is measured later,
# by tests/ha/startup_replay.sh, and does not include this step.
NS_COUNT="${STARTUP_REPLAY_NAMESPACES:-128}"
POOL="${RBD_POOL:-rbd}"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1; exit}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
NQN="nqn.2016-06.io.spdk:cnode-startup"

if [ -z "$GW1_NAME" ] || [ -z "$GW1_IP" ]; then
    echo "startup_replay setup: gateway 1 is not running" >&2
    exit 1
fi

echo "startup_replay setup: subsystem $NQN on $GW1_NAME ($GW1_IP)"
docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 subsystem add --subsystem "$NQN" --no-group-append
docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 listener add --subsystem "$NQN" --host-name "$GW1_NAME" --traddr "$GW1_IP" --trsvcid 4420
docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 host add --subsystem "$NQN" --host-nqn "*"

i=1
while [ "$i" -le "$NS_COUNT" ]; do
    echo "startup_replay setup: namespace $i / $NS_COUNT"
    docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 namespace add --subsystem "$NQN" --nsid "$i" --rbd-pool "$POOL" --rbd-image "startup_image_$i" --size 10M --rbd-create-image -l 1
    i=$((i + 1))
done

docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500 get_subsystems
