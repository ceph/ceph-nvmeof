set -xe

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
GW2_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"
NQN="nqn.2016-06.io.spdk:cnode1"

docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 subsystem add --subsystem  $NQN -t
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 namespace add --subsystem $NQN --rbd-pool rbd --rbd-image demo_image1 --size 10M --rbd-create-image -l 1
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 namespace add --subsystem $NQN --rbd-pool rbd --rbd-image demo_image2 --size 10M --rbd-create-image -l 2
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 listener add  --subsystem $NQN --gateway-name $GW1_NAME --traddr $GW1_IP --trsvcid 4420
docker-compose  run --rm nvmeof-cli --server-address $GW2_IP --server-port 5500 listener add  --subsystem $NQN --gateway-name $GW2_NAME --traddr $GW2_IP --trsvcid 4420
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 host add      --subsystem $NQN --host "*"
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 get_subsystems
docker-compose  run --rm nvmeof-cli --server-address $GW2_IP --server-port 5500 get_subsystems

