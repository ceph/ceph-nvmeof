set -xe

# GW name by index
gw_name() {
  i=$1
  docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}'
}

gw_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(gw_name $1)"
}

num_subs() {
  # Count the number of elements in the outer list
  echo "$1" | jq -r '.subsystems' | jq length
}

num_nss() {
  echo "$1" | jq ".subsystems[$2].namespaces | length"
}

num_listeners() {
  echo "$1" | jq ".subsystems[$2].listen_addresses | length"
}

#
# MAIN
#
NUM_SUBSYSTEMS=2
NUM_GATEWAYS=4
NUM_NAMESPACES=32

# Setup
for i in $(seq $NUM_SUBSYSTEMS); do
    NQN="nqn.2016-06.io.spdk:cnode$i"
    docker-compose  run --rm nvmeof-cli --server-address $(gw_ip 1) --server-port 5500 subsystem add --subsystem $NQN --max-namespaces $NUM_NAMESPACES
    for n in $(seq $NUM_NAMESPACES); do
       IMAGE="image_${i}_${n}"
       L=$(expr $n % $NUM_GATEWAYS + 1)
       docker-compose  run --rm nvmeof-cli --server-address $(gw_ip 1) --server-port 5500 namespace add --subsystem $NQN --rbd-pool rbd --rbd-image $IMAGE --size 10M --rbd-create-image -l $L
    done
    for g in $(seq $NUM_GATEWAYS); do
        GW_NAME=$(gw_name $g)
        GW_IP=$(gw_ip $g)
        ADDR=0.0.0.0
        PORT=4420
        docker-compose  run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 listener add  --subsystem $NQN --host-name $GW_NAME --traddr $ADDR --trsvcid $PORT
    done
done

# Verify
for g in $(seq $NUM_GATEWAYS); do
  for i in $(seq 10); do
    echo "Verify $i $GW_NAME $GW_IP"
    GW_NAME=$(gw_name $g)
    GW_IP=$(gw_ip $g)
    subs=$(docker-compose  run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems 2>&1 | sed 's/Get subsystems://')

    # verify all resources found in get subsystems
    if [ "$(num_subs "$subs")" -ne $NUM_SUBSYSTEMS -o \
         "$(num_nss "$subs" 0)" -ne $NUM_NAMESPACES -o \
         "$(num_nss "$subs" 1)" -ne $NUM_NAMESPACES -o \
         "$(num_listeners "$subs" 1)" -ne 1 -o \
         "$(num_listeners "$subs" 1)" -ne 1 ]; then

        echo "Not ready $i $GW_NAME $GW_IP"
	sleep 5
        continue
    fi
    echo "Ready $i $GW_NAME $GW_IP"
    break
  done
done
