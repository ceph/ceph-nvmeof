set -xe

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
GW2_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"
NQN="nqn.2016-06.io.spdk:cnode17"

create_namespace() {
  GW_IP=$1
  NSID=$2
  ANA_GRP=$3
  IMAGE="test_image_$NSID"

  docker-compose  run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 namespace add --subsystem $NQN --nsid $NSID --rbd-pool rbd --rbd-image $IMAGE --size 10 --rbd-create-image -l $ANA_GRP
}

delete_namespaces() {
  GW_IP=$1
  FIRST=$2
  INC=$3
  LAST=$4

  for i in $(seq $FIRST $INC $LAST); do
    docker-compose  run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 namespace del --subsystem $NQN --nsid $i
  done
}

create_namespaces() {
  GW_IP=$1
  ANA_GRP=$2
  FIRST=$3
  INC=$4
  LAST=$5

  for i in $(seq $FIRST $INC $LAST); do
    create_namespace $GW_IP $i $ANA_GRP
  done
}



num_nss() {
  echo "$1" | jq ".subsystems[$2].namespaces | length"
}

verify_num_namespaces() {
  GW_IP=$1
  EXPECTED_NAMESPACES=$2
  NQN_INDEX=1 # the tested subsystem is expected to be a second one, after tests/ha/setup.

  for i in $(seq 100); do
    subs=$(docker-compose  run -T --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems 2>&1 1>/dev/null | grep -v Creating | sed 's/Get subsystems://')
    nss="$(num_nss "$subs" $NQN_INDEX)"
    if [ "$nss" -ne "$EXPECTED_NAMESPACES" ]; then
      echo "Not ready $GW_IP $nss $EXPECTED_NAMESPACES"
      sleep 1
      continue
    fi
    echo "Ready $GW_IP $nss"
    return
  done
  echo ‼️TIMEOUT
  exit 1
}

#
# MAIN
#
NO_NAMESPACE=0
ALL_NAMESPACES=200
HALF_NAMESPACES=$(expr $ALL_NAMESPACES / 2)
GW1_ANA=1
GW1_FIRST=1
GW2_FIRST=2
GW2_ANA=2
GW_INC=2

echo "ℹ️  Step 1: create subsystem $NQN"
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 subsystem add --subsystem  $NQN
verify_num_namespaces $GW1_IP $NO_NAMESPACE
verify_num_namespaces $GW2_IP $NO_NAMESPACE

echo "ℹ️  Step 2: create namespaces"
create_namespaces $GW1_IP $GW1_ANA $GW1_FIRST $GW_INC $ALL_NAMESPACES &
create_namespaces $GW2_IP $GW2_ANA $GW2_FIRST $GW_INC $ALL_NAMESPACES &
wait
verify_num_namespaces $GW1_IP $ALL_NAMESPACES
verify_num_namespaces $GW2_IP $ALL_NAMESPACES

echo "ℹ️  Step 3: delete half of namespaces"
delete_namespaces $GW1_IP $(expr $HALF_NAMESPACES + $GW1_FIRST) $GW_INC $ALL_NAMESPACES &
delete_namespaces $GW2_IP $(expr $HALF_NAMESPACES + $GW2_FIRST) $GW_INC $ALL_NAMESPACES &
wait
verify_num_namespaces $GW1_IP $HALF_NAMESPACES
verify_num_namespaces $GW2_IP $HALF_NAMESPACES

echo "ℹ️  Step 4: delete the restof namespaces"
delete_namespaces $GW1_IP $GW1_FIRST $GW_INC $HALF_NAMESPACES
delete_namespaces $GW2_IP $GW2_FIRST $GW_INC $HALF_NAMESPACES
wait
verify_num_namespaces $GW1_IP $NO_NAMESPACE
verify_num_namespaces $GW2_IP $NO_NAMESPACE

echo "ℹ️  Step 5: delete subsystem $NQN"
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 subsystem del --subsystem  $NQN
