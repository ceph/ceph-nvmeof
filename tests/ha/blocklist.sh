source .env

verify_blocklist() {
  stopped_gw_name=$1
  NODE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $stopped_gw_name)
  BLOCKLIST=$(docker compose exec -T ceph ceph osd blocklist ls)
  
  echo "verifying there is at least 1 entry in the blocklist related to the stopped gateway"
  if echo "$BLOCKLIST" | grep -q "$NODE_IP"; then
    echo "ip $NODE_IP for the stopped gateway was found the blocklist."
  else
      echo "ip $NODE_IP for node the stopped gateway was not found in blocklist."
      exit 1
  fi

  echo "verifying there are no entries in the blocklist which are not related to the stopped gateway"
  if echo "$BLOCKLIST" | grep -qv "$NODE_IP"; then
    echo "found at least 1 entry in blocklist which is not related to gateway in the stopped gateway. failing"
    exit 1
  else
      echo "didn't find unexpected entries which are not relaetd to the stopped gateway."
  fi
  echo "blocklist verification successful"
}

echo "obtaining gw1 container id and its ip"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')

echo "clearing blocklist"
docker compose exec -T ceph ceph osd blocklist clear

echo "shutting down gw1:$GW1_NAME"
docker stop $GW1_NAME 

echo "waiting for 30s after shutdown"
sleep 30

verify_blocklist "$GW1_NAME"
