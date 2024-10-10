source .env

echo "obtaining gw1 container id and its ip"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
NODE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $GW1_NAME)

echo "clearing blocklist"
docker compose exec -T ceph ceph osd blocklist clear

echo "shutting down gw1:$GW1_NAME"
docker stop $GW1_NAME 

echo "waiting for 30s after shutdown"
sleep 30

BLOCKLIST=$(docker compose exec -T ceph ceph osd blocklist ls)
echo $BLOCKLIST

echo "validating there is at least 1 entriy in the blocklist related to gw1"
if echo "$BLOCKLIST" | grep -q "$NODE_IP"; then
    echo "gw1 has been successfully stopped and its IP $NODE_IP is in the blocklist."
else
    echo "ip $NODE_IP for node gw1 was not found in blocklist."
    exit 1
fi

echo "validating there are no entries in the blocklist which are not related to gw1"
if echo "$BLOCKLIST" | grep -qv "$NODE_IP"; then
    echo "found at least 1 entry in blocklist which is not related to gw1 in the stopped gw1. failing"
    exit 1
else
    echo "didn't find entries unrelaetd to gw1."
fi
