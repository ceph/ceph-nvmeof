echo 111
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
echo $GW1_NAME
echo 222
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $GW1_NAME

docker compose exec -T ceph ceph orch set backend cephadm
docker compose exec -T ceph ceph orch ps --refresh

NODE_IP=$(docker compose exec -T ceph ceph orch host ls --format json | jq -r ".[] | select(.hostname==\"$GW1_NAME\") | .public_addr")
echo $NODE_IP

# # Check if NODE_IP was found
# if [ -z "$NODE_IP" ]; then
#     echo "Error: Could not find IP for node $NODE."
#     exit 1
# fi

# # Stop the node
# ceph orch daemon stop "$NODE"

# # Wait for a moment to ensure the node has stopped
# sleep 2

# # Check the blocklist for the node's IP
# BLOCKLIST=$(ceph osd blocklist ls)

# if echo "$BLOCKLIST" | grep -q "$NODE_IP"; then
#     echo "Node $NODE has been successfully stopped and its IP $NODE_IP is in the blocklist."
# else
#     echo "Error: IP $NODE_IP for node $NODE not found in blocklist."
#     exit 1
# fi


# docker compose exec ceph ceph orch daemon stop {NODE}

# docker compose exec ceph ceph osd blocklist ls

# docker compose exec ceph ceph orch daemon start