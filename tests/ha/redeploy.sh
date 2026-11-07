#!/bin/bash
set -xe
# See
# - https://github.com/spdk/spdk/blob/master/doc/jsonrpc.md
# - https://spdk.io/doc/nvmf_multipath_howto.html

GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
#GW3_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /3/ {print $1}')


ip1="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
ip2="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"
#ip3="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW3_NAME")"

NQN1="nqn.2016-06.io.spdk:cnode1"
NQN2="nqn.2016-06.io.spdk:cnode2"
#NQN3="nqn.2016-06.io.spdk:cnode3"

NUM_SUBSYSTEMS=2
MAX_NAMESPACE=1

check_nvme_gateways_avail() {
    local summary
    # Direct pipeline: bash never stores or prints the JSON body
    summary=$(docker compose exec -T ceph ceph nvme-gw show rbd '' 2>/dev/null | jq -r '
        if has("Created Gateways:") and (."Created Gateways:" | length > 0) then
            (."Created Gateways:" | map("\(.["gw-id"]): \(.Availability)") | join(", ")) as $gws |
            if (."Created Gateways:" | all(.Availability == "AVAILABLE")) then
                "OK | \($gws)"
            else
                "FAIL | \($gws)"
            end
        else
            "FAIL | No gateways found"
        end
    ' 2>/dev/null)

    if [[ "$summary" =~ ^OK ]]; then
        echo "SUCCESS: $summary"
        return 0
    else
        echo "ERROR: ${summary:-Failed to retrieve gateway status}" >&2
        return 1
    fi
}

check_nvme_gw_disabled() {
    local gw_id="$1"

    if [[ -z "$gw_id" ]]; then
        echo "ERROR: Missing gateway ID argument." >&2
        return 1
    fi

    local summary
    # Direct pipeline: bash never stores or traces the full JSON payload
    summary=$(docker compose exec -T ceph ceph nvme-gw show rbd '' 2>/dev/null | jq -r --arg gw "$gw_id" '
        if has("Created Gateways:") and (."Created Gateways:" | length > 0) then
            (."Created Gateways:" | map(select(.["gw-id"] == $gw)) | .[0]) as $target |
            if $target then
                if $target["admin state"] == "DISABLED" then
                    "OK | Gateway \($gw) admin state is DISABLED"
                else
                    "FAIL | Gateway \($gw) admin state is \($target["admin state"] // "UNKNOWN")"
                end
            else
                "FAIL | Gateway \($gw) not found"
            end
        else
            "FAIL | No gateways found"
        end
    ' 2>/dev/null)

    if [[ "$summary" =~ ^OK ]]; then
        echo "SUCCESS: $summary"
        return 0
    else
        echo "ERROR: ${summary:-Failed to retrieve gateway status}" >&2
        return 1
    fi
}
     
  docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN1 --no-group-append
  docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN2 --no-group-append 
 # docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN3 --no-group-append
  #docker compose run -T --rm nvmeof-cli --server-address $ip1 --server-port 5500 subsystem add -n $NQN3 --no-group-append
  sleep 2
  docker compose  run --rm nvmeof-cli --server-address $ip1  --server-port 5500 listener add  --subsystem $NQN1 --host-name $GW1_NAME --traddr $ip1 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip1  --server-port 5500 listener add  --subsystem $NQN2 --host-name $GW1_NAME --traddr $ip1 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip2  --server-port 5500 listener add  --subsystem $NQN1 --host-name $GW2_NAME --traddr $ip2 --trsvcid 4420
  docker compose  run --rm nvmeof-cli --server-address $ip2  --server-port 5500 listener add  --subsystem $NQN2 --host-name $GW2_NAME --traddr $ip2 --trsvcid 4420

  #docker compose  run --rm nvmeof-cli --server-address $ip2  --server-port 5500 listener add  --subsystem $NQN3 --host-name $GW3_NAME --traddr $ip3 --trsvcid 4420

  echo "ℹ️ ℹ️  Create namespaces with explicit LB = 1"

 for i in $NUM_SUBSYSTEMS; do
 #for i in $(seq 2..$NUM_SUBSYSTEMS); do
    NQN="nqn.2016-06.io.spdk:cnode$i"
    for num in $(seq $MAX_NAMESPACE);
    do
      image_name="demo_image$(expr \( $num + 5 \) \* $i)"
      echo $image_name
      docker compose  run --rm nvmeof-cli --server-address $ip2 --server-port 5500 namespace add --subsystem $NQN --rbd-pool rbd --rbd-image $image_name  --size 10M --rbd-create-image -l 1 --force
    done
 done

echo "ℹ️ ℹ️  configured 2 GWs and subsystems and namespaces"
sleep 2

echo "ℹ️ ℹ️  Check all GWs are in AVAILABLE before the test"
if check_nvme_gateways_avail ; then
    echo "Gateways ready."
else
    echo "Gateways check failed."
    exit 1
fi

echo "ℹ️ ℹ️  1. ok-to-stop without optional parameters- check return == 0"
if docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' > /dev/null 2>&1; then
    echo "SUCCESS: Safe to stop (exit code 0)"
else
    echo "ERROR: Not safe to stop"
    exit 1
fi
if check_nvme_gateways_avail ; then
    echo "Gateways ready."
else
    echo "Gateways check failed."
    exit 1
fi

echo "ℹ️ ℹ️  2. ok-to-stop with optional parameter gw-id -  check return == 0"
if docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' $GW1_NAME > /dev/null 2>&1; then
    echo "SUCCESS: Safe to stop (exit code 0)"
else
    echo "ERROR: Not safe to stop"
    exit 1
fi
if check_nvme_gateways_avail ; then
    echo "Gateways ready."
else
    echo "Gateways check failed."
    exit 1
fi

echo "ℹ️ ℹ️  3. ok-to-stop with optional parameter Bad gw-id -  check return == -ENOENT"
res=$(docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' qwerty 2>&1 || true)
# echo " res = $res"

if echo "$res" | grep -q "ENOENT"; then
    echo "SUCCESS: Found ENOENT in output."
else
    echo "ERROR: ENOENT not found - test failed." >&2
    exit 1
fi

echo "ℹ️ ℹ️  4. ok-to-stop with optional parameter  gw-id and disable-if-ok = false"
if docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' $GW1_NAME false > /dev/null 2>&1; then
    echo "SUCCESS: Safe to stop (exit code 0)"
else
    echo "ERROR: Not safe to stop"
    exit 1
fi
if check_nvme_gateways_avail ; then
    echo "Gateways ready."
else
    echo "Gateways check failed."
    exit 1
fi

echo "ℹ️ ℹ️  5. ok-to-stop with optional parameter  gw-id and disable-if-ok = true"
if docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' $GW1_NAME true > /dev/null 2>&1; then
    echo "SUCCESS: Safe to stop (exit code 0)"
else
    echo "ERROR: Not safe to stop"
    exit 1
fi
if check_nvme_gw_disabled $GW1_NAME; then
    echo "Gateway $GW1_NAME disabled .OK."
else
    echo "Gateways check failed."
    exit 1
fi

echo "ℹ️ ℹ️  6. ok-to-stop no params  -  check return == -EBUSY"
res=$(docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' 2>&1 || true)
# echo " res = $res"

if echo "$res" | grep -q "EBUSY"; then
    echo "SUCCESS: Found EBUSY in output."
else
    echo "ERROR: EBUSY not found - test failed." >&2
    exit 1
fi

echo "ℹ️ ℹ️  7. ok-to-stop   - try stop already stopped gw check return == -EALREADY"
res=$(docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' $GW1_NAME true 2>&1 || true)
# echo " res = $res"

if echo "$res" | grep -q "EALREADY"; then
    echo "SUCCESS: Found EALREADY in output."
else
    echo "ERROR: EALREADY not found - test failed." >&2
    exit 1
fi

echo "ℹ️ ℹ️  8. ok-to-stop   - try stop already stopped gw check return == -EALREADY"
res=$(docker compose exec -T ceph ceph nvme-gw ok-to-stop rbd '' $GW1_NAME 2>&1 || true)
# echo " res = $res"

if echo "$res" | grep -q "EALREADY"; then
    echo "SUCCESS: Found EALREADY in output."
else
    echo "ERROR: EALREADY not found - test failed." >&2
    exit 1
fi

echo "ℹ️ ℹ️  test passed"
exit 0
