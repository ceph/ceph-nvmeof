set -xe
set -o pipefail

GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW_NAME")"
CLIENT=client.admin
OPTION=nvmeof_gateway_log_level

level_line() {
  docker compose run --rm nvmeof-cli --output stdio \
    --server-address "$GW_IP" --server-port 5500 gw get_log_level
}

config_get() {
  docker compose exec -T ceph ceph config get "$CLIENT" "$OPTION" | tr -d '\r[:space:]'
}

wait_for_level() {
  want="$1"
  deadline=$((SECONDS + 15))
  while true; do
    output=$(level_line || true)
    if grep -F "Gateway log level is \"${want}\"" <<<"$output"; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf '%s\n' "$output"
      config_get || true
      exit 1
    fi
    sleep 1
  done
}

echo "ℹ️  Step 1: conf file level"

output=$(level_line)
printf '%s\n' "$output"
grep -F 'Gateway log level is "debug"' <<<"$output"

echo "ℹ️  Step 2: set INFO"

docker compose exec -T ceph ceph config set "$CLIENT" "$OPTION" INFO
wait_for_level info

echo "ℹ️  Step 3: rejected level stays at info"

docker compose exec -T ceph ceph config set "$CLIENT" "$OPTION" not-a-level
got=$(config_get)
test "$got" = "not-a-level"
stay_deadline=$((SECONDS + 5))
while true; do
  output=$(level_line || true)
  if ! grep -F 'Gateway log level is "info"' <<<"$output"; then
    printf '%s\n' "$output"
    exit 1
  fi
  if (( SECONDS >= stay_deadline )); then
    break
  fi
  sleep 1
done

echo "ℹ️  Step 4: remove stored level"

docker compose exec -T ceph ceph config rm "$CLIENT" "$OPTION"
wait_for_level debug
