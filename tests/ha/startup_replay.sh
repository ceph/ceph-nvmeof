set -eu

# The images and namespaces already exist. This script restarts gateways and
# reads their logs. It does not call "gw info", because that takes rpc_lock
# during replay. Scale 1 is one restart. Scales 2-4 record the new gateway's
# first boot, then restart every gateway in the group together.
NS_COUNT="${STARTUP_REPLAY_NAMESPACES:-128}"
POOL="${RBD_POOL:-rbd}"
INIT_TIMEOUT="${STARTUP_REPLAY_INIT_TIMEOUT:-900}"
if [ -n "${GITHUB_WORKSPACE:-}" ]; then
  ROOT="$GITHUB_WORKSPACE"
else
  ROOT=$(pwd)
fi
REPORT="$ROOT/tests/ha/startup_replay_report.py"
SUMMARY="${STARTUP_REPLAY_SUMMARY:-/tmp/startup_replay_summary.tsv}"
WATCH_DIR=$(mktemp -d)
: > "$SUMMARY"

GW_GROUP=$(grep group ceph-nvmeof.conf | sed 's/^[^=]*=//' | sed 's/^ *//' | sed 's/ *$//')
CLUSTER_CONNECTIONS=$(sed -n 's/^[[:space:]]*cluster_connections[[:space:]]*=[[:space:]]*//p' ceph-nvmeof.conf | head -n 1)
if [ -z "$CLUSTER_CONNECTIONS" ]; then
  CLUSTER_CONNECTIONS=32
fi

stop_watches() {
  if [ -f "$WATCH_DIR/pids" ]; then
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$WATCH_DIR/pids"
    rm -f "$WATCH_DIR/pids"
  fi
}

cleanup() {
  stop_watches
  rm -rf "$WATCH_DIR"
}
trap cleanup EXIT

utc_now() {
  python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))'
}

gw_cid() {
  docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk -v n="$1" '$2 ~ /nvmeof/ && $2 ~ n {print $1; exit}'
}

gw_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

port_watch() {
  python3 - "$1" "$2" <<'PY'
import datetime
import socket
import sys
import time

ip, out = sys.argv[1], sys.argv[2]
first = True
while True:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        sock.connect((ip, 5500))
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        kind = "late" if first else "ok"
        with open(out, "w", encoding="utf-8") as handle:
            handle.write(kind + " " + now + "\n")
        break
    except OSError:
        first = False
        time.sleep(0.2)
    finally:
        sock.close()
PY
}

start_port_watch() {
  idx=$1
  cid=$(gw_cid "$idx")
  ip=$(gw_ip "$cid")
  if [ -z "$ip" ]; then
    echo "startup_replay: gateway $idx has no IP" >&2
    exit 1
  fi
  rm -f "$WATCH_DIR/$idx.port"
  port_watch "$ip" "$WATCH_DIR/$idx.port" &
  echo $! >> "$WATCH_DIR/pids"
}

measure_gateway() {
  event=$1
  scale=$2
  idx=$3
  cid=$(gw_cid "$idx")
  if [ -z "$cid" ]; then
    echo "startup_replay: gateway $idx is not running" >&2
    exit 1
  fi
  started=$(docker inspect -f '{{.State.StartedAt}}' "$cid")
  tz=$(docker exec "$cid" date +%z 2>/dev/null || true)
  if [ -z "$tz" ]; then
    tz=+0000
  fi
  echo "startup_replay: wait for $event scale=$scale gateway=$idx cid=$cid"
  deadline=$(( $(date +%s) + INIT_TIMEOUT ))
  ready=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    # Discovery logs "Initialization is over (discovery-...)" into this same
    # container. Wait for the gateway process, not that earlier line.
    if docker logs -t --since "$started" "$cid" 2>&1 | grep -q "Initialization is over (gateway-"; then
      ready=1
      break
    fi
    sleep 2
  done
  if [ "$ready" -ne 1 ]; then
    echo "startup_replay: timed out after ${INIT_TIMEOUT}s event=$event scale=$scale gateway=$idx" >&2
    docker logs -t --since "$started" --tail 80 "$cid" >&2 || true
  fi
  n=0
  while [ ! -s "$WATCH_DIR/$idx.port" ] && [ "$n" -lt 25 ]; do
    n=$((n + 1))
    sleep 0.2
  done
  port_at=""
  port_kind=""
  if [ -s "$WATCH_DIR/$idx.port" ]; then
    port_kind=$(awk '{print $1; exit}' "$WATCH_DIR/$idx.port")
    port_at=$(awk '{print $2; exit}' "$WATCH_DIR/$idx.port")
  fi
  log=$(mktemp)
  docker logs -t --since "$started" "$cid" >"$log" 2>&1 || true
  set +e
  python3 "$REPORT" \
    --event "$event" \
    --scale "$scale" \
    --gateway "gw${idx}" \
    --started-at "$started" \
    --port-open-at "$port_at" \
    --port-open-kind "$port_kind" \
    --tz-offset "$tz" \
    --namespaces "$NS_COUNT" \
    --cluster-connections "$CLUSTER_CONNECTIONS" \
    --log-file "$log" \
    --summary "$SUMMARY"
  rc=$?
  set -e
  rm -f "$log"
  if [ "$rc" -ne 0 ] || [ "$ready" -ne 1 ]; then
    echo "startup_replay: failed event=$event scale=$scale gateway=$idx" >&2
    cat "$SUMMARY" >&2 || true
    exit 1
  fi
}

scale_to() {
  target=$1
  echo "startup_replay: scale nvmeof to $target"
  docker compose up -d --scale "nvmeof=$target" --no-recreate nvmeof
  i=$((registered + 1))
  while [ "$i" -le "$target" ]; do
    cid=""
    n=0
    while [ "$n" -lt 30 ]; do
      cid=$(gw_cid "$i")
      if [ -n "$cid" ]; then
        break
      fi
      n=$((n + 1))
      sleep 1
    done
    if [ -z "$cid" ]; then
      echo "startup_replay: gateway $i did not appear" >&2
      exit 1
    fi
    echo "startup_replay: nvme-gw create $cid pool=$POOL group=$GW_GROUP"
    docker compose exec -T ceph ceph nvme-gw create "$cid" "$POOL" "$GW_GROUP"
    i=$((i + 1))
  done
  registered=$target
}

restart_all() {
  scale=$1
  stop_watches
  echo "startup_replay: restart $scale gateways together"
  pids=""
  i=1
  while [ "$i" -le "$scale" ]; do
    cid=$(gw_cid "$i")
    if [ -z "$cid" ]; then
      echo "startup_replay: gateway $i is not running" >&2
      exit 1
    fi
    docker restart -t 20 "$cid" &
    pids="$pids $!"
    i=$((i + 1))
  done
  for pid in $pids; do
    wait "$pid"
  done
  i=1
  while [ "$i" -le "$scale" ]; do
    start_port_watch "$i"
    i=$((i + 1))
  done
  i=1
  while [ "$i" -le "$scale" ]; do
    measure_gateway restart_all "$scale" "$i"
    i=$((i + 1))
  done
}

registered=1

cid=$(gw_cid 1)
if [ -z "$cid" ]; then
  echo "startup_replay: gateway 1 is not running" >&2
  exit 1
fi
echo "startup_replay: restart gateway 1"
docker restart -t 20 "$cid"
start_port_watch 1
measure_gateway restart 1 1

scale=2
while [ "$scale" -le 4 ]; do
  scale_to "$scale"
  stop_watches
  echo "startup_replay: first boot of gateway $scale"
  start_port_watch "$scale"
  measure_gateway first_boot "$scale" "$scale"
  restart_all "$scale"
  scale=$((scale + 1))
done

echo "startup_replay: summary $SUMMARY"
cat "$SUMMARY"
