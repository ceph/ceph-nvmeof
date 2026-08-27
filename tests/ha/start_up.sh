set -e
SCALE=2
POOL="${RBD_POOL:-rbd}"
# Compose needs CEPH_CLUSTER_VERSION, NVMEOF_CEPH_VERSION, and SPDK_CEPH_VERSION.
# Preserve already-set overrides; derive any missing values from CEPH_VERSION.
if [ -z "${CEPH_VERSION:-}" ]; then
  _repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
  if ! derived="$(make -C "$_repo_root" -s ceph-version)"; then
    echo "Failed to derive CEPH_VERSION" >&2
    exit 1
  fi
  if [ -z "$derived" ]; then
    echo "Failed to derive CEPH_VERSION: empty result" >&2
    exit 1
  fi
  export CEPH_VERSION="$derived"
fi
export CEPH_CLUSTER_VERSION="${CEPH_CLUSTER_VERSION:-$CEPH_VERSION}"
export NVMEOF_CEPH_VERSION="${NVMEOF_CEPH_VERSION:-$CEPH_VERSION}"
export SPDK_CEPH_VERSION="${SPDK_CEPH_VERSION:-$CEPH_VERSION}"
# Check if argument is provided
if [ $# -ge 1 ]; then
    # Check if argument is an integer larger or equal than 1
    if [ "$1" -eq "$1" ] 2>/dev/null && [ "$1" -ge 1 ]; then
        # Set variable to the provided argument
        SCALE="$1"
    else
        echo "Error: Argument must be an integer larger than 1." >&2
        exit 1
    fi
fi
echo ℹ️  Starting $SCALE nvmeof gateways
docker compose up -d --remove-orphans --scale nvmeof=$SCALE nvmeof

# Waiting for the ceph container to become healthy
while true; do
  container_status=$(docker inspect --format='{{.State.Health.Status}}' ceph)
  if [ "$container_status" = "healthy" ]; then
    # success
    break
  else
    # Wait for a specific time before checking again
    sleep 1
    printf .
  fi
done
echo ✅ ceph is healthy

echo ℹ️  Increase debug logs level
docker compose exec -T ceph ceph config get mon.a
docker compose exec -T ceph ceph tell mon.a config set debug_mon 20/20
docker compose exec -T ceph ceph tell mon.a config set debug_ms 1/1
docker compose exec -T ceph ceph config get mon.a

echo ℹ️  Running processes of services
docker compose top

echo ℹ️  Send nvme-gw create for all gateways
GW_GROUP=$(grep group ceph-nvmeof.conf | sed 's/^[^=]*=//' | sed 's/^ *//' | sed 's/ *$//')
for i in $(seq $SCALE); do
  GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
  echo  📫 nvme-gw create gateway: \'$GW_NAME\' pool: \'$POOL\', group: \'$GW_GROUP\'
  docker compose exec -T ceph ceph nvme-gw create $GW_NAME $POOL "$GW_GROUP"
done
