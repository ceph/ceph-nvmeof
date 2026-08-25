set -xe

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

echo "ℹ️  Double mon_nvmeofgw_beacon_grace so namespace create/delete does not mark GWs unavailable"
BEACON_GRACE=$(docker compose exec -T ceph ceph config get mon mon_nvmeofgw_beacon_grace | tr -d '\r[:space:]')
BEACON_GRACE_SEC=${BEACON_GRACE%%[^0-9]*}
BEACON_GRACE_DOUBLE=$((BEACON_GRACE_SEC * 2))
echo "ℹ️  mon_nvmeofgw_beacon_grace: $BEACON_GRACE_SEC -> $BEACON_GRACE_DOUBLE"
docker compose exec -T ceph ceph config set mon mon_nvmeofgw_beacon_grace "$BEACON_GRACE_DOUBLE"
docker compose exec -T ceph ceph config get mon mon_nvmeofgw_beacon_grace
docker compose exec -T ceph ceph config dump | grep mon_nvmeofgw_beacon_grace

"$test_dir/setup.sh"
