#!/bin/sh
# Run sanity I/O with rbd_with_spdk_wq, then assert SpdkContextWQ is active.
set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

. "$test_dir/sanity.sh"

# Private SPDK RPCs are not exposed via rpc.py CLI; call them through the
# same JSON-RPC client the gateway uses (spdk.rpc.client).
SPDK_SOCKET=/var/tmp/spdk.sock

echo "ℹ️  Verify SpdkContextWQ is enabled"
enabled=$(docker exec -i "$GW1_NAME" python3 - <<EOF
import json
import spdk.rpc.client as rpc_client
c = rpc_client.JSONRPCClient("$SPDK_SOCKET", None, 60.0)
print(json.dumps(c.call("bdev_rbd_get_with_spdk_wq")))
EOF
)
echo "bdev_rbd_get_with_spdk_wq: $enabled"
[ "$enabled" = "true" ]

echo "ℹ️  Verify SpdkContextWQ processed completions (deq_items > 0)"
stats=$(docker exec -i "$GW1_NAME" python3 - <<EOF
import json
import spdk.rpc.client as rpc_client
c = rpc_client.JSONRPCClient("$SPDK_SOCKET", None, 60.0)
print(json.dumps(c.call("bdev_rbd_get_wq_stats")))
EOF
)
echo "bdev_rbd_get_wq_stats: $stats"
deq=$(echo "$stats" | jq '[.[].deq_items] | add // 0')
[ "$deq" -gt 0 ]
echo "ℹ️  SpdkContextWQ verified: enabled=$enabled deq_items=$deq"
