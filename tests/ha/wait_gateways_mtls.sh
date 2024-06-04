# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

export CLI_TLS_ARGS="--server-cert /etc/ceph/server.crt --client-key /etc/ceph/client.key --client-cert /etc/ceph/client.crt"
$test_dir/wait_gateways.sh 1
