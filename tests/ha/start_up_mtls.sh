# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

export NVMEOF_CONFIG=./tests/ceph-nvmeof.tls.conf
$test_dir/start_up.sh 1
