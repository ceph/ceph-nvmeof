#!/bin/sh

set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

export NVMEOF_CONFIG=./tests/ceph-nvmeof.cluster_pool.conf
$test_dir/start_up.sh
