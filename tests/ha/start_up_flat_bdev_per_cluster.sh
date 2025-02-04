#!/bin/sh

set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

export NVMEOF_CONFIG=./tests/ceph-nvmeof.flat_bdevs_per_cluster.conf
$test_dir/start_up.sh
