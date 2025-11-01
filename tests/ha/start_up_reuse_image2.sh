#!/bin/bash

set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

rm -f /tmp/ceph-nvmeof.conf
cp ceph-nvmeof.conf /tmp/
sed -i 's/^ *group *=.*$/group = group_2/' ceph-nvmeof.conf
$test_dir/start_up.sh 1
