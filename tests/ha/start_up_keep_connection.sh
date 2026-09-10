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
sed -i 's/^.*state_update_notify.*$/state_update_notify = False/' ceph-nvmeof.conf
sed -i 's/^.*state_update_interval_sec.*$/state_update_interval_sec = 60/' ceph-nvmeof.conf
$test_dir/start_up.sh 2
