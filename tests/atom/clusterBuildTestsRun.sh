#!/bin/bash

set -e


VERSION=$1
CEPH_BRANCH=$2
if [ "$3" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/$CEPH_BRANCH/latest/centos/9/ | jq -r ".[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1")
else
    CEPH_SHA=$3
fi
ATOM_SHA=$4
ACTION_URL=$5

echo "IBM_CLOUD_KEY in script: $6"

RUNNER_FOLDER='/home/cephnvme/actions-runner-ceph-m7'
BUSY_FILE='/home/cephnvme/busyServer.txt'

check_cluster_busy() {
    local busy_file=$1
    local action_url=$2

    while true; do
        if [ -f "$busy_file" ]; then
            echo "The server is busy with another GitHub Action job, please wait..."
            sleep 90
        else
            echo "The server is available for use!"
            echo "$action_url" > "$busy_file"
            chmod +rx "$busy_file"
            break
        fi
    done
}

hostname

rm -rf $RUNNER_FOLDER/ceph-nvmeof-atom
sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7/*
sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7
cd $RUNNER_FOLDER

# Cloning atom repo
git clone git@github.ibm.com:NVME-Over-Fiber/ceph-nvmeof-atom.git

# Switch to given SHA
cd ceph-nvmeof-atom
git checkout $ATOM_SHA

# Build atom images based on the cloned repo
sudo docker build -t nvmeof_atom:$ATOM_SHA .

set -x

check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    python3 atom.py \
    --project=nvmeof \
    --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
    --ceph-branch="$CEPH_BRANCH" \
    --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
    --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
    --initiators=1 \
    --gw-group-num=1 \
    --gw-num=2 \
    --gw-to-stop-num=1 \
    --subsystem-num=2 \
    --ns-num=4 \
    --subsystem-max-ns-num=1024 \
    --failover-num=2 \
    --failover-num-after-upgrade=2 \
    --rbd-size=200M \
    --seed=0 \
    --vhosts=4 \
    --fio-devices-num=1 \
    --lb-timeout=20 \
    --config-dbg-mon=10 \
    --config-dbg-ms=1 \
    --nvmeof-daemon-stop \
    --nvmeof-systemctl-stop \
    --mon-client-kill \
    --nvmeof-daemon-remove \
    --redeploy-gws \
    --github-action-deployment \
    --dont-power-off-cloud-vms \
    --skip-lb-group-change-test \
    --skip-block-list-test \
    --skip-multi-hosts-conn-test \
    --skip-ns-rebalancing-test \
    --skip-gw-failover-latency-test \
    --ibm-cloud-key=nokey \
    --github-nvmeof-token=nokey \
    --env=m7
DOCKER_EXIT_STATUS=$?

set +x

if [ $DOCKER_EXIT_STATUS -eq 0 ]; then
    echo "Atom docker run succeeded"
else
    echo "Atom docker run failed!!!"
    exit 1
fi
