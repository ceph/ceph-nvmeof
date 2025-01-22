#!/bin/bash

# if a command fails (returns a non-zero exit code), terminate immediately
# the exit code will be the same as the exit code of the failed command.
# see https://github.com/ceph/ceph-nvmeof/actions/runs/11928539421/job/33246031083
set -e


VERSION=$1
if [ "$2" = "latest" ]; then
    # CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/main/latest/centos/9/ | jq -r ".[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1")
    CEPH_SHA=b5c07ab4f77bffd19c88ef47141176982670de94
else
    CEPH_SHA=$2
fi
ATOM_SHA=$3
ACTION_URL=$4
NIGHTLY=$5

RUNNER_FOLDER='/home/cephnvme/actions-runner-ceph'
BUSY_FILE='/home/cephnvme/busyServer.txt'
RUNNER_NIGHTLY_FOLDER='/home/cephnvme/actions-runner-ceph-nightly'
BUSY_NIGHTLY_FILE='/home/cephnvme/busyServerNightly.txt'

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
if [ "$5" != "nightly" ]; then
    rm -rf $RUNNER_FOLDER/ceph-nvmeof-atom
    sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m6/*
    sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m6
    cd $RUNNER_FOLDER
else
    rm -rf $RUNNER_NIGHTLY_FOLDER/ceph-nvmeof-atom
    sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7/*
    sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7
    cd $RUNNER_NIGHTLY_FOLDER
fi

# Cloning atom repo
git clone git@github.ibm.com:NVME-Over-Fiber/ceph-nvmeof-atom.git

# Switch to given SHA
cd ceph-nvmeof-atom
git checkout $ATOM_SHA

# Build atom images based on the cloned repo
sudo docker build -t nvmeof_atom:$ATOM_SHA .

set -x
if [ "$5" != "nightly" ]; then
    check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
        --initiators=1 \
        --gw-group-num=1 \
        --gw-num=4 \
        --gw-to-stop-num=1 \
        --gw-scale-down-num=1 \
        --subsystem-num=2 \
        --ns-num=4 \
        --subsystem-max-ns-num=1024 \
        --failover-num=2 \
        --failover-num-after-upgrade=2 \
        --rbd-size=200M \
        --seed=0 \
        --fio-devices-num=1 \
        --lb-timeout=20 \
        --config-dbg-mon=10 \
        --config-dbg-ms=1 \
        --nvmeof-daemon-stop \
        --nvmeof-systemctl-stop \
        --mon-leader-stop \
        --mon-client-kill \
        --nvmeof-daemon-remove \
        --redeploy-gws \
        --github-action-deployment \
        --mtls \
        --journalctl-to-console \
        --dont-power-off-cloud-vms \
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --env=m6
else
    check_cluster_busy "$BUSY_NIGHTLY_FILE" "$ACTION_URL"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
        --initiators=1 \
        --gw-group-num=1 \
        --gw-num=4 \
        --gw-to-stop-num=1 \
        --gw-scale-down-num=1 \
        --subsystem-num=118 \
        --ns-num=8 \
        --subsystem-max-ns-num=1024 \
        --failover-num=10 \
        --failover-num-after-upgrade=2 \
        --rbd-size=200M \
        --seed=0 \
        --fio-devices-num=1 \
        --lb-timeout=20 \
        --config-dbg-mon=10 \
        --config-dbg-ms=1 \
        --nvmeof-daemon-stop \
        --nvmeof-systemctl-stop \
        --mon-leader-stop \
        --mon-client-kill \
        --nvmeof-daemon-remove \
        --redeploy-gws \
        --github-action-deployment \
        --journalctl-to-console \
        --dont-power-off-cloud-vms \
        --dont-use-hugepages \
        --concise-output \
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --env=m7
fi
set +x
