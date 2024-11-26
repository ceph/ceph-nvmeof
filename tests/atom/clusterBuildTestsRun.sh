#!/bin/bash

# if a command fails (returns a non-zero exit code), terminate immediately
# the exit code will be the same as the exit code of the failed command.
# see https://github.com/ceph/ceph-nvmeof/actions/runs/11928539421/job/33246031083
set -e


VERSION=$1
if [ "$2" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/main/latest/centos/9/ | jq -r ".[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1")
else
    CEPH_SHA=$2
fi
ATOM_SHA=$3
ACTION_URL=$4
NIGHTLY=$5

RUNNER_FILDER='/home/cephnvme/actions-runner-ceph'

# Check if cluster is busy with another run
while true; do
    if [ -f "/home/cephnvme/busyServer.txt" ]; then
        echo "The server is busy with another github action job, please wait..."
        sleep 90
    else
        echo "The server is available for use!"
        echo $ACTION_URL > /home/cephnvme/busyServer.txt
        chmod +rx /home/cephnvme/busyServer.txt
        break
    fi
done

# Remove previous run data
hostname
rm -rf $RUNNER_FILDER/ceph-nvmeof-atom
sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m6/*
sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m6

# Cloning atom repo
cd $RUNNER_FILDER
git clone git@github.ibm.com:NVME-Over-Fiber/ceph-nvmeof-atom.git

# Switch to given SHA
cd ceph-nvmeof-atom
git checkout $ATOM_SHA

# Build atom images based on the cloned repo
docker build -t nvmeof_atom:$ATOM_SHA .

# Atom test script run
#   Description of the uncleared flags with their default values
#   - Upgrade ceph image target (None)
#   - Upgrade nvmeof image target (None)
#   - Nvmeof cli image use in target (None)
#   - Number of gateways (4)
#   - Number of gateways to stop (1)
#   - Number of gateways after scale down (1)
#   - Number of subsystems (2)
#   - Number of namespaces (4)
#   - Max namespaces per subsystem (1024)
#   - HA failover cycles (2)
#   - HA failover cycles after upgrade (2)
#   - RBD size (200M)
#   - Seed number (0)
#   - FIO use (1=run fio, 0=don't run fio)

set -x
if [ "$5" != "nightly" ]; then
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
        --dont-use-mtls \
        --skip-lb-test \
        --journalctl-to-console \
        --dont-power-off-cloud-vms \
        --env=m6
else
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
        --subsystem-num=10 \
        --ns-num=90 \
        --subsystem-max-ns-num=1024 \
        --failover-num=6 \
        --failover-num-after-upgrade=2 \
        --rbd-size=200M \
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
        --dont-use-mtls \
        --skip-lb-test \
        --journalctl-to-console \
        --dont-power-off-cloud-vms \
        --dont-use-hugepages \
        --env=m6
fi
set +x
