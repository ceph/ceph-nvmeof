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
        python3 cephnvme_atom.py \
        nvmeof \
        quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        quay.io/ceph/nvmeof:"$VERSION" \
        quay.io/ceph/nvmeof-cli:"$VERSION" \
        None None None None None None 1 1 4 1 1 2 4 1024 2 2 200M 0 1 20 10 1 \
        --stopNvmeofDaemon \
        --stopNvmeofSystemctl \
        --stopMonLeader \
        --killMonClient \
        --rmNvmeofDaemon \
        --redeployGWs \
        --gitHubActionDeployment \
        --dontUseMTLS \
        --skiplbTest \
        --journalctlToConsole \
        --dontPowerOffCloudVMs noKey noKey \
        --multiIBMCloudServers_m6
else
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        python3 cephnvme_atom.py \
        nvmeof \
        quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        quay.io/ceph/nvmeof:"$VERSION" \
        quay.io/ceph/nvmeof-cli:"$VERSION" \
        None None None None None None 1 1 4 1 1 10 90 1024 6 2 200M 0 1 20 10 1 \
        --stopNvmeofDaemon \
        --stopNvmeofSystemctl \
        --stopMonLeader \
        --killMonClient \
        --rmNvmeofDaemon \
        --redeployGWs \
        --gitHubActionDeployment \
        --dontUseHUGEPAGES \
        --dontUseMTLS \
        --skiplbTest \
        --journalctlToConsole \
        --dontPowerOffCloudVMs noKey noKey \
        --multiIBMCloudServers_m6
fi
set +x
