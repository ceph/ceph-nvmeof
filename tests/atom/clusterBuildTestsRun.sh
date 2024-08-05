#!/bin/bash

REPO=$1
BRANCH=$2
VERSION=$3
CEPH_SHA=$4
ATOM_SHA=$5

# In case of merge to devel
if [ $BRANCH = "merge" ]; then
    REPO="ceph/ceph-nvmeof"
    BRANCH="devel"
fi

# Atom test script run
#   Description of the uncleared flags with their default values
#   - Upgrade ceph image target (None)
#   - Upgrade nvmeof image target (None)
#   - Nvmeof cli image use in target (None)
#   - Number of gateways (4)
#   - Number of gateways to stop (1)
#   - Number of subsystems (2)
#   - Number of namespaces (4)
#   - Max namespaces per subsystem (1024)
#   - HA failover cycles (2)
#   - HA failover cycles after upgrade (2)
#   - RBD size (200M)
#   - Seed number (0)
#   - FIO use (1=run fio, 0=don't run fio)
sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    python3 cephnvme_atom.py \
    quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
    quay.io/ceph/nvmeof:"$VERSION" \
    quay.io/ceph/nvmeof-cli:"$VERSION" \
    "https://github.com/$REPO" \
    "$BRANCH" \
    None None None 4 1 2 4 1024 2 2 200M 0 1 \
    --stopNvmeofDaemon \
    --stopNvmeofSystemctl \
    --stopMonLeader \
    --gitHubActionDeployment \
    --dontUseMTLS \
    --dontPowerOffCloudVMs noKey \
    --multiIBMCloudServers_m2
