#!/bin/bash

VERSION=$1
if [ "$2" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/main/latest/centos/9/ | jq -r ".[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1")
else
    CEPH_SHA=$2
fi
ATOM_SHA=$3

# Atom test script run
#   Description of the uncleared flags with their default values
#   - Upgrade ceph image target (None)
#   - Upgrade nvmeof image target (None)
#   - Nvmeof cli image use in target (None)
#   - Initiator num (1)
#   - Number of groups (1)
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
echo "sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    python3 cephnvme_atom.py \
    quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
    quay.io/ceph/nvmeof:"$VERSION" \
    quay.io/ceph/nvmeof-cli:"$VERSION" \
    None None None None None None 1 1 4 3 1 2 4 1024 2 2 200M 0 1 20 10 1 \
    --stopNvmeofDaemon \
    --stopNvmeofSystemctl \
    --stopMonLeader \
    --rmNvmeofDaemon \
    --gitHubActionDeployment \
    --dontUseMTLS \
    --skiplbTest \
    --journalctlToConsole \
    --dontPowerOffCloudVMs noKey noKey \
    --multiIBMCloudServers_m2"
sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    python3 cephnvme_atom.py \
    quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
    quay.io/ceph/nvmeof:"$VERSION" \
    quay.io/ceph/nvmeof-cli:"$VERSION" \
    None None None None None None 1 1 4 3 1 10 90 1024 6 2 200M 0 1 20 10 1 \
    --stopNvmeofDaemon \
    --stopNvmeofSystemctl \
    --stopMonLeader \
    --rmNvmeofDaemon \
    --gitHubActionDeployment \
    --dontUseMTLS \
    --skiplbTest \
    --journalctlToConsole \
    --dontPowerOffCloudVMs noKey noKey \
    --multiIBMCloudServers_m2
