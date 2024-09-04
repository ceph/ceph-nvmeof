#!/bin/bash

VERSION=$1
CEPH_SHA=$2
ATOM_SHA=$3

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
sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    python3 cephnvme_atom.py \
    quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
    quay.io/ceph/nvmeof:"$VERSION" \
    quay.io/ceph/nvmeof-cli:"$VERSION" \
    None None None None None None 4 1 1 2 4 1024 2 2 200M 0 1 20 20 1 \
    --stopNvmeofDaemon \
    --stopNvmeofSystemctl \
    --stopMonLeader \
    --rmNvmeofDaemon \
    --gitHubActionDeployment \
    --dontUseMTLS \
    --skipLbalancingTest \
    --journalctlToConsole \
    --dontPowerOffCloudVMs noKey noKey \
    --multiIBMCloudServers_m2
