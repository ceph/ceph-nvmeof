#!/bin/bash

ATOM_BRANCH=$1
ATOM_REPO_OWNER=$2
ATOM_REPO_TOKEN=$3
ATOM_SHA=$4
ATOM=$5

echo "_2_ATOM_BRANCH : $ATOM_BRANCH"
echo "_2_ATOM_REPO_OWNER : $ATOM_REPO_OWNER"
echo "_2_ATOM_REPO_TOKEN : $ATOM_REPO_TOKEN"

TRIMMED_ATOM_REPO_OWNER="${ATOM_REPO_OWNER%?}"

# # In case of merge to devel
# if [ $NVMEOF_REPO_OWNER = 'devel' ]; then
#     NVMEOF_REPO_OWNER='ceph'
# fi

# Remove atom repo folder
rm -rf /home/cephnvme/actions-runner-ceph/ceph-nvmeof-atom

# Check if cluster is busy with another run
while true; do
    if [ -f "/home/cephnvme/busyServer.txt" ]; then
        echo "The server is busy with another github action job, please wait..."
        sleep 90
    else
        echo "The server is available for use!"
        touch /home/cephnvme/busyServer.txt
        chmod +rx /home/cephnvme/busyServer.txt
        break
    fi
done

# Cleanup docker images
sudo docker ps -q | xargs -r sudo docker stop; sudo docker ps -q | xargs -r sudo docker rm -f; sudo yes | docker system prune -fa; docker ps; docker images

# Cloning atom repo
cd /home/cephnvme/actions-runner-ceph
echo "git clone --branch $ATOM_BRANCH https://$TRIMMED_ATOM_REPO_OWNER:$ATOM_REPO_TOKEN@github.ibm.com/NVME-Over-Fiber/ceph-nvmeof-atom.git"
git clone --branch $ATOM_BRANCH https://$TRIMMED_ATOM_REPO_OWNER:$ATOM_REPO_TOKEN@github.ibm.com/NVME-Over-Fiber/ceph-nvmeof-atom.git
if [ $? -ne 0 ]; then
    echo "Error: Failed to clone the atom repository."
    exit 1
fi
