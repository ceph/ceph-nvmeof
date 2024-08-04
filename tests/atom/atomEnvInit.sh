#!/bin/bash

ATOM_REPO_OWNER=$1
NVMEOF_REPO_OWNER=$2
ATOM_REPO_TOKEN=$3
ATOM_BRANCH=$4
ATOM_SHA=$5

echo "_3_ATOM_BRANCH: $ATOM_BRANCH"
echo "_3_ATOM_SHA: $ATOM_SHA"

TRIMMED_ATOM_REPO_OWNER="${ATOM_REPO_OWNER%?}"

cleanup_docker_images() {
    local HOST=$1
    ssh -o StrictHostKeyChecking=no root@$HOST << EOF
    sudo docker ps -q | xargs -r sudo docker stop
    sudo docker ps -q | xargs -r sudo docker rm -f
    sudo yes | sudo docker system prune -fa
    sudo docker ps
    sudo docker images
EOF
}

# In case of merge to devel
if [ $NVMEOF_REPO_OWNER = 'devel' ]; then
    NVMEOF_REPO_OWNER='ceph'
fi

# Remove atom repo folder
rm -rf /home/cephnvme/actions-runner-$NVMEOF_REPO_OWNER/ceph-nvmeof-atom

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

cd /home/cephnvme/actions-runner-$NVMEOF_REPO_OWNER
pwd
ls -lta
echo "git clone --branch $ATOM_BRANCH https://$TRIMMED_ATOM_REPO_OWNER:$ATOM_REPO_TOKEN@github.ibm.com/NVME-Over-Fiber/ceph-nvmeof-atom.git"
git clone --branch $ATOM_BRANCH https://$TRIMMED_ATOM_REPO_OWNER:$ATOM_REPO_TOKEN@github.ibm.com/NVME-Over-Fiber/ceph-nvmeof-atom.git
if [ $? -ne 0 ]; then
    echo "Error: Failed to clone the atom repository."
    exit 1
fi

# Switch to given SHA
cd ceph-nvmeof-atom
git checkout $ATOM_SHA
if [ $? -ne 0 ]; then
    echo "Error: Failed to checkout the specified SHA."
    exit 1
fi

# Build atom images based on the cloned repo
docker build -t nvmeof_atom:$ATOM_SHA /home/cephnvme/actions-runner-$NVMEOF_REPO_OWNER/ceph-nvmeof-atom
if [ $? -ne 0 ]; then
    echo "Error: Failed to build Docker image."
    exit 1
fi

# Remove ceph cluster
docker run -v /root/.ssh:/root/.ssh nvmeof_atom:$ATOM_SHA ansible-playbook -i custom_inventory.ini cephnvmeof_remove_cluster.yaml --extra-vars 'SELECTED_ENV=multiIBMCloudServers_m2'
if [ $? -ne 0 ]; then
    echo "Error: Failed to run cephnvmeof_remove_cluster ansible-playbook."
    exit 1
fi

# Cleanup remain images after ceph cluster removal
HOSTS=("cephnvme-vm9" "cephnvme-vm7" "cephnvme-vm6" "cephnvme-vm1")
for HOST in "${HOSTS[@]}"; do
    echo "Cleaning up Docker images on $HOST"
    cleanup_docker_images "$HOST"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to clean up Docker images on $HOST."
    fi
done

sudo podman ps -q | xargs -r sudo podman stop; sudo podman ps -q | xargs -r sudo podman rm -f; sudo yes | podman system prune -fa; podman ps; podman images
