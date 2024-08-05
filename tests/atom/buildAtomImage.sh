#!/bin/bash

ATOM_SHA=$1

echo "_2_ATOM_SHA : $ATOM_SHA"

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

# Switch to given SHA
cd /home/cephnvme/actions-runner-ceph/ceph-nvmeof-atom
git checkout $ATOM_SHA
if [ $? -ne 0 ]; then
    echo "Error: Failed to checkout the specified SHA."
    exit 1
fi

# Build atom images based on the cloned repo
docker build -t nvmeof_atom:$ATOM_SHA /home/cephnvme/actions-runner-ceph/ceph-nvmeof-atom
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
