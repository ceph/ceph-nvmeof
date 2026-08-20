#!/bin/bash

# if a command fails (returns a non-zero exit code), terminate immediately
# the exit code will be the same as the exit code of the failed command.
# see https://github.com/ceph/ceph-nvmeof/actions/runs/11928539421/job/33246031083
set -e
# pipefail ensures that a pipeline returns the exit status of the first failing command
# rather than the last command (e.g., tee), so docker failures are properly captured
set -o pipefail


VERSION=$1
CEPH_BRANCH=$2
# CEPH_SHA supports:
# - "latest"           - newest ready Shaman build for CEPH_BRANCH is resolved to a commit SHA
# - anything else      - passed through as-is (a release number like "20.2.2" or a raw commit SHA)
if [ "$3" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/$CEPH_BRANCH/latest/centos/9/ | jq -r "[.[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1] | unique | .[0] // empty")
else
    CEPH_SHA=$3
fi
ATOM_SHA=$4
ACTION_URL=$5

echo "CEPH_SHA found is: $CEPH_SHA"

# Choose the Ceph image based on the CEPH_SHA format:
# - release tag ("20.2.2" or "v20.2.1") -> official quay.io/ceph/ceph, tag always prefixed with "v"
# - otherwise ("latest"-resolved or raw commit SHA) -> ceph-ci build image
if echo "$CEPH_SHA" | grep -qE '^[vV]?[0-9]+\.[0-9]+\.[0-9]+$'; then
    CEPH_IMG="quay.io/ceph/ceph:v$(echo "$CEPH_SHA" | sed 's/^[vV]//')"
else
    CEPH_IMG="quay.ceph.io/ceph-ci/ceph:$CEPH_SHA"
fi
echo "CEPH_IMG to be used is: $CEPH_IMG"

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
# Acquire the cluster lock before any setup work (clone/build) so the whole
# run is guarded and a failure during setup does not leave the server without
# a busy marker.
check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
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
# Create a temporary file to capture output and exit status
TEMP_OUTPUT="/tmp/docker_output_$$"
sudo docker run \
    -v /root/.ssh:/root/.ssh \
    nvmeof_atom:"$ATOM_SHA" \
    bash -c "python3 atom_main.py \
    --project=nvmeof \
    --ceph-img="$CEPH_IMG" \
    --ceph-branch="$CEPH_BRANCH" \
    --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
    --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
    --container-runtime docker \
    --initiators=1 \
    --gw-group-num=1 \
    --gw-num=2 \
    --gw-to-stop-num=1 \
    --subsystem-num=2 \
    --ns-num=4 \
    --subsystem-max-ns-num=2048 \
    --failover-num=2 \
    --failover-num-after-upgrade=2 \
    --rbd-size=200M \
    --seed=0 \
    --vhosts=4 \
    --fio-devices-num=1 \
    --lb-timeout=20 \
    --config-dbg-mon=10 \
    --config-dbg-ms=0 \
    --nvmeof-daemon-stop \
    --nvmeof-systemctl-stop \
    --mon-client-kill \
    --nvmeof-daemon-remove \
    --redeploy-gws \
    --github-action-deployment \
    --mtls \
    --journalctl-to-console \
    --dont-power-off-cloud-vms \
    --skip-lb-group-change-test \
    --skip-gw-failover-latency-test \
    --skip-get-subsystems-latency-test \
    --skip-reservations-basic-test \
    --skip-cross-namespace-copy-test \
    --ibm-cloud-key=nokey \
    --github-nvmeof-token=nokey \
    --check-vms-stage \
    --ceph-deploy-stage \
    --nvmeof-setup-stage \
    --initiator-setup-stage \
    --nvmeof-tests-stage \
    --teardown-stage \
    --env=m7; exit \$?" 2>&1 | tee "$TEMP_OUTPUT"
DOCKER_EXIT_STATUS=$?

# Read the output from the temporary file
DOCKER_OUTPUT=$(cat "$TEMP_OUTPUT")
rm -f "$TEMP_OUTPUT"

set +x

# Check for test failures even if Docker exit status is 0
if [ $DOCKER_EXIT_STATUS -eq 0 ]; then
    echo "Atom docker run completed successfully"
    # Additional check: look for pytest failure indicators in the captured output
    # Check if any test failed based on common pytest failure patterns
    echo "DEBUG: Checking for test failure patterns..."
    if echo "$DOCKER_OUTPUT" | grep -E "(failed.*passed|FAILED.*test|_pytest\.outcomes\.Exit.*failure)" > /dev/null; then
        echo "Tests failed despite successful Docker run - forcing failure"
        echo "DEBUG: Found failure patterns in output"
        exit 1
    else
        echo "DEBUG: No failure patterns found in output"
        echo "DEBUG: Docker exit status was 0, considering this a success"
    fi
else
    echo "Atom docker run failed with exit code: $DOCKER_EXIT_STATUS"
    exit $DOCKER_EXIT_STATUS
fi
