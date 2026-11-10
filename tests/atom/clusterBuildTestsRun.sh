#!/bin/bash

# if a command fails (returns a non-zero exit code), terminate immediately
# the exit code will be the same as the exit code of the failed command.
# see https://github.com/ceph/ceph-nvmeof/actions/runs/11928539421/job/33246031083
set -e
set -o pipefail


VERSION=$1
CEPH_BRANCH=$2
if [ "$3" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/$CEPH_BRANCH/latest/centos/9/ | jq -r "[.[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1] | unique | .[0] // empty")
else
    CEPH_SHA=$3
fi
ACTION_URL=$4
BUSY_FILE='/home/cephnvme/busyServer.txt'
ATOM_IMAGE_FILE='/home/cephnvme/atom_image.txt'

if [ ! -r "$ATOM_IMAGE_FILE" ]; then
    echo "ERROR: $ATOM_IMAGE_FILE is missing or not readable on this runner."
    echo "       It must hold the atom test framework image, for example:"
    echo "       echo 'quay.io/<namespace>/nvmeof_atom:latest' > $ATOM_IMAGE_FILE"
    exit 1
fi
ATOM_IMAGE=$(tr -d '[:space:]' < "$ATOM_IMAGE_FILE")
if [ -z "$ATOM_IMAGE" ]; then
    echo "ERROR: $ATOM_IMAGE_FILE is empty, it must hold an atom container image reference."
    exit 1
fi

echo "CEPH_SHA found is: $CEPH_SHA"
echo "ATOM_IMAGE is: $ATOM_IMAGE"


# Choose the Ceph image based on the CEPH_SHA format:
# - release tag ("20.2.2" or "v20.2.1") -> official quay.io/ceph/ceph, tag always prefixed with "v"
# - otherwise ("latest"-resolved or raw commit SHA) -> ceph-ci build image
if echo "$CEPH_SHA" | grep -qE '^[vV]?[0-9]+\.[0-9]+\.[0-9]+$'; then
    CEPH_IMG="quay.io/ceph/ceph:v$(echo "$CEPH_SHA" | sed 's/^[vV]//')"
else
    CEPH_IMG="quay.ceph.io/ceph-ci/ceph:$CEPH_SHA"
fi
echo "CEPH_IMG to be used is: $CEPH_IMG"


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
check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7/*
sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7

for i in 1 2 3; do
    echo "Attempt $i to pull $ATOM_IMAGE..."
    sudo docker pull "$ATOM_IMAGE" && break
    if [ "$i" = 3 ]; then
        echo "ERROR: failed to pull $ATOM_IMAGE after 3 attempts"
        exit 1
    fi
    echo "Pull failed. Retrying in $((i * 10))s..."
    sleep $((i * 10))
done

ATOM_IMAGE_DIGEST=$(sudo docker image inspect --format '{{if .RepoDigests}}{{index .RepoDigests 0}}{{end}}' "$ATOM_IMAGE" 2>/dev/null || true)
echo "Atom image in use: $ATOM_IMAGE (digest: ${ATOM_IMAGE_DIGEST:-unknown})"

set -x
# Create a temporary file to capture output and exit status
TEMP_OUTPUT="/tmp/docker_output_$$"
sudo docker run \
    -v /etc/localtime:/etc/localtime:ro \
    -v /root/.ssh:/root/.ssh \
    -e PYTHONUNBUFFERED=1 \
    "$ATOM_IMAGE" \
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
    --skip-controller-get-io-statistics-test \
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
