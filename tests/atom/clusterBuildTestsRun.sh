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
if [ "$3" = "latest" ]; then
    CEPH_SHA=$(curl -s https://shaman.ceph.com/api/repos/ceph/$CEPH_BRANCH/latest/centos/9/ | jq -r ".[] | select(.archs[] == \"$(uname -m)\" and .status == \"ready\") | .sha1")
else
    CEPH_SHA=$3
fi
ATOM_SHA=$4
ACTION_URL=$5
NIGHTLY=$6

echo "CEPH_SHA found is: $CEPH_SHA"
RUNNER_FOLDER='/home/cephnvme/actions-runner-ceph-m7'
BUSY_FILE='/home/cephnvme/busyServer.txt'
RUNNER_NIGHTLY_FOLDER='/home/cephnvme/actions-runner-ceph-m8'
BUSY_NIGHTLY_FILE='/home/cephnvme/busyServerNightly.txt'

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
if [ "$NIGHTLY" != "nightly" ]; then
    rm -rf $RUNNER_FOLDER/ceph-nvmeof-atom
    sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7/*
    sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m7
    cd $RUNNER_FOLDER
else
    rm -rf $RUNNER_NIGHTLY_FOLDER/ceph-nvmeof-atom
    sudo rm -rf /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m8/*
    sudo ls -lta /root/.ssh/atom_backup/artifact/multiIBMCloudServers_m8
    cd $RUNNER_NIGHTLY_FOLDER
fi

# Cloning atom repo
git clone git@github.ibm.com:NVME-Over-Fiber/ceph-nvmeof-atom.git

# Switch to given SHA
cd ceph-nvmeof-atom
git checkout $ATOM_SHA

# Build atom images based on the cloned repo
sudo docker build -t nvmeof_atom:$ATOM_SHA .

set -x
if [ "$NIGHTLY" != "nightly" ]; then
    check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
    # Create a temporary file to capture output and exit status
    TEMP_OUTPUT="/tmp/docker_output_$$"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        bash -c "python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --ceph-branch="$CEPH_BRANCH" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
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
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --env=m7; exit \$?" 2>&1 | tee "$TEMP_OUTPUT"
    DOCKER_EXIT_STATUS=$?

    # Read the output from the temporary file
    DOCKER_OUTPUT=$(cat "$TEMP_OUTPUT")
    rm -f "$TEMP_OUTPUT"
else
    check_cluster_busy "$BUSY_NIGHTLY_FILE" "$ACTION_URL"
    # Create a temporary file to capture output and exit status
    TEMP_OUTPUT="/tmp/docker_output_$$"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        bash -c "python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --ceph-branch="$CEPH_BRANCH" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
        --initiators=1 \
        --gw-group-num=1 \
        --gw-num=8 \
        --gw-to-stop-num=1 \
        --subsystem-num=103 \
        --ns-num=8 \
        --subsystem-max-ns-num=2048 \
        --failover-num=10 \
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
        --github-action-deployment \
        --dont-power-off-cloud-vms \
        --dont-use-hugepages \
        --skip-lb-group-change-test \
        --skip-gw-failover-latency-test \
        --skip-block-list-test \
        --skip-multi-hosts-conn-test \
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --encryption-key \
        --env=m8; exit \$?" 2>&1 | tee "$TEMP_OUTPUT"
    DOCKER_EXIT_STATUS=$?

    # Read the output from the temporary file
    DOCKER_OUTPUT=$(cat "$TEMP_OUTPUT")
    rm -f "$TEMP_OUTPUT"
fi

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

# TODO- when https://github.com/ceph/ceph-nvmeof/issues/1369 will be fixed, we can uncomment the following lines

# echo "=== Checking logs for errors ==="
# if [ "$NIGHTLY" != "nightly" ]; then
#     ENV="m7"
# else
#     ENV="m8"
# fi
# LOGS_DIR="/home/cephnvme/artifact_${ENV}/multiIBMCloudServers_${ENV}/"
# echo "the current work directory is: $(pwd)"
# if [ ! -d "${LOGS_DIR}" ]; then
#     echo "❌ Logs directory not found: ${LOGS_DIR}"
#     exit 1
# fi
# ls -lta ${LOGS_DIR} || true

# # Check logs for errors
# traceback_found=false
# log_files=$(find "${LOGS_DIR}" -maxdepth 1 -name '*.log' -type f 2>/dev/null || true)

# if [ -n "${log_files}" ]; then
#     echo "Found log files: ${log_files} log files to check for errors:"
#     while IFS= read -r log_file; do
#         if [ -f "${log_file}" ]; then
#             # First check for Traceback (fatal)
#             if grep -q 'Traceback' "${log_file}" 2>/dev/null; then
#                 echo "❌ Traceback found in: ${log_file}"
#                 grep -B 5 -A 30 'Traceback' "${log_file}"
#                 traceback_found=true
#             else
#                 # If no Traceback, check for other error patterns (non-fatal)
#                 for pattern in 'ERROR' 'FATAL' 'Exception:'; do
#                     if grep -q "${pattern}" "${log_file}" 2>/dev/null; then
#                         echo "⚠️  File containing '${pattern}': ${log_file}"
#                         echo "=== ${pattern} in ${log_file} ==="
#                         grep -n -B 2 -A 2 "${pattern}" "${log_file}" 2>/dev/null | head -20 || true
#                         echo "=== END ${pattern} ==="
#                         echo ""
#                         break  # Only show first pattern found in this file
#                     fi
#                 done
#             fi
#         fi
#     done <<< "${log_files}"
# fi

# # Fail if tracebacks found
# if [ "${traceback_found}" = true ]; then
#     echo "❌ Traceback detected in logs — failing the script"
#     exit 1
# else
#     echo "✅ No critical errors found in logs"
# fi
# echo "✅ Log check completed successfully - No critical errors found."
