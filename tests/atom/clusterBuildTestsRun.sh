#!/bin/bash

# if a command fails (returns a non-zero exit code), terminate immediately
# the exit code will be the same as the exit code of the failed command.
# see https://github.com/ceph/ceph-nvmeof/actions/runs/11928539421/job/33246031083
set -e


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

#TODO: remove the line --skip-reservations-basic-test when https://github.com/ceph/ceph-nvmeof/pull/1260 is merged
set -x
if [ "$NIGHTLY" != "nightly" ]; then
    check_cluster_busy "$BUSY_FILE" "$ACTION_URL"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --ceph-branch="$CEPH_BRANCH" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
        --initiators=1 \
        --gw-group-num=1 \
        --gw-num=4 \
        --gw-to-stop-num=1 \
        --gw-scale-down-num=1 \
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
        --config-dbg-ms=1 \
        --nvmeof-daemon-stop \
        --nvmeof-systemctl-stop \
        --mon-leader-stop \
        --mon-client-kill \
        --nvmeof-daemon-remove \
        --redeploy-gws \
        --github-action-deployment \
        --mtls \
        --journalctl-to-console \
        --dont-power-off-cloud-vms \
        --skip-lb-group-change-test \
        --skip-gw-failover-latency-test \
        --skip-reservations-basic-test \
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --env=m7
    DOCKER_EXIT_STATUS=$?
else
    check_cluster_busy "$BUSY_NIGHTLY_FILE" "$ACTION_URL"
    sudo docker run \
        -v /root/.ssh:/root/.ssh \
        nvmeof_atom:"$ATOM_SHA" \
        python3 atom.py \
        --project=nvmeof \
        --ceph-img=quay.ceph.io/ceph-ci/ceph:"$CEPH_SHA" \
        --ceph-branch="$CEPH_BRANCH" \
        --gw-img=quay.io/ceph/nvmeof:"$VERSION" \
        --cli-img=quay.io/ceph/nvmeof-cli:"$VERSION" \
        --initiators=1 \
        --gw-group-num=1 \
        --gw-num=8 \
        --gw-to-stop-num=1 \
        --gw-scale-down-num=1 \
        --subsystem-num=100 \
        --ns-num=6 \
        --subsystem-max-ns-num=2048 \
        --failover-num=10 \
        --failover-num-after-upgrade=2 \
        --rbd-size=200M \
        --seed=0 \
        --vhosts=4 \
        --fio-devices-num=1 \
        --lb-timeout=20 \
        --config-dbg-mon=10 \
        --config-dbg-ms=1 \
        --nvmeof-daemon-stop \
        --nvmeof-systemctl-stop \
        --mon-leader-stop \
        --mon-client-kill \
        --nvmeof-daemon-remove \
        --github-action-deployment \
        --dont-power-off-cloud-vms \
        --dont-use-hugepages \
        --skip-lb-group-change-test \
        --skip-gw-failover-latency-test \
        --skip-reservations-basic-test \
        --skip-block-list-test \
        --skip-multi-hosts-conn-test \
        --ibm-cloud-key=nokey \
        --github-nvmeof-token=nokey \
        --env=m8
    DOCKER_EXIT_STATUS=$?
fi

set +x

if [ $DOCKER_EXIT_STATUS -eq 0 ]; then
    echo "Atom docker run succeeded"
else
    echo "Atom docker run failed!!!"
    exit 1
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