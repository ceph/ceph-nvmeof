#!/bin/bash
#
# qos_fio_run
#
# Reproduces a field issue: a single gateway, initiators connected with only
# one IO queue and a very short nvme_core io_timeout, then a "bully" (4k qd32)
# and a "victim" (16k qd16) fio job hammering every connected device at once.
#
# Flow:
#   1. verify exactly one gateway is running and serving
#   2. create a subsystem with two auto-visible namespaces and set QOS limits
#   3. nvme disconnect-all, set io_timeout=5, connect every subsystem with
#      --nr-io-queues=1
#   4. sequential-write every namespace so RBD objects exist (a fresh thin
#      image completes reads locally and never reaches Ceph)
#   5. clear dmesg, launch 2 bully and 2 victim fio processes on this VM
#   6. soak, then check nvme list / fio liveness / dmesg
#   7. killall -9 fio
#
# The test FAILS if dmesg grows any error/fail/timeout/block/disconnect line
# after the connections were established.
#
# Start the environment with a single gateway before running this:
#     ./tests/ha/start_up.sh 1 && ./tests/ha/wait_gateways.sh 1
#
# Must run on a host with nvme-cli, docker, fio, and the nvme-tcp module loadable.
#
set -e

if [ -z "${QOS_TS_WRAPPED:-}" ]; then
    export QOS_TS_WRAPPED=1
    set +e              # never abort the wrapper, always report the real status
    set -o pipefail
    bash "$0" "$@" 2>&1 | while IFS= read -r line; do
        printf '%(%b%d %H:%M:%S)T - %s\n' -1 "$line"
    done
    exit "${PIPESTATUS[0]}"
fi

NQN="nqn.2016-06.io.spdk:cnode1"
DISCOVERY_NQN="nqn.2014-08.org.nvmexpress.discovery"
POOL="${RBD_POOL:-rbd}"
IMAGE1="qos_image1"
IMAGE2="qos_image2"
IMAGE_SIZE="${IMAGE_SIZE:-200MB}"
TRSVCID="${NVMEOF_IO_PORT:-4420}"

# QOS limit for this runner. Bandwidth limits are left unset.
QOS_RW_IOPS="${QOS_RW_IOPS:-150}"
IO_TIMEOUT="${IO_TIMEOUT:-5}"
NR_IO_QUEUES="${NR_IO_QUEUES:-1}"
FIO_RUNTIME="${FIO_RUNTIME:-1800}"            # 30 minutes of IO
FIO_PER_KIND="${FIO_PER_KIND:-2}"
FIO_SOAK="${FIO_SOAK:-$((FIO_RUNTIME - 60))}"
DMESG_FAIL_PATTERN="${DMESG_FAIL_PATTERN:-error|fail|timeout|block|disconnect}"
DMESG_IGNORE_PATTERN="${DMESG_IGNORE_PATTERN:-veth|br-[0-9a-f]+: port|entered (blocking|forwarding|disabled|listening) state|promiscuous mode}"

ORIG_IO_TIMEOUT=$(cat /sys/module/nvme_core/parameters/io_timeout 2>/dev/null || echo 30)
DMESG_DUMPED=""

printf -v _bar '%130s' ''
BANNER=${_bar// /=}
SECBAR=${_bar// /#}
unset _bar

#
# Echo a command, then run it.
#
run() {
    echo "$BANNER"
    echo "$*"
    echo "$BANNER"
    "$@"
}

section() {
    echo "$SECBAR"
    echo "### $*"
    echo "$SECBAR"
}

#
# List NVMe namespace block devices, skipping the per-controller
# /dev/nvmeXcYnZ aliases that native multipath creates.
#
list_nvme_blockdevs() {
    local d
    for d in /dev/nvme*n*; do
        [ -b "$d" ] || continue
        case "$d" in
            *c[0-9]*n[0-9]*) continue ;;
        esac
        printf '%s\n' "$d"
    done | sort
}

#
# Names of the running nvmeof gateway containers (excludes cli / discovery).
#
running_gateways() {
    docker ps --format '{{.ID}}\t{{.Names}}' |
        awk '$2 ~ /nvmeof/ && $2 !~ /cli/ && $2 !~ /discovery/ {print $1}'
}

#
# Always stop fio, disconnect, restore io_timeout and dump dmesg.
#
cleanup() {
    local rc=$?
    set +e

    section "Cleanup"
    run sudo killall -9 fio
    run sudo nvme disconnect-all
    echo "$BANNER"
    echo "echo $ORIG_IO_TIMEOUT > /sys/module/nvme_core/parameters/io_timeout"
    echo "$BANNER"
    echo "$ORIG_IO_TIMEOUT" | sudo tee /sys/module/nvme_core/parameters/io_timeout

    if [ -z "$DMESG_DUMPED" ]; then
        section "dmesg -T (kernel log since the connections were established)"
        echo "$BANNER"
        echo "sudo dmesg -T"
        echo "$BANNER"
        sudo dmesg -T
    fi

    if [ $rc -eq 0 ]; then
        echo "✅ qos_fio_run passed"
    else
        echo "❌ qos_fio_run failed with exit code $rc"
    fi
    exit $rc
}
trap cleanup EXIT

section "Discovering the gateway"
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | grep -v discovery |
           awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
echo "ℹ️  gateway: container $GW1_NAME ip $GW1_IP"
run docker ps --format '{{.ID}}\t{{.Names}}\t{{.Status}}'

CLI1=(docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500)
CLI1_JSON=(docker compose run --rm nvmeof-cli --server-address "$GW1_IP" --server-port 5500
           --output stdio --format json)

#
# This test deliberately runs against a single gateway. The caller is expected
# to have started exactly one (tests/ha/start_up.sh 1); verify that before
# creating anything, so a miscounted environment fails here rather than
# halfway through the IO run.
#
section "Verifying exactly one gateway is running"

count=$(running_gateways | wc -l)
if [ "$count" -ne 1 ]; then
    echo "❌ expected exactly 1 running nvmeof gateway, found $count"
    echo "   this test must be started with: ./tests/ha/start_up.sh 1"
    run docker ps --format '{{.ID}}\t{{.Names}}\t{{.Status}}'
    exit 1
fi

state=$(docker inspect -f '{{.State.Status}}' "$GW1_NAME")
if [ "$state" != "running" ]; then
    echo "❌ gateway $GW1_NAME is in state '$state', expected 'running'"
    exit 1
fi

# A container that is up is not the same as a gateway that answers gRPC.
echo "ℹ️  waiting for the gateway to answer gRPC"
gw_ready=""
for _ in $(seq 30); do
    # each attempt also spends a second or two starting the cli container
    if "${CLI1[@]}" subsystem list > /dev/null 2>&1; then
        gw_ready=1
        break
    fi
    sleep 2
    printf .
done
echo
if [ -z "$gw_ready" ]; then
    echo "❌ gateway $GW1_NAME did not answer gRPC"
    run docker logs --tail 50 "$GW1_NAME"
    exit 1
fi
echo "✅ exactly one gateway ($GW1_NAME) is running and answering gRPC"

run "${CLI1[@]}" gateway info

#
# Setup. Both namespaces live in load balancing group 1, which the single
# gateway owns, so there is no ANA failover to wait on. Namespaces are
# auto-visible and the subsystem accepts any host, so a plain "nvme connect"
# (no --hostnqn) sees both.
#
section "Setup: subsystem, namespaces, listener and host"

run "${CLI1[@]}" subsystem add --subsystem "$NQN" --no-group-append

run "${CLI1[@]}" namespace add --subsystem "$NQN" --rbd-pool "$POOL" \
    --rbd-image "$IMAGE1" --size "$IMAGE_SIZE" --rbd-create-image \
    --load-balancing-group 1

run "${CLI1[@]}" namespace add --subsystem "$NQN" --rbd-pool "$POOL" \
    --rbd-image "$IMAGE2" --size "$IMAGE_SIZE" --rbd-create-image \
    --load-balancing-group 1

run "${CLI1[@]}" listener add --subsystem "$NQN" --host-name "$GW1_NAME" \
    --traddr "$GW1_IP" --trsvcid "$TRSVCID"

run "${CLI1[@]}" host add --subsystem "$NQN" --host-nqn "*"

#
# QOS - read/write IOPS only, applied to both namespaces.
#
section "Applying QOS limits (rw-ios/s=$QOS_RW_IOPS)"

for nsid in 1 2; do
    run "${CLI1[@]}" namespace set_qos --subsystem "$NQN" --nsid "$nsid" \
        --rw-ios-per-second "$QOS_RW_IOPS"
done

section "Gateway configuration"

run "${CLI1[@]}" gateway info
run "${CLI1[@]}" subsystem list
run "${CLI1[@]}" namespace list --subsystem "$NQN"
run "${CLI1[@]}" listener list --subsystem "$NQN"
run "${CLI1[@]}" host list --subsystem "$NQN"

#
# Initiator side.
#
section "Preparing the initiators"

run sudo modprobe nvme-tcp
run sudo nvme disconnect-all

echo "$BANNER"
echo "echo $IO_TIMEOUT > /sys/module/nvme_core/parameters/io_timeout"
echo "$BANNER"
echo "$IO_TIMEOUT" | sudo tee /sys/module/nvme_core/parameters/io_timeout
echo "ℹprevious io_timeout was $ORIG_IO_TIMEOUT"

section "Connecting every subsystem with --nr-io-queues=$NR_IO_QUEUES"

echo "$BANNER"
echo "${CLI1_JSON[*]} subsystem list | jq -r '.subsystems[].nqn'"
echo "$BANNER"
SUBSYSTEMS=$("${CLI1_JSON[@]}" subsystem list | jq -r '.subsystems[].nqn' |
             grep -v "^${DISCOVERY_NQN}$" || true)
if [ -z "$SUBSYSTEMS" ]; then
    echo "❌ no subsystems reported by the gateway"
    exit 1
fi
echo "subsystems to connect:"
echo "$SUBSYSTEMS"

for nqn in $SUBSYSTEMS; do
    run sudo nvme connect -t tcp -a "$GW1_IP" -s "$TRSVCID" -n "$nqn" \
        --nr-io-queues="$NR_IO_QUEUES"
done

echo "waiting for namespace block devices to appear"
for _ in $(seq 30); do
    mapfile -t DEVICES < <(list_nvme_blockdevs)
    [ "${#DEVICES[@]}" -eq 2 ] && break
    sleep 1
    printf .
done
echo

mapfile -t DEVICES < <(list_nvme_blockdevs)
if [ "${#DEVICES[@]}" -ne 2 ]; then
    echo "❌ expected 2 namespace block devices after connecting, found ${#DEVICES[@]}"
    run sudo nvme list-subsys
    exit 1
fi
echo "✅ connected devices: ${DEVICES[*]}"

run sudo nvme list
run sudo nvme list-subsys

ndev=${#DEVICES[@]}

#
# Fresh RBD images are thin: unwritten extents are completed with zeroes
# inside librbd/SPDK and never become OSD reads. Sequential-write each
# namespace through the NVMe devices first (non-zero pattern, whole device)
# so the read jobs below hit real RBD objects and show up in
# "rbd perf image iostat".
#
section "Filling each namespace so later reads reach the RBDs"

idx=0
for dev in "${DEVICES[@]}"; do
    idx=$((idx + 1))
    echo "$BANNER"
    echo "sudo fio --name=fill${idx} --filename=${dev} --ioengine=libaio --direct=1 --rw=write --bs=1M --iodepth=16 --size=100% --buffer_pattern=0x55"
    echo "$BANNER"
    if ! sudo fio --name="fill${idx}" --filename="$dev" --ioengine=libaio --direct=1 \
        --rw=write --bs=1M --iodepth=16 --size=100% --buffer_pattern=0x55; then
        echo "❌ sequential fill failed; later reads would not reach the RBD objects"
        exit 1
    fi
done
echo "✅ filled ${#DEVICES[@]} namespace(s); RBD objects now exist for read IO"

#
# Clear the kernel ring buffer so the post-run scan only sees messages
# produced while fio was running. There is a single kernel on this host, so
# one clear covers both initiator connections.
#
section "Clearing dmesg now that all connections are established"
run sudo dmesg -C
run sudo dmesg -T

#
# Bullies (4k qd32) and victims (16k qd16) are fio processes on this VM.
# Jobs are spread round-robin across the connected devices.
#
section "Launching fio: ${FIO_PER_KIND} bullies + ${FIO_PER_KIND} victims across ${#DEVICES[@]} device(s)"

start_fio_jobs() {
    local kind="$1" bs="$2" iodepth="$3"
    local idx dev
    for idx in $(seq 1 "$FIO_PER_KIND"); do
        dev="${DEVICES[$(( (idx - 1) % ndev ))]}"
        echo "$BANNER"
        echo "nohup sudo fio --thread --name=${kind}${idx} --filename=${dev} --ioengine=libaio --direct=1 --rw=read --bs=${bs} --iodepth=${iodepth} --numjobs=1 --time_based --runtime=${FIO_RUNTIME} >/dev/null 2>&1 &"
        echo "$BANNER"
        # --thread keeps this job inside one process. Without it fio forks a
        # second process with the same command line.
        nohup sudo fio --thread --name="${kind}${idx}" --filename="$dev" --ioengine=libaio --direct=1 \
            --rw=read --bs="$bs" --iodepth="$iodepth" --numjobs=1 \
            --time_based --runtime="$FIO_RUNTIME" >/dev/null 2>&1 &
    done
}

sudo killall -9 fio >/dev/null 2>&1 || true
start_fio_jobs bully 4k 32
start_fio_jobs victim 16k 16

EXPECTED_FIO=$((FIO_PER_KIND * 2))
echo "ℹ️  waiting for exactly $EXPECTED_FIO fio processes"
ready=""
for _ in $(seq 12); do
    fio_now=$(pgrep -c -x fio || true)
    if [ "${fio_now:-0}" -eq "$EXPECTED_FIO" ]; then
        ready=1
        break
    fi
    if [ "${fio_now:-0}" -gt "$EXPECTED_FIO" ]; then
        echo "❌ $fio_now fio processes started; expected exactly $EXPECTED_FIO"
        pgrep -ax fio || true
        exit 1
    fi
    sleep 5
done
if [ -z "$ready" ]; then
    echo "❌ fio did not start"
    pgrep -ax fio || true
    exit 1
fi
echo "ℹ️  exactly $EXPECTED_FIO fio processes are running (runtime ${FIO_RUNTIME}s)"
echo "ℹ️  soaking for ${FIO_SOAK}s before checking"

soaked=0
while [ "$soaked" -lt "$FIO_SOAK" ]; do
    sleep 30
    soaked=$((soaked + 30))
    echo "ℹ️  soaking... ${soaked}/${FIO_SOAK}s elapsed"
done

#
# Post-soak checks, run while fio is still going.
#
section "Post-soak checks"

run sudo nvme list
echo "$BANNER"
echo "nvme list | wc -l"
echo "$BANNER"
sudo nvme list | wc -l

echo "$BANNER"
echo "pgrep -ax fio"
echo "$BANNER"
pgrep -ax fio || true

echo "$BANNER"
echo "pgrep -c -x fio"
echo "$BANNER"
pgrep -c -x fio || true

# Count only processes named fio. The four jobs must still be the only ones.
fio_now=$(pgrep -c -x fio || true)
if [ "${fio_now:-0}" -ne "$EXPECTED_FIO" ]; then
    echo "❌ ${fio_now:-0} fio processes; expected exactly $EXPECTED_FIO"
    FIO_DEAD=1
else
    echo "ℹ️  exactly $fio_now fio processes still running"
    FIO_DEAD=0
fi

section "dmesg -T (kernel log since the connections were established)"
echo "$BANNER"
echo "sudo dmesg -T"
echo "$BANNER"
sudo dmesg -T
DMESG_DUMPED=1

#
# Verdict.
#
section "Verdict"

FAILURES=$(sudo dmesg -T |
           grep -iE "$DMESG_FAIL_PATTERN" |
           grep -viE "$DMESG_IGNORE_PATTERN" || true)

if [ -n "$FAILURES" ]; then
    echo "❌ dmesg reported failures after the connections were established:"
    echo "$BANNER"
    echo "$FAILURES"
    echo "$BANNER"
    echo "   (matched /$DMESG_FAIL_PATTERN/i, ignoring /$DMESG_IGNORE_PATTERN/i)"
    exit 1
fi
echo "✅ dmesg is clean - no error/fail/timeout/block/disconnect lines"

if [ "$FIO_DEAD" -ne 0 ]; then
    echo "❌ expected exactly $EXPECTED_FIO fio processes when the soak ended"
    exit 1
fi
echo "✅ exactly $EXPECTED_FIO fio processes are still running"
