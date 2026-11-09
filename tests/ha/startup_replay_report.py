#!/usr/bin/env python3
"""Report one gateway startup from a `docker logs -t` capture.

Python lines are timed with the docker UTC prefix. SPDK lines are timed with
the microsecond prefix SPDK embeds, shifted by the container timezone, because
SPDK stderr is block-buffered and the docker prefix on those lines can be late.
"""

import argparse
import datetime
import re
import sys

DOCKER_TS = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?"
    r"(?:Z|[+-]\d{2}:\d{2}))\s+(?P<msg>.*)$"
)
SPDK_TS = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]"
)
NS_RE = re.compile(r"Received request to add namespace (\d+) to ")
CREATE_BDEV_RE = re.compile(
    r"Received request to create (?:read write|read only) bdev (\S+) from"
)
ALLOC_RE = re.compile(r"Allocated cluster name='([^']+)'")
ADD_BDEV_RE = re.compile(r"Received request to add (bdev_\S+) to ")
LOCK_RETRY_RE = re.compile(r"will try again in ([0-9]*\.?[0-9]+) seconds")
LOCK_OK_RE = re.compile(
    r"Succeeded to lock OMAP file \(([^)]+)\) after (\d+) retries"
)
PY_PID_RE = re.compile(
    r"^\[[0-9]{2}-[A-Za-z]{3}-[0-9]{4} [0-9:]{8}\] [A-Z]+ \S+:\d+ \((\d+)\): "
)
TZ_RE = re.compile(r"^[+-]\d{4}$")
GATEWAY_MARKER = "(gateway-"

NS_KINDS = {
    "ns",
    "create_bdev",
    "allocated",
    "spdk_register",
    "spdk_xxx",
    "spdk_add",
    "bdev_debug",
    "add_bdev",
    "subsystem_debug",
    "image_id",
}


def parse_rfc3339(text):
    text = text.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    match = re.match(
        r"(?P<head>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
        r"(?P<frac>\.\d+)?"
        r"(?P<off>[+-]\d{2}:\d{2})$",
        text,
    )
    if not match:
        raise ValueError(f"not an RFC3339 timestamp: {text}")
    digits = (match.group("frac") or ".0")[1:7].ljust(6, "0")
    off = match.group("off")
    sign = 1 if off[0] == "+" else -1
    tz = datetime.timezone(
        sign * datetime.timedelta(hours=int(off[1:3]), minutes=int(off[4:6]))
    )
    stamp = datetime.datetime.strptime(match.group("head"), "%Y-%m-%dT%H:%M:%S")
    stamp = stamp.replace(microsecond=int(digits), tzinfo=tz)
    return stamp.astimezone(datetime.timezone.utc)


def parse_spdk_time(text, tz_offset):
    stamp = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f")
    sign = 1 if tz_offset[0] == "+" else -1
    delta = sign * datetime.timedelta(
        hours=int(tz_offset[1:3]), minutes=int(tz_offset[3:5])
    )
    return stamp.replace(tzinfo=datetime.timezone(delta)).astimezone(
        datetime.timezone.utc
    )


def classify(msg):
    match = NS_RE.search(msg)
    if match:
        return "ns", int(match.group(1))
    match = CREATE_BDEV_RE.search(msg)
    if match:
        return "create_bdev", match.group(1)
    match = ALLOC_RE.search(msg)
    if match:
        return "allocated", match.group(1)
    match = ADD_BDEV_RE.search(msg)
    if match:
        return "add_bdev", match.group(1)
    if "subsystem_add_ns:" in msg:
        return "subsystem_debug", None
    if "rbd disk to lun" in msg:
        return "spdk_add", None
    if "XXX bdev_rbd_create(" in msg:
        return "spdk_xxx", None
    # Python debug is "bdev_rbd_create: bdev_<uuid>, cluster_name ...".
    # SPDK's own prefix also contains "bdev_rbd_create:".
    if re.search(r"bdev_rbd_create: \S+, cluster_name ", msg):
        return "bdev_debug", None
    if "rbd_register_cluster " in msg:
        return "spdk_register", None
    if "already included" in msg or "set image id " in msg:
        return "image_id", None
    if "Starting serve, monitor client version" in msg:
        return "starting_serve", None
    if "SPDK process id:" in msg:
        return "spdk_pid", None
    if "Started SPDK with version" in msg:
        return "started_spdk", None
    # Discovery emits the same three lines with "(discovery-...)".
    if "Start update from " in msg and GATEWAY_MARKER in msg:
        return "start_update", None
    if "Update complete (" in msg and GATEWAY_MARKER in msg:
        return "update_complete", None
    if "Initialization is over" in msg and GATEWAY_MARKER in msg:
        return "init_over", None
    match = LOCK_RETRY_RE.search(msg)
    if match:
        return "lock_retry", float(match.group(1))
    match = LOCK_OK_RE.search(msg)
    if match:
        return "lock_ok", (match.group(1), int(match.group(2)))
    if "Locked OMAP file before reading" in msg:
        return "lock_hold_start", None
    if "Released OMAP file lock after reading" in msg:
        return "lock_hold_end", None
    # Confirmed exclusive expiry says "has expired". "might have expired"
    # is also logged for the shared lock, so match the exclusive word.
    if ("the exclusive lock has expired" in msg
            or "the exclusive lock might have expired" in msg):
        return "exclusive_expiry", None
    if "differs from OMAP file version" in msg:
        return "version_mismatch", None
    return None, None


def load_events(path, tz_offset):
    events = []
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = DOCKER_TS.match(line.rstrip("\n"))
            if not match:
                continue
            msg = match.group("msg")
            kind, data = classify(msg)
            if kind is None:
                continue
            spdk = SPDK_TS.match(msg)
            if spdk:
                when = parse_spdk_time(spdk.group("ts"), tz_offset)
            else:
                when = parse_rfc3339(match.group("ts"))
            pid_match = PY_PID_RE.match(msg)
            pid = int(pid_match.group(1)) if pid_match else None
            events.append({"ts": when, "kind": kind, "data": data, "pid": pid})
    events.sort(key=lambda item: item["ts"])
    return events


def first(events, kind, before=None, after=None):
    found = None
    for event in events:
        if event["kind"] != kind:
            continue
        if after is not None and event["ts"] < after:
            continue
        if before is not None and event["ts"] > before:
            continue
        found = event
        if before is None:
            return found
    return found


def seconds(start, end):
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return ordered[low] * (1.0 - frac) + ordered[high] * frac


def fmt_s(value):
    if value is None:
        return "n/a"
    return f"{value:12.6f} s"


def fmt_stats(values):
    if not values:
        return "n=0"
    return (
        f"n={len(values):<4} "
        f"min={fmt_s(min(values))} "
        f"p50={fmt_s(percentile(values, 50))} "
        f"p95={fmt_s(percentile(values, 95))} "
        f"max={fmt_s(max(values))} "
        f"sum={fmt_s(sum(values))}"
    )


def kind_time(events, kind):
    event = first(events, kind)
    if event is None:
        return None
    return event["ts"]


def between(events, start_kind, end_kind):
    return seconds(kind_time(events, start_kind), kind_time(events, end_kind))


def build_namespaces(events, start_update, update_complete):
    begins = [
        event for event in events
        if event["kind"] == "ns"
        and start_update <= event["ts"] <= update_complete
    ]
    namespaces = []
    for index, begin in enumerate(begins):
        if index + 1 < len(begins):
            boundary = begins[index + 1]["ts"]
            last = False
        else:
            boundary = update_complete
            last = True
        window = [
            event for event in events
            if begin["ts"] <= event["ts"] < boundary and event["kind"] in NS_KINDS
        ]
        allocated = first(window, "allocated")
        namespaces.append({
            "nsid": begin["data"],
            "last": last,
            "new_cluster": allocated is not None,
            "cluster": allocated["data"] if allocated is not None else None,
            "events": window,
            "gap": seconds(begin["ts"], boundary),
        })
    return namespaces


def slices_for(ns):
    events = ns["events"]
    open_start = "allocated" if ns["new_cluster"] else "create_bdev"
    if kind_time(events, "spdk_xxx") is not None:
        open_end = "spdk_xxx"
        register = between(events, "spdk_xxx", "spdk_add")
    else:
        # This SPDK build does not emit "XXX bdev_rbd_create(". The next
        # timestamped line is "Add rbd disk to lun", after bdev_register.
        open_end = "spdk_add"
        register = None
    return {
        "checks": between(events, "ns", "create_bdev"),
        "rados_connect": between(events, "create_bdev", "spdk_register")
        if ns["new_cluster"] else None,
        "cluster_rpc_return": between(events, "spdk_register", "allocated")
        if ns["new_cluster"] else None,
        "rbd_open_watch_stat": between(events, open_start, open_end),
        "bdev_register": register,
        "bdev_rpc_return": between(events, "spdk_add", "bdev_debug"),
        "subsystem_add_ns": between(events, "add_bdev", "subsystem_debug"),
        "image_id_metadata": between(events, "subsystem_debug", "image_id"),
    }


def cell(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_summary(path, row):
    header = (
        "scale\tevent\tgateway\tinit_over_s\treplay_s\tlock_wait_s\t"
        "lock_hold_s\tns_count\tnew_n\tnew_p50_s\treused_n\treused_p50_s\t"
        "open_p50_s\tconnect_p50_s\tport_5500_s\tport_bounded\t"
        "slowest_nsid\tslowest_s\n"
    )
    try:
        empty = open(path, "r", encoding="utf-8").read() == ""
    except FileNotFoundError:
        empty = True
    with open(path, "a", encoding="utf-8") as handle:
        if empty:
            handle.write(header)
        handle.write("\t".join(cell(value) for value in row) + "\n")


def emit(label, value):
    print(f"  {label:<40} {value}")


def report(args):
    if not TZ_RE.match(args.tz_offset):
        print(f"error: timezone offset must look like +0000, got {args.tz_offset}",
              file=sys.stderr)
        return 1

    started = parse_rfc3339(args.started_at)
    events = load_events(args.log_file, args.tz_offset)
    init_over = first(events, "init_over")
    start_update = None
    update_complete = None
    if init_over is not None:
        start_update = first(events, "start_update", before=init_over["ts"])
        if start_update is not None:
            update_complete = first(
                events, "update_complete",
                after=start_update["ts"], before=init_over["ts"],
            )
    if start_update is None:
        start_update = first(events, "start_update")
    if update_complete is None and start_update is not None:
        update_complete = first(events, "update_complete", after=start_update["ts"])

    start_ts = start_update["ts"] if start_update else None
    complete_ts = update_complete["ts"] if update_complete else None
    init_ts = init_over["ts"] if init_over else None

    serve = first(events, "starting_serve")
    gateway_pid = serve.get("pid") if serve is not None else None
    lock_wait = 0.0 if gateway_pid is not None else None
    lock_retries = 0 if gateway_pid is not None else None
    hold_start = None
    hold_end = None
    expiry = 0
    version_mismatch = 0
    limit = complete_ts or init_ts
    for event in events:
        if limit is not None and event["ts"] > limit:
            continue
        gateway_lock = gateway_pid is not None and event.get("pid") == gateway_pid
        if gateway_lock and event["kind"] == "lock_retry" and (
            start_ts is None or event["ts"] < start_ts
        ):
            lock_wait += event["data"]
        elif gateway_lock and event["kind"] == "lock_ok" and (
            start_ts is None or event["ts"] < start_ts
        ):
            lock_retries = event["data"][1]
        elif gateway_lock and event["kind"] == "lock_hold_start" and (
            start_ts is None or event["ts"] <= start_ts
        ):
            hold_start = event["ts"]
            hold_end = None
        elif gateway_lock and event["kind"] == "lock_hold_end" and hold_start is not None:
            if start_ts is None or event["ts"] <= start_ts:
                hold_end = event["ts"]
        elif event["kind"] == "exclusive_expiry" and (
            gateway_pid is None or event.get("pid") == gateway_pid
        ):
            expiry += 1
        elif event["kind"] == "version_mismatch":
            version_mismatch += 1
    lock_hold = seconds(hold_start, hold_end)

    namespaces = []
    if start_ts is not None and complete_ts is not None:
        namespaces = build_namespaces(events, start_ts, complete_ts)
    for ns in namespaces:
        ns["slices"] = slices_for(ns)

    new_gaps = [ns["gap"] for ns in namespaces if ns["new_cluster"] and ns["gap"] is not None]
    reused_gaps = [
        ns["gap"] for ns in namespaces if not ns["new_cluster"] and ns["gap"] is not None
    ]
    all_gaps = [ns["gap"] for ns in namespaces if ns["gap"] is not None]

    def collect(name, pred):
        found = []
        for ns in namespaces:
            if not pred(ns):
                continue
            value = ns["slices"].get(name)
            if value is not None:
                found.append(value)
        return found

    open_all = collect("rbd_open_watch_stat", lambda ns: True)
    open_new = collect("rbd_open_watch_stat", lambda ns: ns["new_cluster"])
    open_reused = collect("rbd_open_watch_stat", lambda ns: not ns["new_cluster"])
    connect = collect("rados_connect", lambda ns: ns["new_cluster"])

    port_at = None
    port_bounded = False
    if args.port_open_at:
        port_at = parse_rfc3339(args.port_open_at)
        port_bounded = args.port_open_kind == "late"

    before_ns = None
    after_ns = None
    if namespaces and start_ts is not None and namespaces[0]["events"]:
        before_ns = seconds(start_ts, namespaces[0]["events"][0]["ts"])
    if namespaces and complete_ts is not None:
        last_image = first(namespaces[-1]["events"], "image_id")
        if last_image is not None:
            after_ns = seconds(last_image["ts"], complete_ts)

    replay = seconds(start_ts, complete_ts)
    init_over_s = seconds(started, init_ts)
    port_s = seconds(started, port_at)

    print(
        f"startup_replay scale={args.scale} event={args.event} "
        f"gateway={args.gateway} tz={args.tz_offset} "
        f"cluster_connections={args.cluster_connections}"
    )
    print("  clocks: python lines use the docker timestamp; "
          "SPDK lines use the embedded microsecond prefix")
    print("  note: rados_connect includes get_image_size and pool checks; "
          "reused rbd_open_watch_stat includes them too; "
          "a new cluster's rbd_open_watch_stat starts after Allocated cluster")
    if any(kind_time(ns["events"], "spdk_xxx") is None
           and ns["slices"].get("rbd_open_watch_stat") is not None
           for ns in namespaces):
        print("  note: rbd_open_watch_stat ends at \"Add rbd disk to lun\" "
              "and includes bdev_register; this log has no "
              "XXX bdev_rbd_create line")
    print("  note: the last namespace gap runs until Update complete and "
          "includes listener replay")

    emit(f"{args.event}_to_init_over", fmt_s(init_over_s))
    emit(f"{args.event}_to_starting_serve",
         fmt_s(seconds(started, kind_time(events, "starting_serve"))))
    emit("starting_serve_to_spdk_pid",
         fmt_s(between(events, "starting_serve", "spdk_pid")))
    emit("spdk_pid_to_started_spdk",
         fmt_s(between(events, "spdk_pid", "started_spdk")))
    port_text = fmt_s(port_s)
    if port_bounded and port_s is not None:
        port_text = f"<= {fmt_s(port_s).strip()}  (open before probe)"
    emit(f"{args.event}_to_port_5500", port_text)
    emit(f"{args.event}_to_start_update", fmt_s(seconds(started, start_ts)))
    retries = "n/a" if lock_retries is None else str(lock_retries)
    emit("omap_shared_lock_wait", f"{fmt_s(lock_wait)}  retries={retries}")
    emit("omap_shared_lock_hold", fmt_s(lock_hold))
    emit("start_update_to_update_complete", fmt_s(replay))
    emit("update_complete_to_init_over", fmt_s(seconds(complete_ts, init_ts)))
    emit("before_namespaces", fmt_s(before_ns))
    emit("after_last_namespace", fmt_s(after_ns))
    emit("namespace_gaps all", fmt_stats(all_gaps))
    emit("namespace_gaps new_cluster", fmt_stats(new_gaps))
    emit("namespace_gaps reused_cluster", fmt_stats(reused_gaps))
    emit("checks", fmt_stats(collect("checks", lambda ns: True)))
    emit("rados_connect", fmt_stats(connect))
    emit("cluster_rpc_return", fmt_stats(collect("cluster_rpc_return", lambda ns: True)))
    emit("rbd_open_watch_stat all", fmt_stats(open_all))
    emit("rbd_open_watch_stat new_cluster", fmt_stats(open_new))
    emit("rbd_open_watch_stat reused_cluster", fmt_stats(open_reused))
    emit("bdev_register", fmt_stats(collect("bdev_register", lambda ns: True)))
    emit("bdev_rpc_return", fmt_stats(collect("bdev_rpc_return", lambda ns: True)))
    emit("subsystem_add_ns", fmt_stats(collect("subsystem_add_ns", lambda ns: True)))
    emit("image_id_metadata", fmt_stats(collect("image_id_metadata", lambda ns: True)))
    emit("omap_exclusive_expiry", str(expiry))
    emit("omap_version_reload", str(version_mismatch))
    emit("ns_count", str(len(namespaces)))

    slowest = None
    for ns in namespaces:
        if ns["gap"] is None:
            continue
        if slowest is None or ns["gap"] > slowest["gap"]:
            slowest = ns
    slowest_id = None
    slowest_s = None
    if slowest is not None:
        slowest_id = slowest["nsid"]
        slowest_s = slowest["gap"]
        kind = "new" if slowest["new_cluster"] else "reused"
        tail = " last" if slowest["last"] else ""
        print(
            f"  slowest nsid={slowest['nsid']} cluster={kind}{tail} "
            f"total={fmt_s(slowest['gap'])}"
        )
        for name in (
            "checks",
            "rados_connect",
            "cluster_rpc_return",
            "rbd_open_watch_stat",
            "bdev_register",
            "bdev_rpc_return",
            "subsystem_add_ns",
            "image_id_metadata",
        ):
            value = slowest["slices"].get(name)
            if value is None and name in (
                "rados_connect", "cluster_rpc_return", "bdev_register"
            ):
                continue
            print(f"    {name:<30} {fmt_s(value)}")

    failed = False
    if init_over is None:
        print("error: Initialization is over was not logged", file=sys.stderr)
        failed = True
    if start_update is None or update_complete is None:
        print("error: Start update / Update complete pair was not logged",
              file=sys.stderr)
        failed = True
    ids = [ns["nsid"] for ns in namespaces]
    expected_ids = list(range(1, args.namespaces + 1))
    if sorted(ids) != expected_ids:
        print(
            f"error: replayed namespaces {ids} != 1..{args.namespaces}",
            file=sys.stderr,
        )
        failed = True

    write_summary(args.summary, [
        args.scale,
        args.event,
        args.gateway,
        init_over_s,
        replay,
        lock_wait,
        lock_hold,
        len(namespaces),
        len(new_gaps),
        percentile(new_gaps, 50),
        len(reused_gaps),
        percentile(reused_gaps, 50),
        percentile(open_all, 50),
        percentile(connect, 50),
        port_s,
        "yes" if port_bounded else "no",
        "" if slowest_id is None else slowest_id,
        slowest_s,
    ])
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--scale", type=int, required=True)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--port-open-at", default="")
    parser.add_argument("--port-open-kind", default="", choices=["", "ok", "late"])
    parser.add_argument("--tz-offset", required=True)
    parser.add_argument("--namespaces", type=int, required=True)
    parser.add_argument("--cluster-connections", default="32")
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args()
    return report(args)


if __name__ == "__main__":
    sys.exit(main())
