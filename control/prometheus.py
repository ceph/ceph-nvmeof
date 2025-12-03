#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: pcuzner@ibm.com
#

import os
import time
import threading
import inspect
import types
import math
import spdk.rpc as rpc

from .proto import gateway_pb2 as pb2
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client.core import CounterMetricFamily, InfoMetricFamily
from prometheus_client import start_http_server, GC_COLLECTOR
from functools import wraps
from .utils import NICS

COLLECTION_ELAPSED_WARNING = 0.8   # Percentage of the refresh interval before a warning is issued
REGISTRY.unregister(GC_COLLECTOR)  # Turn off garbage collector metrics

logger = None


def ttl(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        assert inspect.isgeneratorfunction(method)
        with self.lock:
            now = time.time()
            if now - self.last_obs >= self.interval:
                self.last_obs = now
                self.metrics_cache.clear()
                for metric in method(self, *args, **kwargs):
                    self.metrics_cache.append(metric)
                    yield metric
            else:
                logger.debug('Returning content from cache')
                for metric in self.metrics_cache:
                    yield metric
    return wrapped


def timer(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        st = time.time()
        result = method(self, *args, **kwargs)
        elapsed = time.time() - st
        if hasattr(self, 'method_timings'):
            self.method_timings[method.__name__] = elapsed
        return result
    return call


def connection_cache_with_timer(ttl_seconds=60):
    def decorator(method):
        @wraps(method)
        def call(self, *args, **kwargs):
            st = time.time()
            cache_age = st - self.connection_map_cache_time

            # Check if cache is still valid
            if self.connection_map_cache is not None and cache_age < ttl_seconds:
                # Cache hit
                result = self.connection_map_cache
                logger.debug(
                    f'Connection cache hit (age: {cache_age:.1f}s of{ttl_seconds}s TTL)')
            else:
                # Cache miss - do the expensive work
                logger.debug(f'Connection cache miss (age: {cache_age:.1f}s), refreshing...')

                result = method(*args, **kwargs)

                # Update cache
                self.connection_map_cache = result
                self.connection_map_cache_time = st

                logger.debug(f'Connection cache refreshed (valid for {ttl_seconds}s)')

            # Record timing
            elapsed = time.time() - st
            if hasattr(self, 'method_timings'):
                self.method_timings[method.__name__] = elapsed

            return result
        return call
    return decorator


def start_httpd(**kwargs):
    """Start the prometheus http endpoint, catching any exception"""
    try:
        start_http_server(**kwargs)
    except Exception:
        logger.error("Failed to start the prometheus http server", exc_info=True)
        return False
    return True


def start_exporter(spdk_rpc_client, config, gateway_rpc, logger_to_use):
    """Start the prometheus exporter and register the NVMeOF custom collector"""

    global logger
    logger = logger_to_use
    port = config.getint_with_default("gateway", "prometheus_port", 10008)
    ssl = config.getboolean_with_default("gateway", "prometheus_exporter_ssl", True)
    mode = 'https' if ssl else 'http'

    if ssl:
        cert_filepath = config.get('mtls', 'server_cert')
        key_filepath = config.get('mtls', 'server_key')

        if os.path.exists(cert_filepath) and os.path.exists(key_filepath):
            httpd_ok = start_httpd(port=port, certfile=cert_filepath, keyfile=key_filepath)
        else:
            httpd_ok = False
            logger.error("Unable to start prometheus exporter - missing cert/key file(s)")
    else:
        # SSL mode explicitly disabled by config option
        httpd_ok = start_httpd(port=port)

    if httpd_ok:
        logger.info(f"Prometheus exporter running in {mode} mode, listening on port {port}")
        REGISTRY.register(NVMeOFCollector(spdk_rpc_client, config, gateway_rpc))


class NVMeOFCollector:
    """Provide a prometheus endpoint for nvmeof gateway statistics"""

    def __init__(self, spdk_rpc_client, config, gateway_rpc):
        self.spdk_rpc_client = spdk_rpc_client
        self.gateway_rpc = gateway_rpc
        self.metric_prefix = "ceph_nvmeof"
        self.gw_config = config
        _bdev_pools = config.get_with_default('gateway', 'prometheus_bdev_pools', '')
        self.bdev_pools = _bdev_pools.split(',') if _bdev_pools else []
        self.interval = config.getint_with_default('gateway',
                                                   'prometheus_stats_interval',
                                                   10)
        self.original_interval = self.interval
        self.slow_cycles_count = 0
        self.fast_cycles_count = 0
        self.slow_down_factor = config.getfloat_with_default(
            'gateway',
            'prometheus_frequency_slow_down_factor',
            3.0)
        self.cycles_to_adjust_speed = config.getint_with_default(
            'gateway',
            'prometheus_cycles_to_adjust_speed',
            3)

        self.lock = threading.Lock()
        self.hostname = os.getenv('NODE_NAME') or os.getenv('HOSTNAME')

        # gw metadata is static, so fetch the data only at startup
        self.gw_metadata, self.group = self._get_gw_metadata()  # proto.gateway_pb2.gateway_info

        self.bdev_info = []
        self.bdev_io_stats = {}
        self.spdk_thread_stats = {}
        self.subsystems = []
        self.connections = {}
        self.method_timings = {}

        # Cache for connection map
        self.connection_map_cache = None           # Store cached connection data
        self.connection_map_cache_time = 0           # Last time the connection cache was refreshed
        cache_ttl = self.gw_config.getint_with_default(
            "gateway", "prometheus_connection_list_cache_expiration", 60)
        decorated_method = connection_cache_with_timer(cache_ttl)(self._get_connection_map)
        self._get_connection_map = types.MethodType(decorated_method, self)

        if self.bdev_pools:
            logger.info(f"Stats restricted to bdevs in the following pool(s): "
                        f"{','.join(self.bdev_pools)}")
        else:
            logger.info("Stats for all bdevs will be provided")

        self.metrics_cache = []

        # age the last obs time, so the first scrape will return values
        self.last_obs = time.time() - self.interval

    def _get_gw_metadata(self):
        """Fetch Gateway metadata"""
        ver = os.getenv("NVMEOF_VERSION")
        req = pb2.get_gateway_info_req(cli_version=ver)
        metadata = self.gateway_rpc.get_gateway_info(req)

        # Since empty values result in a missing label, when the group name is not
        # defined _null_ is used to ensure any promql queries will always work against
        # a valid label.
        metadata_group = None
        try:
            metadata_group = metadata.group
        except AttributeError:
            logger.exception("metadata.group is not defined")
            metadata_group = None

        if not metadata_group:
            metadata_group = self.gateway_rpc.gateway_group

        if not metadata_group:
            metadata_group = "_null_"

        return (metadata, metadata_group)

    @timer
    def _get_bdev_info(self):
        try:
            return rpc.bdev.bdev_get_bdevs(self.spdk_rpc_client)
        except Exception:
            logger.exception("Error trying to call bdev_get_bdevs()")
            return []

    @timer
    def _get_bdev_io_stats(self):
        try:
            return rpc.bdev.bdev_get_iostat(self.spdk_rpc_client)
        except Exception:
            logger.exception("Error trying to call bdev_get_iostat()")
            return {}

    @timer
    def _get_spdk_thread_stats(self):
        try:
            return rpc.app.thread_get_stats(self.spdk_rpc_client)
        except Exception:
            logger.exception("Error trying to call thread_get_stats()")
            return {}

    @timer
    def _get_subsystems(self):
        """Fetch aggregated subsystem information"""
        subsystems_info = self.gateway_rpc.get_subsystems(pb2.get_subsystems_req(), None)
        return subsystems_info.subsystems

    def _get_connection_map(self, subsystem_list):
        """Fetch connection information for all defined subsystems"""
        connection_map = {}
        for subsys in subsystem_list:
            resp = self.gateway_rpc.list_connections(pb2.list_connections_req(
                subsystem=subsys.nqn, clear_alerts=True))
            if resp.status != 0:
                logger.error(f"Exporter failed to fetch connection info for "
                             f"{subsys.nqn}: {resp.error_message}")
                continue
            connection_map[subsys.nqn] = resp
        return connection_map

    def _get_data(self):
        """Gather data from the SPDK"""
        self.bdev_info = self._get_bdev_info()
        logger.debug("Done with _get_bdev_info()")
        self.bdev_io_stats = self._get_bdev_io_stats()
        logger.debug("Done with _get_bdev_io_stats()")
        self.spdk_thread_stats = self._get_spdk_thread_stats()
        logger.debug("Done with _get_spdk_thread_stats()")
        self.subsystems = self._get_subsystems()
        logger.debug("Done with _get_subsystems()")
        self.connections = self._get_connection_map(self.subsystems)
        logger.debug("Done with _get_connection_map()")

    def _log_timings(self):
        """Log timing for each method"""
        t = self.method_timings
        logger.debug(f"_get_bdev_info(): {t.get('_get_bdev_info', 0):.2f}s")
        logger.debug(f"_get_bdev_io_stats(): {t.get('_get_bdev_io_stats', 0):.2f}s")
        logger.debug(f"_get_spdk_thread_stats(): {t.get('_get_spdk_thread_stats', 0):.2f}s")
        logger.debug(f"_get_subsystems(): {t.get('_get_subsystems', 0):.2f}s")
        logger.debug(f"_get_connection_map(): {t.get('_get_connection_map', 0):.2f}s")

    @ttl
    def collect(self):
        """Generator function returning SPDK data in Prometheus exposition format

        This method is called when the client receives a scrape request from the
        Prometheus Server.
        """
        bdev_lookup = set()

        logger.debug(f"Collecting stats from the SPDK. Interval is {self.interval} seconds")
        self._get_data()
        self._log_timings()
        elapsed = sum(self.method_timings.values())
        if elapsed > self.interval:
            logger.error(f"Stats refresh time {elapsed:.3f} > interval time of "
                         f"{self.interval} seconds")
        elif elapsed > self.interval * COLLECTION_ELAPSED_WARNING:
            logger.warning(f"Stats refresh of {elapsed:.2f}s is close to exceeding "
                           f"the interval {self.interval} seconds")
        else:
            logger.debug(f"Stats refresh completed in {elapsed:.3f} seconds. "
                         f"Interval is {self.interval} seconds")

        if self.slow_down_factor > 0:
            factor_elapsed = int(math.ceil(elapsed * self.slow_down_factor))
            if factor_elapsed > self.interval:
                self.slow_cycles_count += 1
                self.fast_cycles_count = 0
                if self.slow_cycles_count >= self.cycles_to_adjust_speed:
                    logger.debug(f"Will increase interval after {self.slow_cycles_count} "
                                 f"slow cycles. Old interval: {self.interval}, "
                                 f"elapsed: {elapsed:.3f}, factored: {factor_elapsed} "
                                 f"({elapsed * self.slow_down_factor:.3f})")
                    self.interval = factor_elapsed
                    self.slow_cycles_count = 0
                    logger.warning(f"Stats refresh time, {elapsed:.3f} seconds, takes too much "
                                   f"of the interval. "
                                   f"Increase interval to {self.interval} seconds.")
            elif factor_elapsed < self.interval - 5:
                self.slow_cycles_count = 0
                self.fast_cycles_count += 1
                if self.fast_cycles_count >= self.cycles_to_adjust_speed:
                    if self.interval > self.original_interval:
                        logger.debug(f"Will decrease interval after {self.fast_cycles_count} "
                                     f"fast cycles. Old interval: {self.interval}, "
                                     f"elapsed: {elapsed:.3f}, factored: {factor_elapsed} "
                                     f"({elapsed * self.slow_down_factor:.3f})")
                        self.fast_cycles_count = 0
                        self.interval = max(factor_elapsed, self.original_interval)
                        logger.info(f"Stats refresh time, {elapsed:.3f} seconds, is down. "
                                    f"Decrease interval to {self.interval} seconds.")
            else:
                self.slow_cycles_count = 0
                self.fast_cycles_count = 0

        gateway_info = InfoMetricFamily(
            f"{self.metric_prefix}_gateway",
            "Gateway information",
            value={
                'spdk_version': self.gw_metadata.spdk_version,
                'version': self.gw_metadata.version,
                'addr': self.gw_metadata.addr,
                'port': self.gw_metadata.port,
                'name': self.gw_metadata.name,
                'hostname': self.hostname,
                'group': self.group,
                'load_balancing_group': str(self.gw_metadata.load_balancing_group),
            })
        yield gateway_info

        bdev_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_bdev_metadata",
            "BDEV Metadata",
            labels=["bdev_name", "pool_name", "namespace", "rbd_name", "block_size"])
        bdev_capacity = GaugeMetricFamily(
            f"{self.metric_prefix}_bdev_capacity_bytes",
            "BDEV Capacity",
            labels=["bdev_name"])

        for bdev in self.bdev_info:
            bdev_name = bdev.get('name')
            try:
                rbd_info = bdev["driver_specific"]["rbd"]
            except KeyError:
                logger.debug(f"no rbd information present for bdev {bdev.get('name')}, skipping")
                continue

            rbd_pool = rbd_info.get('pool_name')
            rbd_namespace = rbd_info.get('namespace', '')  # namespace is not currently present
            rbd_image = rbd_info.get('rbd_name')
            if self.bdev_pools:
                if rbd_pool not in self.bdev_pools:
                    continue

            bdev_lookup.add(bdev_name)
            bdev_metadata.add_metric([
                bdev_name,
                rbd_pool,
                rbd_namespace,
                rbd_image,
                str(bdev.get('block_size'))], 1)
            bdev_size = bdev.get("block_size") * bdev.get("num_blocks")
            bdev_capacity.add_metric([bdev.get("name")], bdev_size)

        yield bdev_capacity
        yield bdev_metadata

        tick_rate = self.bdev_io_stats.get("tick_rate")

        bdev_read_ops = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_reads_completed_total",
            "Total number of read operations completed",
            labels=["bdev_name"])
        bdev_write_ops = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_writes_completed_total",
            "Total number of write operations completed",
            labels=["bdev_name"])
        bdev_read_bytes = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_read_bytes_total",
            "Total number of bytes read successfully",
            labels=["bdev_name"])
        bdev_write_bytes = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_written_bytes_total",
            "Total number of bytes written successfully",
            labels=["bdev_name"])
        bdev_read_seconds = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_read_seconds_total",
            "Total time spent servicing READ I/O",
            labels=["bdev_name"])
        bdev_write_seconds = CounterMetricFamily(
            f"{self.metric_prefix}_bdev_write_seconds_total",
            "Total time spent servicing WRITE I/O",
            labels=["bdev_name"])

        for bdev in self.bdev_io_stats.get("bdevs", []):
            bdev_name = bdev.get('name')
            if bdev_name not in bdev_lookup:
                logger.debug(f"i/o stats for bdev {bdev_name} skipped. Either not an rbd bdev, "
                             f"or excluded by 'prometheus_bdev_pools'")
                continue

            bdev_read_ops.add_metric([bdev_name], bdev.get("num_read_ops"))
            bdev_write_ops.add_metric([bdev_name], bdev.get("num_write_ops"))
            bdev_read_bytes.add_metric([bdev_name], bdev.get("bytes_read"))
            bdev_write_bytes.add_metric([bdev_name], bdev.get("bytes_written"))

            if tick_rate:
                bdev_read_seconds.add_metric([bdev_name],
                                             (bdev.get("read_latency_ticks") / tick_rate))
                bdev_write_seconds.add_metric([bdev_name],
                                              (bdev.get("write_latency_ticks") / tick_rate))

        yield bdev_read_ops
        yield bdev_write_ops
        yield bdev_read_bytes
        yield bdev_write_bytes
        yield bdev_read_seconds
        yield bdev_write_seconds

        reactor_utilization = CounterMetricFamily(
            f"{self.metric_prefix}_reactor_seconds_total",
            "time reactor thread active with I/O",
            labels=["name", "mode"])

        for spdk_thread in self.spdk_thread_stats.get("threads", []):
            if "poll" not in spdk_thread["name"]:
                continue
            if tick_rate:
                reactor_utilization.add_metric([spdk_thread.get("name"), "busy"],
                                               (spdk_thread.get("busy") / tick_rate))
                reactor_utilization.add_metric([spdk_thread.get("name"), "idle"],
                                               (spdk_thread.get("idle") / tick_rate))

        yield reactor_utilization

        subsystem_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_metadata",
            "Metadata describing the subsystem configuration",
            labels=["nqn", "serial_number", "model_number", "allow_any_host",
                    "ha_enabled", "group"])
        subsystem_listeners = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_listener_count",
            "Number of listener addresses used by the subsystem",
            labels=["nqn"])
        subsystem_host_count = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_host_count",
            "Number of hosts defined to the subsystem",
            labels=["nqn"])
        subsystem_namespace_limit = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_namespace_limit",
            "Maximum namespaces supported",
            labels=["nqn"])
        subsystem_namespace_count = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_namespace_count",
            "Number of namespaces associated with the subsystem",
            labels=["nqn"])
        subsystem_namespace_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_namespace_metadata",
            "Namespace information for the subsystem",
            labels=["nqn", "nsid", "bdev_name", "anagrpid"])
        host_connection_state = GaugeMetricFamily(
            f"{self.metric_prefix}_host_connection_state",
            "Host connection state 0=disconnected, 1=connected",
            labels=["gw_name", "nqn", "host_nqn", "host_addr"])
        host_keep_alive_timeout = GaugeMetricFamily(
            f"{self.metric_prefix}_host_keepalive_timeout",
            "Host keepalive timeout 0=no, 1=yes",
            labels=["gw_name", "nqn", "host_nqn"])

        listener_map = {}

        for subsys in self.subsystems:
            nqn = subsys.nqn
            if not nqn or "discovery" in nqn:
                continue
            subsys_is_open = "yes" if subsys.allow_any_host else "no"

            # extract any listen addresses from the subsystem
            for listener in subsys.listen_addresses:
                if listener.traddr in listener_map:
                    listener_map[listener.traddr].append(nqn)
                else:
                    listener_map[listener.traddr] = [nqn]

            subsystem_metadata.add_metric([nqn, subsys.serial_number, subsys.model_number,
                                           subsys_is_open, "yes", self.group], 1)
            subsystem_listeners.add_metric([nqn], len(subsys.listen_addresses))
            subsystem_host_count.add_metric([nqn], len(subsys.hosts))
            subsystem_namespace_count.add_metric([nqn], len(subsys.namespaces))
            subsystem_namespace_limit.add_metric([nqn], subsys.max_namespaces)
            for ns in subsys.namespaces:
                subsystem_namespace_metadata.add_metric([
                    nqn,
                    str(ns.nsid),
                    ns.bdev_name,
                    str(ns.anagrpid)
                ], 1)

            conn_list = []
            try:
                conn_list = self.connections[nqn].connections
            except KeyError:
                logger.debug(f"couldn't find {nqn} in connection list, skipping")
            for conn in conn_list:
                host_connection_state.add_metric([
                    self.gw_metadata.name,
                    nqn,
                    conn.nqn,
                    f"{conn.traddr}:{conn.trsvcid}" if conn.connected else "<n/a>"
                ], 1 if conn.connected else 0)
                host_keep_alive_timeout.add_metric([
                    self.gw_metadata.name,
                    nqn,
                    conn.nqn
                ], 1 if conn.disconnected_due_to_keepalive_timeout else 0)

        yield subsystem_metadata
        yield subsystem_listeners
        yield subsystem_host_count
        yield subsystem_namespace_count
        yield subsystem_namespace_limit
        yield subsystem_namespace_metadata
        yield host_connection_state
        yield host_keep_alive_timeout

        subsystem_listener_iface_info = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_listener_iface_info",
            "Interface information",
            labels=["device", "operstate", "duplex", "mac_address"])
        subsystem_listener_iface_speed_bytes = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_listener_iface_speed_bytes",
            "Link speed of the Listener interface",
            labels=["device"])
        subsystem_listener_iface_nqn_info = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_listener_iface_nqn_info",
            "Subsystem usage of a NIC device",
            labels=["device", "nqn"])

        nics = NICS()
        for addr in listener_map.keys():
            device_name = nics.addresses.get(addr)
            if not device_name:
                # listener defined to an unbound address!
                continue
            try:
                nic = nics.adapters[device_name]
            except KeyError:
                continue
            subsystem_listener_iface_info.add_metric([
                device_name,
                nic.operstate,
                nic.duplex,
                nic.mac_address
            ], 1)
            subsystem_listener_iface_speed_bytes.add_metric([
                device_name
            ], nic.speed)

            for nqn in listener_map[addr]:
                subsystem_listener_iface_nqn_info.add_metric([
                    device_name,
                    nqn
                ], 1)

        yield subsystem_listener_iface_info
        yield subsystem_listener_iface_speed_bytes
        yield subsystem_listener_iface_nqn_info

        method_runtimes = GaugeMetricFamily(
            f"{self.metric_prefix}_rpc_method_seconds",
            "Run times of the RPC method calls",
            labels=["method"])
        for name, value in self.method_timings.items():
            method_runtimes.add_metric([name], value)
        yield method_runtimes
