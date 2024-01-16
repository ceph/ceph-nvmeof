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
import logging
import threading
import inspect
import spdk.rpc as rpc

from prometheus_client.core import REGISTRY, GaugeMetricFamily, CounterMetricFamily, InfoMetricFamily
from prometheus_client import start_http_server, GC_COLLECTOR
from typing import NamedTuple
from functools import wraps

COLLECTION_ELAPSED_WARNING = 0.8   # Percentage of the refresh interval before a warning message is issued
REGISTRY.unregister(GC_COLLECTOR)  # Turn off garbage collector metrics

logger = logging.getLogger(__name__)


class RBD(NamedTuple):
    pool: str
    namespace: str
    image: str


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


def start_httpd(**kwargs):
    """Start the prometheus http endpoint, catching any exception"""
    try:
        start_http_server(**kwargs)
    except Exception:
        logger.error("Failed to start the prometheus http server", exc_info=True)
        return False
    return True


def start_exporter(spdk_rpc_client, config):
    """Start the prometheus exporter and register the NVMeOF custom collector"""

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
        REGISTRY.register(NVMeOFCollector(spdk_rpc_client, config))


class NVMeOFCollector:
    """Provide a prometheus endpoint for nvmeof gateway statistics"""

    def __init__(self, spdk_rpc_client, config):
        self.spdk_rpc_client = spdk_rpc_client
        self.metric_prefix = "ceph_nvmeof"
        self.gw_config = config
        _bdev_pools = config.get_with_default('gateway', 'prometheus_bdev_pools', '')
        self.bdev_pools = _bdev_pools.split(',') if _bdev_pools else []
        self.interval = config.getint_with_default('gateway', 'prometheus_stats_inteval', 10)
        self.lock = threading.Lock()

        self.spdk_version = {}
        self.bdev_info = []
        self.bdev_io_stats = {}
        self.spdk_thread_stats = {}
        self.subsystems = []
        self.method_timings = {}

        if self.bdev_pools:
            logger.info(f"Stats restricted to bdevs in the following pool(s): {','.join(self.bdev_pools)}")
        else:
            logger.info("Stats for all bdevs will be provided")

        self.metrics_cache = []

        # age the last obs time, so the first scrape will return values
        self.last_obs = time.time() - self.interval

    @timer
    def _get_version(self):
        return rpc.spdk_get_version(self.spdk_rpc_client)

    @timer
    def _get_bdev_info(self):
        return rpc.bdev.bdev_get_bdevs(self.spdk_rpc_client)

    @timer
    def _get_bdev_io_stats(self):
        return rpc.bdev.bdev_get_iostat(self.spdk_rpc_client)

    @timer
    def _get_spdk_thread_stats(self):
        return rpc.app.thread_get_stats(self.spdk_rpc_client)

    @timer
    def _get_subsystems(self):
        return rpc.nvmf.nvmf_get_subsystems(self.spdk_rpc_client)

    def _get_rpc_data(self):
        """Gather data from the SPDK"""
        self.spdk_version = self._get_version()
        self.bdev_info = self._get_bdev_info()
        self.bdev_io_stats = self._get_bdev_io_stats()
        self.spdk_thread_stats = self._get_spdk_thread_stats()
        self.subsystems = self._get_subsystems()

    @ttl
    def collect(self):
        """Generator function returning SPDK data in Prometheus exposition format

        This method is called when the client receives a scrape request from the
        Prometheus Server.
        """
        bdev_lookup = {}

        logger.debug("Collecting stats from the SPDK")
        self._get_rpc_data()

        elapsed = sum(self.method_timings.values())
        if elapsed > self.interval:
            logger.error(f"Stats refresh time > interval time of {self.interval} secs")
        elif elapsed > self.interval * COLLECTION_ELAPSED_WARNING:
            logger.warning(f"Stats refresh of {elapsed:.2f}s is close to exceeding the interval {self.interval}s")
        else:
            logger.debug(f"Stats refresh completed in {elapsed:.3f} secs.")

        spdk = InfoMetricFamily(
            f"{self.metric_prefix}_spdk_info",
            "SPDK Version information",
            value={'version': self.spdk_version.get("version", "Unknown")})
        yield spdk

        bdev_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_bdev_metadata",
            "BDEV Metadata",
            labels=["bdev_name", "pool_name", "namespace", "rbd_name"])
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

            bdev_lookup[bdev_name] = RBD(rbd_pool, rbd_namespace, rbd_image)
            bdev_metadata.add_metric([bdev_name, rbd_pool, rbd_namespace, rbd_image], 1)
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
                logger.debug(f"i/o stats for bdev {bdev_name} skipped. Either not an rbd bdev, or excluded by 'prometheus_bdev_pools'")
                continue

            bdev_read_ops.add_metric([bdev_name], bdev.get("num_read_ops"))
            bdev_write_ops.add_metric([bdev_name], bdev.get("num_write_ops"))
            bdev_read_bytes.add_metric([bdev_name], bdev.get("bytes_read"))
            bdev_write_bytes.add_metric([bdev_name], bdev.get("bytes_written"))

            bdev_read_seconds.add_metric([bdev_name], (bdev.get("read_latency_ticks") / tick_rate))
            bdev_write_seconds.add_metric([bdev_name], (bdev.get("write_latency_ticks") / tick_rate))

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
            reactor_utilization.add_metric([spdk_thread.get("name"), "busy"], (spdk_thread.get("busy") / tick_rate))
            reactor_utilization.add_metric([spdk_thread.get("name"), "idle"], (spdk_thread.get("idle") / tick_rate))

        yield reactor_utilization

        subsystem_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_metadata",
            "Metadata describing the subsystem configuration",
            labels=["nqn", "serial_number", "model_number", "allow_any_host"])
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
        subsystem_namespace_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_subsystem_namespace_metadata",
            "Namespace information for the subsystem",
            labels=["nqn", "nsid", "bdev_name", "name"])

        for subsys in self.subsystems:
            nqn = subsys.get("nqn", "")
            if not nqn or "discovery" in nqn:
                continue
            subsys_is_open = "yes" if subsys.get("allow_any_host") else "no"
            subsystem_metadata.add_metric([nqn, subsys.get("serial_number"), subsys.get("model_number"), subsys_is_open], 1)
            subsystem_listeners.add_metric([nqn], len(subsys.get("listen_addresses", [])))
            subsystem_host_count.add_metric([nqn], len(subsys.get("hosts", [])))
            subsystem_namespace_limit.add_metric([nqn], subsys.get("max_namespaces"))
            for ns in subsys.get("namespaces", []):
                subsystem_namespace_metadata.add_metric([nqn, str(ns.get("nsid")), ns.get("bdev_name"), ns.get("name")], 1)

        yield subsystem_metadata
        yield subsystem_listeners
        yield subsystem_host_count
        yield subsystem_namespace_limit
        yield subsystem_namespace_metadata

        method_runtimes = GaugeMetricFamily(
            f"{self.metric_prefix}_rpc_method_seconds",
            "Run times of the RPC method calls",
            labels=["method"])
        for name, value in self.method_timings.items():
            method_runtimes.add_metric([name], value)
        yield method_runtimes
