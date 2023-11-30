#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: pcuzner@ibm.com
#

import os
import logging
import spdk.rpc as rpc

from prometheus_client.core import REGISTRY, GaugeMetricFamily, CounterMetricFamily
from prometheus_client import start_http_server


def start_exporter(spdk_rpc_client, port):
    """Start the prometheus exporter and register the NVMeOF collector"""
    start_http_server(port)
    REGISTRY.register(NVMeOFCollector(spdk_rpc_client))


class NVMeOFCollector:
    """Provide a prometheus endpoint for nvmeof gateway statistics"""

    def __init__(self, spdk_rpc_client):
        self.logger = logging.getLogger(__name__)
        self.spdk_rpc_client = spdk_rpc_client
        self.metric_prefix = "ceph_nvmeof"

    def collect(self):
        self.logger.debug("Processing prometheus scrape request")

        spdk_version = rpc.spdk_get_version(self.spdk_rpc_client)
        spdk = GaugeMetricFamily(
            f"{self.metric_prefix}_spdk_metadata",
            "SPDK Version information", 
            labels=["version"])
        spdk.add_metric([spdk_version.get("version", "Unknown")],1)
        yield spdk

        bdev_info = rpc.bdev.bdev_get_bdevs(self.spdk_rpc_client)
        bdev_metadata = GaugeMetricFamily(
            f"{self.metric_prefix}_bdev_metadata", 
            "BDEV Metadata", 
            labels=["bdev_name","pool_name","rbd_name"])
        bdev_capacity = GaugeMetricFamily(
            f"{self.metric_prefix}_bdev_capacity_bytes", 
            "BDEV Capacity", 
            labels=["bdev_name"])

        for bdev in bdev_info:
            bdev_size = bdev.get("block_size") * bdev.get("num_blocks")
            bdev_capacity.add_metric([bdev.get("name")], bdev_size)
            rbd_info = {}
            try:
                rbd_info = bdev["driver_specific"]["rbd"]
            except KeyError:
                print("no rbd information present?")
            else:
                bdev_metadata.add_metric([bdev.get("name"), rbd_info.get("pool_name"), rbd_info.get("rbd_name")], 1)
        
        yield bdev_capacity
        yield bdev_metadata
     
        bdev_io_stats = rpc.bdev.bdev_get_iostat(self.spdk_rpc_client)

        tick_rate = bdev_io_stats.get("tick_rate")

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

        for bdev in bdev_io_stats.get("bdevs", []):
            bdev_read_ops.add_metric([bdev.get("name")], bdev.get("num_read_ops", 0))
            bdev_write_ops.add_metric([bdev.get("name")], bdev.get("num_write_ops", 0))
            bdev_read_bytes.add_metric([bdev.get("name")], bdev.get("bytes_read", 0))
            bdev_write_bytes.add_metric([bdev.get("name")], bdev.get("bytes_written", 0))

            bdev_read_seconds.add_metric([bdev.get("name")], (bdev.get("read_latency_ticks") / tick_rate))
            bdev_write_seconds.add_metric([bdev.get("name")], (bdev.get("write_latency_ticks") / tick_rate))

        yield bdev_read_ops
        yield bdev_write_ops
        yield bdev_read_bytes
        yield bdev_write_bytes
        yield bdev_read_seconds
        yield bdev_write_seconds

        thread_stats = rpc.app.thread_get_stats(self.spdk_rpc_client)
        reactor_utilization = CounterMetricFamily(
            f"{self.metric_prefix}_reactor_seconds_total", 
            "time reactor thread active with I/O", 
            labels=["name", "mode"])
        
        for spdk_thread in thread_stats.get("threads", []):
            if "poll" not in spdk_thread["name"]:
                continue
            reactor_utilization.add_metric([spdk_thread.get("name"), "busy"], (spdk_thread.get("busy") / tick_rate))
            reactor_utilization.add_metric([spdk_thread.get("name"), "idle"], (spdk_thread.get("idle") / tick_rate))
       
        yield reactor_utilization 
             
        subsystems = rpc.nvmf.nvmf_get_subsystems(self.spdk_rpc_client)
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

        for subsys in subsystems:
            nqn = subsys.get("nqn", "")
            if not nqn or "discovery" in nqn:
                continue
            subsys_is_open = "yes" if subsys.get("allow_any_host") else "no"
            subsystem_metadata.add_metric([nqn, subsys.get("serial_number"), subsys.get("model_number"), subsys_is_open], 1)
            subsystem_listeners.add_metric([nqn], len(subsys.get("listen_addresses", [])))
            subsystem_host_count.add_metric([nqn], len(subsys.get("hosts", [])))
            subsystem_namespace_limit.add_metric([nqn], subsys.get("max_namespaces"))
            for ns in subsys.get("namespaces", []):
                subsystem_namespace_metadata.add_metric([nqn, str(ns.get("nsid")), ns.get("bdev_name"), ns.get("name")],1)

        yield subsystem_metadata
        yield subsystem_listeners
        yield subsystem_host_count
        yield subsystem_namespace_limit
        yield subsystem_namespace_metadata
