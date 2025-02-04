#
#  Copyright (c) 2025 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#

from abc import ABC, abstractmethod
from .config import GatewayConfig
from collections import defaultdict
from typing import Dict
import spdk.rpc.bdev as rpc_bdev


# Interface for cluster allocation strategy
class ClusterAllocationStrategy(ABC):
    @abstractmethod
    def get_cluster(self, anagrp: int) -> str:
        """Get cluster name, used by bdev allocation"""
        pass

    @abstractmethod
    def put_cluster(self, name: str) -> None:
        """Free cluster name, updates the reference count"""
        pass

    # Protected methods used by concrete classes
    def _init_common(self, config: GatewayConfig, gs) -> None:
        """Init common cluster context management variables"""
        self.config = config
        self.gs = gs
        self.librbd_core_mask = self.config.get_with_default(
            "spdk", "librbd_core_mask", None
        )
        self.rados_id = self.config.get_with_default("ceph", "id", "")
        if self.rados_id == "":
            self.rados_id = None

    def _alloc_cluster(self, name: str) -> str:
        """Allocates a new Rados cluster context with SPDK"""
        nonce = rpc_bdev.bdev_rbd_register_cluster(
            self.gs.spdk_rpc_client,
            name=name,
            user_id=self.rados_id,
            core_mask=self.librbd_core_mask,
        )
        self.gs.set_cluster_nonce(name, nonce)
        return name

    def _free_cluster(self, name: str) -> str:
        """Unregister SPDK Rados cluster context"""
        ret = rpc_bdev.bdev_rbd_unregister_cluster(self.gs.spdk_rpc_client, name=name)
        self.gs.logger.info(f"Free cluster {name=} {ret=}")
        assert ret


class AnaGrpBdevsPerCluster(ClusterAllocationStrategy):
    def __init__(self, config: GatewayConfig, gs) -> None:
        """Init cluster context management variables"""
        self._init_common(config, gs)
        self.clusters = defaultdict(dict)
        self.bdevs_per_cluster = self.config.getint("spdk", "bdevs_per_cluster")
        if self.bdevs_per_cluster < 1:
            raise Exception(
                f"invalid configuration: spdk.bdevs_per_cluster "
                f"{self.bdevs_per_cluster} < 1"
            )
        self.gs.logger.info(f"NVMeoF bdevs per cluster: {self.bdevs_per_cluster}")

    def get_cluster(self, anagrp: int) -> str:
        """Returns cluster name, enforcing bdev per cluster context"""
        cluster_name = None
        for name in self.clusters[anagrp]:
            if self.clusters[anagrp][name] < self.bdevs_per_cluster:
                cluster_name = name
                break

        if not cluster_name:
            cluster_name = self._alloc_cluster_name(anagrp)
            self._alloc_cluster(cluster_name)
            self.clusters[anagrp][cluster_name] = 1
        else:
            self.clusters[anagrp][cluster_name] += 1
        self.gs.logger.info(
            f"get_cluster {cluster_name=} number bdevs: "
            f"{self.clusters[anagrp][cluster_name]}"
        )
        return cluster_name

    def put_cluster(self, name: str) -> None:
        """Free cluster by name, update reference count"""
        for anagrp in self.clusters:
            if name in self.clusters[anagrp]:
                self.clusters[anagrp][name] -= 1
                assert self.clusters[anagrp][name] >= 0
                # free the cluster context if no longer used by any bdev
                if self.clusters[anagrp][name] == 0:
                    self._free_cluster(name)
                    self.clusters[anagrp].pop(name)
                else:
                    self.gs.logger.info(
                        f"put_cluster {name=} number bdevs: "
                        f"{self.clusters[anagrp][name]}"
                    )
                return

        assert (
            False
        ), f"Cluster {name} is not found"  # we should find the cluster in our state

    def _alloc_cluster_name(self, anagrp: int) -> str:
        """Allocates a new cluster name for ana group"""
        x = 0
        while True:
            name = f"cluster_context_{anagrp}_{x}"
            if name not in self.clusters[anagrp]:
                return name
            x += 1


class FlatBdevsPerCluster(ClusterAllocationStrategy):
    def __init__(self, config: GatewayConfig, gs) -> None:
        """Init cluster context management variables"""
        self._init_common(config, gs)
        self.flat_clusters: Dict[str, int] = {}
        self.flat_bdevs_per_cluster = self.config.getint(
            "spdk", "flat_bdevs_per_cluster"
        )
        if self.flat_bdevs_per_cluster < 1:
            raise Exception(
                f"invalid configuration: spdk.flat_bdevs_per_cluster "
                f"{self.flat_bdevs_per_cluster} < 1"
            )
        self.gs.logger.info(
            f"NVMeoF flat bdevs per cluster: {self.flat_bdevs_per_cluster}"
        )

    def get_cluster(self, anagrp: int) -> str:
        """Returns cluster name, bdev per cluster context while ignoring the ana group"""
        cluster_name = None
        for name in self.flat_clusters:
            if self.flat_clusters[name] < self.flat_bdevs_per_cluster:
                cluster_name = name
                break

        if not cluster_name:
            cluster_name = self._alloc_cluster_name()
            self._alloc_cluster(cluster_name)
            self.flat_clusters[cluster_name] = 1
        else:
            self.flat_clusters[cluster_name] += 1
        self.gs.logger.info(
            f"get_cluster {cluster_name=} number bdevs: "
            f"{self.flat_clusters[cluster_name]}"
        )
        return cluster_name

    def put_cluster(self, name: str) -> None:
        """Free cluster by name, update reference count"""
        if name in self.flat_clusters:
            self.flat_clusters[name] -= 1
            assert self.flat_clusters[name] >= 0
            # free the cluster context if no longer used by any bdev
            if self.flat_clusters[name] == 0:
                self._free_cluster(name)
                self.flat_clusters.pop(name)
            else:
                self.gs.logger.info(
                    f"put_cluster {name=} number bdevs: " f"{self.flat_clusters[name]}"
                )
            return

        assert (
            False
        ), f"Cluster {name} is not found"  # we should find the cluster in our state

    def _alloc_cluster_name(self) -> str:
        """Allocates a new cluster name, disregard ana group"""
        x = 0
        while True:
            name = f"cluster_context_{x}"
            if name not in self.flat_clusters:
                return name
            x += 1


# Cluster pool implementation of ClusterAllocationStrategy
class ClusterPoolAllocator(ClusterAllocationStrategy):
    def __init__(self, config: GatewayConfig, gs) -> None:
        """Init cluster context management variables"""
        self._init_common(config, gs)
        self.pool_size = self.config.getint("spdk", "cluster_pool_size")
        if self.pool_size < 1:
            raise Exception(
                f"invalid configuration: spdk.cluster_pool_size "
                f"{self.cluster_pool_size} < 1"
            )
        self.gs.logger.info(f"NVMeoF cluster pool size: {self.pool_size}")
        # Initialize cluster names as "cluster_1", "cluster_2", ..., "cluster_n"
        self.clusters = [f"cluster_{i + 1}" for i in range(self.pool_size)]
        # Initialize usage counts for each cluster
        self.usage_counts: Dict[str, int] = {cluster: 0 for cluster in self.clusters}

    def get_cluster(self, anagrp: int) -> str:
        """
        Allocate the cluster with the minimum usage count.

        Args:
            anagrp (int): The ANA group (unused in this implementation).

        Returns:
            str: The name of the allocated cluster.
        """
        # Find the cluster with the minimum usage count
        min_cluster = min(self.usage_counts, key=self.usage_counts.get)  # type: ignore
        # Lazy cluster allocation
        if self.usage_counts[min_cluster] == 0:
            self._alloc_cluster(min_cluster)
        # Increment the usage count for the allocated cluster
        self.usage_counts[min_cluster] += 1
        return min_cluster

    def put_cluster(self, name: str) -> None:
        """
        Release a cluster and update its usage count.

        Args:
            name (str): The name of the cluster to release.

        Raises:
            AssertionError: If the cluster name does not exist in the pool.
                            If invalid usage count detected.
        """
        # Assert that the cluster name exists in the pool
        assert name in self.usage_counts, f"Cluster {name} does not exist in the pool."

        # Decrement the usage count for the released cluster
        assert (
            self.usage_counts[name] > 0
        ), f"Cluster {name} invalid usage count {self.usage_counts[name]}."
        self.usage_counts[name] -= 1

        # Lazy deallocate
        if self.usage_counts[name] == 0:
            self._free_cluster(name)


# Factory function to return an instance of an cluster allocator according to config
def get_cluster_allocator(config: GatewayConfig, gs) -> ClusterAllocationStrategy:
    if config.is_param_defined("spdk", "bdevs_per_cluster"):
        return AnaGrpBdevsPerCluster(config, gs)
    elif config.is_param_defined("spdk", "flat_bdevs_per_cluster"):
        return FlatBdevsPerCluster(config, gs)
    elif config.is_param_defined("spdk", "cluster_pool_size"):
        return ClusterPoolAllocator(config, gs)
    else:
        raise ValueError("Unknown cluster allocator in the config")
