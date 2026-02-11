#
#  Copyright (c) 2024 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: leonidc@il.ibm.com
#

import threading
import time
from .proto import gateway_pb2 as pb2


class Rebalance:
    """Miscellaneous functions which do rebalance of ANA groups
    """

    INVALID_LOAD_BALANCING_GROUP = 18446744073709551616    # should be bigger than any valid NSID
    INVALID_LOCATION = "DEADBEEF"

    def __init__(self, gateway_service):
        self.logger = gateway_service.logger
        self.gw_srv = gateway_service
        self.ceph_utils = gateway_service.ceph_utils
        self.ana_grp_location = {}  # fill location of each ana grp for responses of monitor
        self.rebalance_period_sec = gateway_service.config.getint_with_default(
            "gateway",
            "rebalance_period_sec",
            7)
        self.rebalance_max_ns_to_change_lb_grp = gateway_service.config.getint_with_default(
            "gateway",
            "max_ns_to_change_lb_grp",
            8)
        self.last_scale_down_ts = time.time()
        self.rebalance_event = threading.Event()
        self.logger.info(f" Starting rebalance thread: period: {self.rebalance_period_sec},"
                         f" max number ns to move: {self.rebalance_max_ns_to_change_lb_grp}")
        self.auto_rebalance = threading.Thread(target=self.auto_rebalance_task,
                                               daemon=True, args=(self.rebalance_event,))
        self.auto_rebalance.start()           # start the thread

    def auto_rebalance_task(self, death_event):
        """Periodically calls for auto rebalance."""
        self.logger.debug(f"Rebalance thread id is {self.auto_rebalance.native_id}")
        while (self.rebalance_period_sec > 0):
            while self.gw_srv.gateway_state.update_is_active_lock.locked():
                time.sleep(0.5)         # wait until update is over

            for i in range(self.rebalance_max_ns_to_change_lb_grp):
                try:
                    rc = self.gw_srv.execute_grpc_function(self.rebalance_logic, None, "context")
                    if rc == 1:
                        self.logger.debug(f"Nothing found for rebalance, break at {i} iteration")
                        break
                except Exception:
                    self.logger.exception("Exception in auto rebalance")
                    if death_event:
                        death_event.set()
                    raise
                time.sleep(0.01)          # release lock for 10ms after rebalancing each 1 NS
            time.sleep(self.rebalance_period_sec)

    def find_min_loaded_group(self, grp_list) -> int:
        min_load = Rebalance.INVALID_LOAD_BALANCING_GROUP
        chosen_ana_group = 0
        chosen_nqn = "null"
        for ana_grp in self.gw_srv.ana_grp_ns_load:
            if ana_grp in grp_list:
                self.logger.debug(f"ana-group {ana_grp} total load "
                                  f"{self.gw_srv.ana_grp_ns_load[ana_grp]}")
                if self.gw_srv.ana_grp_ns_load[ana_grp] <= min_load:
                    min_load = self.gw_srv.ana_grp_ns_load[ana_grp]
                    chosen_ana_group = ana_grp
        min_load = Rebalance.INVALID_LOAD_BALANCING_GROUP
        self.logger.debug(f"chosen ana-group {chosen_ana_group}")
        if chosen_ana_group != 0:
            for nqn in self.gw_srv.ana_grp_subs_load[chosen_ana_group]:
                self.logger.debug(f"chosen ana-group {chosen_ana_group} nqn {nqn} load "
                                  f"{self.gw_srv.ana_grp_subs_load[chosen_ana_group][nqn]}")
                if self.gw_srv.ana_grp_subs_load[chosen_ana_group][nqn] < min_load:
                    min_load = self.gw_srv.ana_grp_subs_load[chosen_ana_group][nqn]
                    chosen_nqn = nqn
        return chosen_ana_group, chosen_nqn

    def find_min_loaded_group_in_subsys(self, nqn, grp_list) -> int:
        min_load = Rebalance.INVALID_LOAD_BALANCING_GROUP
        chosen_ana_group = 0
        min_groups = set()
        for ana_grp in grp_list:
            if self.gw_srv.ana_grp_ns_load[ana_grp] == 0:
                self.gw_srv.ana_grp_subs_load[ana_grp][nqn] = 0
                self.logger.debug(f"chosen ana_grp {ana_grp}, min load = {0}")
                return 0, ana_grp
        for ana_grp in self.gw_srv.ana_grp_subs_load:
            if ana_grp in grp_list:
                if nqn in self.gw_srv.ana_grp_subs_load[ana_grp]:
                    if self.gw_srv.ana_grp_subs_load[ana_grp][nqn] < min_load:
                        min_load = self.gw_srv.ana_grp_subs_load[ana_grp][nqn]
                        self.logger.debug(f"min load candidate: ana {ana_grp}"
                                          f" nqn {nqn} load {min_load}")
                        min_groups = {ana_grp}
                    elif self.gw_srv.ana_grp_subs_load[ana_grp][nqn] == min_load:
                        min_groups.add(ana_grp)
                else:            # still  no load on this ana and subs
                    self.gw_srv.ana_grp_subs_load[ana_grp][nqn] = 0
                    if self.gw_srv.ana_grp_subs_load[ana_grp][nqn] < min_load:
                        min_load = 0
                        min_groups = {ana_grp}
                    elif self.gw_srv.ana_grp_subs_load[ana_grp][nqn] == min_load:
                        min_groups.add(ana_grp)
        min_load = Rebalance.INVALID_LOAD_BALANCING_GROUP
        for ana_grp in min_groups:
            # chose the minimum loaded ana group from the ana groups in min_groups set
            self.logger.debug(f"pass min_grops set: ana_grp {ana_grp} "
                              f"load {self.gw_srv.ana_grp_ns_load[ana_grp]}")
            # find minimum loaded self.gw_srv.ana_grp_ns_load
            if self.gw_srv.ana_grp_ns_load[ana_grp] < min_load:
                min_load = self.gw_srv.ana_grp_ns_load[ana_grp]
                self.logger.debug(f"chosen ana_grp {ana_grp}, min load = {min_load}")
                chosen_ana_group = ana_grp
        return min_load, chosen_ana_group

    def get_location_of_invalid_anagrp(self, anagrp):
        ns = self.gw_srv.subsystem_nsid_bdev_and_uuid.get_all_namespaces_by_ana_group_id(anagrp)
        for nsid, subsys in ns:
            ns_info = self.gw_srv.subsystem_nsid_bdev_and_uuid.find_namespace(subsys, nsid)
            if ns_info.empty():
                continue
            loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(ns_info.location)
            if loc_grps_list:
                self.logger.info(f"Found location for invalid LB group {anagrp} "
                                 f"{ns_info.location} by ns-info")
                return ns_info.location, False

        # probably only namespases with invalid location reside in invalid anagrp
        # find any valid location for them
        if self.ana_grp_location:
            location = next(iter(self.ana_grp_location.values()))
            self.logger.warning(f"Found location for invalid LB group {anagrp} "
                                f"{location} from first found valid locations")
            return location, True
        return Rebalance.INVALID_LOCATION, False

    # 1. Not allowed to perform regular rebalance when scale_down rebalance is ongoing
    # 2. Monitor each time defines what GW is responsible for regular rebalance(fairness logic),
    #    so there will not be collisions between the GWs
    #    and reballance results will be accurate. Monitor in nvme-gw show response publishes the
    #    index of ANA group that is currently responsible for rebalance
    def rebalance_logic(self, request, context) -> int:
        now = time.time()
        rebalance_attr = ()
        grps_list = self.ceph_utils.get_number_created_gateways(self.gw_srv.gateway_pool,
                                                                self.gw_srv.gateway_group, False)
        num_all_active_ana_groups = len(grps_list)
        worker_ana_group = self.ceph_utils.get_rebalance_ana_group()
        self.logger.debug(f"Called rebalance logic: current rebalancing ana "
                          f"group {worker_ana_group}")
        if worker_ana_group == 0:
            self.logger.info(f"Auto rebalance is not supported - index {worker_ana_group}")
            return 1
        ongoing_scale_down_rebalance = False
        invalid_ana_group = 0
        if not self.ceph_utils.is_rebalance_supported():
            self.logger.info("Auto rebalance is not supported with the curent ceph version")
            return 1
        for ana_grp in self.gw_srv.ana_grp_state:
            # internally valid group
            if self.gw_srv.ana_grp_ns_load[ana_grp] != 0:
                # monitor considers it invalid since GW owner was deleted
                if ana_grp not in grps_list:
                    ongoing_scale_down_rebalance = True
                    self.logger.info(f"Scale-down rebalance is ongoing for LB group {ana_grp} "
                                     f"current load {self.gw_srv.ana_grp_ns_load[ana_grp]}")
                    self.last_scale_down_ts = now
                    invalid_ana_group = ana_grp
        ana_location_dict = self.ceph_utils.get_ana_grp_location()
        for ana_grp in ana_location_dict:  # always keep updated internal dictionary
            self.ana_grp_location[ana_grp] = ana_location_dict[ana_grp]
        for ana_grp in self.gw_srv.ana_grp_state:
            if self.gw_srv.ana_grp_state[ana_grp] == pb2.ana_state.OPTIMIZED:
                location = ana_location_dict[ana_grp]
                # original location of the ana group, we pass in loop
                # also valid even if GW in deleting
                loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(location)
                num_active_ana_groups = len(loc_grps_list)
                if num_active_ana_groups == 0:
                    self.logger.warning(f"Found active LB group {ana_grp} belonging "
                                        f"to the invalid location {location}")
                    return 1
                if ana_grp not in grps_list:
                    self.logger.info(f"Found optimized LB group {ana_grp} that handles the "
                                     f"group of deleted GW. Number NS in group "
                                     f"{self.gw_srv.ana_grp_ns_load[ana_grp]} - Start NS rebalance")
                    if self.gw_srv.ana_grp_ns_load[ana_grp] >= \
                       self.rebalance_max_ns_to_change_lb_grp:
                        num = self.rebalance_max_ns_to_change_lb_grp
                    else:
                        num = self.gw_srv.ana_grp_ns_load[ana_grp]
                    if num > 0:
                        min_ana_grp, chosen_nqn = self.find_min_loaded_group(loc_grps_list)
                        self.logger.info(f"Start rebalance (scale down) destination ana group "
                                         f"{min_ana_grp}, subsystem {chosen_nqn}"
                                         f"location {location} ")
                        # scale down rebalance
                        self.ns_rebalance(context, ana_grp, min_ana_grp, 1, "0", location)
                        return 0
                    else:
                        self.logger.info(f"warning: empty group {ana_grp} of Deleting "
                                         f"GW still appears Optimized")
                        return 1
                else:
                    # keep  hysteresis interval between scale-down and regular rebalance
                    hysteresis = 2.5 * self.rebalance_period_sec
                    if not ongoing_scale_down_rebalance \
                       and ((now - self.last_scale_down_ts) > hysteresis) \
                       and (self.gw_srv.ana_grp_state[worker_ana_group] == pb2.ana_state.OPTIMIZED):
                        # if this optimized ana group == worker-ana-group
                        # or (for improve rebalance performance)
                        # worker-ana-group and this ana-group are in optimized state on this GW
                        rc = self.periodic_scan_ns_location(context, ana_grp, location)
                        if not rc:
                            return rc
                        # need to search  all nqns not only inside the current load
                        for nqn in self.gw_srv.ana_grp_subs_load[ana_grp]:
                            num_ns_in_nqn = len(
                                self.gw_srv.subsystem_nsid_bdev_and_uuid.
                                get_all_namespaces_with_location(location, nqn))
                            target_subs_per_ana = num_ns_in_nqn / num_active_ana_groups
                            self.logger.debug(f"loop: nqn {nqn} LB group {ana_grp} load "
                                              f"{self.gw_srv.ana_grp_subs_load[ana_grp][nqn]}, "
                                              f"num-ns in nqn {num_ns_in_nqn}, target_subs_per_ana "
                                              f"{target_subs_per_ana} ")
                            if self.gw_srv.ana_grp_subs_load[ana_grp][nqn] > target_subs_per_ana:
                                self.logger.debug(f"max-nqn load "
                                                  f"{self.gw_srv.ana_grp_subs_load[ana_grp][nqn]} "
                                                  f"nqn {nqn} ")
                                min_load, min_ana_grp = \
                                    self.find_min_loaded_group_in_subsys(nqn, loc_grps_list)

                                my_eq_more = (self.gw_srv.ana_grp_subs_load[ana_grp][nqn] - 1) >= \
                                             (self.gw_srv.ana_grp_subs_load[min_ana_grp][nqn] + 1)

                                worth = (self.gw_srv.ana_grp_ns_load[ana_grp] -         # noqa: W504
                                         self.gw_srv.ana_grp_ns_load[min_ana_grp] > 1)  # noqa: W504
                                if my_eq_more:
                                    self.logger.info(f"Start rebalance (regular) in subsystem "
                                                     f"{nqn}, dest LB group {min_ana_grp} "
                                                     f"load per subs {min_load}"
                                                     f" location {location}")
                                    # regular rebalance
                                    self.ns_rebalance(context, ana_grp, min_ana_grp, 1,
                                                      nqn, location)
                                    return 0
                                else:
                                    # add to tuple : ana , min-ana , nqn , worth
                                    if worth:
                                        rebalance_attr = (ana_grp, min_ana_grp, nqn, worth)
                                    self.logger.debug(f"Found min loaded subsystem {nqn}, ana "
                                                      f"{min_ana_grp}, load {min_load} does not "
                                                      f"fit rebalance criteria!")
                                    continue
            if ongoing_scale_down_rebalance \
               and (num_all_active_ana_groups == self.ceph_utils.num_gws):
                # this GW feels scale_down condition on ana_grp but no GW in Deleting
                # state in the current mon.map. So need to change LB group for all NS
                # related to the invalid group - group that was deleted by GW monitor
                self.logger.info(f"Detected deleted LB group {invalid_ana_group}")
                if (self.gw_srv.ana_grp_state[worker_ana_group]) == pb2.ana_state.OPTIMIZED:
                    force_rebalance = False
                    if invalid_ana_group in self.ana_grp_location:
                        location = self.ana_grp_location[invalid_ana_group]
                    else:
                        location, force_rebalance = \
                            self.get_location_of_invalid_anagrp(invalid_ana_group)
                    self.logger.info(f"Found location for invalid LB group{invalid_ana_group}"
                                     f" location {location} force rebalance {force_rebalance}")
                    if location == Rebalance.INVALID_LOCATION:
                        self.logger.warning(f"location not found for LB grp {invalid_ana_group}")
                        return 0
                    loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(location)
                    min_ana_grp, chosen_nqn = self.find_min_loaded_group(loc_grps_list)
                    if min_ana_grp != 0 and chosen_nqn != "null" and invalid_ana_group != 0:
                        self.logger.info(f"Start rebalance (deadlock resolving) dest. LB group "
                                         f" {min_ana_grp}, subsystem {chosen_nqn}")
                        self.ns_rebalance(context, invalid_ana_group, min_ana_grp, 1, "0",
                                          location, force_rebalance)
                        return 0
                    else:
                        self.logger.warning(f"rebalance (deadlock resolving) is not allowed "
                                            f" invalid group {invalid_ana_group},"
                                            f" subsystem {chosen_nqn}")
        # if tuple is not empty
        if rebalance_attr:
            location = self.ana_grp_location[rebalance_attr[0]]
            self.logger.info(
                f"Start rebalance (fixing regular) in subsystem "
                f"ana {rebalance_attr[0]}, dest ana {rebalance_attr[1]} nqn {rebalance_attr[2]}"
                f"location {location}")
            self.ns_rebalance(context, rebalance_attr[0], rebalance_attr[1], 1,
                              rebalance_attr[2], location)
            return 0
        return 1

    def periodic_scan_ns_location(self, context, ana_id, ana_location) -> int:
        ns = self.gw_srv.subsystem_nsid_bdev_and_uuid.get_all_namespaces_by_ana_group_id(ana_id)
        for nsid, subsys in ns:
            ns_info = self.gw_srv.subsystem_nsid_bdev_and_uuid.find_namespace(subsys, nsid)
            if ns_info.location != ana_location:
                self.logger.warning(f"Found nsid {nsid} nqn {subsys} location {ns_info.location}"
                                    f" location {ana_location} need to change LB group")
                # rought rebalance
                loc_grps_list = self.ceph_utils.get_ana_grp_list_per_location(ns_info.location)
                if len(loc_grps_list) != 0:
                    min_ana_grp, chosen_nqn = self.find_min_loaded_group(loc_grps_list)
                    if min_ana_grp == 0:
                        self.logger.warning("not found the candidate for NS {nsid}")
                        return 1
                    self.logger.info(f"Start rebalance to LB grp with location = ns-location"
                                     f" destination ana group "
                                     f"{min_ana_grp}, subsystem {chosen_nqn}"
                                     f" location {ns_info.location} ")
                    self.ns_rebalance(context, ana_id, min_ana_grp, 1, "0", ns_info.location)
                    return 0
                else:
                    self.logger.warning(f"Impossible to find correct LB group for ns {nsid}"
                                        f" nqn {subsys} location {ns_info.location}")
        return 1

    def ns_rebalance(self, context, ana_id, dest_ana_id, num, subs_nqn, location,
                     force_rebalance=False) -> int:
        now = time.time()
        num_rebalanced = 0
        self.logger.info(f"== rebalance started == for subsystem {subs_nqn}, LB grp {ana_id}, "
                         f"dest. anagrp {dest_ana_id}, num ns {num}"
                         f"location {location} time {now} force {force_rebalance}")
        ns = self.gw_srv.subsystem_nsid_bdev_and_uuid.get_all_namespaces_by_ana_group_id(ana_id)
        self.logger.debug(f"Doing loop on {ana_id} ")
        for nsid, subsys in ns:
            ns_info = self.gw_srv.subsystem_nsid_bdev_and_uuid.find_namespace(subsys, nsid)
            self.logger.debug(f"nsid {nsid} nqn {subsys} location {ns_info.location} to rebalance:")
            if not force_rebalance and ns_info.location != location:
                self.logger.warning(f"namespace with wrong location: {ns_info.location} in LB "
                                    f"group {ana_id} nsid {nsid} nqn {subsys} ")
                continue
            if subsys == subs_nqn or subs_nqn == "0":
                self.logger.info(f"nsid for change_load_balancing: {nsid}, "
                                 f"{subsys}, LB group: {ana_id}")
                change_lb_group_req = pb2.namespace_change_load_balancing_group_req(
                    subsystem_nqn=subsys, nsid=nsid, anagrpid=dest_ana_id, auto_lb_logic=True)
                if not self.gw_srv.up_and_running:
                    self.logger.warning("SPDK is not up and running!")
                    return 0

                ret = self.gw_srv.namespace_change_load_balancing_group_safe(change_lb_group_req,
                                                                             context)
                self.logger.debug(f"ret namespace_change_load_balancing_group  {ret}")
                num_rebalanced += 1
                if num_rebalanced >= num:
                    self.logger.info(f"== Completed rebalance in {time.time() - now} sec for "
                                     f"{num} namespaces from anagrp {ana_id} to {dest_ana_id} ")
                    return 0
        return 0
