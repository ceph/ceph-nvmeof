#
#  Copyright (c) 2024 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gbregman@ibm.com
#

import uuid
import errno
import os

class GatewayEnumUtils:
    def get_value_from_key(e_type, keyval, ignore_case = False):
        val = None
        try:
            key_index = e_type.keys().index(keyval)
            val = e_type.values()[key_index]
        except ValueError:
            pass
        except IndexError:
            pass

        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.lower(), False)
        if ignore_case and val == None and type(keyval) == str:
            val = get_value_from_key(e_type, keyval.upper(), False)

        return val

    def get_key_from_value(e_type, val):
        keyval = None
        try:
            val_index = e_type.values().index(val)
            keyval = e_type.keys()[val_index]
        except ValueError:
            pass
        except IndexError:
            pass
        return keyval

class GatewayUtils:
    DISCOVERY_NQN = "nqn.2014-08.org.nvmexpress.discovery"

    # We need to enclose IPv6 addresses in brackets before concatenating a colon and port number to it
    def escape_address_if_ipv6(addr) -> str:
        ret_addr = addr
        if ":" in addr and not addr.strip().startswith("["):
            ret_addr = f"[{addr}]"
        return ret_addr

    def is_discovery_nqn(nqn) -> bool:
        return nqn == GatewayUtils.DISCOVERY_NQN

    def is_valid_rev_domain(rev_domain):
        DOMAIN_LABEL_MAX_LEN = 63

        domain_parts = rev_domain.split(".")
        for lbl in domain_parts:
            if not lbl:
                return (errno.EINVAL, f"empty domain label doesn't start with a letter")

            if len(lbl) > DOMAIN_LABEL_MAX_LEN:
                return (errno.EINVAL, f"domain label {lbl} is too long")

            if not lbl[0].isalpha():
                return (errno.EINVAL, f"domain label {lbl} doesn't start with a letter")

            if lbl.endswith("-"):
                return (errno.EINVAL, f"domain label {lbl} doesn't end with an alphanumeric character")
            if not lbl.replace("-", "").isalnum():
                return (errno.EINVAL, f"domain label {lbl} contains a character which is not [a-z,A-Z,0-9,'-','.']")

        return (0, os.strerror(0))

    def is_valid_uuid(uuid_val) -> bool:
        UUID_STRING_LENGTH = len(str(uuid.uuid4()))

        if len(uuid_val) != UUID_STRING_LENGTH:
            return False

        uuid_parts = uuid_val.split("-")
        if len(uuid_parts) != 5:
            return False
        if len(uuid_parts[0]) != 8:
            return False
        if len(uuid_parts[1]) != 4:
            return False
        if len(uuid_parts[2]) != 4:
            return False
        if len(uuid_parts[3]) != 4:
            return False
        if len(uuid_parts[4]) != 12:
            return False

        for u in uuid_parts:
            try:
                n = int(u, 16)
            except ValueError:
                return False

        return True

    def is_valid_nqn(nqn):
        NQN_MIN_LENGTH = 11
        NQN_MAX_LENGTH = 223
        NQN_PREFIX = "nqn."
        UUID_STRING_LENGTH = len(str(uuid.uuid4()))
        NQN_UUID_PREFIX = "nqn.2014-08.org.nvmexpress:uuid:"
        NQN_UUID_PREFIX_LENGTH = len(NQN_UUID_PREFIX)

        if type(nqn) != str:
            return (errno.EINVAL, f"Invalid type {type(nqn)} for NQN, must be a string")

        try:
            b = nqn.encode(encoding="utf-8")
        except UnicodeEncodeError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", must have an UTF-8 encoding")

        if len(nqn) < NQN_MIN_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too short, minimal length is {NQN_MIN_LENGTH}")

        if len(nqn) > NQN_MAX_LENGTH:
            return (errno.EINVAL, f"NQN \"{nqn}\" is too long, maximal length is {NQN_MAX_LENGTH}")
        if GatewayUtils.is_discovery_nqn(nqn):
            # The NQN is technically valid but we will probably reject it later as being a discovery one
            return (0, os.strerror(0))

        if nqn.startswith(NQN_UUID_PREFIX):
            if len(nqn) != NQN_UUID_PREFIX_LENGTH + UUID_STRING_LENGTH:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not the correct length")
            uuid_part = nqn[NQN_UUID_PREFIX_LENGTH : ]
            if not GatewayUtils.is_valid_uuid(uuid_part):
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": UUID is not formatted correctly")
            return (0, os.strerror(0))

        if not nqn.startswith(NQN_PREFIX):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\", doesn't start with \"{NQN_PREFIX}\"")

        nqn_no_prefix = nqn[len(NQN_PREFIX) : ]
        date_part = nqn_no_prefix[ : 8]
        rev_domain_part = nqn_no_prefix[8 : ]
        if not date_part.endswith("."):
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
        date_part = date_part[ : -1]
        try:
            year_part, month_part = date_part.split("-")
            if len(year_part) != 4 or len(month_part) != 2:
                return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")
            n = int(year_part)
            n = int(month_part)
        except ValueError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": invalid date code")

        try:
            rev_domain_part, user_part = rev_domain_part.split(":", 1)
        except ValueError:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": must contain a user specified name starting with a \":\"")

        if not user_part:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": must contain a user specified name starting with a \":\"")

        rc = GatewayUtils.is_valid_rev_domain(rev_domain_part)
        if rc[0] != 0:
            return (errno.EINVAL, f"Invalid NQN \"{nqn}\": reverse domain is not formatted correctly: {rc[1]}")

        return (0, os.strerror(0))
