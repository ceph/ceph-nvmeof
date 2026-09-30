#
#  Copyright (c) 2026 IBM, Inc.
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#

"""Live gateway configuration.

Seeded from the conf file. apply_config replaces one key at a time.
A rejected key keeps the last applied value.
"""

_MISSING = object()


class ConfigRejected(Exception):
    def __init__(self, section, key, error):
        self.section = section
        self.key = key
        self.error = error
        super().__init__(f"{section}/{key}: {error}")


def _parse(kind, value):
    if kind == "int":
        text = str(value).strip()
        if text.startswith(("+", "-")):
            number = text[1:]
            sign = text[0]
        else:
            number = text
            sign = ""
        if not number.isdigit():
            raise ValueError(f"expected an integer, got '{value}'")
        return int(sign + number)
    if kind == "float":
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError(f"expected a float, got '{value}'")
    if kind == "bool":
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        raise ValueError(f"expected a boolean, got '{value}'")
    if kind == "str":
        return "" if value is None else str(value)
    raise ValueError(f"unsupported type {kind}")


def _read_conf(parser, section, key, kind, default):
    if not parser.has_section(section) or not parser.has_option(section, key):
        return default
    raw = parser.get(section, key)
    return _parse(kind, raw)


# section, key, type, default, pile. Defaults match NvmeofServiceSpec.
_DAEMON_KEYS = (
    ("gateway", "subsystem_cache_expiration", "int", 30),
    ("gateway", "allowed_consecutive_spdk_ping_failures", "int", 1),
    ("gateway", "spdk_ping_interval_in_seconds", "float", 2.0),
    ("gateway", "ping_spdk_under_lock", "bool", False),
    ("gateway-logs", "log_level", "str", "INFO"),
    ("gateway-logs", "log_files_enabled", "bool", True),
    ("gateway-logs", "log_files_rotation_enabled", "bool", True),
    ("gateway-logs", "verbose_log_messages", "bool", True),
    ("gateway-logs", "max_log_file_size_in_mb", "int", 10),
    ("gateway-logs", "max_log_files_count", "int", 20),
    ("gateway-logs", "max_log_directory_backups", "int", 10),
    ("spdk", "timeout", "float", 60.0),
    ("spdk", "notifications_interval", "int", 60),
    ("monitor", "timeout", "float", 1.0),
)

_GROUP_KEYS = (
    ("gateway", "force_tls", "bool", False),
    ("gateway", "state_update_notify", "bool", True),
    ("gateway", "state_update_interval_sec", "int", 5),
    ("gateway", "break_update_interval_sec", "int", 25),
    ("gateway", "rebalance_period_sec", "int", 7),
    ("gateway", "max_ns_to_change_lb_grp", "int", 8),
    ("gateway", "verify_nqns", "bool", True),
    ("gateway", "verify_keys", "bool", True),
    ("gateway", "verify_listener_ip", "bool", True),
    ("gateway", "omap_file_lock_duration", "int", 20),
    ("gateway", "omap_file_lock_retries", "int", 30),
    ("gateway", "omap_file_lock_retry_sleep_interval", "float", 1.0),
    ("gateway", "omap_file_update_reloads", "int", 10),
    ("gateway", "omap_file_update_attempts", "int", 500),
    ("gateway", "max_hosts_per_namespace", "int", 16),
    ("gateway", "max_namespaces_with_netmask", "int", 1000),
    ("gateway", "max_subsystems", "int", 128),
    ("gateway", "max_hosts", "int", 2048),
    ("gateway", "max_namespaces", "int", 4096),
    ("gateway", "max_namespaces_per_subsystem", "int", 512),
    ("gateway", "max_hosts_per_subsystem", "int", 128),
)


class ConfigRegistry:
    def __init__(self, config):
        self._conf = {}
        self._defaults = {}
        self._values = {}
        self._specs = {}
        self._group = set()
        self._rejected = set()
        self._overridden = set()
        self._parser = config.config
        parser = self._parser
        for section, key, kind, default in _DAEMON_KEYS:
            self._add(parser, section, key, kind, default, group=False)
        for section, key, kind, default in _GROUP_KEYS:
            self._add(parser, section, key, kind, default, group=True)

    def _add(self, parser, section, key, kind, default, group):
        ident = (section, key)
        self._specs[ident] = kind
        self._defaults[ident] = default
        if group:
            self._group.add(ident)
        seeded = _read_conf(parser, section, key, kind, default)
        self._conf[ident] = seeded
        self._values[ident] = seeded

    def contains(self, section, key):
        return (section, key) in self._specs

    def overridden(self, section, key):
        """True after a snapshot has replaced the conf-file value."""
        return (section, key) in self._overridden

    def _conf_value(self, ident):
        section, key = ident
        return _read_conf(self._parser, section, key, self._specs[ident], self._defaults[ident])

    def get(self, section, key, default=_MISSING):
        ident = (section, key)
        if ident not in self._values:
            if default is _MISSING:
                raise KeyError(f"{section}/{key}")
            return default
        return self._values[ident]

    def require(self, section, key):
        """Fail a management call that depends on a rejected group key."""
        ident = (section, key)
        if ident in self._rejected:
            raise ConfigRejected(section, key, "group config key was rejected")

    def apply(self, daemon_entries, group_entries):
        """Apply each entry on its own. Returns a list of (section, key, error)."""
        rejects = []
        for entry in list(daemon_entries) + list(group_entries):
            error = self._apply_one(entry)
            if error is not None:
                rejects.append((entry.section, entry.key, error))
        return rejects

    def _apply_one(self, entry):
        ident = (entry.section, entry.key)
        spec = self._specs.get(ident)
        if spec is None:
            return "unknown or bootstrap key"
        if not entry.present:
            value = self._conf_value(ident)
            try:
                self._on_value(entry.section, entry.key, value)
            except Exception as exc:
                if ident in self._group:
                    self._rejected.add(ident)
                return str(exc)
            self._values[ident] = value
            self._conf[ident] = value
            self._overridden.discard(ident)
            self._rejected.discard(ident)
            return None
        try:
            value = _parse(spec, entry.value)
        except ValueError as exc:
            if ident in self._group:
                self._rejected.add(ident)
            return str(exc)
        try:
            self._on_value(entry.section, entry.key, value)
        except Exception as exc:
            if ident in self._group:
                self._rejected.add(ident)
            return str(exc)
        self._values[ident] = value
        self._overridden.add(ident)
        self._rejected.discard(ident)
        return None

    def _on_value(self, section, key, value):
        """Hook for live readers. The registry value changes only if this returns."""
        return None
