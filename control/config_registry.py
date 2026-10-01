#
#  Copyright (c) 2026 IBM, Inc.
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#

"""Live gateway configuration.

The snapshot carries the conf type. apply_config replaces one key at a time.
A rejected key keeps the last applied value. A key with no stored value is
read from the conf file.
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


# Match config_type in control/proto/monitor.proto.
CONFIG_TYPE_UNKNOWN = 0
CONFIG_TYPE_INT = 1
CONFIG_TYPE_FLOAT = 2
CONFIG_TYPE_BOOL = 3
CONFIG_TYPE_STR = 4

_KINDS = {
    CONFIG_TYPE_INT: "int",
    CONFIG_TYPE_FLOAT: "float",
    CONFIG_TYPE_BOOL: "bool",
    CONFIG_TYPE_STR: "str",
}


def _parser_has(parser, section, key):
    return parser.has_section(section) and parser.has_option(section, key)


class ConfigRegistry:
    def __init__(self, config):
        self._values = {}
        self._specs = {}
        self._group = set()
        self._rejected = set()
        self._overridden = set()
        self._listener = None
        self._parser = config.config

    def contains(self, section, key):
        return (section, key) in self._specs

    def overridden(self, section, key):
        """True after a snapshot has replaced the conf-file value."""
        return (section, key) in self._overridden

    def get(self, section, key, default=_MISSING):
        ident = (section, key)
        if ident not in self._values:
            if default is _MISSING:
                raise KeyError(f"{section}/{key}")
            return default
        return self._values[ident]

    def set_listener(self, listener):
        """Called with (section, key, value) before the registry stores a new value."""
        self._listener = listener

    def require(self, section, key):
        """Fail a management call that depends on a rejected group key."""
        ident = (section, key)
        if ident in self._rejected:
            raise ConfigRejected(section, key, "group config key was rejected")

    def apply(self, daemon_entries, group_entries):
        """Apply each entry on its own. Returns a list of (section, key, error)."""
        rejects = []
        for entry in daemon_entries:
            error = self._apply_one(entry, group=False)
            if error is not None:
                rejects.append((entry.section, entry.key, error))
        for entry in group_entries:
            error = self._apply_one(entry, group=True)
            if error is not None:
                rejects.append((entry.section, entry.key, error))
        return rejects

    def _note_pile(self, ident, group):
        if group:
            self._group.add(ident)
        else:
            self._group.discard(ident)

    def _fail(self, ident, group, error):
        self._note_pile(ident, group)
        if group:
            self._rejected.add(ident)
        return error

    def _apply_one(self, entry, group):
        ident = (entry.section, entry.key)
        kind = _KINDS.get(int(getattr(entry, "type", CONFIG_TYPE_UNKNOWN)))
        if kind is None:
            return self._fail(ident, group, "unknown config type")
        self._note_pile(ident, group)
        self._specs[ident] = kind
        if not entry.present:
            if not _parser_has(self._parser, entry.section, entry.key):
                # The call site's get_*_with_default is the fallback. Do not
                # invent a value for the listener.
                self._values.pop(ident, None)
                self._overridden.discard(ident)
                self._rejected.discard(ident)
                return None
            try:
                value = _parse(kind, self._parser.get(entry.section, entry.key))
            except ValueError as exc:
                return self._fail(ident, group, str(exc))
            try:
                self._on_value(entry.section, entry.key, value)
            except Exception as exc:
                return self._fail(ident, group, str(exc))
            self._values[ident] = value
            self._overridden.discard(ident)
            self._rejected.discard(ident)
            return None
        try:
            value = _parse(kind, entry.value)
        except ValueError as exc:
            return self._fail(ident, group, str(exc))
        try:
            self._on_value(entry.section, entry.key, value)
        except Exception as exc:
            return self._fail(ident, group, str(exc))
        self._values[ident] = value
        self._overridden.add(ident)
        self._rejected.discard(ident)
        return None

    def _on_value(self, section, key, value):
        """Hook for live readers. The registry value changes only if this returns."""
        if self._listener is not None:
            self._listener(section, key, value)
