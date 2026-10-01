import configparser
import os
import tempfile
import unittest

from control.config import GatewayConfig
from control.config_registry import CONFIG_TYPE_BOOL
from control.config_registry import CONFIG_TYPE_FLOAT
from control.config_registry import CONFIG_TYPE_INT
from control.config_registry import CONFIG_TYPE_UNKNOWN
from control.config_registry import ConfigRegistry
from control.config_registry import ConfigRejected


class _Entry:
    def __init__(self, section, key, present, value="", type=CONFIG_TYPE_INT):
        self.section = section
        self.key = key
        self.present = present
        self.value = value
        self.type = type


class _Config:
    def __init__(self, text):
        self.config = configparser.ConfigParser()
        self.config.read_string(text)


class TestConfigRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = ConfigRegistry(_Config("""
[gateway]
max_namespaces = 100
verify_nqns = true
[spdk]
timeout = 15
"""))

    def test_nothing_is_stored_until_a_snapshot(self):
        with self.assertRaises(KeyError):
            self.registry.get("gateway", "max_namespaces")
        self.assertFalse(self.registry.overridden("gateway", "max_namespaces"))

    def test_apply_one_key_and_reject_another(self):
        rejects = self.registry.apply(
            [_Entry("spdk", "timeout", True, "nope", CONFIG_TYPE_FLOAT),
             _Entry("gateway", "subsystem_cache_expiration", True, "12")],
            [_Entry("gateway", "max_namespaces", True, "80")])
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0][0], "spdk")
        self.assertEqual(rejects[0][1], "timeout")
        with self.assertRaises(KeyError):
            self.registry.get("spdk", "timeout")
        self.assertFalse(self.registry.overridden("spdk", "timeout"))
        self.assertEqual(self.registry.get("gateway", "subsystem_cache_expiration"), 12)
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 80)

    def test_present_false_restores_conf_file(self):
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "80")])
        self.assertTrue(self.registry.overridden("gateway", "max_namespaces"))
        self.registry.apply([], [_Entry("gateway", "max_namespaces", False, "")])
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 100)
        self.assertFalse(self.registry.overridden("gateway", "max_namespaces"))

    def test_present_false_uses_the_current_parser(self):
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "80")])
        self.registry._parser["gateway"]["max_namespaces"] = "30"
        self.registry.apply([], [_Entry("gateway", "max_namespaces", False, "")])
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 30)

    def test_present_false_without_a_conf_option_clears_the_override(self):
        seen = []

        def record(section, key, value):
            seen.append((section, key, value))

        self.registry.set_listener(record)
        self.registry.apply(
            [_Entry("gateway", "subsystem_cache_expiration", True, "12")], [])
        seen.clear()
        self.registry.apply(
            [_Entry("gateway", "subsystem_cache_expiration", False, "")], [])
        self.assertEqual(seen, [])
        self.assertFalse(self.registry.overridden("gateway", "subsystem_cache_expiration"))
        with self.assertRaises(KeyError):
            self.registry.get("gateway", "subsystem_cache_expiration")

    def test_gateway_config_reads_the_parser_until_a_snapshot(self):
        handle = tempfile.NamedTemporaryFile("w", delete=False)
        handle.write("[gateway]\nmax_namespaces = 100\n")
        handle.close()
        try:
            config = GatewayConfig(handle.name)
            config.config["gateway"]["max_namespaces"] = "40"
            self.assertEqual(config.getint("gateway", "max_namespaces"), 40)
            config.registry.apply([], [_Entry("gateway", "max_namespaces", True, "80")])
            self.assertEqual(config.getint("gateway", "max_namespaces"), 80)
            config.config["gateway"]["max_namespaces"] = "30"
            config.registry.apply([], [_Entry("gateway", "max_namespaces", False, "")])
            self.assertEqual(config.getint("gateway", "max_namespaces"), 30)
        finally:
            os.remove(handle.name)

    def test_unknown_type_is_rejected(self):
        rejects = self.registry.apply(
            [_Entry("gateway", "name", True, "gw", CONFIG_TYPE_UNKNOWN)],
            [_Entry("gateway", "not_a_key", True, "1", CONFIG_TYPE_UNKNOWN)])
        self.assertEqual(len(rejects), 2)
        self.assertEqual(rejects[0][2], "unknown config type")
        self.assertEqual(rejects[1][2], "unknown config type")
        with self.assertRaises(ConfigRejected):
            self.registry.require("gateway", "not_a_key")
        self.registry.require("gateway", "name")

    def test_listener_failure_keeps_the_previous_value(self):
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "80")])

        def fail(section, key, value):
            raise RuntimeError("cannot apply")

        self.registry.set_listener(fail)
        rejects = self.registry.apply(
            [], [_Entry("gateway", "max_namespaces", True, "50")])
        self.assertEqual(len(rejects), 1)
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 80)
        with self.assertRaises(ConfigRejected):
            self.registry.require("gateway", "max_namespaces")

        seen = {}

        def ok(section, key, value):
            seen[(section, key)] = value

        self.registry.set_listener(ok)
        rejects = self.registry.apply(
            [], [_Entry("gateway", "max_namespaces", True, "50")])
        self.assertEqual(rejects, [])
        self.assertEqual(seen[("gateway", "max_namespaces")], 50)
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 50)
        self.registry.require("gateway", "max_namespaces")

    def test_present_false_notifies_the_listener(self):
        seen = []

        def record(section, key, value):
            seen.append((section, key, value))

        self.registry.set_listener(record)
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "80")])
        seen.clear()
        self.registry.apply([], [_Entry("gateway", "max_namespaces", False, "")])
        self.assertEqual(seen, [("gateway", "max_namespaces", 100)])

    def test_group_reject_blocks_require_until_a_later_apply(self):
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "bad")])
        with self.assertRaises(ConfigRejected):
            self.registry.require("gateway", "max_namespaces")
        with self.assertRaises(KeyError):
            self.registry.get("gateway", "max_namespaces")
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "90")])
        self.registry.require("gateway", "max_namespaces")
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 90)

    def test_daemon_reject_does_not_block_require(self):
        self.registry.apply(
            [_Entry("spdk", "timeout", True, "nope", CONFIG_TYPE_FLOAT)], [])
        self.registry.require("spdk", "timeout")

    def test_bool_and_float_types(self):
        rejects = self.registry.apply(
            [_Entry("spdk", "timeout", True, "1.5", CONFIG_TYPE_FLOAT)],
            [_Entry("gateway", "verify_nqns", True, "false", CONFIG_TYPE_BOOL)])
        self.assertEqual(rejects, [])
        self.assertEqual(self.registry.get("spdk", "timeout"), 1.5)
        self.assertIs(self.registry.get("gateway", "verify_nqns"), False)


if __name__ == "__main__":
    unittest.main()
