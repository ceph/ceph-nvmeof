import configparser
import os
import tempfile
import unittest

from control.config import GatewayConfig
from control.config_registry import ConfigRegistry
from control.config_registry import ConfigRejected


class _Entry:
    def __init__(self, section, key, present, value=""):
        self.section = section
        self.key = key
        self.present = present
        self.value = value


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

    def test_seed_uses_conf_file(self):
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 100)
        self.assertEqual(self.registry.get("spdk", "timeout"), 15.0)
        self.assertEqual(self.registry.get("gateway", "force_tls"), False)

    def test_apply_one_key_and_reject_another(self):
        rejects = self.registry.apply(
            [_Entry("spdk", "timeout", True, "nope"),
             _Entry("gateway", "subsystem_cache_expiration", True, "12")],
            [_Entry("gateway", "max_namespaces", True, "80")])
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0][0], "spdk")
        self.assertEqual(rejects[0][1], "timeout")
        self.assertEqual(self.registry.get("spdk", "timeout"), 15.0)
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

    def test_unknown_and_bootstrap_are_rejected(self):
        rejects = self.registry.apply(
            [_Entry("gateway", "name", True, "gw")],
            [_Entry("gateway", "not_a_key", True, "1")])
        self.assertEqual(len(rejects), 2)

    def test_listener_failure_keeps_the_previous_value(self):
        def fail(section, key, value):
            raise RuntimeError("cannot apply")

        self.registry.set_listener(fail)
        rejects = self.registry.apply(
            [], [_Entry("gateway", "max_namespaces", True, "50")])
        self.assertEqual(len(rejects), 1)
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 100)

        seen = {}

        def ok(section, key, value):
            seen[(section, key)] = value

        self.registry.set_listener(ok)
        rejects = self.registry.apply(
            [], [_Entry("gateway", "max_namespaces", True, "50")])
        self.assertEqual(rejects, [])
        self.assertEqual(seen[("gateway", "max_namespaces")], 50)
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 50)

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
        self.assertEqual(self.registry.get("gateway", "max_namespaces"), 100)
        self.registry.apply([], [_Entry("gateway", "max_namespaces", True, "90")])
        self.registry.require("gateway", "max_namespaces")


if __name__ == "__main__":
    unittest.main()
