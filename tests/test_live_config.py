import logging
import os
import shutil
import tempfile
from types import SimpleNamespace

import pytest

from control.config import (
    CONFIG_TYPE_BOOL,
    CONFIG_TYPE_FLOAT,
    CONFIG_TYPE_INT,
    CONFIG_TYPE_STR,
    CONFIG_TYPE_UNKNOWN,
    GatewayConfig,
)
from control.utils import GatewayLogger


def _entry(section, key, present, value, kind):
    return SimpleNamespace(section=section, key=key, present=present, value=value, type=kind)


@pytest.fixture
def live_config():
    directory = tempfile.mkdtemp()
    path = os.path.join(directory, "ceph-nvmeof.conf")
    with open(path, "w") as conf:
        conf.write(
            "[gateway]\n"
            "name = live-config\n"
            "rebalance_period_sec = 7\n"
            "max_ns_to_change_lb_grp = 8\n"
            "spdk_ping_interval_in_seconds = 2.0\n"
            "state_update_interval_sec = 5\n"
            "[gateway-logs]\n"
            "log_directory = " + directory + "\n"
            "log_files_enabled = true\n"
            "log_files_rotation_enabled = true\n"
            "max_log_file_size_in_mb = 10\n"
            "max_log_files_count = 20\n"
            "verbose_log_messages = true\n"
            "log_level = INFO\n"
            "[spdk]\n"
            "timeout = 60.0\n"
            "notifications_interval = 60\n"
        )
    prior_logger = GatewayLogger.logger
    prior_handler = GatewayLogger.handler
    prior_init = GatewayLogger.init_executed
    nvmeof_logger = logging.getLogger("nvmeof")
    root_logger = logging.getLogger()
    prior_handlers = list(nvmeof_logger.handlers)
    prior_handler_state = [
        (handler, handler.level, handler.formatter) for handler in prior_handlers]
    prior_nvmeof_level = nvmeof_logger.level
    prior_root_level = root_logger.level
    try:
        yield GatewayConfig(path)
    finally:
        for handler in list(nvmeof_logger.handlers):
            if handler not in prior_handlers:
                nvmeof_logger.removeHandler(handler)
                handler.close()
        for handler, level, formatter in prior_handler_state:
            handler.setLevel(level)
            handler.setFormatter(formatter)
        nvmeof_logger.setLevel(prior_nvmeof_level)
        root_logger.setLevel(prior_root_level)
        GatewayLogger.logger = prior_logger
        GatewayLogger.handler = prior_handler
        GatewayLogger.init_executed = prior_init
        shutil.rmtree(directory, ignore_errors=True)


def test_snapshot_applies_good_key_and_rejects_bad_key(live_config):
    applied = []

    def handler(section, key, value):
        if key == "max_ns_to_change_lb_grp":
            raise ValueError("bad")
        applied.append((section, key, value))

    rejects = live_config.apply_snapshot([
        _entry("gateway", "rebalance_period_sec", True, "11", CONFIG_TYPE_INT),
        _entry("gateway", "max_ns_to_change_lb_grp", True, "nope", CONFIG_TYPE_INT),
    ], handler)
    assert live_config.getint_with_default("gateway", "rebalance_period_sec", 7) == 11
    assert live_config.getint_with_default("gateway", "max_ns_to_change_lb_grp", 8) == 8
    assert applied == [("gateway", "rebalance_period_sec", 11)]
    assert len(rejects) == 1
    assert rejects[0][1] == "max_ns_to_change_lb_grp"


def test_present_false_restores_conf_file(live_config):
    live_config.apply_snapshot([
        _entry("gateway", "rebalance_period_sec", True, "11", CONFIG_TYPE_INT),
    ], lambda section, key, value: None)
    rejects = live_config.apply_snapshot([
        _entry("gateway", "rebalance_period_sec", False, "", CONFIG_TYPE_INT),
    ], lambda section, key, value: None)
    assert rejects == []
    assert live_config.getint_with_default("gateway", "rebalance_period_sec", 0) == 7


def test_present_false_without_conf_option_uses_fallback(live_config):
    called = []

    def handler(section, key, value):
        called.append((section, key, value))

    live_config.apply_snapshot([
        _entry("gateway", "ping_spdk_under_lock", True, "true", CONFIG_TYPE_BOOL),
    ], handler)
    assert live_config.getboolean_with_default("gateway", "ping_spdk_under_lock", False) is True
    called.clear()
    rejects = live_config.apply_snapshot([
        _entry("gateway", "ping_spdk_under_lock", False, "", CONFIG_TYPE_BOOL),
    ], handler)
    assert rejects == []
    assert called == []
    assert live_config.getboolean_with_default("gateway", "ping_spdk_under_lock", False) is False


def test_unknown_type_is_rejected(live_config):
    live_config.apply_snapshot([
        _entry("gateway", "rebalance_period_sec", True, "11", CONFIG_TYPE_INT),
    ], lambda section, key, value: None)
    rejects = live_config.apply_snapshot([
        _entry("gateway", "rebalance_period_sec", True, "4", CONFIG_TYPE_UNKNOWN),
    ], lambda section, key, value: None)
    assert len(rejects) == 1
    assert "unknown config type" in rejects[0][2]
    assert live_config.getint("gateway", "rebalance_period_sec") == 11


def test_loops_read_the_snapshot_interval(live_config):
    live_config.apply_snapshot([
        _entry("gateway", "spdk_ping_interval_in_seconds", True, "9.5", CONFIG_TYPE_FLOAT),
        _entry("gateway", "rebalance_period_sec", True, "3", CONFIG_TYPE_INT),
        _entry("gateway", "state_update_interval_sec", True, "12", CONFIG_TYPE_INT),
        _entry("spdk", "notifications_interval", True, "0", CONFIG_TYPE_INT),
        _entry("gateway-logs", "log_level", True, "DEBUG", CONFIG_TYPE_STR),
    ], lambda section, key, value: None)
    assert live_config.getfloat_with_default("gateway", "spdk_ping_interval_in_seconds", 2.0) == 9.5
    assert live_config.getint_with_default("gateway", "rebalance_period_sec", 7) == 3
    assert live_config.getint_with_default("gateway", "state_update_interval_sec", 5) == 12
    assert live_config.getint_with_default("spdk", "notifications_interval", 60) == 0
    assert live_config.get_with_default("gateway-logs", "log_level", "INFO") == "DEBUG"


def test_handler_failure_keeps_last_timeout(live_config):
    timeouts = []

    def handler(section, key, value):
        if key == "timeout" and value <= 0:
            raise ValueError("timeout")
        if key == "timeout":
            timeouts.append(value)

    live_config.apply_snapshot([
        _entry("spdk", "timeout", True, "10", CONFIG_TYPE_FLOAT),
    ], handler)
    rejects = live_config.apply_snapshot([
        _entry("spdk", "timeout", True, "0", CONFIG_TYPE_FLOAT),
        _entry("gateway", "rebalance_period_sec", True, "4", CONFIG_TYPE_INT),
    ], handler)
    assert timeouts == [10.0]
    assert len(rejects) == 1
    assert live_config.getfloat_with_default("spdk", "timeout", 60.0) == 10.0
    assert live_config.getint_with_default("gateway", "rebalance_period_sec", 7) == 4


def test_logger_early_return_keeps_rotation_fields(live_config):
    GatewayLogger.logger = None
    GatewayLogger.handler = None
    GatewayLogger.init_executed = False
    first = GatewayLogger(live_config)
    second = GatewayLogger(live_config)
    assert second.max_log_file_size_in_mb == 10
    assert second.max_log_files_count == 20
    assert second.handler is first.handler
    root = logging.getLogger()
    root_formatters = [handler.formatter for handler in root.handlers]
    second.apply_runtime_config("verbose_log_messages", False)
    assert [handler.formatter for handler in root.handlers] == root_formatters
    second.apply_runtime_config("max_log_file_size_in_mb", 3)
    second.apply_runtime_config("max_log_files_count", 4)
    second.apply_runtime_config("log_files_rotation_enabled", True)
    assert second.handler.maxBytes == 3 * 1024 * 1024
    assert second.handler.backupCount == 4
    assert second.handler.rotator == GatewayLogger.log_file_rotate
    with pytest.raises(ValueError):
        second.apply_runtime_config("log_level", "not-a-level")
