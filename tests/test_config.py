import pytest
import socket
from pydantic import ValidationError
from unittest import mock
from control.config import (GatewaySubConfig, CephSubConfig, MtlsSubConfig,
                            SpdkSubConfig)


class TestGatewaySubConfig:
    """Tests for gateway section of GatewayConfig."""

    @pytest.fixture(autouse=True)
    def config_init(self) -> None:
        """Initializes test config with sample values."""
        self.config = {
            "name": "GatewayName",
            "group": "GatewayGroup",
            "addr": "127.0.0.1",
            "port": "5500",
            "enable_auth": "False",
            "state_update_notify": "True",
            "state_update_interval_sec": "5"
        }

    def test_validate_config(self):
        """Confirms successful parsing of config."""
        GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("field", [
        "name", "group", "addr", "port", "enable_auth", "state_update_notify",
        "state_update_interval_sec"
    ])
    def test_missing_field(self, field):
        """Confirms error on missing field."""
        self.config.pop(field)
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)

    def test_name_missing_value_set_default(self):
        """Confirms name defaults to local hostname on missing value."""
        self.config["name"] = ""
        parsed = GatewaySubConfig.parse_obj(self.config)
        assert parsed.name == socket.gethostname()

    def test_group_missing_value_pass(self):
        """Confirms lack of error on missing group value."""
        self.config["group"] = ""
        GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value",
                             ["", None, "NotParsableAsIPAddr", -1])
    def test_addr_invalid(self, bad_value):
        """Confirms error on invalid addr value."""
        self.config["addr"] = bad_value
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value",
                             ["", None, -1, 0, 65536, "NotParsableAsInt"])
    def test_port_invalid(self, bad_value):
        """Confirms error on invalid port value."""
        self.config["port"] = bad_value
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value", ["", None, "NotParsableAsBool"])
    def test_enable_auth_invalid(self, bad_value):
        """Confirms error on invalid enable_auth value."""
        self.config["enable_auth"] = bad_value
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value", ["", None, "NotParsableAsBool"])
    def test_state_update_notify_invalid(self, bad_value):
        """Confirms error on invalid state_update_notify value."""
        self.config["state_update_notify"] = bad_value
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value",
                             ["", None, -1, 0, "NotParsableAsInt"])
    def test_state_update_interval_sec_invalid(self, bad_value):
        """Confirms error on invalid state_update_interval_sec value."""
        self.config["state_update_interval_sec"] = bad_value
        with pytest.raises(ValidationError):
            GatewaySubConfig.parse_obj(self.config)


@mock.patch("os.path.isfile", return_value=True)
class TestCephSubConfig:
    """Tests for ceph section of GatewayConfig.
    
    Mocks success for config_file path validation.
    """

    @pytest.fixture(autouse=True)
    def config_init(self) -> None:
        """Initializes test config with sample values."""
        self.config = {"pool": "rbd", "config_file": "/etc/ceph/ceph.conf"}

    def test_validate_config(self, mock_config_exists):
        """Confirms successful parsing of config."""
        CephSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("field", ["pool", "config_file"])
    def test_missing_field(self, mock_config_exists, field):
        """Confirms error on missing field."""
        self.config.pop(field)
        with pytest.raises(ValidationError):
            CephSubConfig.parse_obj(self.config)

    def test_pool_missing_value(self, mock_config_exists):
        """Confirms error on missing pool value."""
        self.config["pool"] = ""
        with pytest.raises(ValidationError):
            CephSubConfig.parse_obj(self.config)


class TestMtlsSubConfig:
    """Tests for mtls section of GatewayConfig."""

    @pytest.fixture(autouse=True)
    def config_init(self) -> None:
        """Initializes test config with sample values."""
        self.config = {
            "server_key": "./server.key",
            "client_key": "./client.key",
            "server_cert": "./server.crt",
            "client_cert": "./client.crt"
        }

    def test_validate_config(self):
        """Confirms successful parsing of config."""
        MtlsSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize(
        "field", ["server_key", "client_key", "server_cert", "client_cert"])
    def test_missing_field(self, field):
        """Confirms error on missing field."""
        self.config.pop(field)
        with pytest.raises(ValidationError):
            MtlsSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize(
        "field", ["server_key", "client_key", "server_cert", "client_cert"])
    def test_missing_value_pass(self, field):
        """Confirms lack of error on missing field values."""
        self.config[field] = ""
        MtlsSubConfig.parse_obj(self.config)


@mock.patch("os.path.isfile", return_value=True)
class TestSpdkSubConfig:
    """Tests for spdk section of GatewayConfig.
    
    Mocks success for spdk target path validation.
    """

    @pytest.fixture(autouse=True)
    def config_init(self) -> None:
        """Initializes test config with sample values."""
        self.config = {
            "spdk_path": "/path/to/spdk",
            "tgt_path": "spdk/build/bin/nvmf_tgt",
            "rpc_socket": "/var/tmp/spdk.sock",
            "timeout": "60.0",
            "log_level": "ERROR",
            "conn_retries": "10",
            "tgt_cmd_extra_args": "-m 0x3 -L all",
            "transports": "tcp",
            "transport_tcp_options": "{\"max_queue_depth\": 16}",
        }

    def test_validate_config(self, mock_spdk_exists):
        """Confirms successful parsing of config."""
        SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize(
        "field",
        ["spdk_path", "tgt_path", "rpc_socket", "timeout", "log_level"])
    def test_missing_field(self, mock_spdk_exists, field):
        """Confirms error on missing field."""
        self.config.pop(field)
        with pytest.raises(ValidationError):
            SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("field, default",
                             [("conn_retries", 10), ("transports", "tcp"),
                              ("tgt_cmd_extra_args", None),
                              ("transport_tcp_options", None)])
    def test_missing_field_set_default(self, mock_spdk_exists, field, default):
        """Confirms missing fields in config are set to defaults."""
        self.config.pop(field)
        parsed = SpdkSubConfig.parse_obj(self.config)
        assert getattr(parsed, field) == default

    @pytest.mark.parametrize("bad_value", ["", None, "NotParsableAsFloat"])
    def test_timeout_invalid(self, mock_spdk_exists, bad_value):
        """Confirms error on invalid timeout value."""
        self.config["timeout"] = bad_value
        with pytest.raises(ValidationError):
            SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value", ["", None, "NotALogLevel", 100])
    def test_log_level_invalid(self, mock_spdk_exists, bad_value):
        """Confirms error on invalid log_level value."""
        self.config["log_level"] = bad_value
        with pytest.raises(ValidationError):
            SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("good_value", [
        "error", "ERROR", "warning", "WARNING", "info", "INFO", "notice",
        "NOTICE", "debug", "DEBUG"
    ])
    def test_log_level_valid(self, mock_spdk_exists, good_value):
        """Confirms lack of error on valid log_level value."""
        self.config["log_level"] = good_value
        SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value", ["", None, "NotParsableAsInt"])
    def test_conn_retries_invalid(self, mock_spdk_exists, bad_value):
        """Confirms error on invalid conn_retries value."""
        self.config["conn_retries"] = bad_value
        with pytest.raises(ValidationError):
            SpdkSubConfig.parse_obj(self.config)

    @pytest.mark.parametrize("bad_value", [-1, "NotParsableAsJSON"])
    def test_transport_tcp_options_invalid(self, mock_spdk_exists, bad_value):
        """Confirms error on invalid transport_tcp_options value."""
        self.config["transport_tcp_options"] = bad_value
        with pytest.raises(ValidationError):
            SpdkSubConfig.parse_obj(self.config)

    def test_extra_field(self, mock_spdk_exists):
        """Confirms lack of error on extra field not specified in class."""
        self.config["fake_field"] = "fake_value"
        parsed = SpdkSubConfig.parse_obj(self.config)
        assert parsed.fake_field == "fake_value"
