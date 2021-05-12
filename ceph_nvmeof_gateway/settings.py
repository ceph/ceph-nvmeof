import configparser
import logging


def _convert_str_to_bool(value):
    """
    Convert true/false/yes/no/1/0 to boolean
    """

    if isinstance(value, bool):
        return value

    value = str(value).lower()
    if value in ['1', 'true', 'yes']:
        return True
    elif value in ['0', 'false', 'no']:
        return False
    raise ValueError(value)


class Setting(object):
    def __init__(self, name, type_str, def_val):
        self.name = name
        self.type_str = type_str
        self.def_val = def_val

    def __contains__(self, key):
        return key == self.def_val


class BoolSetting(Setting):
    def __init__(self, name, def_val):
        super(BoolSetting, self).__init__(name, "bool", def_val)

    def to_str(self, norm_val):
        if norm_val:
            return "true"
        else:
            return "false"

    def normalize(self, raw_val):
        try:
            # for compat we also support Yes/No and 1/0
            return _convert_str_to_bool(raw_val)
        except ValueError:
            raise ValueError("expected true or false for {}".format(self.name))


class StrSetting(Setting):
    def __init__(self, name, def_val):
        super(StrSetting, self).__init__(name, "str", def_val)

    def to_str(self, norm_val):
        return str(norm_val)

    def normalize(self, raw_val):
        return str(raw_val)


class IntSetting(Setting):
    def __init__(self, name, min_val, max_val, def_val):
        self.min_val = min_val
        self.max_val = max_val
        super(IntSetting, self).__init__(name, "int", def_val)

    def to_str(self, norm_val):
        return str(norm_val)

    def normalize(self, raw_val):
        try:
            val = int(raw_val)
        except ValueError:
            raise ValueError("expected integer for {}".format(self.name))

        if val < self.min_val:
            raise ValueError("expected integer >= {} for {}".
                             format(self.min_val, self.name))
        if val > self.max_val:
            raise ValueError("expected integer <= {} for {}".
                             format(self.max_val, self.name))
        return val


class Settings:
    def __init__(self, cli_overrides=None):
        self._cli_overrides = cli_overrides or {}
        self._attrs = {}
        self._attrs['config'] = Config()

    def load(self, config_file=None):
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file or self._cli_overrides['conf'])

        if config_parser.has_section('config'):
            self.config.refresh(config_parser['config'],
                                self._cli_overrides)

    def __getattr__(self, name):
        return self._attrs[name]


class Config:
    DEFAULTS = {
        "api_host": StrSetting("api_host", "::"),
        "api_port": IntSetting("api_port", 1, 65535, 5000),

        "ceph_config": StrSetting("ceph_config", ""),
        "client_name": StrSetting("client_name", "client.admin"),
        "pool_name": StrSetting("pool_name", "rbd"),
        "db_name": StrSetting("db_name", "ceph_nvmeof_gateway"),

        "logger_level": IntSetting("logger_level", logging.DEBUG, logging.CRITICAL,
                                   logging.DEBUG),
        "log_to_stderr": BoolSetting("log_to_stderr", True),
        "log_to_stderr_prefix": StrSetting("log_to_stderr_prefix", ""),
        "log_to_file": BoolSetting("log_to_file", False),
    }

    def __init__(self):
        self._attrs = self._get_defaults()

    def refresh(self, options, cli_overrides):
        attrs = self._get_defaults()
        for key, setting in Config.DEFAULTS:
            if key in options:
                attrs[key] = setting.normalize(options[key])
        self._attrs = attrs

    def _get_defaults(self):
        return {key: setting.def_val for (key, setting) in Config.DEFAULTS.items()}

    def __getattr__(self, name):
        return self._attrs[name]
