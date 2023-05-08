#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import os
import socket
from pathlib import Path
from typing import Optional
from pydantic import (BaseModel, Extra, IPvAnyAddress, PositiveInt, Json,
                      FilePath, conint, constr, validator, root_validator)


class GatewaySubConfig(BaseModel):
    """Validates config file settings and types for gateway.

    Required fields report error on missing keys and on invalid values for 
    present keys.

    Required Fields:
        name: Gateway identifier (default: local hostname)
        group: Gateway group identifier
        addr: Gateway server listener IP address
        port: Gateway server listener port (1-65535)
        enable_auth: Use mTLS for client-server authentication
        state_update_notify: Enable watch/notify for updates
        state_update_interval_sec: Polling interval for updates (>0s)
    """
    name: str
    group: str
    addr: IPvAnyAddress
    port: conint(ge=1, le=65535)
    enable_auth: bool
    state_update_notify: bool
    state_update_interval_sec: PositiveInt

    @validator('name')
    def set_default_name(cls, name):
        """Sets gateway name to the local hostname if unspecified in config."""
        if not name:
            return socket.gethostname()
        return name


class CephSubConfig(BaseModel):
    """Validates config file settings and types for ceph.

    Required fields report error on missing keys and on invalid values for 
    present keys.

    Required Fields:
        pool: Ceph pool to store gateway state object
        config_file: Ceph config file
    """
    pool: constr(min_length=1)
    config_file: str

    @validator('config_file')
    def check_log_level(cls, config_file):
        """Confirms the config file exists at specified path."""
        if not os.path.isfile(Path(config_file)):
            raise ValueError(f'Invalid Ceph config file path: {config_file}')
        return config_file


class MtlsSubConfig(BaseModel):
    """Validates config file settings and types for MTLS.

    Required fields report error on missing keys and on invalid values for 
    present keys.

    Required Fields:
        server_key: Server key
        client_key: Client key
        server_cert: Server certificate
        client_cert: Client certificate
    """
    server_key: str
    client_key: str
    server_cert: str
    client_cert: str


class SpdkSubConfig(BaseModel, extra=Extra.allow):
    """Validates config file settings and types for SPDK.

    Allows extra fields.

    Required fields report error on missing keys and on invalid values for 
    present keys. Required-Optional and Optional fields do not report error on 
    missing keys, but report error on invalid values on present keys. 
    Required-Optional fields set values to defaults on missing keys. Optional 
    fields set values to None on missing keys.

    Required Fields:
        spdk_path: Path to parent directoy of SPDK
        tgt_path: Relative path to SPDK target application
        rpc_socket: RPC domain socket path
        timeout: Timeout in seconds to wait for RPC response
        log_level: Log level for SPDK
        

    Required-Optional Fields:
        conn_retries: Number of tries to connect to the RPC server (default: 10)
        transports: Transport types (default: tcp)

    Optional Fields:
        tgt_cmd_extra_args: Extra arguments to SPDK target application
        transport_tcp_options: JSON options for initializing tcp transport
    """
    spdk_path: str
    tgt_path: str
    rpc_socket: str
    timeout: float
    log_level: constr(to_upper=True)
    conn_retries: int = 10
    tgt_cmd_extra_args: Optional[str]
    transports: constr(to_lower=True) = "tcp"
    transport_tcp_options: Optional[Json]

    @root_validator
    def check_spdk_path(cls, values):
        """Confirms the SPDK target application exists at specified path."""
        spdk_path, tgt_path = values.get('spdk_path'), values.get('tgt_path')
        path = os.path.join(spdk_path, tgt_path)
        if not os.path.isfile(path):
            raise ValueError(f'Invalid SPDK target path: {path}')
        return values

    @validator('log_level')
    def check_log_level(cls, level):
        """Confirms the log level is valid in SPDK."""
        valid = ["ERROR", "WARNING", "NOTICE", "INFO", "DEBUG"]
        if level not in valid:
            raise ValueError(f"Invalid log level. Must be one of: {valid}")
        return level


class GatewayConfig(BaseModel):
    """Validates and returns formatted config file settings."""
    DEFAULT: dict
    gateway: GatewaySubConfig
    ceph: CephSubConfig
    mtls: MtlsSubConfig
    spdk: SpdkSubConfig
