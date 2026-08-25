#  ############################
#  Copyright (c) 2026 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gadi.didi@ibm.com
#

from typing import List, Optional, Dict, Tuple
import ssl
import time
from kmip.pie import client
from kmip.pie import exceptions

from .utils import GatewayLogger
from .config import GatewayConfig


class NVMeoFKMIPClient:

    """
    NVMeoFKMIPClient for NVMe-oF gateway
    Retrieves encryption keys and stores them in memory (volatile)
    """

    class KMIPConnection:
        """Single KMIP connection"""

        def __init__(
            self,
            hostname: str,
            port: int,
            cert: str,
            key: str,
            ca: str,
            gw_logger_object: GatewayLogger,
        ):
            """
            Initialize KMIP connection

            Args:
                hostname: KMIP server endpoint hostname
                port: KMIP server endpoint port
                cert: Client certificate path
                key: Client key path
                ca: CA certificate path
                logger: Logger instance
            """
            self.logger = gw_logger_object.logger
            self.hostname = hostname
            self.port = port
            self.cert = cert
            self.key = key
            self.ca = ca

            self.client: Optional[client.ProxyKmipClient] = None
            self.connected = False

        def connect(self) -> None:
            """Establish connection to KMIP server endpoint"""
            if self.connected:
                return
            try:
                self.logger.debug(f"Connecting to KMIP server endpoint {self.hostname}:{self.port}")
                start_time = time.monotonic()

                # Create KMIP client
                self.client = client.ProxyKmipClient(
                    hostname=self.hostname,
                    port=self.port,
                    cert=self.cert,
                    key=self.key,
                    ca=self.ca
                )

                # Open connection
                self.client.open()

                elapsed = time.monotonic() - start_time
                self.connected = True
                self.logger.info(f"Connected to KMIP server endpoint {self.hostname}:{self.port} "
                                 f"in {elapsed:.3f} seconds")
            except exceptions.ClientConnectionFailure:
                self.logger.warning(f"Connection already exists to endpoint "
                                    f"{self.hostname}:{self.port}")
                return

            except Exception:
                self.logger.exception(
                    f"Unexpected error connecting to endpoint {self.hostname}:{self.port}")
                self.connected = False
                self.client = None
                raise

        def disconnect(self) -> None:
            """Close connection"""
            if not self.connected or self.client is None:
                self.logger.debug(f"Already disconnected from endpoint {self.hostname}:{self.port}")
                return
            try:
                self.client.close()
                self.logger.debug(f"Disconnected from KMIP server endpoint "
                                  f"{self.hostname}:{self.port}")
            except Exception:
                self.logger.exception(f"Error disconnecting from endpoint "
                                      f"{self.hostname}:{self.port}")
                raise
            finally:
                self.connected = False
                self.client = None

        def get_key(self, key_uuid: str) -> bytes:
            """
            Retrieve encryption key from KMIP server endpoint

            Args:
                key_uuid: Unique identifier of the key

            Returns:
                bytes: Encryption key material

            Raises:
                RuntimeError: If not connected
                Exception: If retrieval fails after all retries
            """
            if not self.connected or self.client is None:
                raise RuntimeError("Not connected to KMIP server endpoint")

            # First attempt
            try:
                self.logger.debug(f"Retrieving key {key_uuid} from KMIP server endpoint "
                                  f"{self.hostname}:{self.port}")
                start_time = time.monotonic()

                result = self.client.get(key_uuid)

                elapsed = time.monotonic() - start_time
                key_bytes = result.value
                self.logger.info(f"Retrieved {len(key_bytes)}-byte key {key_uuid} "
                                 f"from {self.hostname}:{self.port} "
                                 f"in {elapsed:.3f} seconds")
                return key_bytes

            except (ssl.SSLError,      # SSL/TLS errors
                    OSError,           # OS/network errors
                    EOFError,          # Server closed connection
                    ConnectionError    # Connection errors
                    ) as e:
                # First attempt failed - connection likely stale
                first_attempt_elapsed = time.monotonic() - start_time
                self.logger.warning(
                    f"Connection error retrieving key {key_uuid} "
                    f"from {self.hostname}:{self.port} "
                    f"after {first_attempt_elapsed:.3f} seconds (attempt 1/2): "
                    f"{type(e).__name__}: {e}"
                )

            except Exception:
                # Non-connection errors (auth, key-not-found, KMIP operation failures)
                # Log with traceback then propagate to try next endpoint
                first_attempt_elapsed = time.monotonic() - start_time
                self.logger.exception(
                    f"Operation failed retrieving key {key_uuid} "
                    f"from {self.hostname}:{self.port} "
                    f"after {first_attempt_elapsed:.3f} seconds"
                )
                raise  # Propagate to get_key_for_rbd_image() to try next endpoint

            # Second attempt - reconnect and retry
            try:
                start_time = time.monotonic()

                # Reconnect
                self.logger.info(f"Attempting to reconnect to {self.hostname}:{self.port}")
                if self.client is not None:
                    try:
                        self.disconnect()
                    except Exception:
                        pass
                self.client = None
                self.connect()
                if not self.connected or self.client is None:
                    raise RuntimeError(
                        f"Reconnect to KMIP server endpoint {self.hostname}:{self.port} "
                        f"did not establish an active connection"
                    )
                self.logger.info(f"Successfully reconnected to {self.hostname}:{self.port}")

                # Retry operation
                self.logger.debug(f"Retrieving key {key_uuid} from {self.hostname}:{self.port} "
                                  f"(attempt 2/2)")

                result = self.client.get(key_uuid)

                elapsed = time.monotonic() - start_time
                key_bytes = result.value
                self.logger.info(f"Retrieved {len(key_bytes)}-byte key {key_uuid} "
                                 f"from {self.hostname}:{self.port} "
                                 f"in {elapsed:.3f} seconds (after reconnect)")
                return key_bytes

            except Exception:
                # Reconnect or retry failed
                elapsed = time.monotonic() - start_time
                self.logger.exception(
                    f"Failed to retrieve key {key_uuid} from {self.hostname}:{self.port} "
                    f"after reconnect (attempt 2/2) "
                    f"after {elapsed:.3f} seconds"
                )
                raise  # Preserve full traceback

    def __init__(self, logger_config: GatewayConfig, cert_path: str, key_path: str, ca_path: str):
        """
        Initialize KMIP client

        Args:
            logger_config: Logger configuration
            cert_path: Path to client certificate
            key_path: Path to client key
            ca_path: Path to CA certificate
        """
        self.gw_logger_object = GatewayLogger(logger_config)
        self.logger = self.gw_logger_object.logger
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path

        # Active connections: {hostname: KMIPConnection}
        self.active_connections: Dict[Tuple[str, int], NVMeoFKMIPClient.KMIPConnection] = {}

    def get_key_for_rbd_image(self, key_uuid: str, endpoints: List[Tuple[str, int]]) -> bytes:
        """
        Get encryption key for RBD image.
        Tries each server endpoint in order until key is found.

        Args:
            key_uuid: Key UUID string
            endpoints: List of KMIP server endpoints IP and port tuples

        Returns:
            bytes: Encryption key material

        Raises:
            Exception: If failed to connect to any server endpoint or retrieve key
            OR If key not found on any server endpoint.
        """
        overall_start = time.monotonic()
        errors = []
        for hostname, port in endpoints:
            try:
                # Get or create connection to this specific server endpoint
                conn = self._get_or_create_connection(hostname, port)
                # Try to get the key from this server
                key_bytes = conn.get_key(key_uuid)

                overall_elapsed = time.monotonic() - overall_start
                self.logger.info(f"Successfully retrieved key {key_uuid} from {hostname}:{port} "
                                 f"(total time: {overall_elapsed:.3f} seconds)")
                return key_bytes
            except Exception as e:
                # This server doesn't have the key or failed, try next
                error_detail = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
                self.logger.warning(f"Failed to get key {key_uuid} from {hostname}:{port}: "
                                    f"{error_detail}")
                errors.append(f"{hostname}:{port}: {error_detail}")
                continue

        # All endpoints failed
        overall_elapsed = time.monotonic() - overall_start
        error_msg = (f"Failed to retrieve key {key_uuid} from any KMIP server endpoint "
                     f"(tried for {overall_elapsed:.3f} seconds). Errors: {errors}")
        self.logger.error(error_msg)
        raise Exception(error_msg)

    def _get_or_create_connection(
            self,
            hostname: str,
            port: int) -> 'NVMeoFKMIPClient.KMIPConnection':
        """
        Get or create connection to a specific server endpoint

        Args:
            hostname: Server endpoint hostname
            port: Server endpoint port

        Returns:
            KMIPConnection: Working connection to this server endpoint

        Raises:
            Exception: If connection fails
        """
        server_endpoint_key = (hostname, port)

        # Check if we already have a connection to this server
        if server_endpoint_key in self.active_connections:
            conn = self.active_connections[server_endpoint_key]
            if conn.connected:
                self.logger.debug(f"Reusing existing connection to {hostname}:{port}")
                return conn

        # Create new connection
        start_time = time.monotonic()
        self.logger.debug(f"Creating new connection to {hostname}:{port}")

        conn = NVMeoFKMIPClient.KMIPConnection(
            hostname=hostname,
            port=port,
            cert=self.cert_path,
            key=self.key_path,
            ca=self.ca_path,
            gw_logger_object=self.gw_logger_object
        )

        # Connect
        conn.connect()

        # Store in active connections
        self.active_connections[server_endpoint_key] = conn

        elapsed = time.monotonic() - start_time
        self.logger.info(f"Successfully created and connected to {hostname}:{port} "
                         f"in {elapsed:.3f} seconds")
        return conn

    def disconnect_all(self) -> None:
        """Close all connections and clear cache"""
        self.logger.info("Disconnecting all KMIP connections")

        for server_endpoint_key, conn in list(self.active_connections.items()):
            try:
                conn.disconnect()
            except Exception:
                hostname, port = server_endpoint_key
                self.logger.exception(f"Error disconnecting from {hostname}:{port}")

        self.active_connections.clear()

        self.logger.info("All KMIP connections closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        """
        Clean up on exit
        Returns:
            False: Don't suppress exceptions
        """
        try:
            self.disconnect_all()
        except Exception:
            self.logger.exception("Error during cleanup of KMIP connections")
