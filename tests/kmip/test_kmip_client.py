#  ############################
#  Copyright (c) 2026 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gadi.didi@ibm.com
#

"""
Test script for NVMeoFKMIPClient

Prerequisites:
1. PyKMIP mock server installed: pip install pykmip
2. Generate test certificates (see setup_certificates.sh below)
3. Run this script: python3 test_kmip_client.py
"""

import subprocess
import time
import sys
import os
from typing import List, Tuple
from kmip.pie import client
from kmip.pie import objects
from kmip import enums


# Get the project root (2 levels up from tests/kmip/)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from control.kmip_client import NVMeoFKMIPClient     # noqa: E402
from control.config import GatewayConfig             # noqa: E402

print(f"Python path: {project_root}")


def print_separator():
    print("=" * 70)


class KMIPServerManager:
    """Manages multiple KMIP mock server endpoints for testing"""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.endpoints = []  # List of (hostname, port, process)

    def start_server(self, hostname: str, port: int) -> None:
        """Start a KMIP mock server subprocess"""

        # Get path to dummy server script
        server_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'dummy_kmip_server.py'
        )
        # Start server process
        process = subprocess.Popen(
            [
                sys.executable,
                server_script,
                '--address', hostname,
                '--port', str(port),
                '--base-dir', self.base_dir
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.endpoints.append((hostname, port, process))

        # Wait for server to be ready and print output
        time.sleep(2)

        # Check if process is still running
        if process.poll() is not None:
            # Process died
            output = process.stdout.read()
            print(f"Server {hostname}:{port} failed to start:")
            print(output)
            raise RuntimeError(f"Server {hostname}:{port} failed to start")

        print(f"Started KMIP server {hostname}:{port} (PID: {process.pid})")

    def stop_all(self):
        """Stop all running endpoints"""
        for hostname, port, process in self.endpoints:
            if process.poll() is None:  # Still running
                print(f"Stopping server endpoint {hostname}:{port} (PID: {process.pid})")
                try:
                    process.terminate()
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

        # remove the database files (for next test run)
        for hostname, port, _ in self.endpoints:
            db_path = os.path.join(self.base_dir, f'kmip_server_{port}.db')
            if os.path.exists(db_path):
                os.remove(db_path)

        self.endpoints.clear()


def setup_test_keys(endpoints: List[Tuple[str, int]], base_dir: str):
    """
    Populate KMIP server endpoints with test keys in non-overlapping ranges

    Server endpoint 1: key-1, key-2, key-3 -> UUIDs 1,2,3
    Server endpoint 2: key-1, key-2, key-3, key-4, key-5  -> UUIDs 1,2,3,4,5
    Server endpoint 3: key-1, key-2, key-3, key-4, key-5, key-6, key-7 -> UUIDs 1,2,3,4,5,6,7

    This way:
    - key-1, key-2, key-3 exist on ALL server endpoints (UUIDs 1,2,3)
    - key-4, key-5 exist ONLY on server endpoints 2&3 (UUIDs 4,5)
    - key-6, key-7 exist ONLY on server endpoint 3 (UUIDs 6,7)
    """

    keys_per_server = [
        (['key-1', 'key-2', 'key-3'], endpoints[0]),                            # Endpoint 1: 3 keys
        (['key-1', 'key-2', 'key-3', 'key-4', 'key-5'], endpoints[1]),          # Endpoint 2: 5 keys
        (['key-1', 'key-2', 'key-3', 'key-4', 'key-5',
          'key-6', 'key-7'], endpoints[2])                                      # Endpoint 3: 7 keys
    ]

    # Store mapping: {key_name: {(hostname, port): uuid}}
    key_map = {}

    for key_names, (hostname, port) in keys_per_server:
        server_num = keys_per_server.index((key_names, (hostname, port))) + 1
        print(f"\nPopulating Server endpoint {server_num} - ({hostname}:{port}) with "
              f"keys: {key_names}")

        c = client.ProxyKmipClient(
            hostname=hostname,
            port=port,
            cert=f'{base_dir}/certs/client_cert.pem',
            key=f'{base_dir}/certs/client_key.pem',
            ca=f'{base_dir}/certs/ca_cert.pem'
        )

        c.open()

        for key_name in key_names:
            try:
                # Create AES-256 key
                key_uuid = c.create(
                    algorithm=enums.CryptographicAlgorithm.AES,
                    length=256,
                    name=key_name
                )

                # Activate it
                c.activate(key_uuid)

                # Store mapping
                if key_name not in key_map:
                    key_map[key_name] = {}
                key_map[key_name][(hostname, port)] = key_uuid

                print(f"  Created key '{key_name}' with UUID: {key_uuid}")
            except Exception as e:
                print(f"  Error creating key '{key_name}': {e}")

            try:
                # Create also passphrases using key names
                secret_data = objects.SecretData(key_name.encode(),
                                                 enums.SecretDataType.PASSWORD,
                                                 masks=[enums.CryptographicUsageMask.DERIVE_KEY])
                password_id = c.register(secret_data)
                c.activate(password_id)
                print(f"  Created passphrase '{key_name}' with ID: {password_id}")
            except Exception as e:
                print(f"  Error creating passphrase '{key_name}': {e}")

        c.close()
    return key_map


def create_test_config(base_dir: str) -> GatewayConfig:
    """Create a minimal test config"""
    config_path = f"{base_dir}/test_gateway.conf"

    with open(config_path, 'w') as f:
        f.write("""[gateway]
name=test-gateway

[gateway-logs]
log_directory=/tmp/kmip_test_logs
log_files_enabled=false
log_level=DEBUG
""")

    return GatewayConfig(config_path)


def run_tests(base_dir: str, key_map: dict):
    """Run test scenarios"""

    print_separator()
    print("KMIP CLIENT TEST SUITE")
    print_separator()

    # Create config
    config = create_test_config(base_dir)

    # Initialize KMIP client
    kmip_client = NVMeoFKMIPClient(
        logger_config=config,
        cert_path=f'{base_dir}/certs/client_cert.pem',
        key_path=f'{base_dir}/certs/client_key.pem',
        ca_path=f'{base_dir}/certs/ca_cert.pem'
    )

    endpoints = [
        ('127.0.0.1', 5696),
        ('127.0.0.1', 5697),
        ('127.0.0.1', 5698)
    ]

    # Track test results
    total_tests = 9
    passed_tests = 0

    print("\nKey Distribution:")
    print("  Server 1 (5696): key-1, key-2, key-3              (UUIDs: 1,2,3)")
    print("  Server 2 (5697): key-1, key-2, key-3, key-4, key-5    (UUIDs: 1,2,3,4,5)")
    print("  Server 3 (5698): key-1, key-2, key-3, key-4, key-5, "
          "key-6, key-7 (UUIDs: 1,2,3,4,5,6,7)")
    print()

    # Test Case 1: Key exists on first server
    print_separator()
    print("TEST 1: Key exists on first server (GREEN CASE)")
    print_separator()
    try:
        key1_uuid = key_map['key-1'][endpoints[0]]
        key = kmip_client.get_key_for_rbd_image(key1_uuid, endpoints)
        print(f"SUCCESS: Retrieved key-1 ({len(key)} bytes)")
        print(f"  First 8 bytes: {key[:8].hex()}")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 2: Key-4 NOT on server 1, exists on endpoints 2&3
    print_separator()
    print("TEST 2: Key-4 missing on server 1, exists on endpoints 2&3")
    print_separator()
    try:
        # key-4 only exists on server 2
        key4_uuid = key_map['key-4'][endpoints[1]]
        key = kmip_client.get_key_for_rbd_image(key4_uuid, endpoints)
        print(f"SUCCESS: Retrieved key-4 from server 2 ({len(key)} bytes)")
        print(f"  First 8 bytes: {key[:8].hex()}")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 3: Key exists on multiple endpoints (should get from first available)
    print_separator()
    print("TEST 3: Key exists on multiple endpoints")
    print_separator()
    try:
        # key-2 exists on both server 1 and server 2
        key2_uuid = key_map['key-2'][endpoints[0]]
        key = kmip_client.get_key_for_rbd_image(key2_uuid, endpoints)
        print(f"SUCCESS: Retrieved key-2 (should be from server 1) ({len(key)} bytes)")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 4: Connection reuse
    print_separator()
    print("TEST 4: Connection reuse (fetch key-1 again)")
    print_separator()
    try:
        key1_uuid = key_map['key-1'][endpoints[0]]
        key = kmip_client.get_key_for_rbd_image(key1_uuid, endpoints)
        print(f"SUCCESS: Retrieved key-1 using cached connection ({len(key)} bytes)")
        print(f"  Active connections: {len(kmip_client.active_connections)}")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 5: Key doesn't exist on any server
    print_separator()
    print("TEST 5: Key doesn't exist on any server")
    print_separator()
    try:
        junk_uuid = '99999'  # FYI - the dummy kmip server use simple integer strings as UUIDs..
        key = kmip_client.get_key_for_rbd_image(junk_uuid, endpoints)
        print(f"UNEXPECTED: Should have failed but got key ({len(key)} bytes)")
    except Exception as e:
        print("SUCCESS: Correctly failed with error")
        print(f"  Error: {e}")
        passed_tests += 1

    # Test Case 6: Wrong server address
    print_separator()
    print("TEST 6: Cannot connect to server (wrong address)")
    print_separator()
    bad_endpoints = [
        ('192.0.2.1', 5696),  # Non-routable address
        ('127.0.0.1', 5697)   # Good address
    ]
    try:
        # key-4 exists on server 2
        key4_uuid = key_map['key-4'][endpoints[1]]
        key = kmip_client.get_key_for_rbd_image(key4_uuid, bad_endpoints)
        print(f"SUCCESS: Failover worked, got key from endpoint 2 ({len(key)} bytes)")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 7: All endpoints unreachable
    print_separator()
    print("TEST 7: All endpoints unreachable")
    print_separator()
    bad_endpoints_all = [
        ('192.0.2.1', 5696),
        ('192.0.2.2', 5697),
    ]
    try:
        key1_uuid = key_map['key-1'][endpoints[0]]
        key = kmip_client.get_key_for_rbd_image(key1_uuid, bad_endpoints_all)
        print("UNEXPECTED: Should have failed but got key")
    except Exception as e:
        print("SUCCESS: Correctly failed")
        print(f"  Error: {str(e)[:100]}...")
        passed_tests += 1

    # Test Case 8: Different key from different server
    print_separator()
    print("TEST 8: Fetch key-6 (only on server 3)")
    print_separator()
    try:
        key6_uuid = key_map['key-6'][endpoints[2]]
        key = kmip_client.get_key_for_rbd_image(key6_uuid, endpoints)
        print(f"SUCCESS: Retrieved key-6 from server 3 ({len(key)} bytes)")
        print(f"  Active connections: {len(kmip_client.active_connections)}")
        for server_key in kmip_client.active_connections:
            print(f"    - {server_key[0]}:{server_key[1]}")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    # Test Case 9: Fetch from subset of endpoints
    print_separator()
    print("TEST 9: Fetch key using only endpoints 2 and 3")
    print_separator()
    subset_endpoints = [
        ('127.0.0.1', 5697),
        ('127.0.0.1', 5698)
    ]
    try:
        # key-5 exists on both server 2 and 3
        key5_uuid = key_map['key-5'][endpoints[1]]
        key = kmip_client.get_key_for_rbd_image(key5_uuid, subset_endpoints)
        print(f"SUCCESS: Retrieved key-5 from subset ({len(key)} bytes)")
        passed_tests += 1
    except Exception as e:
        print(f"FAILED: {e}")

    print_separator()
    print("TEST RESULTS")
    print_separator()
    print(f"Total tests: {total_tests}")
    print(f"Passed:      {passed_tests}")
    print(f"Failed:      {total_tests - passed_tests}")
    print_separator()

    # Cleanup
    print_separator()
    print("Cleanup: Disconnecting all connections")
    print_separator()
    kmip_client.disconnect_all()
    print(f"Disconnected. Active connections: {len(kmip_client.active_connections)}")


def main():
    """Main test runner"""

    # first call to setup_kmip_test.sh for initial setup (generate certs, create config, etc)

    try:
        setup_script = os.path.join(project_root, "tests", "kmip", "setup_kmip_test.sh")
        result = subprocess.run([setup_script, "/tmp/kmip_test"], check=True,
                                capture_output=True, text=True)
        print("Setup script output:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Setup script failed: {e}")
        sys.exit(1)

    base_dir = "/tmp/kmip_test"

    # Create directory structure
    os.makedirs(f"{base_dir}/certs", exist_ok=True)
    os.makedirs(f"{base_dir}/policies", exist_ok=True)
    os.makedirs(f"{base_dir}/logs", exist_ok=True)

    print("KMIP Client Test Setup")
    print("=" * 70)

    # Check if certificates exist
    if not os.path.exists(f"{base_dir}/certs/ca_cert.pem"):
        print("\nCertificates not found!")
        print("\nPlease run the setup script first:")
        print(f"  ./setup_kmip_test.sh {base_dir}")
        sys.exit(1)

    print("Certificates found")

    # Start server endpoints
    print("\nStarting KMIP mock server endpoints...")
    server_manager = KMIPServerManager(base_dir)

    try:
        # Start 3 server endpoints on different ports
        endpoints = [
            ('127.0.0.1', 5696),
            ('127.0.0.1', 5697),
            ('127.0.0.1', 5698)
        ]

        for hostname, port in endpoints:
            server_manager.start_server(hostname, port)

        print("All server endpoints started")

        # Populate server endpoints with keys
        print("\nPopulating server endpoints with test keys...")
        key_map = setup_test_keys(endpoints, base_dir)
        print("Keys created")

        # Run tests
        run_tests(base_dir, key_map)

        print_separator()
        print("TEST SUITE COMPLETE")
        print_separator()

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\nTest setup failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nStopping all server endpoints...")
        server_manager.stop_all()
        print("All server endpoints stopped")


if __name__ == '__main__':
    main()
