#  ############################
#  Copyright (c) 2026 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gadi.didi@ibm.com
#

"""
KMIP Mock Server

Usage:
    # Use defaults (127.0.0.1:5696)
    python3 dummy_kmip_server.py

    # Custom port
    python3 dummy_kmip_server.py --port 5697

    # Custom address and port
    python3 dummy_kmip_server.py --address 127.0.0.1 --port 5697

    # Custom config file
    python3 dummy_kmip_server.py --config my_server.conf

    # Custom base directory
    python3 dummy_kmip_server.py --base-dir /tmp/kmip_test
"""

import argparse
from kmip.services.server import KmipServer
import os
import sys


def print_separator():
    print("=" * 70)


def create_default_config(address: str, port: int, base_dir: str) -> str:
    """Create default server config if it doesn't exist"""

    config_path = os.path.join(base_dir, f'server_{port}.conf')

    # Don't overwrite existing config
    if os.path.exists(config_path):
        return config_path

    # Ensure directories exist
    os.makedirs(os.path.join(base_dir, 'certs'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'policies'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)

    config_content = f"""[server]
hostname={address}
port={port}
certificate_path={base_dir}/certs/server_cert.pem
key_path={base_dir}/certs/server_key.pem
ca_path={base_dir}/certs/ca_cert.pem
auth_suite=TLS1.2
policy_path={base_dir}/policies
logging_level=INFO
database_path={base_dir}/kmip_server_{port}.db
"""

    with open(config_path, 'w') as f:
        f.write(config_content)

    print(f"Created default config: {config_path}")

    return config_path


def main():
    parser = argparse.ArgumentParser(
        description='KMIP Mock Server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--address', '-a',
        default='127.0.0.1',
        help='Server address (default: 127.0.0.1)'
    )

    parser.add_argument(
        '--port', '-p',
        type=int,
        default=5696,
        help='Server port (default: 5696)'
    )

    parser.add_argument(
        '--config', '-c',
        default=None,
        help='Path to server config file (default: auto-generated)'
    )

    parser.add_argument(
        '--base-dir', '-d',
        default='.',
        help='Base directory for config, logs, certs (default: current directory)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='DEBUG',
        help='Logging level (default: DEBUG)'
    )

    args = parser.parse_args()

    # Resolve paths
    base_dir = os.path.abspath(args.base_dir)

    # Create directories
    os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'policies'), exist_ok=True)

    # Determine config file
    if args.config:
        config_path = args.config
    else:
        config_path = create_default_config(args.address, args.port, base_dir)

    # Log path
    log_path = os.path.join(base_dir, 'logs', f'kmip_server_{args.port}.log')

    # Print startup info
    print_separator()
    print("KMIP Mock Server")
    print_separator()
    print(f"Address:    {args.address}:{args.port}")
    print(f"Config:     {config_path}")
    print(f"Logs:       {log_path}")
    print(f"Base dir:   {base_dir}")
    print(f"Log level:  {args.log_level}")
    print("\nPress Ctrl+C to stop")
    print_separator()

    # Check if certificates exist
    cert_path = os.path.join(base_dir, 'certs', 'server_cert.pem')
    if not os.path.exists(cert_path):
        print("\nWARNING: Certificates not found!")
        print(f"   Expected: {os.path.join(base_dir, 'certs')}/*.pem")
        print("\n   Generate certificates first using setup_kmip_test.sh")
        print_separator()
        sys.exit(1)

    # Create and start server
    server = KmipServer(
        config_path=config_path,
        log_path=log_path
    )

    try:
        # Bind socket
        server.start()
        print(f"\n✓ Server socket bound to {args.address}:{args.port}")

        # Start listening
        print("Server is now listening for connections...\n")
        server.serve()

    except KeyboardInterrupt:
        print("\n\nShutting down server...")
        server.stop()
        print("Server stopped cleanly")

    except OSError as e:
        if "Address already in use" in str(e):
            print(f"\nError: Port {args.port} is already in use")
            print("   Try a different port with: --port <PORT>")
        else:
            print(f"\nError: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        server.stop()
        sys.exit(1)


if __name__ == '__main__':
    main()
