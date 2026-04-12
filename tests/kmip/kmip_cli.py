#!/usr/bin/env python3
#  ############################
#  Copyright (c) 2026 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: gadi.didi@ibm.com
#

"""
KMIP Administrative CLI Tool

Manage passphrases/secrets on a KMIP server

Usage:
    # Create and activate a passphrase in one step
    kmip_cli.py create-passphrase --name "my-secret" --value "secretdata"

    # Retrieve a passphrase
    kmip_cli.py get --uuid <uuid>

    # Destroy a passphrase
    kmip_cli.py destroy --uuid <uuid>

    # Get passphrase info
    kmip_cli.py info --uuid <uuid>
"""

import argparse
import sys
import json
from typing import Optional, Dict, Any
from kmip.pie import client
from kmip.pie import objects
from kmip import enums


class KMIPCli:
    """KMIP Command Line Interface"""

    def __init__(
        self,
        hostname: str = '127.0.0.1',
        port: int = 5696,
        cert: str = '/kmip/certs/client_cert.pem',
        key: str = '/kmip/certs/client_key.pem',
        ca: str = '/kmip/certs/ca_cert.pem',
        json_output: bool = False
    ):
        """Initialize KMIP CLI"""
        self.hostname = hostname
        self.port = port
        self.cert = cert
        self.key = key
        self.ca = ca
        self.json_output = json_output
        self.client: Optional[client.ProxyKmipClient] = None

    def connect(self) -> None:
        """Establish connection to KMIP server"""
        try:
            self.client = client.ProxyKmipClient(
                hostname=self.hostname,
                port=self.port,
                cert=self.cert,
                key=self.key,
                ca=self.ca
            )
            self.client.open()
        except Exception as e:
            self._error(f"Failed to connect to KMIP server {self.hostname}:{self.port}: {e}")
            sys.exit(1)

    def disconnect(self) -> None:
        """Close connection to KMIP server"""
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass

    def _output(self, message: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Output message or JSON"""
        if self.json_output:
            output = {"status": "success", "message": message}
            if data:
                output.update(data)
            print(json.dumps(output, indent=2))
        else:
            print(message)
            if data:
                for key, value in data.items():
                    print(f"  {key}: {value}")

    def _error(self, message: str) -> None:
        """Output error message"""
        if self.json_output:
            print(json.dumps({"status": "error", "message": message}, indent=2))
        else:
            print(f"ERROR: {message}", file=sys.stderr)

    def create_passphrase(self, name: str, value: str) -> None:
        """Create and activate a passphrase/secret in one step"""
        try:
            # Create passphrase
            secret_data = objects.SecretData(
                value.encode(),
                enums.SecretDataType.PASSWORD,
                masks=[enums.CryptographicUsageMask.DERIVE_KEY],
                name=name
            )

            secret_uuid = self.client.register(secret_data)

            # Activate it immediately
            self.client.activate(secret_uuid)

            self._output(
                f"Created and activated passphrase '{name}'",
                {"uuid": secret_uuid, "name": name, "type": "passphrase", "state": "active"}
            )

        except Exception as e:
            self._error(f"Failed to create passphrase: {e}")
            sys.exit(1)

    def get_passphrase(self, passphrase_uuid: str) -> None:
        """Retrieve and display a passphrase"""
        try:
            passphrase_data = self.client.get(passphrase_uuid)
            passphrase_bytes = passphrase_data.value

            preview = passphrase_bytes[:32].decode('utf-8', errors='replace')
            if len(passphrase_bytes) > 32:
                preview += "..."
            else:
                preview = passphrase_bytes.decode('utf-8', errors='replace')

            self._output(
                f"Retrieved passphrase {passphrase_uuid}",
                {
                    "uuid": passphrase_uuid,
                    "length_bytes": len(passphrase_bytes),
                    "value_preview": preview
                }
            )

        except Exception as e:
            self._error(f"Failed to retrieve passphrase {passphrase_uuid}: {e}")
            sys.exit(1)

    def destroy_passphrase(self, passphrase_uuid: str) -> None:
        """Destroy a passphrase"""
        try:
            self.client.destroy(passphrase_uuid)
            self._output(
                f"Destroyed passphrase {passphrase_uuid}",
                {"uuid": passphrase_uuid, "state": "destroyed"}
            )

        except Exception as e:
            self._error(f"Failed to destroy passphrase {passphrase_uuid}: {e}")
            sys.exit(1)

    def get_passphrase_info(self, passphrase_uuid: str) -> None:
        """Get passphrase metadata"""
        try:
            # Get attributes
            attrs = self.client.get_attributes(uid=passphrase_uuid)

            info = {
                "uuid": passphrase_uuid,
                "attributes": {}
            }

            # Extract useful attributes
            for attr in attrs:
                if hasattr(attr, 'attribute_name'):
                    attr_name = attr.attribute_name.value
                else:
                    attr_name = str(attr)

                if hasattr(attr, 'attribute_value'):
                    attr_value = str(attr.attribute_value)
                else:
                    attr_value = str(attr)

                info["attributes"][attr_name] = attr_value

            self._output(f"Passphrase information for {passphrase_uuid}", info)

        except Exception as e:
            self._error(f"Failed to get passphrase info for {passphrase_uuid}: {e}")
            sys.exit(1)

    def list_passphrases(self) -> None:
        """List all passphrase UUIDs"""
        try:
            # Locate all secret data objects
            uuids = self.client.locate()

            if not uuids:
                self._output("No passphrases found")
            else:
                if self.json_output:
                    self._output(
                        f"Found {len(uuids)} passphrase(s)",
                        {"count": len(uuids), "uuids": uuids}
                    )
                else:
                    # For text, print header then manually loop for custom format
                    print(f"Found {len(uuids)} passphrase(s):")
                    for uuid in uuids:
                        print(f" uuid: {uuid}")

        except Exception as e:
            self._error(f"Failed to list passphrases: {e}")
            sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='KMIP Administrative CLI Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Connection arguments
    parser.add_argument(
        '--hostname', '-H',
        default='127.0.0.1',
        help='KMIP server hostname (default: 127.0.0.1)'
    )

    parser.add_argument(
        '--port', '-P',
        type=int,
        default=5696,
        help='KMIP server port (default: 5696)'
    )

    parser.add_argument(
        '--cert',
        default='/kmip/certs/client_cert.pem',
        help='Client certificate path (default: /kmip/certs/client_cert.pem)'
    )

    parser.add_argument(
        '--key',
        default='/kmip/certs/client_key.pem',
        help='Client key path (default: /kmip/certs/client_key.pem)'
    )

    parser.add_argument(
        '--ca',
        default='/kmip/certs/ca_cert.pem',
        help='CA certificate path (default: /kmip/certs/ca_cert.pem)'
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # create-passphrase (auto-activates)
    create_pass_parser = subparsers.add_parser(
        'create-passphrase',
        help='Create and activate a passphrase/secret in one step'
    )
    create_pass_parser.add_argument(
        '--name', required=True, help='Passphrase name (for reference)')
    create_pass_parser.add_argument(
        '--value', required=True, help='Passphrase value')
    create_pass_parser.add_argument(
        '-o', '--output', choices=['text', 'json'],
        default='text', help='Output format')

    # get
    get_parser = subparsers.add_parser('get', help='Retrieve a passphrase')
    get_parser.add_argument('--uuid', required=True, help='Passphrase UUID')
    get_parser.add_argument(
        '-o', '--output', choices=['text', 'json'],
        default='text', help='Output format')

    # destroy
    destroy_parser = subparsers.add_parser(
        'destroy', help='Destroy a passphrase')
    destroy_parser.add_argument('--uuid', required=True, help='Passphrase UUID')
    destroy_parser.add_argument(
        '-o', '--output', choices=['text', 'json'],
        default='text', help='Output format')

    # info
    info_parser = subparsers.add_parser(
        'info', help='Get passphrase metadata')
    info_parser.add_argument('--uuid', required=True, help='Passphrase UUID')
    info_parser.add_argument(
        '-o', '--output', choices=['text', 'json'],
        default='text', help='Output format')

    # list
    list_parser = subparsers.add_parser(
        'list', help='List all passphrase UUIDs')
    list_parser.add_argument(
        '-o', '--output', choices=['text', 'json'],
        default='text', help='Output format')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cli = KMIPCli(
        hostname=args.hostname,
        port=args.port,
        cert=args.cert,
        key=args.key,
        ca=args.ca,
        json_output=(args.output == 'json')
    )

    # Connect to server
    cli.connect()

    try:
        # Execute command
        if args.command == 'create-passphrase':
            cli.create_passphrase(args.name, args.value)

        elif args.command == 'get':
            cli.get_passphrase(args.uuid)

        elif args.command == 'destroy':
            cli.destroy_passphrase(args.uuid)

        elif args.command == 'info':
            cli.get_passphrase_info(args.uuid)

        elif args.command == 'list':
            cli.list_passphrases()

    finally:
        cli.disconnect()


if __name__ == '__main__':
    main()
