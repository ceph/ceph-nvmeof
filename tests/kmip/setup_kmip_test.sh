#!/bin/bash
# Setup script for KMIP test environment

BASE_DIR=${1:-/tmp/kmip_test}
CERTS_DIR="$BASE_DIR/certs"

echo "Setting up KMIP test environment in $BASE_DIR"

# Create directories
mkdir -p "$CERTS_DIR"
mkdir -p "$BASE_DIR/policies"
mkdir -p "$BASE_DIR/logs"

cd "$CERTS_DIR"

# Generate CA
echo "Generating CA certificate..."
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 \
  -nodes -keyout ca_key.pem -out ca_cert.pem \
  -subj "/CN=KMIP Test CA" 2>/dev/null

# Generate server certificate
echo "Generating server certificate..."
openssl req -newkey rsa:4096 -nodes \
  -keyout server_key.pem \
  -out server_req.pem \
  -subj "/CN=localhost" 2>/dev/null

cat > server_ext.cnf << EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -in server_req.pem \
  -CA ca_cert.pem -CAkey ca_key.pem \
  -CAcreateserial -out server_cert.pem \
  -days 365 -sha256 \
  -extfile server_ext.cnf 2>/dev/null

# Generate client certificate
echo "Generating client certificate..."
openssl req -newkey rsa:4096 -nodes \
  -keyout client_key.pem \
  -out client_req.pem \
  -subj "/CN=test_client" 2>/dev/null

cat > client_ext.cnf << EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client_req.pem \
  -CA ca_cert.pem -CAkey ca_key.pem \
  -CAcreateserial -out client_cert.pem \
  -days 365 -sha256 \
  -extfile client_ext.cnf 2>/dev/null

# Verify
echo ""
echo "Verifying certificates..."
openssl verify -CAfile ca_cert.pem server_cert.pem
openssl verify -CAfile ca_cert.pem client_cert.pem

echo ""
echo "Setup complete!"
echo ""
echo "Run tests with:"
echo "python3 test_kmip_client.py"