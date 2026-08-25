# Dockerfile for KMIP Mock Server
# This container includes all certificates and is ready to accept client connections

FROM python:3.11-slim

LABEL description="KMIP Mock Server with TLS certificates"

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openssl \
        ca-certificates \
        git && \
    rm -rf /var/lib/apt/lists/*

# Install ceph PyKMIP (master) with Python 3.12 TLS compatibility fix
RUN pip install --no-cache-dir git+https://github.com/ceph/PyKMIP.git

# Create working directory
WORKDIR /kmip

# Create directory structure
RUN mkdir -p /kmip/certs /kmip/policies /kmip/logs

# Copy server script and setup script
COPY dummy_kmip_server.py /kmip/
COPY setup_kmip_test.sh /kmip/

# Make setup script executable
RUN chmod +x /kmip/setup_kmip_test.sh

# Generate certificates during build (fail-fast if cert generation fails)
RUN sh -eu /kmip/setup_kmip_test.sh /kmip

# Expose KMIP port
EXPOSE 5696

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 5696)); s.close()" || exit 1

# Default command - run KMIP server
CMD ["python3", "dummy_kmip_server.py", "--address", "0.0.0.0", "--port", "5696", "--base-dir", "/kmip"]
