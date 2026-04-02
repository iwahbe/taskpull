from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def tmp_path_factory_certs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Generate CA and server certs valid for localhost/127.0.0.1.

    Returns (ca_cert, ca_key, server_cert, server_key).
    """
    cert_dir = tmp_path / "certs"
    cert_dir.mkdir()

    ca_key = cert_dir / "ca-key.pem"
    ca_cert = cert_dir / "ca.pem"
    server_key = cert_dir / "server-key.pem"
    server_cert = cert_dir / "server.pem"

    ca_ext = cert_dir / "ca-ext.cnf"
    ca_ext.write_text(
        "[v3_ca]\n"
        "basicConstraints = critical, CA:TRUE\n"
        "keyUsage = critical, keyCertSign, cRLSign\n"
    )

    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=test-ca",
            "-extensions",
            "v3_ca",
            "-config",
            str(ca_ext),
        ],
        check=True,
        capture_output=True,
    )

    csr_path = cert_dir / "server.csr"
    subprocess.run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(server_key),
            "-out",
            str(csr_path),
            "-nodes",
            "-subj",
            "/CN=localhost",
        ],
        check=True,
        capture_output=True,
    )

    san_conf = cert_dir / "san.cnf"
    san_conf.write_text("[v3_req]\nsubjectAltName = DNS:localhost,IP:127.0.0.1\n")

    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(csr_path),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_cert),
            "-days",
            "1",
            "-extfile",
            str(san_conf),
            "-extensions",
            "v3_req",
        ],
        check=True,
        capture_output=True,
    )

    return ca_cert, ca_key, server_cert, server_key
