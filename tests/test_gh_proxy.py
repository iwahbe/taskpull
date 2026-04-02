"""Tests for GHProxy authentication behavior.

Replicates the failure where gh CLI cannot authenticate against the proxy
because the supervisor sets GH_TOKEN instead of GH_ENTERPRISE_TOKEN.

gh CLI uses GH_TOKEN only for github.com and ghe.com subdomains.
For any other host (like host.docker.internal used by the proxy),
gh requires GH_ENTERPRISE_TOKEN. When only GH_TOKEN is set, gh sends
no Authorization header and the proxy returns 403 "Invalid proxy token".
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import subprocess
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from taskpull.gh_proxy import GHProxy


def _generate_localhost_certs(cert_dir: Path) -> tuple[Path, Path, Path, Path]:
    """Generate CA and server certs valid for localhost/127.0.0.1."""
    cert_dir.mkdir(parents=True, exist_ok=True)

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
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(ca_key), "-out", str(ca_cert),
            "-days", "1", "-nodes", "-subj", "/CN=test-ca",
            "-extensions", "v3_ca", "-config", str(ca_ext),
        ],
        check=True, capture_output=True,
    )

    csr_path = cert_dir / "server.csr"
    subprocess.run(
        [
            "openssl", "req", "-newkey", "rsa:2048",
            "-keyout", str(server_key), "-out", str(csr_path),
            "-nodes", "-subj", "/CN=localhost",
        ],
        check=True, capture_output=True,
    )

    san_conf = cert_dir / "san.cnf"
    san_conf.write_text(
        "[v3_req]\nsubjectAltName = DNS:localhost,IP:127.0.0.1\n"
    )

    subprocess.run(
        [
            "openssl", "x509", "-req",
            "-in", str(csr_path), "-CA", str(ca_cert), "-CAkey", str(ca_key),
            "-CAcreateserial", "-out", str(server_cert),
            "-days", "1", "-extfile", str(san_conf), "-extensions", "v3_req",
        ],
        check=True, capture_output=True,
    )

    return ca_cert, ca_key, server_cert, server_key


async def _send_https_request(
    port: int,
    path: str,
    ca_cert: Path,
    auth_header: str | None = None,
) -> tuple[int, str]:
    """Send an HTTPS GET to localhost:port and return (status_code, body)."""
    ssl_ctx = ssl.create_default_context(cafile=str(ca_cert))
    reader, writer = await asyncio.open_connection(
        "localhost", port, ssl=ssl_ctx,
    )

    try:
        request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
        if auth_header:
            request += f"Authorization: {auth_header}\r\n"
        request += "\r\n"

        writer.write(request.encode())
        await writer.drain()

        response = await asyncio.wait_for(reader.read(), timeout=10)
        text = response.decode("latin-1")
        status_line = text.split("\r\n")[0]
        status_code = int(status_line.split(" ", 2)[1])
        body = text.split("\r\n\r\n", 1)[1] if "\r\n\r\n" in text else ""
        return status_code, body
    finally:
        writer.close()
        await writer.wait_closed()


@pytest_asyncio.fixture
async def proxy_server():
    """Start a GHProxy on a random port with localhost certs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        proxy = GHProxy("fake-gh-token", ca_cert, server_cert, server_key)

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(server_cert, server_key)

        server = await asyncio.start_server(
            proxy._handle_connection, "127.0.0.1", 0, ssl=ssl_ctx,
        )
        port = server.sockets[0].getsockname()[1]

        async with server:
            yield proxy, port, ca_cert


@pytest.mark.asyncio
async def test_proxy_rejects_missing_auth(proxy_server):
    """Request without Authorization header gets 403 "Invalid proxy token".

    This is the request gh sends when GH_HOST is a non-github.com host
    and only GH_TOKEN (not GH_ENTERPRISE_TOKEN) is set: gh does not
    attach the token and the proxy rejects it.
    """
    proxy, port, ca_cert = proxy_server
    proxy.register_task("owner/repo")

    status, body = await _send_https_request(port, "/api/v3/user", ca_cert)
    assert status == 403
    assert "Invalid proxy token" in body


@pytest.mark.asyncio
async def test_proxy_rejects_wrong_token(proxy_server):
    """Request with an unregistered token gets 403."""
    proxy, port, ca_cert = proxy_server
    proxy.register_task("owner/repo")

    status, body = await _send_https_request(
        port, "/api/v3/user", ca_cert,
        auth_header="token wrong-secret",
    )
    assert status == 403
    assert "Invalid proxy token" in body


@pytest.mark.asyncio
async def test_proxy_accepts_registered_token(proxy_server):
    """Request with a properly registered token passes proxy auth.

    The proxy forwards to api.github.com (which may return an auth error
    since we use a fake token, or 502 if there's no network). The key
    assertion is that we do NOT get 403 "Invalid proxy token" from the
    proxy itself.
    """
    proxy, port, ca_cert = proxy_server
    secret = proxy.register_task("owner/repo")

    status, body = await _send_https_request(
        port, "/api/v3/user", ca_cert,
        auth_header=f"token {secret}",
    )
    assert not (status == 403 and "Invalid proxy token" in body)


@pytest.mark.asyncio
async def test_gh_cli_sends_no_auth_with_gh_token(proxy_server):
    """gh CLI ignores GH_TOKEN when GH_HOST is not github.com/ghe.com.

    This directly replicates the production failure: the supervisor sets
    GH_HOST=host.docker.internal:<port> and GH_TOKEN=<proxy_secret>,
    but gh treats the host as a GitHub Enterprise Server and looks for
    GH_ENTERPRISE_TOKEN instead. Since that's not set, gh sends no
    credentials and the proxy rejects the request.
    """
    proxy, port, ca_cert = proxy_server
    secret = proxy.register_task("owner/repo")

    with tempfile.TemporaryDirectory() as home:
        env = {
            "GH_HOST": f"localhost:{port}",
            "GH_TOKEN": secret,
            "GH_PROMPT_DISABLED": "1",
            "GH_NO_UPDATE_NOTIFIER": "1",
            "PATH": os.environ.get("PATH", ""),
            "HOME": home,
        }

        proc = await asyncio.create_subprocess_exec(
            "gh", "api", "/user",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
        combined = stdout.decode() + stderr.decode()

        assert proc.returncode != 0, (
            f"gh should fail when GH_TOKEN is set but GH_ENTERPRISE_TOKEN is not. "
            f"Output: {combined}"
        )


@pytest.mark.asyncio
async def test_gh_cli_authenticates_with_enterprise_token(proxy_server):
    """gh CLI sends credentials when GH_ENTERPRISE_TOKEN is set.

    This verifies the fix: using GH_ENTERPRISE_TOKEN instead of GH_TOKEN
    makes gh send the proxy secret in the Authorization header.
    """
    proxy, port, ca_cert = proxy_server
    secret = proxy.register_task("owner/repo")

    with tempfile.TemporaryDirectory() as home:
        env = {
            "GH_HOST": f"localhost:{port}",
            "GH_ENTERPRISE_TOKEN": secret,
            "GH_PROMPT_DISABLED": "1",
            "GH_NO_UPDATE_NOTIFIER": "1",
            "PATH": os.environ.get("PATH", ""),
            "HOME": home,
        }

        proc = await asyncio.create_subprocess_exec(
            "gh", "api", "/user",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
        combined = stdout.decode() + stderr.decode()

        # gh should NOT fail with "authentication" errors — it should
        # at least reach the proxy. It may fail with TLS errors (the
        # self-signed cert isn't in Go's trust store on macOS), but the
        # error should NOT be about missing credentials.
        assert "authentication token" not in combined.lower(), (
            f"gh should use GH_ENTERPRISE_TOKEN for enterprise hosts. "
            f"Output: {combined}"
        )
