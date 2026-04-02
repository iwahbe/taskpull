"""E2E tests for GHProxy with a fake upstream GitHub server.

These tests start the proxy and a local fake GitHub API server,
then verify permission checks, token injection, and blocking.
"""

from __future__ import annotations

import asyncio
import json
import ssl
import subprocess
from pathlib import Path

import pytest

from taskpull.gh_proxy import GHProxy, parse_github_repo

_check_permission = GHProxy._check_permission
_extract_token = GHProxy._extract_token


class TestParseGithubRepo:
    def test_ssh_url(self):
        assert parse_github_repo("git@github.com:owner/repo.git") == "owner/repo"

    def test_ssh_url_no_dotgit(self):
        assert parse_github_repo("git@github.com:owner/repo") == "owner/repo"

    def test_https_url(self):
        assert parse_github_repo("https://github.com/owner/repo.git") == "owner/repo"

    def test_https_url_no_dotgit(self):
        assert parse_github_repo("https://github.com/owner/repo") == "owner/repo"

    def test_invalid_url(self):
        with pytest.raises(ValueError, match="cannot parse"):
            parse_github_repo("https://gitlab.com/owner/repo")


class TestExtractToken:
    def test_token_prefix(self):
        assert _extract_token({"authorization": "token abc123"}) == "abc123"

    def test_bearer_prefix(self):
        assert _extract_token({"authorization": "Bearer xyz"}) == "xyz"

    def test_no_auth(self):
        assert _extract_token({}) is None

    def test_unknown_scheme(self):
        assert _extract_token({"authorization": "Basic foo"}) is None


class TestCheckPermission:
    def test_get_allowed(self):
        allowed, _, inject = _check_permission("GET", "/repos/o/r/issues", b"", "o/r")
        assert allowed is True
        assert inject is True

    def test_post_to_allowed_repo(self):
        allowed, _, inject = _check_permission("POST", "/repos/o/r/issues", b"", "o/r")
        assert allowed is True
        assert inject is True

    def test_post_to_wrong_repo(self):
        allowed, _, inject = _check_permission(
            "POST", "/repos/other/repo/issues", b"", "o/r"
        )
        assert allowed is True
        assert inject is False

    def test_post_to_non_repo_endpoint(self):
        allowed, _, inject = _check_permission("POST", "/user/repos", b"", "o/r")
        assert allowed is True
        assert inject is False

    def test_graphql_query_allowed(self):
        body = json.dumps({"query": "{ viewer { login } }"}).encode()
        allowed, _, inject = _check_permission("POST", "/graphql", body, "o/r")
        assert allowed is True
        assert inject is True

    def test_graphql_mutation_blocked(self):
        body = json.dumps({"query": "mutation { createIssue { id } }"}).encode()
        allowed, reason, _ = _check_permission("POST", "/graphql", body, "o/r")
        assert allowed is False
        assert "mutation" in reason.lower()

    def test_api_v3_prefix_stripped(self):
        allowed, _, inject = _check_permission(
            "POST", "/api/v3/repos/o/r/pulls", b"", "o/r"
        )
        assert allowed is True
        assert inject is True

    def test_case_insensitive_repo_match(self):
        allowed, _, inject = _check_permission(
            "POST", "/repos/Owner/Repo/issues", b"", "owner/repo"
        )
        assert allowed is True
        assert inject is True

    def test_delete_to_allowed_repo(self):
        allowed, _, inject = _check_permission(
            "DELETE", "/repos/o/r/issues/1", b"", "o/r"
        )
        assert allowed is True
        assert inject is True

    def test_graphql_unparseable_body(self):
        allowed, reason, _ = _check_permission("POST", "/graphql", b"not json", "o/r")
        assert allowed is False
        assert "parse" in reason.lower()


def _generate_localhost_certs(cert_dir: Path) -> tuple[Path, Path, Path, Path]:
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


async def _fake_github_server() -> tuple[asyncio.Server, int, list[dict]]:
    """Start a plain TCP server that acts as a fake GitHub API.

    Records received requests and returns 200 OK with a JSON body.
    """
    received: list[dict] = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            return

        parts = request_line.decode().strip().split(" ", 2)
        method, path = parts[0], parts[1] if len(parts) > 1 else "/"

        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            decoded = line.decode().strip()
            if ":" in decoded:
                k, v = decoded.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        body = b""
        cl = int(headers.get("content-length", "0"))
        if cl > 0:
            body = await reader.readexactly(cl)

        received.append(
            {
                "method": method,
                "path": path,
                "headers": headers,
                "body": body.decode() if body else "",
            }
        )

        response_body = json.dumps({"status": "ok"}).encode()
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            + f"Content-Length: {len(response_body)}\r\n".encode()
            + b"Connection: close\r\n"
            + b"\r\n"
            + response_body
        )
        writer.write(response)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port, received


async def _send_proxy_request(
    proxy_port: int,
    ca_cert: Path,
    method: str,
    path: str,
    auth_header: str | None = None,
    body: bytes = b"",
) -> tuple[int, str]:
    ssl_ctx = ssl.create_default_context(cafile=str(ca_cert))
    reader, writer = await asyncio.open_connection(
        "localhost",
        proxy_port,
        ssl=ssl_ctx,
    )
    try:
        headers = "Host: localhost\r\nConnection: close\r\n"
        if auth_header:
            headers += f"Authorization: {auth_header}\r\n"
        if body:
            headers += f"Content-Length: {len(body)}\r\n"
        request = f"{method} {path} HTTP/1.1\r\n{headers}\r\n".encode() + body

        writer.write(request)
        await writer.drain()

        response = await asyncio.wait_for(reader.read(), timeout=10)
        text = response.decode("latin-1")
        status_line = text.split("\r\n")[0]
        status_code = int(status_line.split(" ", 2)[1])
        resp_body = text.split("\r\n\r\n", 1)[1] if "\r\n\r\n" in text else ""
        return status_code, resp_body
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_proxy_forwards_read_with_token(tmp_path: Path):
    """GET requests are forwarded to upstream with the real GH token injected."""
    cert_dir = tmp_path / "certs"
    ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

    fake_server, upstream_port, received = await _fake_github_server()

    proxy = GHProxy(
        "real-gh-token",
        ca_cert,
        server_cert,
        server_key,
        upstream_host="127.0.0.1",
        upstream_port=upstream_port,
    )
    secret = proxy.register_task("owner/repo", "test-task")

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(server_cert, server_key)
    proxy_server = await asyncio.start_server(
        proxy._handle_connection,
        "127.0.0.1",
        0,
        ssl=ssl_ctx,
    )
    proxy_port = proxy_server.sockets[0].getsockname()[1]

    async with fake_server, proxy_server:
        status, body = await _send_proxy_request(
            proxy_port,
            ca_cert,
            "GET",
            "/repos/owner/repo/issues",
            auth_header=f"token {secret}",
        )

    assert status == 200
    assert len(received) == 1
    assert received[0]["method"] == "GET"
    assert received[0]["path"] == "/repos/owner/repo/issues"
    assert received[0]["headers"]["authorization"] == "token real-gh-token"


@pytest.mark.asyncio
async def test_proxy_write_to_allowed_repo(tmp_path: Path):
    """POST to the allowed repo is forwarded with the real token."""
    cert_dir = tmp_path / "certs"
    ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

    fake_server, upstream_port, received = await _fake_github_server()

    proxy = GHProxy(
        "real-gh-token",
        ca_cert,
        server_cert,
        server_key,
        upstream_host="127.0.0.1",
        upstream_port=upstream_port,
    )
    secret = proxy.register_task("owner/repo", "test-task")

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(server_cert, server_key)
    proxy_server = await asyncio.start_server(
        proxy._handle_connection,
        "127.0.0.1",
        0,
        ssl=ssl_ctx,
    )
    proxy_port = proxy_server.sockets[0].getsockname()[1]

    async with fake_server, proxy_server:
        status, _ = await _send_proxy_request(
            proxy_port,
            ca_cert,
            "POST",
            "/repos/owner/repo/issues",
            auth_header=f"token {secret}",
            body=json.dumps({"title": "test"}).encode(),
        )

    assert status == 200
    assert len(received) == 1
    assert received[0]["headers"]["authorization"] == "token real-gh-token"


@pytest.mark.asyncio
async def test_proxy_write_to_wrong_repo_strips_token(tmp_path: Path):
    """POST to a different repo is forwarded but WITHOUT the real token."""
    cert_dir = tmp_path / "certs"
    ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

    fake_server, upstream_port, received = await _fake_github_server()

    proxy = GHProxy(
        "real-gh-token",
        ca_cert,
        server_cert,
        server_key,
        upstream_host="127.0.0.1",
        upstream_port=upstream_port,
    )
    secret = proxy.register_task("owner/repo", "test-task")

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(server_cert, server_key)
    proxy_server = await asyncio.start_server(
        proxy._handle_connection,
        "127.0.0.1",
        0,
        ssl=ssl_ctx,
    )
    proxy_port = proxy_server.sockets[0].getsockname()[1]

    async with fake_server, proxy_server:
        status, _ = await _send_proxy_request(
            proxy_port,
            ca_cert,
            "POST",
            "/repos/other/repo/issues",
            auth_header=f"token {secret}",
        )

    assert status == 200
    assert len(received) == 1
    assert "authorization" not in received[0]["headers"]


@pytest.mark.asyncio
async def test_proxy_blocks_graphql_mutation(tmp_path: Path):
    """GraphQL mutation requests are blocked with 403."""
    cert_dir = tmp_path / "certs"
    ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

    fake_server, upstream_port, received = await _fake_github_server()

    proxy = GHProxy(
        "real-gh-token",
        ca_cert,
        server_cert,
        server_key,
        upstream_host="127.0.0.1",
        upstream_port=upstream_port,
    )
    secret = proxy.register_task("owner/repo", "test-task")

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(server_cert, server_key)
    proxy_server = await asyncio.start_server(
        proxy._handle_connection,
        "127.0.0.1",
        0,
        ssl=ssl_ctx,
    )
    proxy_port = proxy_server.sockets[0].getsockname()[1]

    mutation_body = json.dumps({"query": "mutation { createIssue { id } }"}).encode()

    async with fake_server, proxy_server:
        status, body = await _send_proxy_request(
            proxy_port,
            ca_cert,
            "POST",
            "/graphql",
            auth_header=f"token {secret}",
            body=mutation_body,
        )

    assert status == 403
    assert "mutation" in body.lower()
    assert len(received) == 0
