"""Tests for GHProxy authentication and callback behavior."""

from __future__ import annotations

import asyncio
import base64
import json
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


async def _send_https_request(
    port: int,
    path: str,
    ca_cert: Path,
    auth_header: str | None = None,
) -> tuple[int, str]:
    """Send an HTTPS GET to localhost:port and return (status_code, body)."""
    ssl_ctx = ssl.create_default_context(cafile=str(ca_cert))
    reader, writer = await asyncio.open_connection(
        "localhost",
        port,
        ssl=ssl_ctx,
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
            proxy._handle_connection,
            "127.0.0.1",
            0,
            ssl=ssl_ctx,
        )
        port = server.sockets[0].getsockname()[1]

        async with server:
            yield proxy, port, ca_cert


@pytest.mark.asyncio
async def test_proxy_rejects_wrong_token(proxy_server):
    """Request with an unregistered token gets 403."""
    proxy, port, ca_cert = proxy_server
    proxy.register_task("owner/repo", "test-task")

    status, body = await _send_https_request(
        port,
        "/api/v3/user",
        ca_cert,
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
    secret = proxy.register_task("owner/repo", "test-task")

    status, body = await _send_https_request(
        port,
        "/api/v3/user",
        ca_cert,
        auth_header=f"token {secret}",
    )
    assert not (status == 403 and "Invalid proxy token" in body)


@pytest.mark.asyncio
async def test_proxy_accepts_basic_auth(proxy_server):
    """Request with Basic auth containing a registered proxy secret passes."""
    proxy, port, ca_cert = proxy_server
    secret = proxy.register_task("owner/repo", "test-task")

    creds = base64.b64encode(f"x-access-token:{secret}".encode()).decode()
    status, body = await _send_https_request(
        port,
        "/api/v3/user",
        ca_cert,
        auth_header=f"Basic {creds}",
    )
    assert not (status == 403 and "Invalid proxy token" in body)


@pytest.mark.asyncio
async def test_pr_created_callback_fires_on_pulls_post():
    """_maybe_notify_pr_created calls the callback for a successful PR create."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_pr_created(task_id: str, pr_number: int, pr_url: str) -> None:
            results.append((task_id, pr_number, pr_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_pr_created=on_pr_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        body = json.dumps(
            {
                "number": 42,
                "html_url": "https://github.com/owner/repo/pull/42",
            }
        ).encode()

        await proxy._maybe_notify_pr_created(
            "POST",
            "/repos/owner/repo/pulls",
            201,
            body,
            secret,
        )

        assert results == [
            ("my-task", 42, "https://github.com/owner/repo/pull/42"),
        ]


@pytest.mark.asyncio
async def test_pr_created_callback_skips_non_create():
    """_maybe_notify_pr_created ignores non-PR-create requests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_pr_created(task_id: str, pr_number: int, pr_url: str) -> None:
            results.append((task_id, pr_number, pr_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_pr_created=on_pr_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        body = json.dumps(
            {"number": 1, "html_url": "https://github.com/owner/repo/pull/1"}
        ).encode()

        # GET should not trigger
        await proxy._maybe_notify_pr_created(
            "GET", "/repos/owner/repo/pulls", 200, body, secret
        )
        # Wrong status code
        await proxy._maybe_notify_pr_created(
            "POST", "/repos/owner/repo/pulls", 422, body, secret
        )
        # Sub-path (e.g. comments on a PR)
        await proxy._maybe_notify_pr_created(
            "POST", "/repos/owner/repo/pulls/1/comments", 201, body, secret
        )
        # Wrong token
        await proxy._maybe_notify_pr_created(
            "POST", "/repos/owner/repo/pulls", 201, body, "bad-token"
        )

        assert results == []


@pytest.mark.asyncio
async def test_issue_created_callback_fires_on_issues_post():
    """_maybe_notify_issue_created calls the callback for a successful issue create."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_issue_created(
            task_id: str, issue_number: int, issue_url: str
        ) -> None:
            results.append((task_id, issue_number, issue_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_issue_created=on_issue_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        body = json.dumps(
            {
                "number": 7,
                "html_url": "https://github.com/owner/repo/issues/7",
            }
        ).encode()

        await proxy._maybe_notify_issue_created(
            "POST",
            "/repos/owner/repo/issues",
            201,
            body,
            secret,
        )

        assert results == [
            ("my-task", 7, "https://github.com/owner/repo/issues/7"),
        ]


@pytest.mark.asyncio
async def test_issue_created_callback_skips_non_create():
    """_maybe_notify_issue_created ignores non-issue-create requests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_issue_created(
            task_id: str, issue_number: int, issue_url: str
        ) -> None:
            results.append((task_id, issue_number, issue_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_issue_created=on_issue_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        body = json.dumps(
            {"number": 1, "html_url": "https://github.com/owner/repo/issues/1"}
        ).encode()

        # GET should not trigger
        await proxy._maybe_notify_issue_created(
            "GET", "/repos/owner/repo/issues", 200, body, secret
        )
        # Wrong status code
        await proxy._maybe_notify_issue_created(
            "POST", "/repos/owner/repo/issues", 422, body, secret
        )
        # Sub-path (e.g. comments on an issue)
        await proxy._maybe_notify_issue_created(
            "POST", "/repos/owner/repo/issues/1/comments", 201, body, secret
        )
        # Wrong token
        await proxy._maybe_notify_issue_created(
            "POST", "/repos/owner/repo/issues", 201, body, "bad-token"
        )

        assert results == []


@pytest.mark.asyncio
async def test_read_request_eof_mid_headers_does_not_hang():
    """_read_request returns promptly when EOF arrives mid-headers (no blank line)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)
        proxy = GHProxy("fake-gh-token", ca_cert, server_cert, server_key)

        reader = asyncio.StreamReader()
        reader.feed_data(b"GET /api/v3/user HTTP/1.1\r\nHost: api.github.com\r\n")
        reader.feed_eof()

        result = await asyncio.wait_for(proxy._read_request(reader), timeout=1.0)

        assert result == ("GET", "/api/v3/user", {"host": "api.github.com"}, b"")


@pytest.mark.asyncio
async def test_read_response_eof_mid_headers_does_not_hang():
    """_read_response returns promptly when EOF arrives mid-headers (no blank line)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)
        proxy = GHProxy("fake-gh-token", ca_cert, server_cert, server_key)

        reader = asyncio.StreamReader()
        reader.feed_data(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n")
        reader.feed_eof()

        raw, status_code, body = await asyncio.wait_for(
            proxy._read_response(reader), timeout=1.0
        )

        assert status_code == 200
        assert raw == b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        assert body == b""


@pytest.mark.asyncio
async def test_read_request_complete_parses_correctly():
    """_read_request correctly parses a well-formed HTTP request."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)
        proxy = GHProxy("fake-gh-token", ca_cert, server_cert, server_key)

        request_bytes = (
            b"POST /repos/owner/repo/pulls HTTP/1.1\r\n"
            b"Host: api.github.com\r\n"
            b"Content-Length: 4\r\n"
            b"\r\n"
            b"body"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(request_bytes)
        reader.feed_eof()

        result = await asyncio.wait_for(proxy._read_request(reader), timeout=1.0)

        assert result == (
            "POST",
            "/repos/owner/repo/pulls",
            {"host": "api.github.com", "content-length": "4"},
            b"body",
        )


@pytest.mark.asyncio
async def test_read_response_complete_parses_correctly():
    """_read_response correctly parses a well-formed HTTP response."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)
        proxy = GHProxy("fake-gh-token", ca_cert, server_cert, server_key)

        response_bytes = (
            b"HTTP/1.1 201 Created\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 6\r\n"
            b"\r\n"
            b"hello!"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(response_bytes)
        reader.feed_eof()

        raw, status_code, body = await asyncio.wait_for(
            proxy._read_response(reader), timeout=1.0
        )

        assert status_code == 201
        assert body == b"hello!"
        assert raw == response_bytes


@pytest.mark.asyncio
async def test_graphql_pr_created_callback_fires():
    """_maybe_notify_graphql_pr_created fires when a createPullRequest mutation succeeds."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_pr_created(task_id: str, pr_number: int, pr_url: str) -> None:
            results.append((task_id, pr_number, pr_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_pr_created=on_pr_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        request_body = json.dumps(
            {
                "query": "mutation CreatePullRequest($input: CreatePullRequestInput!) { createPullRequest(input: $input) { pullRequest { id number url } } }",
                "variables": {"input": {"repositoryId": "R_abc"}},
            }
        ).encode()
        response_body = json.dumps(
            {
                "data": {
                    "createPullRequest": {
                        "pullRequest": {
                            "id": "PR_abc",
                            "number": 42,
                            "url": "https://github.com/owner/repo/pull/42",
                        }
                    }
                }
            }
        ).encode()

        await proxy._maybe_notify_graphql_pr_created(
            "POST", "/graphql", 200, request_body, response_body, secret
        )

        assert results == [
            ("my-task", 42, "https://github.com/owner/repo/pull/42"),
        ]


@pytest.mark.asyncio
async def test_graphql_pr_created_callback_skips_non_mutation():
    """_maybe_notify_graphql_pr_created ignores non-mutation requests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_pr_created(task_id: str, pr_number: int, pr_url: str) -> None:
            results.append((task_id, pr_number, pr_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_pr_created=on_pr_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        query_body = json.dumps(
            {
                "query": "{ viewer { login } }",
            }
        ).encode()
        response_body = json.dumps({"data": {"viewer": {"login": "me"}}}).encode()

        await proxy._maybe_notify_graphql_pr_created(
            "POST", "/graphql", 200, query_body, response_body, secret
        )
        # Wrong status code
        mutation_body = json.dumps(
            {
                "query": "mutation { createPullRequest(input: $input) { pullRequest { id number url } } }",
            }
        ).encode()
        await proxy._maybe_notify_graphql_pr_created(
            "POST", "/graphql", 403, mutation_body, b"{}", secret
        )
        # Wrong path
        await proxy._maybe_notify_graphql_pr_created(
            "POST", "/repos/o/r/pulls", 200, mutation_body, b"{}", secret
        )

        assert results == []


@pytest.mark.asyncio
async def test_graphql_issue_created_callback_fires():
    """_maybe_notify_graphql_issue_created fires when a createIssue mutation succeeds."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_issue_created(
            task_id: str, issue_number: int, issue_url: str
        ) -> None:
            results.append((task_id, issue_number, issue_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_issue_created=on_issue_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        request_body = json.dumps(
            {
                "query": "mutation CreateIssue($input: CreateIssueInput!) { createIssue(input: $input) { issue { id number url } } }",
                "variables": {"input": {"repositoryId": "R_abc"}},
            }
        ).encode()
        response_body = json.dumps(
            {
                "data": {
                    "createIssue": {
                        "issue": {
                            "id": "I_abc",
                            "number": 7,
                            "url": "https://github.com/owner/repo/issues/7",
                        }
                    }
                }
            }
        ).encode()

        await proxy._maybe_notify_graphql_issue_created(
            "POST", "/graphql", 200, request_body, response_body, secret
        )

        assert results == [
            ("my-task", 7, "https://github.com/owner/repo/issues/7"),
        ]


@pytest.mark.asyncio
async def test_graphql_issue_created_callback_skips_non_mutation():
    """_maybe_notify_graphql_issue_created ignores non-mutation requests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        results: list[tuple[str, int, str]] = []

        async def on_issue_created(
            task_id: str, issue_number: int, issue_url: str
        ) -> None:
            results.append((task_id, issue_number, issue_url))

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
            on_issue_created=on_issue_created,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        query_body = json.dumps(
            {
                "query": "{ viewer { login } }",
            }
        ).encode()
        response_body = json.dumps({"data": {"viewer": {"login": "me"}}}).encode()

        await proxy._maybe_notify_graphql_issue_created(
            "POST", "/graphql", 200, query_body, response_body, secret
        )
        # Wrong status code
        mutation_body = json.dumps(
            {
                "query": "mutation { createIssue(input: $input) { issue { id number url } } }",
            }
        ).encode()
        await proxy._maybe_notify_graphql_issue_created(
            "POST", "/graphql", 403, mutation_body, b"{}", secret
        )
        # Wrong path
        await proxy._maybe_notify_graphql_issue_created(
            "POST", "/repos/o/r/issues", 200, mutation_body, b"{}", secret
        )

        assert results == []


def test_cache_repo_node_ids_legacy_format():
    """Legacy v1 node IDs (MDEw...) are cached, not just v2 (R_...) IDs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_dir = Path(tmpdir) / "certs"
        ca_cert, _, server_cert, server_key = _generate_localhost_certs(cert_dir)

        proxy = GHProxy(
            "fake-gh-token",
            ca_cert,
            server_cert,
            server_key,
        )
        secret = proxy.register_task("owner/repo", "my-task")

        legacy_node_id = base64.b64encode(b"010:Repository104365663").decode()
        response_body = json.dumps(
            {
                "data": {
                    "repository": {
                        "id": legacy_node_id,
                        "name": "repo",
                        "owner": {"login": "owner"},
                    }
                }
            }
        ).encode()

        proxy._cache_repo_node_ids(secret, response_body)

        assert proxy._repo_node_cache[secret][legacy_node_id] == "owner/repo"
