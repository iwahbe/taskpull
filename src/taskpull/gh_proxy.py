from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import secrets
import shutil
import ssl
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path

log = logging.getLogger(__name__)


_CERT_VERSION = "2"


def generate_certs(cert_dir: Path) -> tuple[Path, Path, Path, Path]:
    """Generate CA and server certs for the GH proxy.

    Returns (ca_cert, ca_key, server_cert, server_key).
    Skips generation if certs already exist and are up-to-date.
    """
    cert_dir.mkdir(parents=True, exist_ok=True)

    ca_key = cert_dir / "ca-key.pem"
    ca_cert = cert_dir / "ca.pem"
    server_key = cert_dir / "server-key.pem"
    server_cert = cert_dir / "server.pem"
    version_file = cert_dir / "version"

    if ca_cert.exists() and server_cert.exists():
        if version_file.exists() and version_file.read_text().strip() == _CERT_VERSION:
            return ca_cert, ca_key, server_cert, server_key
        log.info("cert version mismatch, regenerating")
        shutil.rmtree(cert_dir)
        cert_dir.mkdir(parents=True, exist_ok=True)

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
            "365",
            "-nodes",
            "-subj",
            "/CN=taskpull-ca",
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
            "/CN=host.docker.internal",
        ],
        check=True,
        capture_output=True,
    )

    san_conf = cert_dir / "san.cnf"
    san_conf.write_text(
        "[v3_req]\nsubjectAltName = DNS:host.docker.internal,DNS:api.github.com,DNS:github.com\n"
    )

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
            "365",
            "-extfile",
            str(san_conf),
            "-extensions",
            "v3_req",
        ],
        check=True,
        capture_output=True,
    )

    for name in ["server.csr", "san.cnf", "ca.srl"]:
        (cert_dir / name).unlink(missing_ok=True)

    version_file.write_text(_CERT_VERSION)
    log.info("generated TLS certs in %s", cert_dir)
    return ca_cert, ca_key, server_cert, server_key


def parse_github_repo(remote_url: str) -> str:
    """Extract 'owner/repo' from a git remote URL.

    Handles both SSH (git@github.com:owner/repo.git) and
    HTTPS (https://github.com/owner/repo.git) formats.
    """
    m = re.match(r"git@github\.com:(.+?)(?:\.git)?$", remote_url)
    if m:
        return m.group(1)
    m = re.match(r"https://github\.com/(.+?)(?:\.git)?$", remote_url)
    if m:
        return m.group(1)
    raise ValueError(f"cannot parse GitHub owner/repo from: {remote_url}")


class GHProxy:
    def __init__(
        self,
        gh_token: str,
        ca_cert: Path,
        server_cert: Path,
        server_key: Path,
        on_pr_created: Callable[[str, int, str], Awaitable[None]] | None = None,
        upstream_host: str = "api.github.com",
        upstream_port: int = 443,
    ):
        self._gh_token = gh_token
        self._server_cert = server_cert
        self._server_key = server_key
        self._ca_cert = ca_cert
        self._on_pr_created = on_pr_created
        self._upstream_host = upstream_host
        self._upstream_port = upstream_port
        self._token_map: dict[str, str] = {}
        self._task_map: dict[str, str] = {}

    @property
    def ca_cert_path(self) -> Path:
        return self._ca_cert

    def register_task(self, owner_repo: str, task_id: str) -> str:
        secret = secrets.token_urlsafe(32)
        self._token_map[secret] = owner_repo
        self._task_map[secret] = task_id
        return secret

    def restore_task(self, secret: str, owner_repo: str, task_id: str) -> None:
        """Re-register an existing proxy secret (e.g. after daemon restart)."""
        self._token_map[secret] = owner_repo
        self._task_map[secret] = task_id

    def unregister_task(self, secret: str) -> None:
        self._token_map.pop(secret, None)
        self._task_map.pop(secret, None)

    async def run(self, port: int, shutdown_event: asyncio.Event) -> None:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(self._server_cert, self._server_key)

        server = await asyncio.start_server(
            self._handle_connection,
            "0.0.0.0",
            port,
            ssl=ssl_ctx,
        )
        log.info("GH proxy listening on port %d", port)
        async with server:
            await shutdown_event.wait()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await self._proxy_request(reader, writer)
        except Exception:
            log.exception("GH proxy handler error")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _proxy_request(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        parsed = await self._read_request(reader)
        if parsed is None:
            return

        method, path, headers, body = parsed

        proxy_token = self._extract_token(headers)
        inject_token = False
        if proxy_token is not None:
            allowed_repo = self._token_map.get(proxy_token)
            if allowed_repo is None:
                await self._send_error(writer, 403, "Invalid proxy token")
                return

            allowed, reason, inject_token = self._check_permission(
                method, path, body, allowed_repo
            )
            if not allowed:
                log.warning("GH proxy blocked: %s %s — %s", method, path, reason)
                await self._send_error(writer, 403, reason)
                return

        forwarded_path = path
        if forwarded_path.startswith("/api/v3"):
            forwarded_path = forwarded_path[len("/api/v3") :]
        if not forwarded_path:
            forwarded_path = "/"

        request_host = headers.get("host", "")
        if request_host == "github.com" and self._upstream_host == "api.github.com":
            upstream_host = "github.com"
        else:
            upstream_host = self._upstream_host

        gh_ssl: ssl.SSLContext | None = ssl.create_default_context()
        if self._upstream_port != 443:
            gh_ssl = None
        try:
            gh_reader, gh_writer = await asyncio.open_connection(
                upstream_host,
                self._upstream_port,
                ssl=gh_ssl,
            )
        except Exception as exc:
            await self._send_error(writer, 502, f"Cannot connect to GitHub: {exc}")
            return

        try:
            forward_headers: dict[str, str] = {}
            for k, v in headers.items():
                if k in ("authorization", "host"):
                    continue
                forward_headers[k] = v
            if inject_token:
                if request_host == "github.com":
                    cred = base64.b64encode(
                        f"x-access-token:{self._gh_token}".encode()
                    ).decode()
                    forward_headers["authorization"] = f"Basic {cred}"
                else:
                    forward_headers["authorization"] = f"token {self._gh_token}"
            forward_headers["host"] = upstream_host
            forward_headers["connection"] = "close"

            request_bytes = f"{method} {forwarded_path} HTTP/1.1\r\n".encode()
            for k, v in forward_headers.items():
                request_bytes += f"{k}: {v}\r\n".encode()
            request_bytes += b"\r\n"
            if body:
                request_bytes += body

            gh_writer.write(request_bytes)
            await gh_writer.drain()

            raw, status_code, body = await self._read_response(gh_reader)
            writer.write(raw)
            await writer.drain()

            log.info("GH proxy: %s %s -> forwarded (%d)", method, path, status_code)

            await self._maybe_notify_pr_created(
                method, forwarded_path, status_code, body, proxy_token or ""
            )
        finally:
            gh_writer.close()
            await gh_writer.wait_closed()

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, dict[str, str], bytes] | None:
        request_line = await asyncio.wait_for(reader.readline(), timeout=30)
        if not request_line:
            return None

        parts = request_line.decode().strip().split(" ", 2)
        if len(parts) < 2:
            return None
        method, path = parts[0], parts[1]

        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            decoded = line.decode().strip()
            if ":" in decoded:
                key, value = decoded.split(":", 1)
                headers[key.strip().lower()] = value.strip()

        body = b""
        if "chunked" in headers.get("transfer-encoding", ""):
            chunks = bytearray()
            while True:
                size_line = await reader.readline()
                size = int(size_line.strip(), 16)
                if size == 0:
                    await reader.readline()
                    break
                chunks += await reader.readexactly(size)
                await reader.readline()
            body = bytes(chunks)
        else:
            content_length = int(headers.get("content-length", "0"))
            if content_length > 0:
                body = await reader.readexactly(content_length)

        return method, path, headers, body

    async def _read_response(
        self, reader: asyncio.StreamReader
    ) -> tuple[bytes, int, bytes]:
        """Read an HTTP response.

        Returns (raw_wire_bytes, status_code, decoded_body).
        """
        status_line = await asyncio.wait_for(reader.readline(), timeout=30)
        result = bytearray(status_line)

        parts = status_line.decode("latin-1").strip().split(" ", 2)
        status_code = int(parts[1]) if len(parts) >= 2 else 0

        content_length = -1
        chunked = False
        while True:
            line = await reader.readline()
            result += line
            lower = line.decode("latin-1").strip().lower()
            if lower.startswith("content-length:"):
                content_length = int(lower.split(":", 1)[1].strip())
            if lower.startswith("transfer-encoding:") and "chunked" in lower:
                chunked = True
            if line in (b"\r\n", b"\n"):
                break

        body = bytearray()
        if chunked:
            while True:
                size_line = await reader.readline()
                result += size_line
                size = int(size_line.strip(), 16)
                if size == 0:
                    trailer = await reader.readline()
                    result += trailer
                    break
                chunk = await reader.readexactly(size)
                result += chunk
                body += chunk
                crlf = await reader.readline()
                result += crlf
        elif content_length >= 0:
            if content_length > 0:
                data = await reader.readexactly(content_length)
                result += data
                body += data
        else:
            tail = await reader.read()
            result += tail
            body += tail

        return bytes(result), status_code, bytes(body)

    @staticmethod
    def _extract_token(headers: dict[str, str]) -> str | None:
        auth = headers.get("authorization", "")
        if auth.startswith("token "):
            return auth[len("token ") :]
        if auth.startswith("Bearer "):
            return auth[len("Bearer ") :]
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[len("Basic ") :]).decode()
                if ":" in decoded:
                    return decoded.split(":", 1)[1]
            except Exception:
                pass
        return None

    @staticmethod
    def _check_permission(
        method: str,
        path: str,
        body: bytes,
        allowed_repo: str,
    ) -> tuple[bool, str, bool]:
        """Check if the request is allowed.

        Returns (allowed, reason, inject_token).
        """
        clean = path
        if clean.startswith("/api/v3"):
            clean = clean[len("/api/v3") :]

        if clean == "/graphql" and method == "POST":
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return False, "Cannot parse GraphQL body", False
            query = data.get("query", "") if isinstance(data, dict) else ""
            if re.search(r"\bmutation\b", query, re.IGNORECASE):
                return False, "GraphQL mutations are blocked", False
            return True, "", True

        git_match = re.match(
            r"/([^/]+/[^/]+?)(?:\.git)?/(info/refs|git-(?:upload|receive)-pack)",
            clean,
        )
        if git_match:
            request_repo = git_match.group(1)
            operation = git_match.group(2)
            if "receive-pack" in operation and method == "POST":
                if request_repo.lower() == allowed_repo.lower():
                    return True, "", True
                return True, "", False
            return True, "", True

        if method in ("POST", "PUT", "PATCH", "DELETE"):
            m = re.match(r"/repos/([^/]+/[^/]+)", clean)
            if m:
                request_repo = m.group(1)
                if request_repo.lower() == allowed_repo.lower():
                    return True, "", True
            # Write to a non-matching or non-repo endpoint: allow but
            # strip the auth token so it will fail at GitHub with 401.
            return True, "", False

        return True, "", True

    async def _maybe_notify_pr_created(
        self,
        method: str,
        path: str,
        status_code: int,
        body: bytes,
        proxy_token: str,
    ) -> None:
        if self._on_pr_created is None:
            return
        if method != "POST" or status_code != 201:
            return
        if not re.match(r"/repos/[^/]+/[^/]+/pulls$", path):
            return
        task_id = self._task_map.get(proxy_token)
        if not task_id:
            return
        try:
            data = json.loads(body)
            pr_number = data["number"]
            pr_url = data["html_url"]
        except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
            log.warning("GH proxy: failed to parse PR creation response")
            return
        log.info("GH proxy: detected PR #%d for task %s", pr_number, task_id)
        await self._on_pr_created(task_id, pr_number, pr_url)

    @staticmethod
    async def _send_error(
        writer: asyncio.StreamWriter,
        status: int,
        message: str,
    ) -> None:
        reason = "Forbidden" if status == 403 else "Bad Gateway"
        body = f'{{"message": "{message}"}}'
        response = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
            f"{body}"
        )
        writer.write(response.encode())
        await writer.drain()
