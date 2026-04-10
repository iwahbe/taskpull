#!/usr/bin/env python3
"""Capture GitHub API traffic from `gh` running inside a Docker container.

Runs a mitmproxy reverse proxy with generated TLS certs, launches a Docker
container with the same DNS-redirect + socat setup used in production, executes
a gh command inside, and saves the captured request/response pairs as JSON
fixtures. Auth tokens and OAuth headers are automatically redacted.

Prerequisites:
    - Docker must be running
    - `gh auth login` must have been run (token is sourced from `gh auth token`)

Examples:
    # Capture issue creation
    uv run --with mitmproxy scripts/capture_gh_traffic.py \
        --output tests/fixtures/create_issue.json \
        -- gh issue create --repo iwahbe/taskpull \
           --title "test" --body "test body"

    # Capture issue close
    uv run --with mitmproxy scripts/capture_gh_traffic.py \
        --output tests/fixtures/close_issue.json \
        -- gh issue close 1 --repo iwahbe/taskpull --reason "not planned"

    # Capture PR creation
    uv run --with mitmproxy scripts/capture_gh_traffic.py \
        --output tests/fixtures/create_pr.json \
        -- gh pr create --repo iwahbe/taskpull \
           --title "test" --body "test body" --head some-branch
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from mitmproxy import http, options
from mitmproxy.tools import dump

_DOCKERFILE_CONTENT = """\
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates curl git socat \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update && apt-get install -y gh && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash worker
USER worker
"""

_REDACT_HEADERS = {"authorization", "x-oauth-scopes", "x-oauth-client-id"}


def _scrub_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        k: ("REDACTED" if k.lower() in _REDACT_HEADERS else v)
        for k, v in headers.items()
    }


def _generate_certs(cert_dir: Path) -> tuple[Path, Path, Path, Path]:
    """Generate CA and server certs for the proxy. Returns (ca_cert, ca_key, server_cert, server_key)."""
    cert_dir.mkdir(parents=True, exist_ok=True)

    ca_key = cert_dir / "ca-key.pem"
    ca_cert = cert_dir / "ca.pem"
    server_key = cert_dir / "server-key.pem"
    server_cert = cert_dir / "server.pem"

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
            "/CN=capture-ca",
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
        "[v3_req]\n"
        "subjectAltName = DNS:host.docker.internal,DNS:api.github.com,DNS:github.com\n"
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
            "1",
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

    return ca_cert, ca_key, server_cert, server_key


async def _build_image(image_name: str) -> None:
    """Build a minimal Docker image with gh and socat."""
    with tempfile.TemporaryDirectory() as ctx_dir:
        ctx = Path(ctx_dir)
        (ctx / "Dockerfile").write_text(_DOCKERFILE_CONTENT)

        proc = await asyncio.create_subprocess_exec(
            "docker",
            "buildx",
            "build",
            "--load",
            "-t",
            image_name,
            str(ctx),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "BUILDX_BUILDER": ""},
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            sys.exit(f"docker build failed (rc={proc.returncode}):\n{stdout.decode()}")


class FlowCollector:
    def __init__(self) -> None:
        self.flows: list[dict] = []

    def response(self, flow: http.HTTPFlow) -> None:
        assert flow.response is not None
        self.flows.append(
            {
                "request": {
                    "method": flow.request.method,
                    "url": flow.request.pretty_url,
                    "path": flow.request.path,
                    "headers": _scrub_headers(dict(flow.request.headers)),
                    "body": flow.request.get_text(strict=False),
                },
                "response": {
                    "status_code": flow.response.status_code,
                    "headers": _scrub_headers(dict(flow.response.headers)),
                    "body": flow.response.get_text(strict=False),
                },
            }
        )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture GH API traffic via Docker + mitmproxy"
    )
    parser.add_argument(
        "--output", required=True, help="Path to write captured JSON fixtures"
    )
    parser.add_argument(
        "--port", type=int, default=9123, help="Host port for the reverse proxy"
    )
    parser.add_argument(
        "--docker-image", default="capture-gh-worker", help="Docker image to use"
    )
    parser.add_argument(
        "command",
        nargs="+",
        help="Command to run inside the container (e.g. gh issue create ...)",
    )
    args = parser.parse_args()

    result = subprocess.run(
        ["gh", "auth", "token"], capture_output=True, text=True, check=True
    )
    gh_token = result.stdout.strip()

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Building Docker image {args.docker_image}...")
    await _build_image(args.docker_image)

    with tempfile.TemporaryDirectory(prefix="capture-gh-") as tmpdir:
        tmp = Path(tmpdir)

        cert_dir = tmp / "certs"
        ca_cert, _ca_key, server_cert, server_key = _generate_certs(cert_dir)

        # Combine server cert + key into a single PEM for mitmproxy.
        combined_pem = tmp / "server-combined.pem"
        combined_pem.write_text(server_cert.read_text() + server_key.read_text())

        # Set up two mitmproxy reverse proxies:
        #   api.github.com on args.port     (container 127.0.0.2:443)
        #   github.com     on args.port + 1 (container 127.0.0.3:443)
        api_port = args.port
        git_port = args.port + 1
        collector = FlowCollector()
        opts = options.Options(
            mode=[
                f"reverse:https://api.github.com/@:{api_port}",
                f"reverse:https://github.com/@:{git_port}",
            ],
            certs=[str(combined_pem)],
        )
        master = dump.DumpMaster(opts, with_termlog=False, with_dumper=False)
        master.addons.add(collector)

        container_name = f"capture-gh-{os.getpid()}"
        try:
            # Start the proxy in the background.
            proxy_task = asyncio.create_task(master.run())

            # Wait briefly for the proxy to bind its ports.
            await asyncio.sleep(1)

            # Build the staging directory with the CA cert.
            staging = tmp / "staging"
            staging.mkdir()
            shutil.copy2(str(ca_cert), staging / ".taskpull-ca.pem")

            # Run the Docker container.
            # api.github.com -> 127.0.0.2, github.com -> 127.0.0.3
            # Two socat instances forward each to the correct host proxy port.
            docker_cmd = [
                "docker",
                "run",
                "--rm",
                "--name",
                container_name,
                "--add-host",
                "api.github.com:127.0.0.2",
                "--add-host",
                "github.com:127.0.0.3",
                "--sysctl",
                "net.ipv4.ip_unprivileged_port_start=0",
                "-v",
                f"{staging}:/opt/taskpull",
                "-e",
                f"GITHUB_TOKEN={gh_token}",
                "-e",
                f"GH_TOKEN={gh_token}",
                args.docker_image,
                "bash",
                "-c",
                "cat /etc/ssl/certs/ca-certificates.crt"
                " /opt/taskpull/.taskpull-ca.pem"
                " > /tmp/ca-bundle.pem && "
                "export SSL_CERT_FILE=/tmp/ca-bundle.pem && "
                "export NODE_EXTRA_CA_CERTS=/tmp/ca-bundle.pem && "
                "git config --global http.sslCAInfo /tmp/ca-bundle.pem && "
                "git config --global credential.helper "
                """'!/bin/sh -c \"echo username=x-access-token; echo password=\\$GITHUB_TOKEN\"' && """
                'git config --global url."https://github.com/".insteadOf "git@github.com:" && '
                "{ socat TCP-LISTEN:443,fork,reuseaddr,bind=127.0.0.2"
                f" TCP:host.docker.internal:{api_port} & "
                "while ! socat /dev/null TCP:127.0.0.2:443 2>/dev/null; do "
                "sleep 0.1; done; } && "
                "{ socat TCP-LISTEN:443,fork,reuseaddr,bind=127.0.0.3"
                f" TCP:host.docker.internal:{git_port} & "
                "while ! socat /dev/null TCP:127.0.0.3:443 2>/dev/null; do "
                "sleep 0.1; done; } && "
                + " ".join(shlex.quote(c) for c in args.command),
            ]

            print(f"Running: {' '.join(args.command)}")
            docker_proc = await asyncio.create_subprocess_exec(
                *docker_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await docker_proc.communicate()
            print(stdout.decode())
            if stderr:
                print(stderr.decode(), file=sys.stderr)
            if docker_proc.returncode != 0:
                print(
                    f"Container exited with code {docker_proc.returncode}",
                    file=sys.stderr,
                )

        finally:
            # Shut down the proxy cleanly.
            master.shutdown()
            try:
                await asyncio.wait_for(proxy_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                proxy_task.cancel()

            # Cleanup container if still running.
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                capture_output=True,
            )

        # Write captured flows.
        if collector.flows:
            output_path.write_text(json.dumps(collector.flows, indent=2) + "\n")
            print(
                f"\nCaptured {len(collector.flows)} request/response pairs -> {output_path}"
            )
        else:
            print("No flows captured!", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
