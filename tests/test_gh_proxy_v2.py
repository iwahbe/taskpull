"""Tests for LiveGitHubProxy (gh_proxy_v2) against its documented contract."""

from __future__ import annotations

import asyncio
import json
import ssl

import httpx
import pytest
from starlette.requests import Request

from taskpull.engine_events import (
    EngineEvent,
    IssueClosed,
    IssueCreated,
    PRClosed,
    PRCreated,
    SessionID,
)
from taskpull.gh_proxy_v2 import LiveGitHubProxy, Permissions
from taskpull.state_manager import InMemoryStateManager


class _FakeTls:
    """Stub TlsProvider. `handle()` does not exercise the TLS listener."""

    def server_ssl_context(self) -> ssl.SSLContext:
        return ssl.create_default_context()

    @property
    def ca_cert_bytes(self) -> bytes:
        return b"fake-ca"


def _in_memory_state_factory(model):
    return InMemoryStateManager()


def _make_request(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes = b"",
) -> Request:
    parsed = httpx.URL(url)
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": parsed.scheme,
        "server": (parsed.host, parsed.port or 443),
        "path": parsed.path,
        "raw_path": parsed.path.encode(),
        "query_string": parsed.query,
        "root_path": "",
        "headers": raw_headers,
    }
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


@pytest.mark.asyncio
async def test_rest_create_issue_on_allowed_repo_forwards_and_emits_event():
    """Per the docstring contract: POST /repos/{owner}/{repo}/issues is
    allowed when the path matches the session's `allowed_repo`. The proxy
    must forward the request to GitHub with the real GitHub token
    substituted into the Authorization header, return GitHub's response
    unchanged, and enqueue an `IssueCreated` event carrying the session id
    and the created issue's URL.
    """
    gh_issue = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
        "html_url": "https://github.com/owner/repo/issues/42",
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json=gh_issue)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        request_body = json.dumps({"title": "Something broke"}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(request_body)),
            },
            body=request_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 201
    assert json.loads(bytes(response.body)) == gh_issue

    assert len(captured) == 1
    forwarded = captured[0]
    assert forwarded.method == "POST"
    assert str(forwarded.url) == "https://api.github.com/repos/owner/repo/issues"
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded.content) == {"title": "Something broke"}

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == IssueCreated(
        session_id=session_id,
        issue_url="https://github.com/owner/repo/issues/42",
    )


@pytest.mark.asyncio
async def test_forget_removes_session_and_revokes_token_authority():
    """Per the abstract contract: `forget(session_id)` must "Remove all
    state associated with a session." After forget, the proxy token that
    was bound to that session must no longer be recognised — so an
    incoming request bearing that token cannot be treated as if the
    session's permissions still apply, and the real GitHub token must not
    be substituted.

    Observable behaviour: a request whose Authorization uses the
    forgotten proxy token either pass-through to GitHub with its original
    header (never carrying the real `gh_token`) or is rejected outright.
    Either outcome demonstrates the session was truly forgotten.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(401, json={"message": "Bad credentials"})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        await proxy.forget(session_id)

        body = json.dumps({"title": "Bug"}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(body)),
            },
            body=body,
        )
        response = await proxy.handle(request)

    for req in captured:
        assert "gh-real-token" not in req.headers.get("authorization", "")

    assert response.status_code != 201
    assert queue.empty()
    """Per the docstring contract: a session may only open issues in its
    `allowed_repo`. A POST /repos/{owner}/{repo}/issues targeting a
    different repo must be rejected by the proxy without being forwarded
    to GitHub, and no `IssueCreated` event may be emitted.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json={})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        request_body = json.dumps({"title": "Something broke"}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/repos/other-owner/other-repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(request_body)),
            },
            body=request_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_create_issue_does_not_emit_event_when_github_fails():
    """Per the docstring contract: `IssueCreated` is emitted on successful
    responses only. If GitHub rejects the create-issue request (e.g. with
    a 422 validation error), the proxy passes the failing response through
    to the caller but must not enqueue an `IssueCreated` event.
    """
    gh_error = {"message": "Validation Failed", "errors": []}

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(422, json=gh_error)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        request_body = json.dumps({"title": ""}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(request_body)),
            },
            body=request_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 422
    assert json.loads(bytes(response.body)) == gh_error
    assert len(captured) == 1
    assert queue.empty()


def _receive_pack_body(old_sha: str, new_sha: str, ref: str) -> bytes:
    """Build a minimal git-receive-pack pkt-line request body.

    Wire format: `<4-hex-length><old> <new> <ref>\\x00<caps>\\n0000PACK…`.
    The length prefix includes the 4 bytes of the length prefix itself.
    """
    caps = "report-status-v2 side-band-64k quiet object-format=sha1 agent=git/2.39.5"
    cmd = f"{old_sha} {new_sha} {ref}\x00{caps}\n".encode()
    length = len(cmd) + 4
    pack = b"PACK" + b"\x00\x00\x00\x02" + b"\x00\x00\x00\x00"
    return f"{length:04x}".encode() + cmd + b"0000" + pack


def _push_request(repo: str, ref: str, proxy_token: str) -> Request:
    body = _receive_pack_body("0" * 40, "a" * 40, ref)
    return _make_request(
        method="POST",
        url=f"https://github.com/{repo}.git/git-receive-pack",
        headers={
            "host": "github.com",
            "authorization": f"token {proxy_token}",
            "content-type": "application/x-git-receive-pack-request",
            "content-length": str(len(body)),
        },
        body=body,
    )


@pytest.mark.asyncio
async def test_git_push_establishes_branch_and_restricts_subsequent_pushes():
    """Per the docstring contract for git push (receive-pack):

    - Pushes to the session's `allowed_repo` are permitted.
    - The first push establishes the session's branch.
    - Subsequent pushes must target that same branch; pushes to any other
      branch must be rejected (403) without being forwarded to GitHub.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=b"",
            headers={"content-type": "application/x-git-receive-pack-result"},
        )

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        first = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/iwahbe/tighten", proxy_token)
        )
        assert first.status_code == 200
        assert len(captured) == 1

        second = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/iwahbe/tighten", proxy_token)
        )
        assert second.status_code == 200
        assert len(captured) == 2

        third = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/main", proxy_token)
        )
        assert third.status_code == 403
        assert len(captured) == 2


@pytest.mark.asyncio
async def test_git_push_to_disallowed_repo_is_rejected():
    """Per the docstring contract for git push (receive-pack): pushes are
    only permitted to the session's `allowed_repo`. A push to any other
    repository must be rejected with 403 and not forwarded to GitHub.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=b"",
            headers={"content-type": "application/x-git-receive-pack-result"},
        )

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _push_request("other-owner/other-repo", "refs/heads/feature", proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_git_upload_pack_is_forwarded():
    """Per the docstring contract: "Git upload-pack (clone/fetch)" is a
    read operation and must be forwarded unconditionally. The POST to
    /{owner}/{repo}.git/git-upload-pack carries the client's wants/haves
    negotiation and receives the packfile; the proxy must forward it with
    the real GitHub token and return the upstream body unchanged. No
    events are emitted.
    """
    upload_pack_body = (
        b"0032want aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n00000009done\n"
    )
    pack_response = b"PACK\x00\x00\x00\x02\x00\x00\x00\x00"

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=pack_response,
            headers={"content-type": "application/x-git-upload-pack-result"},
        )

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        request = _make_request(
            method="POST",
            url="https://github.com/iwahbe/taskpull.git/git-upload-pack",
            headers={
                "host": "github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/x-git-upload-pack-request",
                "accept": "application/x-git-upload-pack-result",
                "content-length": str(len(upload_pack_body)),
            },
            body=upload_pack_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 200
    assert bytes(response.body) == pack_response

    assert len(captured) == 1
    forwarded = captured[0]
    assert forwarded.method == "POST"
    assert (
        str(forwarded.url) == "https://github.com/iwahbe/taskpull.git/git-upload-pack"
    )
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == upload_pack_body
    assert queue.empty()


@pytest.mark.asyncio
async def test_basic_auth_header_is_rewritten_with_real_token():
    """Per the proxy's auth contract: sessions are identified by a token in
    the Authorization header. Git over HTTPS uses HTTP Basic auth (the
    client sends `Authorization: Basic <base64(user:token)>`), so the
    proxy must recognise the proxy token when it appears in a Basic auth
    header, and forward a Basic auth header whose password is the real
    GitHub token.

    The forwarded Authorization value must be a valid HTTP Basic header —
    i.e. `"Basic "` followed by a base64 ASCII string — not the Python
    repr of a bytes object.
    """
    import base64 as _b64

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=b"",
            headers={"content-type": "application/x-git-upload-pack-advertisement"},
        )

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        basic_value = _b64.b64encode(f"iwahbe:{proxy_token}".encode()).decode()
        request = _make_request(
            method="GET",
            url="https://github.com/iwahbe/taskpull.git/info/refs?service=git-upload-pack",
            headers={
                "host": "github.com",
                "authorization": f"Basic {basic_value}",
                "user-agent": "git/2.39.5",
            },
        )

        response = await proxy.handle(request)

    assert response.status_code == 200
    assert len(captured) == 1
    forwarded_auth = captured[0].headers["authorization"]
    assert forwarded_auth.startswith("Basic ")
    decoded = _b64.b64decode(forwarded_auth.removeprefix("Basic ")).decode()
    assert decoded == "iwahbe:gh-real-token"


@pytest.mark.asyncio
async def test_rest_create_pr_after_pushing_branch_forwards_and_emits_event():
    """Per the docstring contract: after a session pushes a branch, it may
    open a PR via POST /repos/{owner}/{repo}/pulls whose `head` names the
    pushed branch. The proxy must forward the request with the real GitHub
    token, return the response unchanged, and emit a `PRCreated` event.
    """
    branch = "feature/login"

    gh_pr = {
        "url": "https://api.github.com/repos/owner/repo/pulls/7",
        "node_id": "PR_kwDOABCDEF",
        "number": 7,
        "html_url": "https://github.com/owner/repo/pull/7",
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        return httpx.Response(201, json=gh_pr)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        push = await proxy.handle(
            _push_request("owner/repo", f"refs/heads/{branch}", proxy_token)
        )
        assert push.status_code == 200

        pr_body = json.dumps(
            {"title": "Add login", "head": branch, "base": "main"}
        ).encode()
        pr_request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/pulls",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(pr_body)),
            },
            body=pr_body,
        )

        response = await proxy.handle(pr_request)

    assert response.status_code == 201
    assert json.loads(bytes(response.body)) == gh_pr

    assert len(captured) == 2
    forwarded_pr = captured[1]
    assert forwarded_pr.method == "POST"
    assert str(forwarded_pr.url) == "https://api.github.com/repos/owner/repo/pulls"
    assert forwarded_pr.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded_pr.content) == {
        "title": "Add login",
        "head": branch,
        "base": "main",
    }

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == PRCreated(
        session_id=session_id,
        pr_url="https://github.com/owner/repo/pull/7",
    )


@pytest.mark.asyncio
async def test_rest_patch_issue_created_by_session_is_allowed_and_close_emits_event():
    """Per the docstring contract: PATCH /repos/{owner}/{repo}/issues/{n} is
    allowed when the issue was created earlier in the same session, and the
    proxy must emit `IssueClosed` when such a PATCH sets `state=closed`.

    The create and patch are issued against two distinct proxy instances
    sharing a single state manager, to prove that session ownership of the
    created issue survives a process restart.
    """
    gh_issue = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }
    gh_issue_closed = {**gh_issue, "state": "closed"}

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.method == "POST":
            return httpx.Response(201, json=gh_issue)
        if request.method == "PATCH":
            return httpx.Response(200, json=gh_issue_closed)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()
    shared_state = InMemoryStateManager()

    def shared_state_factory(model):
        return shared_state

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=shared_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        created_event = await asyncio.wait_for(queue.get(), timeout=0.5)
        assert isinstance(created_event, IssueCreated)

        restarted = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=shared_state_factory,
            queue=queue,
        )

        patch_body = json.dumps({"state": "closed"}).encode()
        patch_req = _make_request(
            method="PATCH",
            url="https://api.github.com/repos/owner/repo/issues/42",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(patch_body)),
            },
            body=patch_body,
        )
        response = await restarted.handle(patch_req)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_issue_closed

    assert len(captured) == 2
    forwarded = captured[1]
    assert forwarded.method == "PATCH"
    assert str(forwarded.url) == "https://api.github.com/repos/owner/repo/issues/42"
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded.content) == {"state": "closed"}

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == IssueClosed(
        issue_url="https://github.com/owner/repo/issues/42",
    )


@pytest.mark.asyncio
async def test_rest_patch_issue_not_created_by_session_is_rejected():
    """Per the docstring contract: PATCH /repos/{owner}/{repo}/issues/{n} is
    permitted only when the issue was created earlier in the same session.
    A session that created issue #42 must not be able to mutate issue #99
    (same repo, different number); the PATCH must be rejected with 403 and
    must not be forwarded to GitHub.
    """
    gh_created = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json=gh_created)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        await asyncio.wait_for(queue.get(), timeout=0.5)

        patch_body = json.dumps({"state": "closed"}).encode()
        patch_req = _make_request(
            method="PATCH",
            url="https://api.github.com/repos/owner/repo/issues/99",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(patch_body)),
            },
            body=patch_body,
        )
        response = await proxy.handle(patch_req)

    assert response.status_code == 403
    assert len(captured) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_patch_issue_in_wrong_repo_is_rejected_even_for_matching_number():
    """Per the docstring contract: PATCH /repos/{owner}/{repo}/issues/{n}
    is only allowed when the request targets the session's `allowed_repo`.

    Session ownership is tracked by issue number, so a naive check that
    only verifies the number is in `created_issue_ids` would wrongly
    accept a PATCH targeting the same number in a different repo. The
    proxy must reject such a request with 403 and not forward it — issue
    numbers collide across repos, so cross-repo PATCH via number-collision
    would be a session-scope escape.
    """
    gh_created = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json=gh_created)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        await asyncio.wait_for(queue.get(), timeout=0.5)

        patch_body = json.dumps({"state": "closed"}).encode()
        patch_req = _make_request(
            method="PATCH",
            url="https://api.github.com/repos/other-owner/other-repo/issues/42",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(patch_body)),
            },
            body=patch_body,
        )
        response = await proxy.handle(patch_req)

    assert response.status_code == 403
    assert len(captured) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_create_pr_with_head_not_matching_session_branch_is_rejected():
    """Per the docstring contract: POST /repos/{owner}/{repo}/pulls is only
    permitted when the body's `head` names the session's established
    branch. A request whose `head` points at a different branch must be
    rejected with 403 and must not be forwarded to GitHub.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        return httpx.Response(201, json={})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        push = await proxy.handle(
            _push_request("owner/repo", "refs/heads/feature/login", proxy_token)
        )
        assert push.status_code == 200
        assert len(captured) == 1

        pr_body = json.dumps(
            {"title": "Add login", "head": "main", "base": "production"}
        ).encode()
        pr_request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/pulls",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(pr_body)),
            },
            body=pr_body,
        )

        response = await proxy.handle(pr_request)

    assert response.status_code == 403
    assert len(captured) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_patch_pr_close_emits_pr_closed_event():
    """Per the docstring contract: when a session closes a pull request it
    created (PATCH /repos/{owner}/{repo}/pulls/{n} with `state=closed`),
    the proxy must forward the PATCH and emit a `PRClosed` event carrying
    the PR's URL. `IssueClosed` must not be emitted for PR closes.
    """
    gh_pr = {
        "url": "https://api.github.com/repos/owner/repo/pulls/7",
        "node_id": "PR_kwDOABCDEF",
        "number": 7,
        "html_url": "https://github.com/owner/repo/pull/7",
    }
    gh_pr_closed = {**gh_pr, "state": "closed"}

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        if request.method == "POST":
            return httpx.Response(201, json=gh_pr)
        if request.method == "PATCH":
            return httpx.Response(200, json=gh_pr_closed)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        push = await proxy.handle(
            _push_request("owner/repo", "refs/heads/feature/login", proxy_token)
        )
        assert push.status_code == 200

        pr_body = json.dumps(
            {"title": "Add login", "head": "feature/login", "base": "main"}
        ).encode()
        pr_request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/pulls",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(pr_body)),
            },
            body=pr_body,
        )
        create_resp = await proxy.handle(pr_request)
        assert create_resp.status_code == 201

        created_event = await asyncio.wait_for(queue.get(), timeout=0.5)
        assert isinstance(created_event, PRCreated)

        patch_body = json.dumps({"state": "closed"}).encode()
        patch_request = _make_request(
            method="PATCH",
            url="https://api.github.com/repos/owner/repo/pulls/7",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(patch_body)),
            },
            body=patch_body,
        )
        response = await proxy.handle(patch_request)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_pr_closed

    assert len(captured) == 3
    forwarded_patch = captured[2]
    assert forwarded_patch.method == "PATCH"
    assert str(forwarded_patch.url) == "https://api.github.com/repos/owner/repo/pulls/7"
    assert forwarded_patch.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded_patch.content) == {"state": "closed"}

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == PRClosed(
        pr_url="https://github.com/owner/repo/pull/7",
    )


@pytest.mark.asyncio
async def test_rest_get_is_forwarded_regardless_of_target_repo():
    """Per the docstring contract: REST GET requests are forwarded
    unconditionally, regardless of whether the target path matches the
    session's `allowed_repo`. The proxy must substitute the real GitHub
    token and return the response unchanged. No events are emitted.
    """
    gh_repo = {
        "full_name": "other-owner/other-repo",
        "private": False,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=gh_repo)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        request = _make_request(
            method="GET",
            url="https://api.github.com/repos/other-owner/other-repo",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "accept": "application/vnd.github+json",
            },
        )

        response = await proxy.handle(request)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_repo

    assert len(captured) == 1
    forwarded = captured[0]
    assert forwarded.method == "GET"
    assert str(forwarded.url) == "https://api.github.com/repos/other-owner/other-repo"
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_query_is_forwarded_unconditionally():
    """Per the docstring contract: GraphQL queries are forwarded without
    per-repo restrictions. The proxy must pass the query body through to
    GitHub, substitute the real token, return the response unchanged, and
    emit no events.
    """
    gh_response = {"data": {"viewer": {"login": "octocat"}}}

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=gh_response)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        query_body = json.dumps({"query": "query { viewer { login } }"}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(query_body)),
            },
            body=query_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_response
    assert len(captured) == 1
    forwarded = captured[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded.content) == {"query": "query { viewer { login } }"}
    assert queue.empty()


# Exact body of the createPullRequest mutation and its successful response
# as captured in tests/fixtures/create_pr.json (entry 3 of the array).
_FIXTURE_CREATE_PR_REQUEST = (
    '{"query":"\\n\\t\\tmutation PullRequestCreate($input: CreatePullRequestInput!) {'
    "\\n\\t\\t\\tcreatePullRequest(input: $input) {"
    "\\n\\t\\t\\t\\tpullRequest {"
    "\\n\\t\\t\\t\\t\\tid"
    "\\n\\t\\t\\t\\t\\turl"
    "\\n\\t\\t\\t\\t}"
    "\\n\\t\\t\\t}"
    '\\n\\t}",'
    '"variables":{"input":{'
    '"baseRefName":"main",'
    '"body":"This is a test PR created to capture GH API traffic for proxy tests. Safe to close.",'
    '"draft":false,'
    '"headRefName":"recapture-test",'
    '"maintainerCanModify":true,'
    '"repositoryId":"R_kgDORru3Uw",'
    '"title":"Test PR for proxy capture"'
    "}}}"
).encode()

_FIXTURE_CREATE_PR_RESPONSE = (
    '{"data":{"createPullRequest":{"pullRequest":'
    '{"id":"PR_kwDORru3U87RmFVG",'
    '"url":"https://github.com/iwahbe/taskpull/pull/9"}}}}'
)


@pytest.mark.asyncio
async def test_graphql_create_pull_request_forwards_and_emits_event():
    """Per the docstring contract: a `createPullRequest` GraphQL mutation is
    allowed when its `input.repositoryId` resolves (via a node-id lookup)
    to the session's `allowed_repo` and its `input.headRefName` matches the
    session's established branch. The proxy must forward the mutation
    unchanged, return GitHub's response unchanged, and emit a `PRCreated`
    event carrying the PR URL from the response.

    The mutation request/response bodies here are the verbatim strings
    captured in tests/fixtures/create_pr.json.
    """
    resolve_response = {
        "data": {"node": {"name": "taskpull", "owner": {"login": "iwahbe"}}}
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(200, json=resolve_response)
        if "createPullRequest" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        push = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/recapture-test", proxy_token)
        )
        assert push.status_code == 200

        mutation_request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json; charset=utf-8",
                "content-length": str(len(_FIXTURE_CREATE_PR_REQUEST)),
            },
            body=_FIXTURE_CREATE_PR_REQUEST,
        )

        response = await proxy.handle(mutation_request)

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_CREATE_PR_RESPONSE

    forwarded_mutations = [r for r in captured if b"createPullRequest" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_CREATE_PR_REQUEST

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    # Events must carry the HTML URL (https://github.com/...), the same
    # shape emitted on the REST path. GraphQL's createPullRequest response
    # already returns the HTML URL directly, so the proxy can pass it
    # through as-is.
    assert event == PRCreated(
        session_id=session_id,
        pr_url="https://github.com/iwahbe/taskpull/pull/9",
    )


@pytest.mark.asyncio
async def test_graphql_create_pull_request_in_wrong_repo_is_rejected():
    """Per the docstring contract: a `createPullRequest` GraphQL mutation is
    permitted only when `input.repositoryId` resolves to the session's
    `allowed_repo`. If the node-id lookup resolves to a different repo, the
    proxy must reject the mutation with 403 and must not forward it to
    GitHub's GraphQL endpoint. No `PRCreated` event may be emitted.

    The node-resolution request itself is expected (the proxy issues it to
    make the allow/deny decision); only the mutation must not be forwarded.
    """
    resolve_response = {
        "data": {"node": {"name": "other-repo", "owner": {"login": "other-owner"}}}
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(200, json=resolve_response)
        if "createPullRequest" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        push = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/recapture-test", proxy_token)
        )
        assert push.status_code == 200

        mutation_request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json; charset=utf-8",
                "content-length": str(len(_FIXTURE_CREATE_PR_REQUEST)),
            },
            body=_FIXTURE_CREATE_PR_REQUEST,
        )

        response = await proxy.handle(mutation_request)

    assert response.status_code == 403

    forwarded_mutations = [r for r in captured if b"createPullRequest" in r.content]
    assert forwarded_mutations == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_create_pull_request_when_node_resolution_returns_null_is_rejected():
    """Per the docstring contract: a `createPullRequest` GraphQL mutation
    is only permitted when `input.repositoryId` resolves (via the node API)
    to the session's `allowed_repo`. GitHub's node API returns
    `{"data": {"node": null}}` for node IDs that don't exist or that the
    caller cannot see. In that case the proxy cannot prove the target
    matches the allowed repo, so it must reject the mutation with 403
    and not forward it. Returning 500 or crashing is a contract violation.
    """
    null_resolve_response = {"data": {"node": None}}

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(200, json=null_resolve_response)
        if "createPullRequest" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        push = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/recapture-test", proxy_token)
        )
        assert push.status_code == 200

        mutation_request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json; charset=utf-8",
                "content-length": str(len(_FIXTURE_CREATE_PR_REQUEST)),
            },
            body=_FIXTURE_CREATE_PR_REQUEST,
        )

        response = await proxy.handle(mutation_request)

    assert response.status_code == 403

    forwarded_mutations = [r for r in captured if b"createPullRequest" in r.content]
    assert forwarded_mutations == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_create_pull_request_with_wrong_head_branch_is_rejected():
    """Per the docstring contract: a `createPullRequest` GraphQL mutation is
    permitted only when `input.headRefName` matches the branch the session
    established via `git push`. A session that pushed `feature/login` must
    not be able to open a PR whose head is some other branch (even in the
    session's own allowed repo): the mutation must be rejected with 403 and
    not forwarded to GitHub. No `PRCreated` event is emitted.

    The fixture mutation's `headRefName` is `recapture-test`, so pushing a
    different branch (`feature/login`) before the mutation demonstrates the
    mismatch rejection without relying on the absence of any prior push.
    """
    resolve_response = {
        "data": {"node": {"name": "taskpull", "owner": {"login": "iwahbe"}}}
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(200, json=resolve_response)
        if "createPullRequest" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        push = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/feature/login", proxy_token)
        )
        assert push.status_code == 200

        mutation_request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json; charset=utf-8",
                "content-length": str(len(_FIXTURE_CREATE_PR_REQUEST)),
            },
            body=_FIXTURE_CREATE_PR_REQUEST,
        )

        response = await proxy.handle(mutation_request)

    assert response.status_code == 403

    forwarded_mutations = [r for r in captured if b"createPullRequest" in r.content]
    assert forwarded_mutations == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_unknown_mutation_is_rejected():
    """Per the docstring contract: "All other mutations are rejected." Only
    the enumerated set (createPullRequest, createIssue, updateIssue,
    updatePullRequest, closeIssue, closePullRequest, addLabelsToLabelable,
    removeLabelsFromLabelable) is allowlisted, and the proxy must reject
    any other mutation name with 403 and not forward it to GitHub. No
    events may be emitted.
    """
    mutation_body = json.dumps(
        {
            "query": "mutation DeleteRepo($id: ID!) { deleteRepository(input: {repositoryId: $id}) { clientMutationId } }",
            "variables": {"id": "R_kgDORru3Uw"},
        }
    ).encode()

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"data": {"deleteRepository": {}}})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        request = _make_request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(mutation_body)),
            },
            body=mutation_body,
        )

        response = await proxy.handle(request)

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_add_labels_to_session_created_issue_is_forwarded():
    """Per the docstring contract: POST /repos/{owner}/{repo}/issues/{n}/labels
    is allowed when the issue (by number) was created earlier in the session.
    The proxy must forward the request with the real GitHub token and return
    the response unchanged. No event is emitted for label changes.
    """
    gh_issue = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }
    gh_labels = [{"name": "bug"}, {"name": "priority"}]

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.method == "POST" and request.url.path.endswith("/issues"):
            return httpx.Response(201, json=gh_issue)
        if request.method == "POST" and request.url.path.endswith("/labels"):
            return httpx.Response(200, json=gh_labels)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        created_event = await asyncio.wait_for(queue.get(), timeout=0.5)
        assert isinstance(created_event, IssueCreated)

        label_body = json.dumps({"labels": ["bug", "priority"]}).encode()
        label_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues/42/labels",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(label_body)),
            },
            body=label_body,
        )
        response = await proxy.handle(label_req)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_labels

    assert len(captured) == 2
    forwarded = captured[1]
    assert forwarded.method == "POST"
    assert (
        str(forwarded.url) == "https://api.github.com/repos/owner/repo/issues/42/labels"
    )
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert json.loads(forwarded.content) == {"labels": ["bug", "priority"]}

    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_add_labels_to_issue_not_created_by_session_is_rejected():
    """Per the docstring contract: POST /repos/{owner}/{repo}/issues/{n}/labels
    is only permitted when the issue was created earlier in the same session.
    A session that created issue #42 must not be able to label issue #99 in
    the same repo; the request must be rejected with 403 and not forwarded.
    """
    gh_created = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json=gh_created)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        await asyncio.wait_for(queue.get(), timeout=0.5)

        label_body = json.dumps({"labels": ["bug"]}).encode()
        label_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues/99/labels",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(label_body)),
            },
            body=label_body,
        )
        response = await proxy.handle(label_req)

    assert response.status_code == 403
    assert len(captured) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_delete_label_from_session_created_issue_is_forwarded():
    """Per the docstring contract: DELETE
    /repos/{owner}/{repo}/issues/{n}/labels/{name} is allowed when the
    issue was created earlier in the session. The proxy must forward the
    request with the real GitHub token and return the upstream response
    unchanged. No event is emitted for label changes.
    """
    gh_issue = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }
    gh_remaining_labels = [{"name": "priority"}]

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.method == "POST":
            return httpx.Response(201, json=gh_issue)
        if request.method == "DELETE":
            return httpx.Response(200, json=gh_remaining_labels)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        created_event = await asyncio.wait_for(queue.get(), timeout=0.5)
        assert isinstance(created_event, IssueCreated)

        delete_req = _make_request(
            method="DELETE",
            url="https://api.github.com/repos/owner/repo/issues/42/labels/bug",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "accept": "application/vnd.github+json",
            },
        )
        response = await proxy.handle(delete_req)

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == gh_remaining_labels

    assert len(captured) == 2
    forwarded = captured[1]
    assert forwarded.method == "DELETE"
    assert (
        str(forwarded.url)
        == "https://api.github.com/repos/owner/repo/issues/42/labels/bug"
    )
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_delete_label_from_issue_not_created_by_session_is_rejected():
    """Per the docstring contract: DELETE
    /repos/{owner}/{repo}/issues/{n}/labels/{name} is permitted only when
    the issue was created earlier in the same session. A session that
    created issue #42 must not be able to delete a label from issue #99
    in the same repo; the request must be rejected with 403 and not
    forwarded to GitHub.
    """
    gh_created = {
        "url": "https://api.github.com/repos/owner/repo/issues/42",
        "html_url": "https://github.com/owner/repo/issues/42",
        "node_id": "I_kwDOABCDEF",
        "number": 42,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json=gh_created)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        create_body = json.dumps({"title": "Bug"}).encode()
        create_req = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/issues",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(create_body)),
            },
            body=create_body,
        )
        create_resp = await proxy.handle(create_req)
        assert create_resp.status_code == 201

        await asyncio.wait_for(queue.get(), timeout=0.5)

        delete_req = _make_request(
            method="DELETE",
            url="https://api.github.com/repos/owner/repo/issues/99/labels/bug",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "accept": "application/vnd.github+json",
            },
        )
        response = await proxy.handle(delete_req)

    assert response.status_code == 403
    assert len(captured) == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_rest_write_to_unknown_path_is_rejected():
    """Per the docstring contract: "All other REST write paths are
    rejected." A REST write (POST/PUT/PATCH/DELETE) that does not match
    one of the enumerated allowlisted paths must be rejected with 403 and
    not forwarded, even when it targets the session's `allowed_repo`.

    POST /repos/{owner}/{repo}/deployments is a real GitHub endpoint that
    is outside the proxy's allowlist; it stands in here for any
    non-allowlisted write path.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(201, json={})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="owner/repo")
        )

        body = json.dumps({"ref": "main"}).encode()
        request = _make_request(
            method="POST",
            url="https://api.github.com/repos/owner/repo/deployments",
            headers={
                "host": "api.github.com",
                "authorization": f"token {proxy_token}",
                "content-type": "application/json",
                "content-length": str(len(body)),
            },
            body=body,
        )
        response = await proxy.handle(request)

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_unauthenticated_write_is_passed_through_without_injecting_token():
    """An incoming request with no proxy token must be forwarded as-is,
    without the proxy injecting its real GitHub token. Using DELETE
    /repos/{owner}/{repo} — the repo-deletion endpoint — as the canary:
    if the proxy ever inserted its token here, an unauthenticated caller
    could destroy any repo the token can reach.

    The proxy must neither reject the request nor rewrite its
    Authorization header; upstream GitHub is the authority that decides
    whether to accept it. No events are emitted.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(401, json={"message": "Requires authentication"})

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="owner/repo")
        )

        request = _make_request(
            method="DELETE",
            url="https://api.github.com/repos/owner/repo",
            headers={
                "host": "api.github.com",
                "accept": "application/vnd.github+json",
            },
        )

        response = await proxy.handle(request)

    assert len(captured) == 1
    forwarded = captured[0]
    assert forwarded.method == "DELETE"
    assert str(forwarded.url) == "https://api.github.com/repos/owner/repo"
    assert "gh-real-token" not in forwarded.headers.get("authorization", "")
    assert "authorization" not in {k.lower() for k in forwarded.headers}

    assert response.status_code == 401
    assert queue.empty()


# ---------------------------------------------------------------------------
# Verbatim request/response bodies for GraphQL mutations in tests/fixtures/.
# Pinned so tests exercise the exact bytes the GitHub CLI sends in practice.
# ---------------------------------------------------------------------------

# From tests/fixtures/create_issue.json (exchange 1).
_FIXTURE_CREATE_ISSUE_REQUEST = (
    b'{"query":"\\n\\tmutation IssueCreate($input: CreateIssueInput!) {'
    b"\\n\\t\\tcreateIssue(input: $input) {"
    b"\\n\\t\\t\\tissue {"
    b"\\n\\t\\t\\t\\tid"
    b"\\n\\t\\t\\t\\turl"
    b"\\n\\t\\t\\t}"
    b"\\n\\t\\t}"
    b'\\n\\t}",'
    b'"variables":{"input":{'
    b'"body":"This is a test issue created to capture GH API traffic for proxy tests. Safe to close.",'
    b'"repositoryId":"R_kgDORru3Uw",'
    b'"title":"Test issue for proxy capture"'
    b"}}}"
)
_FIXTURE_CREATE_ISSUE_RESPONSE = (
    '{"data":{"createIssue":{"issue":'
    '{"id":"I_kwDORru3U8780gDh",'
    '"url":"https://github.com/iwahbe/taskpull/issues/1"}}}}'
)

# From tests/fixtures/close_issue.json (exchange 1). Trailing \n preserved.
_FIXTURE_CLOSE_ISSUE_REQUEST = (
    b'{"query":"mutation IssueClose($input:CloseIssueInput!)'
    b'{closeIssue(input: $input){issue{id}}}",'
    b'"variables":{"input":'
    b'{"issueId":"I_kwDORru3U8780gDh","stateReason":"NOT_PLANNED"}}}\n'
)
_FIXTURE_CLOSE_ISSUE_RESPONSE = (
    '{"data":{"closeIssue":{"issue":{"id":"I_kwDORru3U8780gDh"}}}}'
)

# From tests/fixtures/close_pr.json (exchange 1). Trailing \n preserved.
_FIXTURE_CLOSE_PR_REQUEST = (
    b'{"query":"mutation PullRequestClose($input:ClosePullRequestInput!)'
    b'{closePullRequest(input: $input){pullRequest{id}}}",'
    b'"variables":{"input":{"pullRequestId":"PR_kwDORru3U87RmFVG"}}}\n'
)
_FIXTURE_CLOSE_PR_RESPONSE = (
    '{"data":{"closePullRequest":{"pullRequest":{"id":"PR_kwDORru3U87RmFVG"}}}}'
)

# From tests/fixtures/edit_issue.json (exchange 1). Trailing \n preserved.
_FIXTURE_UPDATE_ISSUE_REQUEST = (
    b'{"query":"mutation IssueUpdate($input:UpdateIssueInput!)'
    b'{updateIssue(input: $input){__typename}}",'
    b'"variables":{"input":{'
    b'"id":"I_kwDORru3U87829yE",'
    b'"title":"Edit test issue (graphql updated)",'
    b'"body":"Updated body via GraphQL"}}}\n'
)
_FIXTURE_UPDATE_ISSUE_RESPONSE = (
    '{"data":{"updateIssue":{"__typename":"UpdateIssuePayload"}}}'
)

# From tests/fixtures/edit_pr.json (exchange 2). Trailing \n preserved.
_FIXTURE_UPDATE_PR_REQUEST = (
    b'{"query":"mutation PullRequestUpdate($input:UpdatePullRequestInput!)'
    b'{updatePullRequest(input: $input){__typename}}",'
    b'"variables":{"input":{'
    b'"pullRequestId":"PR_kwDORru3U87RmEUU",'
    b'"title":"Edit test PR (graphql updated)",'
    b'"body":"Updated body via GraphQL"}}}\n'
)
_FIXTURE_UPDATE_PR_RESPONSE = (
    '{"data":{"updatePullRequest":{"__typename":"UpdatePullRequestPayload"}}}'
)

# From tests/fixtures/add_labels.json (exchange 2). Trailing \n preserved.
_FIXTURE_ADD_LABELS_REQUEST = (
    b'{"query":"mutation LabelAdd($input:AddLabelsToLabelableInput!)'
    b'{addLabelsToLabelable(input: $input){__typename}}",'
    b'"variables":{"input":{'
    b'"labelableId":"I_kwDORru3U8783GEO",'
    b'"labelIds":["LA_kwDORru3U88AAAACcDusPQ",'
    b'"LA_kwDORru3U88AAAACcDusVg"]}}}\n'
)
_FIXTURE_ADD_LABELS_RESPONSE = (
    '{"data":{"addLabelsToLabelable":{"__typename":"AddLabelsToLabelablePayload"}}}'
)

# From tests/fixtures/remove_labels.json (exchange 2). Trailing \n preserved.
_FIXTURE_REMOVE_LABELS_REQUEST = (
    b'{"query":"mutation LabelRemove($input:RemoveLabelsFromLabelableInput!)'
    b'{removeLabelsFromLabelable(input: $input){__typename}}",'
    b'"variables":{"input":{'
    b'"labelableId":"I_kwDORru3U8783GEO",'
    b'"labelIds":["LA_kwDORru3U88AAAACcDusPQ"]}}}\n'
)
_FIXTURE_REMOVE_LABELS_RESPONSE = (
    '{"data":{"removeLabelsFromLabelable":'
    '{"__typename":"RemoveLabelsFromLabelablePayload"}}}'
)


def _gql_request(body: bytes, proxy_token: str) -> Request:
    return _make_request(
        method="POST",
        url="https://api.github.com/graphql",
        headers={
            "host": "api.github.com",
            "authorization": f"token {proxy_token}",
            "content-type": "application/json",
            "content-length": str(len(body)),
        },
        body=body,
    )


async def _bootstrap_rest_issue(
    proxy: LiveGitHubProxy, proxy_token: str, url: str, node_id: str
) -> None:
    """Create an issue via REST so the session owns `node_id`."""
    body = json.dumps({"title": "bootstrap"}).encode()
    request = _make_request(
        method="POST",
        url="https://api.github.com/repos/iwahbe/taskpull/issues",
        headers={
            "host": "api.github.com",
            "authorization": f"token {proxy_token}",
            "content-type": "application/json",
            "content-length": str(len(body)),
        },
        body=body,
    )
    response = await proxy.handle(request)
    assert response.status_code == 201, response.body


# ---------------------------------------------------------------------------
# createIssue — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_create_issue_in_allowed_repo_forwards_and_emits_event():
    """Per the docstring contract: a `createIssue` GraphQL mutation is
    allowed when its `input.repositoryId` resolves (via node-id lookup) to
    the session's `allowed_repo`. The proxy must forward the mutation
    unchanged, return GitHub's response unchanged, emit an `IssueCreated`
    event, and record the new issue's node id so subsequent
    updateIssue/closeIssue mutations on the same node are authorised.

    Request/response bodies are the verbatim strings captured in
    tests/fixtures/create_issue.json.
    """
    resolve_response = {
        "data": {"node": {"name": "taskpull", "owner": {"login": "iwahbe"}}}
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(200, json=resolve_response)
        if "createIssue" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_ISSUE_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_CREATE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_CREATE_ISSUE_RESPONSE

    forwarded_mutations = [r for r in captured if b"createIssue" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_CREATE_ISSUE_REQUEST

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    # Events must carry the HTML URL (https://github.com/...), the same
    # shape emitted on the REST path. GraphQL's createIssue response
    # already returns the HTML URL directly.
    assert event == IssueCreated(
        session_id=session_id,
        issue_url="https://github.com/iwahbe/taskpull/issues/1",
    )


@pytest.mark.asyncio
async def test_graphql_create_issue_in_wrong_repo_is_rejected():
    """Per the docstring contract: `createIssue` is rejected when
    `input.repositoryId` does not resolve to the session's `allowed_repo`.
    The mutation must not be forwarded and no event may be emitted.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        body_text = request.content.decode()
        if "node(id:" in body_text:
            return httpx.Response(
                200,
                json={
                    "data": {"node": {"name": "other", "owner": {"login": "someone"}}}
                },
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_CREATE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert all(b"createIssue" not in r.content for r in captured)
    assert queue.empty()


# ---------------------------------------------------------------------------
# closeIssue — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_close_issue_created_by_session_forwards_and_emits_event():
    """Per the docstring contract: `closeIssue` is allowed when its
    `input.issueId` was created earlier in this session. The proxy must
    forward the mutation, return GitHub's response unchanged, and emit an
    `IssueClosed` event carrying the issue's tracked URL.
    """
    rest_issue = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/issues/1",
        "html_url": "https://github.com/iwahbe/taskpull/issues/1",
        "node_id": "I_kwDORru3U8780gDh",
        "number": 1,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/issues") and request.method == "POST":
            return httpx.Response(201, json=rest_issue)
        if b"closeIssue" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_CLOSE_ISSUE_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        await _bootstrap_rest_issue(
            proxy, proxy_token, rest_issue["url"], rest_issue["node_id"]
        )
        # Drain the IssueCreated emitted by the bootstrap.
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_CLOSE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_CLOSE_ISSUE_RESPONSE

    forwarded_mutations = [r for r in captured if b"closeIssue" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_CLOSE_ISSUE_REQUEST

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == IssueClosed(
        issue_url="https://github.com/iwahbe/taskpull/issues/1",
    )


@pytest.mark.asyncio
async def test_graphql_close_issue_not_created_by_session_is_rejected():
    """Per the docstring contract: `closeIssue` is rejected when its
    `input.issueId` was not tracked as created by this session. No
    upstream request, no event.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_CLOSE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


# ---------------------------------------------------------------------------
# closePullRequest — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_close_pr_created_by_session_forwards_and_emits_event():
    """Per the docstring contract: `closePullRequest` is allowed when its
    `input.pullRequestId` was created earlier in this session. The proxy
    must forward the mutation, return GitHub's response unchanged, and
    emit a `PRClosed` event.

    The PR is bootstrapped via REST POST /pulls. Closing via GraphQL
    must emit the same `PRClosed(pr_url=...)` that closing via REST
    would emit — the event stream is channel-independent, and all event
    URLs use the `https://github.com/...` (HTML) shape.
    """
    created_pr = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/pulls/9",
        "html_url": "https://github.com/iwahbe/taskpull/pull/9",
        "node_id": "PR_kwDORru3U87RmFVG",
        "number": 9,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        if request.url.path.endswith("/pulls") and request.method == "POST":
            return httpx.Response(201, json=created_pr)
        if b"closePullRequest" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_CLOSE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        push_resp = await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/recapture-test", proxy_token)
        )
        assert push_resp.status_code == 200
        pr_body = json.dumps(
            {"title": "x", "head": "recapture-test", "base": "main"}
        ).encode()
        create_resp = await proxy.handle(
            _make_request(
                method="POST",
                url="https://api.github.com/repos/iwahbe/taskpull/pulls",
                headers={
                    "host": "api.github.com",
                    "authorization": f"token {proxy_token}",
                    "content-type": "application/json",
                    "content-length": str(len(pr_body)),
                },
                body=pr_body,
            )
        )
        assert create_resp.status_code == 201
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_CLOSE_PR_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_CLOSE_PR_RESPONSE

    forwarded_mutations = [r for r in captured if b"closePullRequest" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_CLOSE_PR_REQUEST

    event = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert event == PRClosed(
        pr_url="https://github.com/iwahbe/taskpull/pull/9",
    )


@pytest.mark.asyncio
async def test_graphql_close_pr_not_created_by_session_is_rejected():
    """Per the docstring contract: `closePullRequest` is rejected when
    its `input.pullRequestId` was not created by this session.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_CLOSE_PR_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


# ---------------------------------------------------------------------------
# updateIssue — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_update_issue_created_by_session_is_forwarded():
    """Per the docstring contract: `updateIssue` is allowed when
    `input.id` was created earlier in this session. The proxy forwards
    the mutation unchanged and returns GitHub's response unchanged. No
    event is emitted (events fire on close, not generic edits).
    """
    rest_issue = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/issues/5",
        "html_url": "https://github.com/iwahbe/taskpull/issues/5",
        "node_id": "I_kwDORru3U87829yE",
        "number": 5,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/issues") and request.method == "POST":
            return httpx.Response(201, json=rest_issue)
        if b"updateIssue" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_UPDATE_ISSUE_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        await _bootstrap_rest_issue(
            proxy, proxy_token, rest_issue["url"], rest_issue["node_id"]
        )
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_UPDATE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_UPDATE_ISSUE_RESPONSE

    forwarded_mutations = [r for r in captured if b"updateIssue" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_UPDATE_ISSUE_REQUEST

    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_update_issue_not_created_by_session_is_rejected():
    """Per the docstring contract: `updateIssue` is rejected when
    `input.id` was not created by this session.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_UPDATE_ISSUE_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


# ---------------------------------------------------------------------------
# updatePullRequest — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_update_pr_created_by_session_is_forwarded():
    """Per the docstring contract: `updatePullRequest` is allowed when
    `input.pullRequestId` was created earlier in this session. The proxy
    forwards the mutation unchanged. No event is emitted.

    The fixture mutation targets PR_kwDORru3U87RmEUU, so the bootstrap
    REST POST is mocked to return that same node id — seeding the
    session's created-PR set so the update is authorised.
    """
    created_pr = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/pulls/6",
        "html_url": "https://github.com/iwahbe/taskpull/pull/6",
        "node_id": "PR_kwDORru3U87RmEUU",
        "number": 6,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/git-receive-pack"):
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/x-git-receive-pack-result"},
            )
        if request.url.path.endswith("/pulls") and request.method == "POST":
            return httpx.Response(201, json=created_pr)
        if b"updatePullRequest" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_UPDATE_PR_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        # Push + open a PR via REST to seed created_pr_ids with RmEUU.
        await proxy.handle(
            _push_request("iwahbe/taskpull", "refs/heads/recapture-test", proxy_token)
        )
        pr_body = json.dumps(
            {"title": "x", "head": "recapture-test", "base": "main"}
        ).encode()
        await proxy.handle(
            _make_request(
                method="POST",
                url="https://api.github.com/repos/iwahbe/taskpull/pulls",
                headers={
                    "host": "api.github.com",
                    "authorization": f"token {proxy_token}",
                    "content-type": "application/json",
                    "content-length": str(len(pr_body)),
                },
                body=pr_body,
            )
        )
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_UPDATE_PR_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_UPDATE_PR_RESPONSE

    forwarded_mutations = [r for r in captured if b"updatePullRequest" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_UPDATE_PR_REQUEST

    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_update_pr_not_created_by_session_is_rejected():
    """Per the docstring contract: `updatePullRequest` is rejected when
    `input.pullRequestId` was not created by this session.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_UPDATE_PR_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


# ---------------------------------------------------------------------------
# addLabelsToLabelable — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_add_labels_to_session_created_labelable_is_forwarded():
    """Per the docstring contract: `addLabelsToLabelable` is allowed when
    `input.labelableId` names an issue or PR created earlier in this
    session. The proxy forwards the mutation unchanged. No event emitted.
    """
    rest_issue = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/issues/10",
        "html_url": "https://github.com/iwahbe/taskpull/issues/10",
        "node_id": "I_kwDORru3U8783GEO",
        "number": 10,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/issues") and request.method == "POST":
            return httpx.Response(201, json=rest_issue)
        if b"addLabelsToLabelable" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_ADD_LABELS_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        await _bootstrap_rest_issue(
            proxy, proxy_token, rest_issue["url"], rest_issue["node_id"]
        )
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_ADD_LABELS_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_ADD_LABELS_RESPONSE

    forwarded_mutations = [r for r in captured if b"addLabelsToLabelable" in r.content]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_ADD_LABELS_REQUEST

    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_add_labels_to_unknown_labelable_is_rejected():
    """Per the docstring contract: `addLabelsToLabelable` is rejected
    when `input.labelableId` was not created by this session.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_ADD_LABELS_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


# ---------------------------------------------------------------------------
# removeLabelsFromLabelable — positive + negative
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_remove_labels_from_session_created_labelable_is_forwarded():
    """Per the docstring contract: `removeLabelsFromLabelable` is allowed
    when `input.labelableId` names an issue or PR created earlier in this
    session. The proxy forwards the mutation unchanged. No event emitted.
    """
    rest_issue = {
        "url": "https://api.github.com/repos/iwahbe/taskpull/issues/10",
        "html_url": "https://github.com/iwahbe/taskpull/issues/10",
        "node_id": "I_kwDORru3U8783GEO",
        "number": 10,
    }

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/issues") and request.method == "POST":
            return httpx.Response(201, json=rest_issue)
        if b"removeLabelsFromLabelable" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_REMOVE_LABELS_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        await _bootstrap_rest_issue(
            proxy, proxy_token, rest_issue["url"], rest_issue["node_id"]
        )
        await asyncio.wait_for(queue.get(), timeout=0.5)

        response = await proxy.handle(
            _gql_request(_FIXTURE_REMOVE_LABELS_REQUEST, proxy_token)
        )

    assert response.status_code == 200
    assert bytes(response.body).decode() == _FIXTURE_REMOVE_LABELS_RESPONSE

    forwarded_mutations = [
        r for r in captured if b"removeLabelsFromLabelable" in r.content
    ]
    assert len(forwarded_mutations) == 1
    forwarded = forwarded_mutations[0]
    assert forwarded.headers["authorization"] == "token gh-real-token"
    assert forwarded.content == _FIXTURE_REMOVE_LABELS_REQUEST

    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_remove_labels_from_unknown_labelable_is_rejected():
    """Per the docstring contract: `removeLabelsFromLabelable` is rejected
    when `input.labelableId` was not created by this session.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue,
        )
        proxy_token, _certs = await proxy.create_proxy_session(
            SessionID("session-abc"), Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_REMOVE_LABELS_REQUEST, proxy_token)
        )

    assert response.status_code == 403
    assert captured == []
    assert queue.empty()


@pytest.mark.asyncio
async def test_graphql_node_resolution_is_persisted_in_state():
    """Per the docstring contract: 'When a mutation references a
    repositoryId not in the cache, the proxy resolves it by querying
    GitHub's node API before making the allow/deny decision. Resolved
    mappings are persisted in the session's repo_node_cache.'

    After a `createIssue` mutation triggers a node-id lookup, the
    session's `repo_nodes` cache must include the repositoryId →
    "{owner}/{name}" mapping, and that mapping must survive a save/load
    round-trip — otherwise a restarted proxy would re-query GitHub on
    every subsequent mutation that references the same repository.

    This test verifies persistence by loading the state directly from
    the shared state manager after the mutation; it makes no assumption
    about *how* the cache is kept in memory (only that the persisted
    snapshot carries it).
    """
    # Defer import so the test is self-documenting about what it inspects.
    from taskpull.gh_proxy_v2 import _ProxyState

    resolve_calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body_text = request.content.decode()
        if "node(id:" in body_text:
            resolve_calls.append(request)
            return httpx.Response(
                200,
                json={
                    "data": {"node": {"name": "taskpull", "owner": {"login": "iwahbe"}}}
                },
            )
        if "createIssue" in body_text:
            return httpx.Response(
                200,
                content=_FIXTURE_CREATE_ISSUE_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    queue: asyncio.Queue[EngineEvent] = asyncio.Queue()
    shared_state: InMemoryStateManager[_ProxyState] = InMemoryStateManager()

    def shared_state_factory(model):
        return shared_state

    async with httpx.AsyncClient(transport=transport) as client:
        proxy = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=shared_state_factory,
            queue=queue,
        )
        session_id = SessionID("session-abc")
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )

        response = await proxy.handle(
            _gql_request(_FIXTURE_CREATE_ISSUE_REQUEST, proxy_token)
        )
        assert response.status_code == 200, response.body

    # Sanity: node resolution actually fired on this first call.
    assert len(resolve_calls) == 1

    # The persisted state must now contain the resolved mapping.
    persisted = await shared_state.load()
    assert persisted is not None
    assert session_id in persisted.sessions
    assert persisted.sessions[session_id].repo_nodes == {
        "R_kgDORru3Uw": "iwahbe/taskpull",
    }


@pytest.mark.asyncio
async def test_close_event_is_channel_independent():
    """Per the user-stated invariant: closing an issue must emit the same
    `IssueClosed` event regardless of whether the close was issued via
    REST PATCH or GraphQL `closeIssue`. The event is an observable of the
    issue itself, not of the channel used to close it.

    Scenario: two independent sessions each create issue #1 in
    iwahbe/taskpull via REST. Session A closes via REST PATCH
    state=closed. Session B closes via GraphQL closeIssue. The resulting
    `IssueClosed` events must compare equal — both carrying the HTML
    URL (https://github.com/...), the canonical event URL shape.
    """
    issue_url = "https://api.github.com/repos/iwahbe/taskpull/issues/1"
    issue_node_id = "I_kwDORru3U8780gDh"
    rest_issue = {
        "url": issue_url,
        "html_url": "https://github.com/iwahbe/taskpull/issues/1",
        "node_id": issue_node_id,
        "number": 1,
    }
    rest_issue_closed = {**rest_issue, "state": "closed"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/issues") and request.method == "POST":
            return httpx.Response(201, json=rest_issue)
        if request.method == "PATCH":
            return httpx.Response(200, json=rest_issue_closed)
        if b"closeIssue" in request.content:
            return httpx.Response(
                200,
                content=_FIXTURE_CLOSE_ISSUE_RESPONSE.encode(),
                headers={"content-type": "application/json; charset=utf-8"},
            )
        return httpx.Response(500)

    async def _setup_session(
        proxy: LiveGitHubProxy, session_id: SessionID, queue: asyncio.Queue
    ) -> str:
        proxy_token, _certs = await proxy.create_proxy_session(
            session_id, Permissions(allowed_repo="iwahbe/taskpull")
        )
        await _bootstrap_rest_issue(proxy, proxy_token, issue_url, issue_node_id)
        await asyncio.wait_for(queue.get(), timeout=0.5)  # drain IssueCreated
        return proxy_token

    transport = httpx.MockTransport(handler)
    queue_rest: asyncio.Queue[EngineEvent] = asyncio.Queue()
    queue_gql: asyncio.Queue[EngineEvent] = asyncio.Queue()

    async with httpx.AsyncClient(transport=transport) as client:
        proxy_rest = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue_rest,
        )
        proxy_gql = LiveGitHubProxy(
            gh_token="gh-real-token",
            http_client=client,
            tls=_FakeTls(),
            state_factory=_in_memory_state_factory,
            queue=queue_gql,
        )

        token_rest = await _setup_session(
            proxy_rest, SessionID("session-rest"), queue_rest
        )
        token_gql = await _setup_session(proxy_gql, SessionID("session-gql"), queue_gql)

        # Session A: REST close.
        patch_body = json.dumps({"state": "closed"}).encode()
        await proxy_rest.handle(
            _make_request(
                method="PATCH",
                url="https://api.github.com/repos/iwahbe/taskpull/issues/1",
                headers={
                    "host": "api.github.com",
                    "authorization": f"token {token_rest}",
                    "content-type": "application/json",
                    "content-length": str(len(patch_body)),
                },
                body=patch_body,
            )
        )

        # Session B: GraphQL close.
        await proxy_gql.handle(_gql_request(_FIXTURE_CLOSE_ISSUE_REQUEST, token_gql))

    rest_event = await asyncio.wait_for(queue_rest.get(), timeout=0.5)
    gql_event = await asyncio.wait_for(queue_gql.get(), timeout=0.5)

    assert rest_event == gql_event
    assert isinstance(rest_event, IssueClosed)
