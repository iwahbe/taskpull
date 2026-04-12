from __future__ import annotations
import base64
import json

import asyncio
import secrets
import logging
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections.abc import Coroutine, Callable
from typing import Protocol

import httpx
from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from starlette.requests import Request
from starlette.responses import Response

from taskpull.engine_events import (
    EngineEvent,
    SessionID,
    PRCreated,
    IssueCreated,
    IssueClosed,
)
from taskpull.state_manager import StateFactory, StateManager

log = logging.getLogger(__name__)


class ProxyToken(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls, serialization=core_schema.to_string_ser_schema()
        )


class TlsProvider(Protocol):
    def server_ssl_context(self) -> ssl.SSLContext: ...

    @property
    def ca_cert_bytes(self) -> bytes: ...


@dataclass(frozen=True)
class Permissions:
    allowed_repo: str  # "owner/repo" format


@dataclass(frozen=True)
class Certs:
    ca_cert: bytes


class _SessionEntry(BaseModel):
    token: ProxyToken
    permissions: Permissions
    repo_node_cache: dict[str, str] = {}
    branch: str | None = None

    created_issue_ids: dict[str, int] = {}  # Map from PR URL or Node ID to PR number
    created_pr_ids: dict[str, int] = {}  # Map from Issue URL or Node ID to Issue number

    def _add_entry(self, url: str, node_id: str, m: dict[str, int], kind: str):
        match url.removeprefix("https://").split("/"):
            case [_host, "repos", _org, _repo, key, number] if key == kind:
                m[url] = number
                m[node_id] = number
        raise ValueError(f"invalid url {url} for {kind}")

    def add_pr(self, url: str, node_id: str):
        self._add_entry(url, node_id, self.created_pr_ids, "pulls")

    def add_issue(self, url: str, node_id: str):
        self._add_entry(url, node_id, self.created_issue_ids, "issues")


class _ProxyState(BaseModel):
    sessions: dict[SessionID, _SessionEntry] = {}


class GitHubProxy(ABC):
    @abstractmethod
    async def create_proxy_session(
        self, session_id: SessionID, permissions: Permissions
    ) -> tuple[ProxyToken, Certs]:
        """Create a proxy session for the given session, returning its token and certs."""
        ...

    @abstractmethod
    async def forget(self, session_id: SessionID) -> None:
        """Remove all state associated with a session."""
        ...

    @abstractmethod
    async def handle(self, request: Request) -> Response:
        """Handle an incoming HTTP request from a session."""
        ...


class LiveGitHubProxy(GitHubProxy):
    """Production GitHub proxy that enforces per-session permissions.

    All requests are authenticated by a proxy token in the Authorization header.
    The proxy maps each token to a session, then decides whether to forward the
    request to GitHub (injecting the real token) or reject it.

    ## Read requests

    All read operations are forwarded unconditionally:
    - GraphQL queries (any query, regardless of target repo)
    - REST GET/HEAD requests
    - Git upload-pack (clone/fetch)

    GraphQL query responses are inspected to cache repository node IDs
    (mapping node ID -> "owner/repo") for later mutation verification.

    ## Write requests

    Write operations are gated per-session. Each session is assigned a single
    allowed_repo ("owner/repo"). The proxy enforces:

    ### Git push (receive-pack)

    - Only allowed to the session's assigned repo.
    - The first push establishes the session's branch. The target ref must not
      already exist on the remote (verified via the refs advertisement).
    - Subsequent pushes must target the same branch.

    ### GraphQL mutations

    Allowed mutations and their constraints:

    - createPullRequest: repositoryId must resolve to the allowed repo. The
      PR's headRefName must match the session's established branch.
    - createIssue: repositoryId must resolve to the allowed repo.
    - updateIssue: the issue must have been created by this session (tracked
      by node ID from the creation response).
    - updatePullRequest: the PR must have been created by this session.
    - closeIssue: the issue must have been created by this session.
    - closePullRequest: the PR must have been created by this session.
    - addLabelsToLabelable: the labelable must have been created by this session.
    - removeLabelsFromLabelable: the labelable must have been created by this session.

    All other mutations are rejected.

    ### REST writes (POST/PUT/PATCH/DELETE)

    - POST /repos/{owner}/{repo}/issues: allowed if repo matches.
    - POST /repos/{owner}/{repo}/pulls: allowed if repo matches. The request
      body's "head" field must match the session's branch.
    - PATCH /repos/{owner}/{repo}/issues/{n}: allowed if the issue was created
      by this session (tracked by issue number from REST creation responses).
    - PATCH /repos/{owner}/{repo}/pulls/{n}: allowed if the PR was created by
      this session.
    - POST /repos/{owner}/{repo}/issues/{n}/labels: allowed if the issue was
      created by this session.
    - DELETE /repos/{owner}/{repo}/issues/{n}/labels/{name}: allowed if the
      issue was created by this session.
    - All other REST write paths are rejected.

    ## Event emission

    Successful responses (both GraphQL and REST) emit events:
    - PRCreated when a pull request is created.
    - IssueCreated when an issue is created.
    - PRClosed when a pull request is closed.
    - IssueClosed when an issue is closed.

    ## Unknown node IDs

    When a mutation references a repositoryId not in the cache, the proxy
    resolves it by querying GitHub's node API before making the allow/deny
    decision. Resolved mappings are persisted in the session's repo_node_cache.
    """

    def __init__(
        self,
        gh_token: str,
        http_client: httpx.AsyncClient,
        tls: TlsProvider,
        state_factory: StateFactory,
        queue: asyncio.Queue[EngineEvent],
    ) -> None:
        self._gh_token = gh_token
        self._http_client = http_client
        self._tls = tls
        self._state: StateManager[_ProxyState] = state_factory(_ProxyState)
        self._queue = queue
        self._sessions: dict[SessionID, _SessionEntry] = {}
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        """Load persisted state. Must be called before first use."""
        if self._loaded:
            return
        self._loaded = True
        saved = await self._state.load()
        if saved is not None:
            self._sessions = saved.sessions

    async def _save(self) -> None:
        await self._state.save(_ProxyState(sessions=self._sessions))

    async def create_proxy_session(
        self, session_id: SessionID, permissions: Permissions
    ) -> tuple[ProxyToken, Certs]:
        await self._ensure_loaded()
        if session_id in self._sessions:
            raise ValueError(f"session {session_id} already registered")
        token = ProxyToken(secrets.token_urlsafe(32))
        self._sessions[session_id] = _SessionEntry(
            token=token,
            permissions=permissions,
        )
        await self._save()
        return token, Certs(ca_cert=self._tls.ca_cert_bytes)

    async def forget(self, session_id: SessionID) -> None:
        await self._ensure_loaded()
        self._sessions.pop(session_id, None)
        await self._save()

    async def handle(self, request: Request) -> Response:
        await self._ensure_loaded()

        auth = self._get_auth(request)
        if not auth:
            log.info(f"passing unauthenticated request through to {request.url}")
            return await self._forward(request)  # Forward with existing auth

        session_id, auth_header = auth
        overwrite_headers = {"authorization": auth_header}
        session = self._sessions[session_id]

        # Read only methods are safe
        if request.method in ("GET", "HEAD"):
            return await self._forward(request, overwrite_headers)

        if request.method == "POST":
            match request.url.path.split("/"):
                case ["graphql"]:
                    return await self._handle_graphql(
                        request, session_id, overwrite_headers
                    )
                case ["repos", org, repo, "pulls"]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(
                            f"Can only open Pull Requests in {org}/{repo}"
                        )
                    body = await request.json()
                    if (
                        session.branch is None
                        or body["head"] != self._sessions[session_id].branch
                    ):
                        return self._reject(
                            "Can only open Pull Requests from branches you have created"
                        )
                    return await self._forward(
                        request,
                        overwrite_headers,
                        on_success=lambda req, resp: self._capture_rest_pr_created(
                            session_id, session, req, resp
                        ),
                    )
                case ["repos", org, repo, "issues"]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only open Issues in {org}/{repo}")
                    return await self._forward(
                        request,
                        overwrite_headers,
                        on_success=lambda req, resp: self._capture_rest_issue_created(
                            session_id, session, req, resp
                        ),
                    )
                case ["repos", org, repo, "issues", number, "labels"]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only edit Issues in {org}/{repo}")
                    if (
                        int(number) not in session.created_issue_ids
                        and int(number) not in session.created_pr_ids
                    ):
                        return self._reject("Can only edit Issues you created")
                    return await self._forward(request, overwrite_headers)

        if request.method == "PATCH":
            match request.url.path.split("/"):
                case ["repos", org, repo, "issues", number]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only edit Issues in {org}/{repo}")
                    if int(number) not in session.created_issue_ids:
                        return self._reject("Can only edit Issues you created")
                    return await self._forward(
                        request,
                        overwrite_headers,
                        on_success=self._capture_rest_issue_edited,
                    )
                case ["repos", org, repo, "pulls", number]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(
                            f"Can only edit Pull Requests in {org}/{repo}"
                        )
                    if int(number) not in session.created_pr_ids:
                        return self._reject("Can only edit Pull Requests you created")
                    return await self._forward(
                        request,
                        overwrite_headers,
                        on_success=self._capture_rest_pr_edited,
                    )

        if request.method == "DELETE":
            match request.url.path.split("/"):
                case ["repos", org, repo, "issues", number, "labels", _name]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only edit Issues in {org}/{repo}")
                    if (
                        int(number) not in session.created_issue_ids
                        and int(number) not in session.created_pr_ids
                    ):
                        return self._reject("Can only edit Issues you created")
                    return await self._forward(request, overwrite_headers)

        return self._reject(
            f"{request.method} {request.url} rejected by taskpull proxy"
        )

    async def _forward(
        self,
        request: Request,
        overwrite: dict[str, str] = {},
        on_success: Callable[[Request, Response], Coroutine] | None = None,
    ) -> Response:
        headers = dict(request.headers)
        headers.pop("host", None)
        headers.update(overwrite)

        upstream = await self._http_client.request(
            method=request.method,
            url=str(request.url),
            headers=headers,
            content=await request.body(),
        )

        response = Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=dict(upstream.headers),
        )

        if on_success is not None and 200 <= upstream.status_code < 300:
            await on_success(request, response)

        return response

    async def _capture_rest_pr_created(
        self,
        session_id: SessionID,
        session: _SessionEntry,
        _request: Request,
        response: Response,
    ):
        body = json.loads(bytes(response.body))
        session.add_pr(body["url"], body["node_id"])
        await self._queue.put(PRCreated(session_id, body["url"]))

    async def _capture_rest_issue_created(
        self,
        session_id: SessionID,
        session: _SessionEntry,
        _request: Request,
        response: Response,
    ):
        body = json.loads(bytes(response.body))
        session.add_issue(body["url"], body["node_id"])
        await self._queue.put(IssueCreated(session_id, body["url"]))

    async def _capture_rest_issue_edited(self, request: Request, response: Response):
        if (await request.json())["state"] == "closed":
            await self._queue.put(IssueClosed(json.loads(bytes(response.body))["url"]))

    async def _capture_rest_pr_edited(self, request: Request, response: Response):
        if (await request.json())["state"] == "closed":
            await self._queue.put(IssueClosed(json.loads(bytes(response.body))["url"]))

    async def _handle_graphql(
        self, request: Request, session_id: SessionID, overwrite: dict[str, str]
    ) -> Response:
        raise NotImplementedError

    def _reject(self, reason: str) -> Response:
        return Response(
            content=json.dumps({"message": reason}),
            status_code=403,
            media_type="application/json",
        )

    def _get_auth(self, request: Request) -> tuple[SessionID, str] | None:
        auth = request.headers.get("authorization")
        if not auth:
            return  # Forward without auth

        if (base64_token := auth.removeprefix("Basic ")) != auth:
            [username, token] = (
                base64.b64decode(base64_token, validate=True)
                .decode()
                .split(":", maxsplit=1)
            )
            try:
                [session_id, _] = next(
                    filter(
                        lambda entry: entry[1].proxy_token == token,
                        self._sessions.items(),
                    )
                )
                return session_id, "Basic " + str(
                    base64.b64encode(f"{username}:{self._gh_token}".encode())
                )
            except StopIteration:
                # No matching session, so pass through
                return

        for prefix in ["Bearer ", "token "]:
            if (token := auth.removeprefix(prefix)) != auth:
                try:
                    [session_id, _] = next(
                        filter(
                            lambda entry: entry[1].proxy_token == token,
                            self._sessions.items(),
                        )
                    )
                    return session_id, prefix + self._gh_token
                except StopIteration:
                    # No matching session, so pass through
                    return

        return
