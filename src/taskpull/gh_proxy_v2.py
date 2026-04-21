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
from typing import Protocol, Any, Literal

import httpx
from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from starlette.requests import Request
from starlette.responses import Response
from graphql import parse as gql_parse
from graphql.language.ast import FieldNode, OperationDefinitionNode, OperationType

from taskpull.engine_events import (
    EngineEvent,
    SessionID,
    PRCreated,
    PRClosed,
    IssueCreated,
    IssueClosed,
)
from taskpull.state_manager import StateFactory, StateManager

log = logging.getLogger(__name__)

_OnSuccess = Callable[[Request, Response], Coroutine]


def _parse_receive_pack_refs(body: bytes) -> list[str] | None:
    """Extract target ref names from a git-receive-pack pkt-line body.

    Each update command is a length-prefixed pkt-line
    `<4-hex-len><old-sha> <new-sha> <ref>\\x00<caps>\\n`; capabilities and
    the trailing LF appear on the first command only. A flush-pkt (`0000`)
    terminates the command list. Returns None on malformed input.
    """
    refs: list[str] = []
    i = 0
    while i + 4 <= len(body):
        try:
            length = int(body[i : i + 4], 16)
        except ValueError:
            return None
        if length == 0:
            return refs
        if length < 4 or i + length > len(body):
            return None
        payload = body[i + 4 : i + length].rstrip(b"\n")
        nul = payload.find(b"\x00")
        if nul >= 0:
            payload = payload[:nul]
        parts = payload.split(b" ", 2)
        if len(parts) != 3:
            return None
        refs.append(parts[2].decode())
        i += length
    return None


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
    branch: str | None = None
    created_issue_ids: dict[str, int] = {}  # Map from PR URL or Node ID to PR number
    created_pr_ids: dict[str, int] = {}  # Map from Issue URL or Node ID to Issue number
    repo_nodes: dict[str, str] = {}  # Map from Repo Node ID to "{org}/{name}"

    def _add_entry(self, url: str, node_id: str, m: dict[str, int], kind: str):
        html_kind = "pull" if kind == "pulls" else kind
        number: str | None = None
        match url.removeprefix("https://").split("/"):
            case [_host, "repos", _org, _repo, key, n] if key == kind:
                number = n
            case [_host, _org, _repo, key, n] if key == html_kind:
                number = n
        if number is None:
            raise ValueError(f"invalid url {url} for {kind}")
        m[url] = int(number)
        m[node_id] = int(number)

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
            match request.url.path.lstrip("/").split("/"):
                case ["graphql"]:
                    return await self._handle_graphql(
                        request, session_id, session, overwrite_headers
                    )
                case [_owner, _repo_name, "git-upload-pack"]:
                    return await self._forward(request, overwrite_headers)
                case [owner, repo_name, "git-receive-pack"]:
                    return await self._handle_git_push(
                        request, session, owner, repo_name, overwrite_headers
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
                    if int(number) not in set(session.created_issue_ids.values()) | set(
                        session.created_pr_ids.values()
                    ):
                        return self._reject("Can only edit Issues you created")
                    return await self._forward(request, overwrite_headers)

        if request.method == "PATCH":
            match request.url.path.lstrip("/").split("/"):
                case ["repos", org, repo, "issues", number]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only edit Issues in {org}/{repo}")
                    if int(number) not in session.created_issue_ids.values():
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
                    if int(number) not in session.created_pr_ids.values():
                        return self._reject("Can only edit Pull Requests you created")
                    return await self._forward(
                        request,
                        overwrite_headers,
                        on_success=self._capture_rest_pr_edited,
                    )

        if request.method == "DELETE":
            match request.url.path.lstrip("/").split("/"):
                case ["repos", org, repo, "issues", number, "labels", _name]:
                    if session.permissions.allowed_repo != f"{org}/{repo}":
                        return self._reject(f"Can only edit Issues in {org}/{repo}")
                    if int(number) not in set(session.created_issue_ids.values()) | set(
                        session.created_pr_ids.values()
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
        on_success: _OnSuccess | None = None,
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
        await self._save()
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
        await self._save()
        await self._queue.put(IssueCreated(session_id, body["url"]))

    async def _capture_rest_issue_edited(self, request: Request, response: Response):
        if (await request.json())["state"] == "closed":
            await self._queue.put(IssueClosed(json.loads(bytes(response.body))["url"]))

    async def _capture_rest_pr_edited(self, request: Request, response: Response):
        if (await request.json())["state"] == "closed":
            await self._queue.put(PRClosed(json.loads(bytes(response.body))["url"]))

    async def _handle_git_push(
        self,
        request: Request,
        session: _SessionEntry,
        owner: str,
        repo_name: str,
        overwrite: dict[str, str],
    ) -> Response:
        repo = repo_name.removesuffix(".git")
        if session.permissions.allowed_repo != f"{owner}/{repo}":
            return self._reject(f"Can only push to {session.permissions.allowed_repo}")

        refs = _parse_receive_pack_refs(await request.body())
        if refs is None:
            return self._reject("Cannot parse git-receive-pack request")
        unique = set(refs)
        if len(unique) != 1:
            return self._reject("git-receive-pack must target a single ref")
        [ref] = unique
        if not ref.startswith("refs/heads/"):
            return self._reject(f"Can only push to a branch, not '{ref}'")
        branch = ref.removeprefix("refs/heads/")

        if session.branch is not None:
            if branch != session.branch:
                return self._reject(
                    f"Can only push to branch '{session.branch}', not '{branch}'"
                )
            return await self._forward(request, overwrite)

        async def _record_branch(_req: Request, _resp: Response):
            session.branch = branch
            await self._save()

        return await self._forward(request, overwrite, on_success=_record_branch)

    async def _handle_graphql(
        self,
        request: Request,
        session_id: SessionID,
        session: _SessionEntry,
        overwrite: dict[str, str],
    ) -> Response:
        body = await request.json()
        on_success_hooks = []
        for defn in gql_parse(body["query"]).definitions:
            if not isinstance(defn, OperationDefinitionNode):
                return self._reject(f"Invalid GraphQL query: {type(defn)}")
            match defn.operation:
                case OperationType.QUERY | OperationType.SUBSCRIPTION:
                    continue
                case OperationType.MUTATION:
                    strategy = await self._handle_graphql_mutation_operation(
                        session_id, session, defn, body.get("variables", {})
                    )
                    if strategy is False:
                        return self._reject(f"Forbidden GraphQL query: {type(defn)}")

                    if strategy is not None:
                        on_success_hooks.append(strategy)
                    continue

            raise NotImplementedError(f"{defn.operation} not handled")

        async def on_success(req, resp):
            await asyncio.gather(*[f(req, resp) for f in on_success_hooks])

        return await self._forward(request, overwrite, on_success)

    async def _handle_graphql_mutation_operation(
        self,
        session_id: SessionID,
        session: _SessionEntry,
        defn: OperationDefinitionNode,
        variables: dict[str, Any],
    ) -> _OnSuccess | None | Literal[False]:
        for sel in defn.selection_set.selections:
            if not isinstance(sel, FieldNode):
                return False
            match sel.name.value:
                case "createPullRequest":
                    input = variables.get("input", {})
                    repo_id = input.get("repositoryId")
                    if not isinstance(repo_id, str):
                        return False
                    resolved = await self._resolve_repo_node(repo_id)
                    if resolved is None or resolved != session.permissions.allowed_repo:
                        return False
                    if (
                        session.branch is None
                        or input.get("headRefName") != session.branch
                    ):
                        return False

                    async def on_pr_created(_req: Request, resp: Response):
                        data = json.loads(bytes(resp.body))
                        pr = data["data"]["createPullRequest"]["pullRequest"]
                        session.add_pr(pr["url"], pr["id"])
                        await self._save()
                        await self._queue.put(PRCreated(session_id, pr["url"]))

                    return on_pr_created
                case _:
                    return False
        return None

    async def _resolve_repo_node(self, node_id: str) -> str | None:
        """Resolve a GitHub node ID to "owner/repo" via the API."""
        resp = await self._http_client.request(
            method="POST",
            url="https://api.github.com/graphql",
            headers={
                "authorization": f"token {self._gh_token}",
                "content-type": "application/json",
            },
            content=json.dumps(
                {
                    "query": "query($id:ID!){node(id:$id){...on Repository{name owner{login}}}}",
                    "variables": {"id": node_id},
                }
            ).encode(),
        )
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
            node = data["data"]["node"]
            return f"{node['owner']['login']}/{node['name']}"
        except KeyError, TypeError:
            return None

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
                        lambda entry: entry[1].token == token,
                        self._sessions.items(),
                    )
                )
                return session_id, "Basic " + base64.b64encode(
                    f"{username}:{self._gh_token}".encode()
                ).decode()
            except StopIteration:
                # No matching session, so pass through
                return

        for prefix in ["Bearer ", "token "]:
            if (token := auth.removeprefix(prefix)) != auth:
                try:
                    [session_id, _] = next(
                        filter(
                            lambda entry: entry[1].token == token,
                            self._sessions.items(),
                        )
                    )
                    return session_id, prefix + self._gh_token
                except StopIteration:
                    # No matching session, so pass through
                    return

        return
