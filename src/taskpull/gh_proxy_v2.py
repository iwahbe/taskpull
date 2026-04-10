from __future__ import annotations

import asyncio
import secrets
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from starlette.requests import Request
from starlette.responses import Response

from taskpull.engine_events import EngineEvent, SessionID
from taskpull.state_manager import StateFactory, StateManager


class ProxyToken(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls, serialization=core_schema.to_string_ser_schema()
        )


class HttpClient(Protocol):
    """Async HTTP client for making upstream requests."""

    async def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        content: bytes,
        ssl_context: ssl.SSLContext | None,
    ) -> Response: ...


class TlsProvider(Protocol):
    def server_ssl_context(self) -> ssl.SSLContext: ...

    @property
    def ca_cert_bytes(self) -> bytes: ...


@dataclass(frozen=True)
class Permissions:
    allowed_repo: str


@dataclass(frozen=True)
class Certs:
    ca_cert: bytes


class _SessionEntry(BaseModel):
    token: ProxyToken
    permissions: Permissions
    repo_node_cache: dict[str, str] = {}


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
    def __init__(
        self,
        gh_token: str,
        http_client: HttpClient,
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
        raise NotImplementedError
