from abc import ABC, abstractmethod

from taskpull.engine_events import SessionID


class SessionManager(ABC):
    @abstractmethod
    async def create(self, prompt: str, location: str) -> SessionID: ...

    @abstractmethod
    async def pause(self, session: SessionID) -> None: ...

    @abstractmethod
    async def resume(self, session: SessionID) -> None: ...

    @abstractmethod
    async def terminate(self, session: SessionID) -> None: ...
