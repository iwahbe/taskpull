from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, Protocol, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class StateManager(ABC, Generic[T]):
    @abstractmethod
    async def save(self, state: T) -> None: ...

    @abstractmethod
    async def load(self) -> T | None: ...


class StateFactory(Protocol):
    def __call__(self, model: type[T]) -> StateManager[T]: ...


class InMemoryStateManager(StateManager[T]):
    def __init__(self) -> None:
        self._state: T | None = None

    async def save(self, state: T) -> None:
        self._state = state.model_copy(deep=True)

    async def load(self) -> T | None:
        return self._state.model_copy(deep=True) if self._state is not None else None


class FileStateManager(StateManager[T]):
    def __init__(self, path: Path, model: type[T]) -> None:
        self._path = path
        self._model = model

    async def save(self, state: T) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(state.model_dump_json(indent=2) + "\n")

    async def load(self) -> T | None:
        if not self._path.exists():
            return None
        with open(self._path) as f:
            return self._model.model_validate_json(f.read())
