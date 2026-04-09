from pathlib import Path

import pytest
from pydantic import BaseModel

from taskpull.state_manager import FileStateManager


class SampleState(BaseModel):
    count: int = 0
    name: str = ""
    tags: list[str] = []


@pytest.mark.asyncio
async def test_load_returns_none_when_missing(tmp_path: Path) -> None:
    fs = FileStateManager(tmp_path / "state.json", SampleState)
    assert await fs.load() is None


@pytest.mark.asyncio
async def test_save_then_load(tmp_path: Path) -> None:
    fs = FileStateManager(tmp_path / "state.json", SampleState)
    await fs.save(SampleState(count=7, name="hello", tags=["a", "b"]))
    loaded = await fs.load()
    assert loaded == SampleState(count=7, name="hello", tags=["a", "b"])


@pytest.mark.asyncio
async def test_save_overwrites(tmp_path: Path) -> None:
    fs = FileStateManager(tmp_path / "state.json", SampleState)
    await fs.save(SampleState(count=1))
    await fs.save(SampleState(count=2, name="updated"))
    loaded = await fs.load()
    assert loaded == SampleState(count=2, name="updated")


@pytest.mark.asyncio
async def test_creates_parent_dirs(tmp_path: Path) -> None:
    fs = FileStateManager(tmp_path / "nested" / "dir" / "state.json", SampleState)
    await fs.save(SampleState(count=42))
    loaded = await fs.load()
    assert loaded == SampleState(count=42)
