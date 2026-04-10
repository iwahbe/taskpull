import enum
from dataclasses import dataclass
from typing import Union


from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class SessionID(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls, serialization=core_schema.to_string_ser_schema()
        )


class TaskName(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls, serialization=core_schema.to_string_ser_schema()
        )


class TaskSource(enum.Enum):
    FILE = "file"
    ADHOC = "adhoc"


class TaskGoal(enum.Enum):
    PR = "pr"
    NONE = "none"
    ISSUE = "issue"


class CiInfo(enum.Enum):
    UNKNOWN = "unknown"
    NONE = "none"
    PASS = "pass"
    FAIL = "fail"
    PENDING = "pending"


@dataclass(frozen=True)
class NewTask:
    """Register a new task to be completed, possibly repeating, may override an existing task."""

    name: TaskName
    prompt: str
    goal: TaskGoal
    location: str
    key: str | None
    repeat: bool
    source: TaskSource


@dataclass(frozen=True)
class RemoveTask:
    """De-register a task."""

    name: TaskName


@dataclass(frozen=True)
class SessionPaused:
    """A session has been paused."""

    session_id: SessionID


@dataclass(frozen=True)
class SessionUnpaused:
    """A session has been unpaused."""

    session_id: SessionID


@dataclass(frozen=True)
class SessionWorking:
    """A session is working."""

    session_id: SessionID


@dataclass(frozen=True)
class SessionIdle:
    """A session is idle."""

    session_id: SessionID


@dataclass(frozen=True)
class SessionTerminated:
    """A session has exited."""

    session_id: SessionID


@dataclass(frozen=True)
class PRCreated:
    """A PR has been created, associated with a session."""

    session_id: SessionID
    pr_url: str


@dataclass(frozen=True)
class IssueCreated:
    """An issue has been created, associated with a session."""

    session_id: SessionID
    issue_url: str


@dataclass(frozen=True)
class PRClosed:
    """A PR has been closed."""

    pr_url: str


@dataclass(frozen=True)
class IssueClosed:
    """An issue has been closed."""

    issue_url: str


@dataclass(frozen=True)
class CIStatus:
    """A report on the CI Status of a PR."""

    pr_url: str
    info: CiInfo


@dataclass(frozen=True)
class RestartSession:
    """An instruction to the engine to restart a session."""

    session_id: SessionID


@dataclass(frozen=True)
class PauseSession:
    """An instruction to the engine to pause a session."""

    session_id: SessionID


@dataclass(frozen=True)
class ResumeSession:
    """An instruction to the engine to resume a paused session."""

    session_id: SessionID


@dataclass(frozen=True)
class ExhaustTask:
    """A task has marked itself as exhausted."""

    session_id: SessionID


@dataclass(frozen=True)
class WakeTask:
    """A task is ready to be woken up."""

    name: TaskName


EngineEvent = Union[
    NewTask,
    RemoveTask,
    SessionPaused,
    SessionUnpaused,
    SessionWorking,
    SessionIdle,
    SessionTerminated,
    PRCreated,
    IssueCreated,
    PRClosed,
    IssueClosed,
    CIStatus,
    RestartSession,
    PauseSession,
    ResumeSession,
    ExhaustTask,
    WakeTask,
]
