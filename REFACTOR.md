I'd like to re-design the daemon to have a pure, testable engine with separate (and
testable) isolated components. By making it event driven instead of waiting on an external
polling cycle, it should also make it more responsive. You are going to help me write a
new refactor doc. The engine will not be timer based so it can be well tested. It will
receive the following events:

    Event =
          | NewTask(name, prompt, goal, location, key, repeat, source) # Register a new task to be completed, possibly repeating, may override an existing task. Source may be file-based or ad-hock
          | RemoveTask(name)                                           # De-register a task
          | SessionPaused(session_id)                                  # A session has been paused
          | SessionUnpaused(session_id)                                # A session has been unpaused
          | SessionWorking(session_id)                                 # A session is working
          | SessionIdle(session_id)                                    # A session is idle
          | SessionTerminated(session_id)                              # A session has exited
          | PRCreated(session_id, pr_url)                              # A PR has been created, associated with a session
          | IssueCreated(session_id, issue_url)                        # An issue has been created, associated with a session
          | PRClosed(pr_url)                                           # A PR has been closed
          | IssueClosed(issue_url)                                     # An issue has been closed
          | CIStatus(pr_url, info)                                     # A report on the CI Status of a PR
          | RestartSession(session_id)                                 # An instruction to the engine to restart a session
          | PauseSession(session_id)                                   # An instruction to the engine to pause a session
          | ResumeSession(session_id)                                  # An instruction to the engine to resume a paused session
          | ExhaustTask(session_id)                                    # A task has marked itself as exhausted
          | WakeTask(name)                                             # A task is ready to be waked up

# Main Loop

The main loop of the daemon will look like this:

```
import SessionManager from tmuxdocker
import Waker from realwaker
import Engine from engine

state_dir = config / ".state"
engine_state = StateManager(state_dir / "engine.json")
waker_state = StateManager(state_dir / "waker.json")
session_state = StateManager(state_dir / "session.json")
queue = asyncio.Queue()
markdown_task_manager = MdTaskManager(queue, config / "tasks")
with IpcHandler(queue) as ipcs:
    with SessionManager(queue, session_state) as sessions:
	    waker = Waker(queue, waker_state)
        engine = Engine(sessions, engine_state, waker)
        clear_extra_tasks(engine, markdown_task_manager) # engine.handle(RemoveTask) for all tasks Md tasks in the engine status not in the dir
        engine.enable()
        while True:
            event = await queue.get()
            match:
                case Shutdown:
                    engine.disable()
                    return
                case ListStatus:
                    ipcs.send_status(engine.status())
                case ...:
                default:
                    engine.handle(event) # At this point, we have filtered out all the non-engine events so this type-checks
```

# Engine

The engine is responsible for making most decisions in the program. It tracks how goals
are implemented, enriches the prompt, and handles setup back-off & broken tasks[^1], closing
and opening new tasks, etc.

[^1]: When a task is terminated when setting up, it is setup backoff and counts towards
    the broken startup state.

The engine will have access to a session manager and a state manager (injected in on creation):

```python
class SessionID(str):
	pass

class SessionManager(ABC):

    # Create a new session and return it's ID
    async def create(prompt: str, location: str) -> SessionID
    # Send the command to pause an existing session, confirmation will come in the event stream
    async def pause(session: SessionID)
    # Send the command to resume a paused session, confirmation will come in the event stream
    async def resume(session: SessionID)
    # Send the command to terminate a session, confirmation will come in the event stream
    async def terminate(session: SessionID)

class StateManager(ABC):

	async def save(state: object)
    async def load() -> object
```

The engine will implement the following public interface:

```python
class Engine:
	def __init__(self, sessions_manager: SessionManager, state_manager: StateManager, waker: Waker):
    	...

    async def handle(self, event: Event):
    	"Handle an event"
    	...

    def status(self) -> Status
    	"A stable view onto the engine state"
        ...

    async def enable(self):
    	"Enable creating new sessions & unpause existing sessions"
        ...

    async def disable(self):
        "stop creating new sessions"
    	...

class Status(DataClass):
	# types for { task_name : { session_id: str, goal: issue | pr | none, prompt: str, location: str, key: str, prs: [{url, status}], issues: [url]} }
```

The `Waker` will allow the `Engine` to schedule future wake-ups without an internal timer.

### Waker

The `Waker` will implement this interface:

```python
class Waker(ABC):
    "Waker is responsible for ensuring that a task is woken up. For a production waker, this must survive loading."
    def schedule(self, wait: time.Duration, task: TaskName):
        ...
```

## Testing

The engine can be trivially scenario tested by injecting a mocked `StateManager` &
`SessionManager`. New engine behavior or bugs in the engine should be fully replaceable by
changing inputs & outputs. The engine is single threaded, so we won't need to worry about
races in the engine itself.

# Session Manager

The session manager owns the docker sessions and their status. It implements
`engine.SessionManager`. On creation, it accepts a `asyncio.Queue` that is uses to enqueue
events observed (`SessionTerminated`, `SessionWorking`, etc.). In addition to
`engine.SessionManager`, the session manager implements the `__enter__` and `__exit__`
protocol to manage it's background polling tasks & the servers associated with the GH
Proxy & `claude` hook & MCP handlers. To enter, the session manager needs to stand up its
hook endpoints, its GH proxy, then resume all sessions paused **on the previous exit**. On
exit, it needs to pause all sessions (blocking), then shutdown the background polling &
servers. The session manager will also accept a `engine.StateManager` (keyed to a
different file) for managing state: the sessions it needs to unpause on next enter.

To enable the TUI, the session manager exposes a function that takes a session ID and
returns the TMUX session it's running.

# Markdown Manager

This class is responsible for watching the `~/.taskpull/tasks` directory and enqueuing
`NewTask` & `RemoveTask` as appropriate. The tasks directory will be watched by
https://pypi.org/project/watchdog/ to ensure timely updates.

## GH Proxy

An important part of the session manager is the GH proxy. The GH proxy is in charge of
intercepting all GH calls and ensuring they are secure. It ensures that a session cannot
act outside of the repo it resides in (if it's repo driven) and that a session cannot edit
an issue or PR that it didn't create.

It's also the mechanism we use to track issues & PRs created. Because the GH Proxy
intercepts all API calls to GH, it can observe successful PR & issue creation and emit
appropriate events to the engine.

## GH Watcher

The main daemon will stand up a GH watcher to check the engine for status to see relevant
issues & PRs, then check GH for the status on a 200 second timer.

The watcher will poll the engine every 10 seconds to ensure that it has an up-to-date list
of PRs & Issues to watch.

### Testing

The GH proxy can be tested in isolation by mocking out both it's incoming requests & the
GH server itself. To record requests & responses, we can run a test script in the full
session manager to attempt to create and edit real issues & PRs.

## Testing

The session manager can be tested by running a test script instead of `claude`. The script
can simulate `claude` by sending the right hook calls, attempting to use `gh`, etc.

# TUI

The TUI is fully driven from the engine status, along with the session manager to get tmux
session IDs. Button presses send events to the engine. The TUI remains a fork of the local
process talking to a remote engine.

# CLI Commands

Like the TUI, CLI sends commands to the daemon (like now) and gets responses back.

# Testing

All testing should be done by scripts. Each module should have a unit test. Boundary
modules (GH Proxy, Sessions) should also have integration tests.

All testing should be automatable.
