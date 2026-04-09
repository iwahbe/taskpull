from abc import ABC
import .engine_events

class SessionManager(ABC):
    # Create a new session and return it's ID
    async def create(prompt: str, location: str) -> SessionID:
    # Send the command to pause an existing session, confirmation will come in the event stream
    async def pause(session: SessionID):
    # Send the command to resume a paused session, confirmation will come in the event stream
    async def resume(session: SessionID):
    # Send the command to terminate a session, confirmation will come in the event stream
    async def terminate(session: SessionID):

class StateManager(ABC):
    async def save(state: Any):
    async def load() -> Any:

class Engine:
	def __init__(self, sessions_manager: SessionManager, state_manager: StateManager, waker: Waker):
    	...

    async def handle(self, event: Event):
    	"Handle an event"
    	...

    def status(self) -> Status:
    	"A stable view onto the engine state"
        ...

    async def disable(self):
    	"Enable creating new sessions & unpause existing sessions"
        ...

    async def disable(self):
        "stop creating new sessions"
    	...
