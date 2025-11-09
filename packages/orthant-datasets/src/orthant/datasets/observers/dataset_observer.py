from asyncio import Queue
from typing import runtime_checkable, Protocol


@runtime_checkable
class DatasetObserver(Protocol):
    async def run_async(self, q_observation_events: Queue):
        ...
