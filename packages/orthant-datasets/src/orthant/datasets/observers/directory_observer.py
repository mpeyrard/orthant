import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Optional, AsyncIterator
from watchfiles import awatch

from .dataset_observer import DatasetObserver
from .events import DatasetObserverEvent


def _canonical_change(name: str) -> str:
    n = name.lower()
    if n in ("added", "created"):
        return "added"
    if n in ("modified", "changed", "updated", "moved"):
        return "modified"
    if n in ("deleted", "removed"):
        return "deleted"
    return n


class DirectoryObserver(DatasetObserver):
    def __init__(self, dataset_directory: Path):
        self._dataset_directory = dataset_directory

        # An optional readiness event consumers/tests can await; it will be set
        # once the observer loop starts (or on shutdown/failure to avoid hanging).
        self.started: Optional[asyncio.Event] = None

    async def run_async(
        self,
        q_observation_events: asyncio.Queue,
        *,
        started: Optional[asyncio.Event] = None,
        watch_fn: Callable[[Path], AsyncIterator] = awatch,
    ) -> None:
        """
        Watch the dataset directory for changes and enqueue normalized
        DatasetObserverEvent objects.

        Parameters:
        - q_observation_events: asyncio.Queue to put DatasetObserverEvent into.
        - started: optional asyncio.Event which will be set to signal readiness.
        - watch_fn: injectable async watch function (defaults to watchfiles.awatch).
        """
        # Ensure there's an event object available to await readiness.
        self.started = started or asyncio.Event()

        # Signal that the observer task has started so callers don't need to
        # rely on sleeps; we set this immediately so tests can proceed to
        # create files. If the underlying watcher fails to attach, we still
        # set the event to avoid hanging waiters.
        self.started.set()

        try:
            async for changes in watch_fn(self._dataset_directory):
                for change_type, changed_path in changes:
                    canonical = _canonical_change(change_type.name)
                    event = DatasetObserverEvent(
                        path=str(changed_path),
                        change=canonical,
                        timestamp=datetime.now(timezone.utc),
                        raw=(change_type, changed_path),
                    )
                    await q_observation_events.put(event)
        except asyncio.CancelledError:
            # allow graceful cancellation
            raise
        finally:
            # Ensure started is set so waiters won't hang if the loop exits.
            if self.started is not None and not self.started.is_set():
                self.started.set()
