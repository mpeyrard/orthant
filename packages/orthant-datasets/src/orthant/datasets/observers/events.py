from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class DatasetObserverEvent:
    """
    Normalized event emitted by dataset observers.

    Attributes:
        path: The string path to the changed file or directory.
        change: A normalized change string, e.g. 'added', 'modified', 'deleted'.
        timestamp: Optional UTC timestamp when the event was observed.
        raw: Optional raw payload from the underlying watcher (kept for debugging).
    """
    path: str
    change: str
    timestamp: Optional[datetime] = None
    raw: Optional[object] = None
