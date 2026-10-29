"""
Retry for Delta writes that lose a race with another writer.

Another task changing a table's metadata (for example adding a comment) at the same moment the
stream writes to it makes Delta reject the write with DELTA_METADATA_CHANGED. Delta applies a
write completely or not at all, so repeating the same write is safe.
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")

CONFLICT_MARKERS = ("DELTA_METADATA_CHANGED", "MetadataChangedException", "ConcurrentAppendException")


def is_write_conflict(exc: BaseException) -> bool:
    """True if the error is a concurrent-update conflict that a repeat of the write can overcome."""
    text = str(exc)
    return any(marker in text for marker in CONFLICT_MARKERS)


def retry_on_conflict(write: Callable[[], T], attempts: int = 4, delay_seconds: float = 2.0,
                      sleep: Callable[[float], None] = time.sleep, on_retry: Callable[[int, BaseException], None] | None = None) -> T:
    """Run `write`; on a write conflict wait (growing delay) and run it again, up to `attempts` runs in total."""
    for attempt in range(1, attempts + 1):
        try:
            return write()
        except Exception as exc:                     # noqa: BLE001 - only conflicts are retried, the rest re-raised
            if attempt == attempts or not is_write_conflict(exc):
                raise
            if on_retry:
                on_retry(attempt, exc)
            sleep(delay_seconds * attempt)
    raise RuntimeError("unreachable")                # pragma: no cover
