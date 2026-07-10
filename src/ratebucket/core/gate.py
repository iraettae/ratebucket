"""A pool-wide pause, shared by every worker, that a burst of 429s raises once.

When a provider answers 429 (Retry-After: 20), fifty in-flight requests can each get
that answer within a second or two. Fifty independent backoffs would sleep the pool
for fifty times too long, or -- worse, if they extend a shared deadline -- for a sum
that keeps growing as late 429s from already-sent requests keep arriving. The pause
has to be raised once and then left alone until it lifts.

The mechanism is a compare-and-swap on a single epoch counter, which is easy to get
right on one event loop and easy to explain. A worker reads the epoch before it sends
its request and presents that epoch back with the 429. `throttle` acts only if the
epoch has not moved since: the first 429 of a burst increments the epoch and sets the
reopen time, and every later 429 that carries the now-stale epoch is a no-op. So the
burst produces one pause, at the Retry-After of whichever 429 landed first.
"""

from __future__ import annotations

from ratebucket.core.clock import Clock

__all__ = ["Gate"]


class Gate:
    """A gate the whole pool passes through, closed by a 429 and reopened by time."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._epoch = 0
        self._open_at = clock.now()

    @property
    def epoch(self) -> int:
        """The current epoch. A worker snapshots this before sending a request."""
        return self._epoch

    @property
    def open_at(self) -> float:
        return self._open_at

    async def pass_(self) -> int:
        """Wait until the gate is open, then return the epoch to send the request under.

        The caller presents the returned epoch back to `throttle` if that request is
        throttled, so a 429 from a request sent before the pause was raised does not
        raise it again.
        """
        while True:
            remaining = self._open_at - self._clock.now()
            if remaining <= 0:
                return self._epoch
            await self._clock.sleep(remaining)

    def throttle(self, retry_after: float, seen_epoch: int) -> bool:
        """Close the gate for `retry_after` seconds, unless this 429 is already stale.

        `seen_epoch` is the epoch the caller read before sending. If the epoch has
        advanced since -- another worker's 429 already closed the gate -- this call is
        ignored and returns False. Otherwise it advances the epoch, sets the reopen
        time, and returns True.
        """
        if seen_epoch < self._epoch:
            return False
        self._epoch += 1
        self._open_at = self._clock.now() + retry_after
        return True
