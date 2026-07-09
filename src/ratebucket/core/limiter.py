"""The token bucket: a resource that refills continuously and is spent in units.

Two departures from the textbook, both load-bearing:

*Lazy refill.* There is no background task topping the bucket up. The level is a
function of time, recomputed from the last reading whenever the bucket is touched.
A timer that adds `rate/60` tokens once a second is a discretisation, and a
discretisation bursts at its boundaries: a bucket that dumps a minute of tokens at
once can, in a window straddling two dumps, grant twice what a continuous bucket
would. Continuous refill is the difference between the invariant holding and not.

*The level may go negative.* When a caller commits more usage than it reserved
(a token estimate that guessed low), the bucket must record the overdraft, not
clamp it away at zero. Clamping would silently under-count the quota and let the
next window over-spend. The only clamp is the upper one, at capacity.

Time is quantised to nanoseconds before any arithmetic. `clock.now()` is seconds
as a float, and `a/1e9 - b/1e9` accumulates rounding that can, at a threshold,
grant one permit too many. Recovering integer nanoseconds first (`round(now*1e9)`,
exact for any run short of ~104 days) makes the elapsed interval exact, so the
bound `permits(W) <= capacity + rate*W` holds with no tolerance constant.
"""

from __future__ import annotations

import math

from ratebucket.core.clock import Clock

__all__ = ["TokenBucket"]

_NANOS_PER_SECOND = 1_000_000_000


class TokenBucket:
    """One quota dimension: a capacity, a refill rate, and a clock to read time."""

    def __init__(self, capacity: float, refill_per_sec: float, clock: Clock) -> None:
        if not math.isfinite(capacity) or capacity < 0:
            raise ValueError(
                f"capacity must be finite and non-negative, got {capacity!r}"
            )
        if not math.isfinite(refill_per_sec) or refill_per_sec < 0:
            raise ValueError(
                f"refill_per_sec must be finite and non-negative, got {refill_per_sec!r}"
            )
        self._capacity = float(capacity)
        self._rate = float(refill_per_sec)
        self._clock = clock
        self._level = self._capacity
        self._last_ns = self._read_ns()

    def _read_ns(self) -> int:
        return round(self._clock.now() * _NANOS_PER_SECOND)

    def _refill(self) -> None:
        now_ns = self._read_ns()
        if now_ns > self._last_ns:
            elapsed = (now_ns - self._last_ns) / _NANOS_PER_SECOND
            self._level = min(self._capacity, self._level + elapsed * self._rate)
            self._last_ns = now_ns

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def rate(self) -> float:
        return self._rate

    def available(self) -> float:
        """Tokens available now, after refill. May be negative."""
        self._refill()
        return self._level

    def try_take(self, n: float) -> bool:
        """Take `n` if the bucket holds at least `n`. Return whether it did."""
        self._refill()
        if self._level >= n:
            self._level -= n
            return True
        return False

    def take(self, n: float) -> None:
        """Take `n` unconditionally, driving the level negative if need be."""
        self._refill()
        self._level -= n

    def give_back(self, n: float) -> None:
        """Return `n` unused tokens, never exceeding capacity."""
        self._refill()
        self._level = min(self._capacity, self._level + n)

    def time_until(self, n: float) -> float:
        """Seconds until the bucket would hold `n` tokens; 0.0 if it already does.

        `inf` when `n` exceeds capacity (refill clamps there, so it is never
        reachable) or the rate is zero. The wait is rounded up to the nanosecond
        the clock quantises to, plus one, so a single sleep always crosses `n`
        rather than landing a rounding-error short and forcing the caller to spin.
        Rounding up wakes the caller later, which can only grant fewer permits.
        """
        self._refill()
        if self._level >= n:
            return 0.0
        if n > self._capacity or self._rate <= 0:
            return math.inf
        deficit = n - self._level
        return (
            math.ceil(deficit / self._rate * _NANOS_PER_SECOND) + 1
        ) / _NANOS_PER_SECOND
