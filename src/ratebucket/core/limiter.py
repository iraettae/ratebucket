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

Above the bucket sits `MultiResourceLimiter`: one arbiter coroutine owning a FIFO
queue of pending requests, each a `(cost, future)` pair. Callers never touch the
buckets. The arbiter looks only at the head of the queue and grants it across every
dimension at once or not at all, which is what rules out the deadlock that two
independent buckets invite (acquire request, wait for tokens; another acquires
tokens, waits for a request; neither proceeds). The price is head-of-line blocking,
addressed by conservative backfilling.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections import deque
from typing import TYPE_CHECKING, Any

from ratebucket.core.clock import Clock

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping

__all__ = [
    "CostExceedsCapacityError",
    "LimiterError",
    "MultiResourceLimiter",
    "Reservation",
    "TokenBucket",
    "UnknownResourceError",
]

_NANOS_PER_SECOND = 1_000_000_000


def _time_to_reach(capacity: float, rate: float, level: float, target: float) -> float:
    """Seconds until a bucket at `level` (with this capacity and rate) holds `target`.

    0.0 if it already does or `target` is non-positive; `inf` if `target` exceeds
    capacity (the refill clamps there) or the rate is zero. The wait is rounded up to
    the nanosecond the clock quantises to, plus one, so a single sleep always crosses
    `target` rather than landing a rounding-error short. Rounding up wakes the caller
    later, which can only grant fewer permits.
    """
    if target <= 0 or level >= target:
        return 0.0
    if target > capacity or rate <= 0:
        return math.inf
    deficit = target - level
    return (math.ceil(deficit / rate * _NANOS_PER_SECOND) + 1) / _NANOS_PER_SECOND


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

    def _level_at(self, now_ns: int) -> float:
        # Refill is computed from the stored origin, not accumulated on every read.
        # Reads leave the origin untouched; only take/give_back move it. So reading
        # the level at several instants between two takes gives values consistent to
        # the last unit in place, instead of drifting by the float error a persisted
        # incremental refill would accumulate.
        elapsed = max(0, now_ns - self._last_ns) / _NANOS_PER_SECOND
        return min(self._capacity, self._level + elapsed * self._rate)

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def rate(self) -> float:
        return self._rate

    def available(self) -> float:
        """Tokens available now. May be negative. Does not mutate the bucket."""
        return self._level_at(self._read_ns())

    def try_take(self, n: float) -> bool:
        """Take `n` if the bucket holds at least `n`. Return whether it did."""
        now_ns = self._read_ns()
        level = self._level_at(now_ns)
        if level >= n:
            self._level = level - n
            self._last_ns = now_ns
            return True
        return False

    def take(self, n: float) -> None:
        """Take `n` unconditionally, driving the level negative if need be."""
        now_ns = self._read_ns()
        self._level = self._level_at(now_ns) - n
        self._last_ns = now_ns

    def give_back(self, n: float) -> None:
        """Return `n` unused tokens, never exceeding capacity."""
        now_ns = self._read_ns()
        self._level = min(self._capacity, self._level_at(now_ns) + n)
        self._last_ns = now_ns

    def time_until(self, n: float) -> float:
        """Seconds until the bucket would hold `n` tokens; 0.0 if it already does.

        See `_time_to_reach` for the exact contract (nanosecond-ceilinged wait, `inf`
        when `n` exceeds capacity or the rate is zero). Does not mutate the bucket.
        """
        return _time_to_reach(
            self._capacity, self._rate, self._level_at(self._read_ns()), n
        )


class LimiterError(RuntimeError):
    """Base class for limiter misuse."""


class UnknownResourceError(LimiterError):
    """A cost named a dimension the limiter has no bucket for."""

    def __init__(self, dimension: str) -> None:
        super().__init__(f"no bucket for resource {dimension!r}")
        self.dimension = dimension


class CostExceedsCapacityError(LimiterError):
    """A cost is larger than a bucket can ever hold, so it could never be granted.

    Caught before the request enters the queue, so it fails fast instead of parking
    behind a refill that can never arrive and blocking everything behind it.
    """

    def __init__(self, dimension: str, cost: float, capacity: float) -> None:
        super().__init__(
            f"cost {cost} for {dimension!r} exceeds its capacity {capacity}; it can never be granted"
        )
        self.dimension = dimension
        self.cost = cost
        self.capacity = capacity


_Entry = tuple["dict[str, int]", "asyncio.Future[float]"]


class MultiResourceLimiter:
    """A single arbiter over several token buckets, granting each request atomically.

    Callers enqueue a cost and await a grant; they never touch the buckets. The lone
    arbiter coroutine looks only at the head of a FIFO queue and either grants it
    across every dimension at once or waits. Granting all-or-nothing is what makes
    the two-limit case deadlock-free: there is no state in which one resource is held
    while another is awaited.
    """

    def __init__(
        self,
        buckets: Mapping[str, TokenBucket],
        clock: Clock,
        *,
        backfill: bool = False,
    ) -> None:
        if not buckets:
            raise ValueError("a limiter needs at least one resource bucket")
        self._buckets: dict[str, TokenBucket] = dict(buckets)
        self._clock = clock
        self._backfill = backfill
        self._queue: deque[_Entry] = deque()
        self._wakeup = asyncio.Event()
        self._arbiter: asyncio.Task[Any] | None = None
        self._spawn: Callable[[Coroutine[Any, Any, Any]], asyncio.Task[Any]] = (
            asyncio.create_task
        )

    # -- lifecycle ----------------------------------------------------------

    def start(
        self,
        spawn: Callable[[Coroutine[Any, Any, Any]], asyncio.Task[Any]] | None = None,
    ) -> None:
        """Launch the arbiter. Pass `clock.spawn` under a VirtualClock so the test
        clock owns the coroutine and can drive it; the default is `create_task`.
        """
        if self._arbiter is not None:
            raise LimiterError("limiter already started")
        if spawn is not None:
            self._spawn = spawn
        self._arbiter = self._spawn(self._run())

    async def aclose(self) -> None:
        if self._arbiter is not None:
            self._arbiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._arbiter
            self._arbiter = None
        # Fail anyone still parked in the queue rather than leave them awaiting a
        # future the (now dead) arbiter will never resolve.
        while self._queue:
            _cost, fut = self._queue.popleft()
            if not fut.done():
                fut.cancel()

    # -- public surface -----------------------------------------------------

    async def acquire(self, cost: Mapping[str, int]) -> float:
        """Take `cost` from every dimension once granted, permanently. Returns the
        grant time. For the reserve-then-reconcile flow, use `reserve` instead.
        """
        self._validate(cost)
        return await self._enqueue(dict(cost))

    def reserve(self, cost: Mapping[str, int]) -> Reservation:
        """An async context manager that reserves `cost` up front and reconciles it
        against actual usage on exit. See `Reservation`.
        """
        self._validate(cost)
        return Reservation(self, dict(cost))

    # -- validation ---------------------------------------------------------

    def _validate(self, cost: Mapping[str, int]) -> None:
        for dimension, amount in cost.items():
            bucket = self._buckets.get(dimension)
            if bucket is None:
                raise UnknownResourceError(dimension)
            if amount < 0:
                # A negative cost would give tokens back on grant, minting quota
                # above capacity. Reject it at the boundary, like an oversized cost.
                raise ValueError(
                    f"cost for {dimension!r} must be non-negative, got {amount}"
                )
            if amount > bucket.capacity:
                raise CostExceedsCapacityError(dimension, amount, bucket.capacity)

    # -- enqueue and cancellation ------------------------------------------

    async def _enqueue(self, cost: dict[str, int]) -> float:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[float] = loop.create_future()
        entry: _Entry = (cost, fut)
        self._queue.append(entry)
        # Removing a cancelled waiter is not optional: left in place, a cancelled
        # head makes the arbiter compute a refill for a caller that will never take
        # it, forever. The callback pulls it out and re-wakes the arbiter.
        fut.add_done_callback(lambda _f: self._on_waiter_done(entry))
        self._wakeup.set()
        try:
            return await fut
        except asyncio.CancelledError:
            # A grant can land a tick before the cancellation reaches us: the future
            # holds a result, yet `await` still raises CancelledError. The tokens
            # were already taken, so give them back rather than leak the whole grant.
            # (For a Reservation this matters most: __aenter__ raising means __aexit__
            # never runs, so this is the only place the reservation can be released.)
            if not fut.cancelled() and fut.done() and fut.exception() is None:
                for dimension, amount in cost.items():
                    self._buckets[dimension].give_back(amount)
                self._wakeup.set()
            raise

    def _on_waiter_done(self, entry: _Entry) -> None:
        with contextlib.suppress(ValueError):
            self._queue.remove(entry)
        self._wakeup.set()

    # -- reconciliation (called by Reservation) ----------------------------

    def _reconcile(self, reserved: dict[str, int], actual: Mapping[str, int]) -> None:
        for dimension, amount in reserved.items():
            used = actual.get(dimension, amount)
            unused = amount - used
            if unused > 0:
                self._buckets[dimension].give_back(unused)
            elif unused < 0:
                # The estimate came in low; the bucket goes negative rather than
                # silently under-counting the overspend.
                self._buckets[dimension].take(-unused)
        self._wakeup.set()

    def _release(self, reserved: dict[str, int]) -> None:
        for dimension, amount in reserved.items():
            self._buckets[dimension].give_back(amount)
        self._wakeup.set()

    # -- the arbiter --------------------------------------------------------

    async def _run(self) -> None:
        head_fut: asyncio.Future[float] | None = None
        head_deadline_ns: float = math.inf
        while True:
            while self._queue and self._queue[0][1].done():
                self._queue.popleft()
            if not self._queue:
                head_fut = None
                await self._wait_for_change(None)
                continue
            cost, fut = self._queue[0]
            if self._try_grant(cost):
                self._queue.popleft()
                fut.set_result(self._clock.now())
                head_fut = None
                continue
            # A waiting head has a promised start. Track it monotonically so
            # re-evaluating it across wakes -- each re-applying the conservative +1ns
            # wake rounding -- can never push it later. A give_back that shortens it is
            # still picked up, because the freshly computed value is then smaller. This
            # is what makes backfill (whose extra wakes trigger the re-evaluations)
            # unable to delay the head, exactly and not merely to within a nanosecond.
            now_ns = round(self._clock.now() * _NANOS_PER_SECOND)
            head_wait = self._head_wait(cost)
            fresh_ns = (
                math.inf
                if head_wait == math.inf
                else now_ns + round(head_wait * _NANOS_PER_SECOND)
            )
            if fut is not head_fut:
                head_fut = fut
                head_deadline_ns = fresh_ns
            else:
                head_deadline_ns = min(head_deadline_ns, fresh_ns)
            wait = (
                math.inf
                if head_deadline_ns == math.inf
                else max(0.0, (head_deadline_ns - now_ns) / _NANOS_PER_SECOND)
            )
            if self._backfill and self._backfill_one(cost, wait):
                continue
            if self._backfill:
                # A backfill candidate can become grantable by refill before the head
                # does; wake when the earliest one might, or it would stall until an
                # unrelated wakeup (forever, if the head's own wait is infinite).
                wait = min(wait, self._earliest_candidate_wait())
            await self._wait_for_change(wait)

    def _try_grant(self, cost: Mapping[str, int]) -> bool:
        # A zero requirement takes nothing and must never gate a grant, even against
        # a bucket that has gone negative.
        if all(n <= 0 or self._buckets[d].available() >= n for d, n in cost.items()):
            for d, n in cost.items():
                if n > 0:
                    self._buckets[d].take(n)
            return True
        return False

    def _head_wait(self, cost: Mapping[str, int]) -> float:
        return max(
            (self._buckets[d].time_until(n) for d, n in cost.items()), default=0.0
        )

    def _earliest_candidate_wait(self) -> float:
        soonest = math.inf
        for index in range(1, len(self._queue)):
            cost, fut = self._queue[index]
            if fut.done():
                continue
            candidate_wait = self._head_wait(cost)
            if candidate_wait > 0:  # already grantable-now ones were just evaluated
                soonest = min(soonest, candidate_wait)
        return soonest

    def _backfill_one(self, head: Mapping[str, int], head_wait: float) -> bool:
        """Grant one queued request out of order, but only if doing so does not push
        the head's start any later. This is conservative backfilling from HPC
        schedulers: the head keeps its computed start, small requests fill the gap.
        """
        for index in range(1, len(self._queue)):
            cost, fut = self._queue[index]
            if fut.done():
                continue
            if self._safe_to_backfill(cost, head, head_wait):
                for d, n in cost.items():
                    self._buckets[d].take(n)
                del self._queue[index]
                fut.set_result(self._clock.now())
                return True
        return False

    def _safe_to_backfill(
        self, candidate: Mapping[str, int], head: Mapping[str, int], head_wait: float
    ) -> bool:
        for dimension in candidate.keys() | head.keys():
            bucket = self._buckets[dimension]
            available = bucket.available()
            want = candidate.get(dimension, 0)
            if want > 0 and want > available:
                return False  # not grantable now
            need = head.get(dimension, 0)
            if need <= 0:
                continue
            # After taking `want` now, would the head reach `need` any later than its
            # existing wait? Computed with the SAME nanosecond-ceilinged arithmetic the
            # arbiter uses for the wait, so backfilling never delays the head, not even
            # by the rounding the linear form would leave behind.
            reach = _time_to_reach(bucket.capacity, bucket.rate, available - want, need)
            if reach > head_wait:
                return False
        return True

    # -- waiting ------------------------------------------------------------

    async def _wait_for_change(self, wait: float | None) -> None:
        # Not asyncio.timeout: that measures real loop time, and the arbiter runs on
        # the injected clock, which under test is virtual. The wait is a clock sleep
        # racing a wakeup Event, both driven by the same clock the buckets read.
        self._wakeup.clear()
        if wait is None or not math.isfinite(wait):
            await self._wakeup.wait()
            return
        timer = self._spawn(self._timer(wait))
        try:
            await self._wakeup.wait()
        finally:
            if not timer.done():
                timer.cancel()

    async def _timer(self, delay: float) -> None:
        await self._clock.sleep(delay)
        self._wakeup.set()


class Reservation:
    """Reserve by upper bound before a request, reconcile by actual usage after.

    On a normal exit you call `commit(actual)` and the bucket is adjusted by the
    difference; if `actual` came in over the reservation the bucket goes negative.
    If the block exits without a commit -- an exception, a cancellation -- the whole
    reservation is released in `__aexit__`, so a failure between acquiring and
    committing cannot leak quota out of the bucket for good.
    """

    def __init__(self, limiter: MultiResourceLimiter, reserved: dict[str, int]) -> None:
        self._limiter = limiter
        self._reserved = reserved
        self._settled = False
        self.granted_at: float | None = None

    async def __aenter__(self) -> Reservation:
        self.granted_at = await self._limiter._enqueue(self._reserved)
        return self

    def commit(self, actual: Mapping[str, int]) -> None:
        if self._settled:
            raise LimiterError("reservation already settled")
        self._limiter._reconcile(self._reserved, actual)
        self._settled = True

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not self._settled:
            self._limiter._release(self._reserved)
            self._settled = True
