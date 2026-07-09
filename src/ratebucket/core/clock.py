"""Time, injected rather than read from the wall.

The one idea here: a rate limiter is a machine over time, and a machine over time
is only testable if time is an argument. So everything downstream takes a `Clock`.
`RealClock` is `time.monotonic` and `asyncio.sleep`. `VirtualClock` is a plain
object the test drives by hand, so a schedule that would take a minute of wall
time runs in microseconds and gives the same answer every run.

`VirtualClock` is never installed as the event loop's clock. Virtual time ends
exactly where a real socket begins: later stages run a real server in the same
loop, and that loop must keep real time or its timeouts break. The clock is an
argument, not a monkeypatch.

Internally the virtual clock counts integer nanoseconds. That is not a micro-
optimisation, it is what makes `advance(a)` then `advance(b)` land in exactly the
same place as `advance(a + b)`: integer addition is associative and float addition
is not (`0.1 + 0.2 != 0.3`). Deadlines are integers, comparisons are exact, and
there is no tolerance constant anywhere in this file.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import math
import time
from collections.abc import Coroutine
from typing import Any, Protocol, cast, runtime_checkable

__all__ = ["Clock", "ClockError", "RealClock", "VirtualClock"]

_NANOS_PER_SECOND = 1_000_000_000


class ClockError(RuntimeError):
    """The virtual clock could not settle: a schedule that never converges."""


@runtime_checkable
class Clock(Protocol):
    """Seconds since an arbitrary, fixed origin, and a way to wait.

    `now()` is monotonic non-decreasing; only differences between readings mean
    anything. `sleep(s)` resumes at a time no earlier than `now() + s`.
    """

    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class RealClock:
    """`time.monotonic` and `asyncio.sleep`. What production runs on."""

    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# The virtual clock's quiescence probe.
#
# advance() must run every coroutine it woke to its next blocking point before it
# returns, or the test's next assertion reads stale state. To do that it has to
# know whether any of its tasks are still runnable, and asyncio exposes no such
# predicate. The one honest signal is Task._fut_waiter: None or a done future
# means the task is ready to run. It is private, so we read it through a guarded
# accessor that raises loudly the day CPython renames it, rather than silently
# under-draining.
# ---------------------------------------------------------------------------
_MISSING: object = object()


def _fut_waiter(task: asyncio.Task[Any]) -> asyncio.Future[Any] | None:
    waiter = getattr(task, "_fut_waiter", _MISSING)
    if waiter is _MISSING:  # pragma: no cover - only on a CPython rename
        raise ClockError(
            "asyncio.Task._fut_waiter is gone; VirtualClock needs a new quiescence probe"
        )
    return cast("asyncio.Future[Any] | None", waiter)


class VirtualClock:
    """Deterministic virtual time. Drive it with `advance`; never install it.

    `advance(dt)` steps to each pending deadline in order, wakes exactly the
    sleepers due there, and runs the loop until nothing more can progress, before
    moving on. It steps rather than jumping straight to the end so that a bucket
    which refills and is drained at an intermediate deadline sees that interleaving
    (jumping to the end and clamping at capacity would silently lose permits).
    """

    #: Two runaway guards. Neither has a natural bound: a Zeno schedule
    #: (0.5, 0.25, 0.125, ...) packs infinitely many deadlines into a finite
    #: window, and a coroutine that never awaits never quiesces. Both must raise,
    #: never silently return, or the whole suite hangs with no diagnostic.
    MAX_STEPS = 100_000
    MAX_YIELDS = 100_000

    def __init__(self, start: float = 0.0) -> None:
        self._now_ns = round(start * _NANOS_PER_SECOND)
        self._heap: list[tuple[int, int, asyncio.Future[None]]] = []
        self._seq = itertools.count()
        self._tasks: set[asyncio.Task[Any]] = set()

    def now(self) -> float:
        return self._now_ns / _NANOS_PER_SECOND

    def now_ns(self) -> int:
        """Exact integer nanoseconds. A test convenience, not part of `Clock`."""
        return self._now_ns

    async def sleep(self, seconds: float) -> None:
        if not math.isfinite(seconds):
            raise ValueError(f"sleep needs a finite duration, got {seconds!r}")
        if seconds <= 0:
            # Still a checkpoint, matching asyncio.sleep(0). A sleep that does not
            # yield turns a fair FIFO arbiter into a starvation machine.
            await asyncio.sleep(0)
            return
        deadline = self._now_ns + round(seconds * _NANOS_PER_SECOND)
        entry = (deadline, next(self._seq), asyncio.get_running_loop().create_future())
        heapq.heappush(self._heap, entry)
        try:
            await entry[2]
        finally:
            # A cancelled sleeper must leave no trace, or advance() computes refill
            # against a corpse at the head of the queue forever.
            try:
                self._heap.remove(entry)
            except ValueError:
                pass
            else:
                heapq.heapify(self._heap)

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        """Create a task the clock owns, so `advance` drains this one and not the
        zombies a previous hypothesis example left on the shared loop.
        """
        task = asyncio.get_running_loop().create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def aclose(self) -> None:
        """Cancel and reap every owned task. Call in a `finally` per example."""
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._heap.clear()

    def _runnable(self) -> bool:
        me = asyncio.current_task()
        for task in self._tasks:
            if task is me or task.done():
                continue
            waiter = _fut_waiter(task)
            # `waiter.done()` is not optional: between fut.set_result() and the
            # task's wakeup actually running, _fut_waiter still points at the now
            # done future. Omitting it drains one step early.
            if waiter is None or waiter.done():
                return True
        return False

    async def _drain(self) -> None:
        yields = 0
        while self._runnable():
            await asyncio.sleep(0)
            yields += 1
            if yields > self.MAX_YIELDS:
                raise ClockError(f"no quiescence after {self.MAX_YIELDS} yields")

    def _prune(self) -> None:
        while self._heap and self._heap[0][2].cancelled():
            heapq.heappop(self._heap)

    def _due(self, target_ns: int) -> bool:
        self._prune()
        return bool(self._heap) and self._heap[0][0] <= target_ns

    def _fire(self, when_ns: int) -> None:
        self._prune()
        while self._heap and self._heap[0][0] <= when_ns:
            _, _, fut = heapq.heappop(self._heap)
            if not fut.done():
                fut.set_result(None)

    async def advance(self, dt: float) -> None:
        if not math.isfinite(dt):
            raise ValueError(f"advance needs a finite duration, got {dt!r}")
        if dt < 0:
            raise ValueError("time cannot go backwards")
        target = self._now_ns + round(dt * _NANOS_PER_SECOND)
        await self._drain()
        steps = 0
        while self._due(target):
            steps += 1
            if steps > self.MAX_STEPS:
                raise ClockError(
                    f"advance({dt}) did not converge in {self.MAX_STEPS} steps"
                )
            self._now_ns = self._heap[0][0]
            self._fire(self._now_ns)
            await self._drain()
        self._now_ns = target
        await self._drain()
