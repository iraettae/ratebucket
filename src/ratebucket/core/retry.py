"""Classifying a failed attempt, and retrying it without hammering the provider.

Retries are not one policy but several decisions that must not be conflated. A 429
is backpressure the whole pool shares, and is handled by the `Gate`, not by this
worker sleeping alone. A 5xx or a dropped connection is a transient fault to back off
from with jitter. A 400 is this one request's fault and must fail only this task. A
401 or an exhausted budget is the run's fault and must stop everything. And a read
timeout is ambiguous -- the request may have been executed and billed -- so retrying
it risks paying twice, and the default is not to.

`RetryPolicy` is hand-written rather than tenacity because tenacity retries inside a
single coroutine and cannot tell the rest of the pool that the provider asked everyone
to slow down. The pool-wide reaction lives in the `Gate`; this policy drives one
worker's attempts and defers to the gate for throttling.

The backoff is full jitter -- `uniform(0, min(cap, base * 2**n))` -- because when many
workers back off at once, spreading each one uniformly across its whole window is what
minimises the chance they collide again on the retry (see Amazon's "Exponential
Backoff And Jitter"; equal jitter and decorrelated jitter both leave more clustering).
The randomness is injected so the choice is reproducible in tests.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import random
    from collections.abc import Awaitable, Callable

    from ratebucket.core.clock import Clock
    from ratebucket.core.gate import Gate

__all__ = ["Outcome", "RetryError", "RetryPolicy", "classify_status"]

_T = TypeVar("_T")


class Outcome(enum.Enum):
    """What to do about the result of one attempt."""

    #: Success.
    OK = enum.auto()
    #: 5xx, connect timeout, dropped connection: back off with jitter and retry.
    RETRYABLE = enum.auto()
    #: 429: close the pool-wide gate; do not back off alone.
    THROTTLED = enum.auto()
    #: Read timeout: the request may have been executed and billed; retry only if allowed.
    AMBIGUOUS = enum.auto()
    #: 4xx: this request is malformed; fail the task, not the run.
    PERMANENT = enum.auto()
    #: 401 or exhausted budget: abort the whole run.
    FATAL = enum.auto()


class RetryError(RuntimeError):
    """Retries were exhausted, or the task deadline passed, without success."""


def classify_status(code: int) -> Outcome:
    """Map an HTTP status code to an outcome. Transport exceptions (read vs connect
    timeout) are classified by the adapter, which knows the transport's exception
    types; this handles the codes a response carries.
    """
    if 200 <= code < 300:
        return Outcome.OK
    if code == 429:
        return Outcome.THROTTLED
    if code == 408:
        return Outcome.RETRYABLE  # request timeout: the server timed out, try again
    if code == 401:
        return Outcome.FATAL  # unauthorised: retrying cannot help
    if 400 <= code < 500:
        return (
            Outcome.PERMANENT
        )  # bad request for this task; other tasks are unaffected
    if 500 <= code < 600:
        return Outcome.RETRYABLE
    return Outcome.PERMANENT


class RetryPolicy:
    """Drives one worker's attempts: classify, back off, defer throttling to the gate."""

    def __init__(
        self,
        clock: Clock,
        rng: random.Random,
        *,
        max_attempts: int = 5,
        base_delay: float = 0.5,
        max_delay: float = 60.0,
        retry_ambiguous: bool = False,
        default_retry_after: float = 1.0,
        task_timeout: float | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._clock = clock
        self._rng = rng
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._retry_ambiguous = retry_ambiguous
        self._default_retry_after = default_retry_after
        self._task_timeout = task_timeout

    def backoff(self, attempt: int) -> float:
        """Full-jitter backoff for a zero-based attempt number."""
        ceiling = min(self._max_delay, self._base_delay * 2.0**attempt)
        return self._rng.uniform(0.0, ceiling)

    async def execute(
        self,
        attempt: Callable[[int], Awaitable[_T]],
        classify: Callable[[Exception], tuple[Outcome, float | None]],
        *,
        gate: Gate,
    ) -> _T:
        """Run `attempt(epoch)` under `gate`, retrying per the classified outcome.

        `attempt` performs one try under the gate epoch it is given and returns its
        result or raises. `classify` maps a raised exception to an outcome and, for a
        throttle, the Retry-After. A 429 closes the gate and does not consume a retry;
        a retryable or (if allowed) ambiguous failure sleeps a jittered backoff; a
        permanent or fatal failure, and an exhausted budget, raise.
        """
        deadline = (
            self._clock.now() + self._task_timeout
            if self._task_timeout is not None
            else None
        )
        tries = 0
        last: Exception | None = None
        while True:
            if deadline is not None and self._clock.now() >= deadline:
                raise RetryError("task deadline exceeded") from last
            epoch = await gate.pass_()
            try:
                return await attempt(epoch)
            except Exception as exc:  # classified and re-raised below; never swallowed
                outcome, retry_after = classify(exc)
                last = exc
                if outcome is Outcome.THROTTLED:
                    after = (
                        retry_after
                        if retry_after is not None
                        else self._default_retry_after
                    )
                    gate.throttle(after, epoch)
                    continue  # the gate holds the pause; a 429 is not a spent retry
                retryable = outcome is Outcome.RETRYABLE or (
                    outcome is Outcome.AMBIGUOUS and self._retry_ambiguous
                )
                if not retryable:
                    raise  # OK never lands here; PERMANENT, FATAL, un-retried AMBIGUOUS
                tries += 1
                if tries >= self._max_attempts:
                    raise
                await self._clock.sleep(self.backoff(tries - 1))
