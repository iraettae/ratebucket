"""RetryPolicy and the error classifier: back off, defer throttling, honour deadlines."""

from __future__ import annotations

import random

import pytest

from ratebucket import (
    Gate,
    Outcome,
    RetryError,
    RetryPolicy,
    VirtualClock,
    classify_status,
)

pytestmark = pytest.mark.asyncio


class FakeError(Exception):
    """An attempt failure carrying its classified outcome, for the driver tests."""

    def __init__(self, outcome: Outcome, retry_after: float | None = None) -> None:
        super().__init__(outcome.name)
        self.outcome = outcome
        self.retry_after = retry_after


def _classify(exc: Exception) -> tuple[Outcome, float | None]:
    assert isinstance(exc, FakeError)
    return exc.outcome, exc.retry_after


def _policy(clock: VirtualClock, **kw: object) -> RetryPolicy:
    return RetryPolicy(clock, random.Random(0), **kw)  # type: ignore[arg-type]


# --- the classifier ---------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "outcome"),
    [
        (200, Outcome.OK),
        (204, Outcome.OK),
        (429, Outcome.THROTTLED),
        (408, Outcome.RETRYABLE),
        (500, Outcome.RETRYABLE),
        (503, Outcome.RETRYABLE),
        (400, Outcome.PERMANENT),
        (404, Outcome.PERMANENT),
        (401, Outcome.FATAL),
    ],
)
async def test_classify_status(code: int, outcome: Outcome) -> None:
    assert classify_status(code) == outcome


# --- full-jitter backoff ----------------------------------------------------


async def test_backoff_is_full_jitter_within_the_ceiling() -> None:
    clock = VirtualClock()
    policy = _policy(clock, base_delay=1.0, max_delay=60.0)
    for attempt in range(8):
        ceiling = min(60.0, 1.0 * 2.0**attempt)
        for _ in range(50):
            assert 0.0 <= policy.backoff(attempt) <= ceiling


async def test_backoff_is_deterministic_for_a_given_seed() -> None:
    clock = VirtualClock()
    a = RetryPolicy(clock, random.Random(42)).backoff(3)
    b = RetryPolicy(clock, random.Random(42)).backoff(3)
    assert a == b


# --- the driver -------------------------------------------------------------


async def _run(
    policy: RetryPolicy, attempt: object, gate: Gate, clock: VirtualClock
) -> dict[str, object]:
    box: dict[str, object] = {}

    async def go() -> None:
        try:
            box["value"] = await policy.execute(attempt, _classify, gate=gate)  # type: ignore[arg-type]
        except Exception as exc:
            box["error"] = exc

    _ = clock.spawn(go())
    await clock.advance(600.0)
    await clock.aclose()
    return box


async def test_execute_returns_on_success() -> None:
    clock = VirtualClock()

    async def attempt(_epoch: int) -> str:
        return "done"

    box = await _run(_policy(clock), attempt, Gate(clock), clock)
    assert box["value"] == "done"


async def test_execute_retries_a_retryable_failure_then_succeeds() -> None:
    clock = VirtualClock()
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        if len(tries) < 3:
            raise FakeError(Outcome.RETRYABLE)
        return "done"

    box = await _run(_policy(clock, max_attempts=5), attempt, Gate(clock), clock)
    assert box["value"] == "done"
    assert len(tries) == 3


async def test_execute_raises_a_permanent_failure_at_once() -> None:
    clock = VirtualClock()
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        raise FakeError(Outcome.PERMANENT)

    box = await _run(_policy(clock), attempt, Gate(clock), clock)
    assert isinstance(box["error"], FakeError)
    assert len(tries) == 1  # not retried


async def test_execute_raises_a_fatal_failure_at_once() -> None:
    clock = VirtualClock()

    async def attempt(_epoch: int) -> str:
        raise FakeError(Outcome.FATAL)

    box = await _run(_policy(clock), attempt, Gate(clock), clock)
    assert isinstance(box["error"], FakeError)


async def test_execute_exhausts_attempts_and_raises_the_last_error() -> None:
    clock = VirtualClock()
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        raise FakeError(Outcome.RETRYABLE)

    box = await _run(_policy(clock, max_attempts=3), attempt, Gate(clock), clock)
    assert isinstance(box["error"], FakeError)
    assert len(tries) == 3


async def test_execute_defers_a_throttle_to_the_gate_without_spending_a_retry() -> None:
    # A 429 closes the gate; the worker waits it out and tries again, and the throttle
    # does not count against max_attempts (it is backpressure, not a failed attempt).
    clock = VirtualClock()
    gate = Gate(clock)
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        if len(tries) <= 4:  # more throttles than max_attempts would allow as retries
            raise FakeError(Outcome.THROTTLED, retry_after=5.0)
        return "done"

    box = await _run(_policy(clock, max_attempts=2), attempt, gate, clock)
    assert box["value"] == "done"
    assert len(tries) == 5
    assert gate.epoch >= 1


async def test_execute_does_not_retry_ambiguous_by_default() -> None:
    clock = VirtualClock()
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        raise FakeError(Outcome.AMBIGUOUS)

    box = await _run(_policy(clock), attempt, Gate(clock), clock)
    assert isinstance(box["error"], FakeError)
    assert len(tries) == 1  # the read-timeout default: do not risk paying twice


async def test_execute_retries_ambiguous_when_allowed() -> None:
    clock = VirtualClock()
    tries: list[int] = []

    async def attempt(_epoch: int) -> str:
        tries.append(1)
        if len(tries) < 2:
            raise FakeError(Outcome.AMBIGUOUS)
        return "done"

    box = await _run(_policy(clock, retry_ambiguous=True), attempt, Gate(clock), clock)
    assert box["value"] == "done"
    assert len(tries) == 2


async def test_execute_gives_up_at_the_task_deadline() -> None:
    clock = VirtualClock()

    async def attempt(_epoch: int) -> str:
        raise FakeError(Outcome.RETRYABLE)

    box = await _run(
        _policy(clock, max_attempts=100, base_delay=10.0, task_timeout=30.0),
        attempt,
        Gate(clock),
        clock,
    )
    assert isinstance(box["error"], RetryError)
