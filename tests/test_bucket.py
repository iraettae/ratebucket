"""TokenBucket: continuous refill, the negative floor, and a spin-free time_until."""

from __future__ import annotations

import math

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ratebucket import TokenBucket, VirtualClock

pytestmark = pytest.mark.asyncio


async def test_starts_full() -> None:
    bucket = TokenBucket(capacity=10.0, refill_per_sec=1.0, clock=VirtualClock())
    assert bucket.available() == 10.0
    assert bucket.capacity == 10.0
    assert bucket.rate == 1.0


async def test_try_take_succeeds_then_fails() -> None:
    bucket = TokenBucket(capacity=3.0, refill_per_sec=0.0, clock=VirtualClock())
    assert bucket.try_take(2.0) is True
    assert bucket.available() == 1.0
    assert bucket.try_take(2.0) is False  # only 1 left, no refill
    assert bucket.available() == 1.0


async def test_take_drives_the_level_negative() -> None:
    bucket = TokenBucket(capacity=5.0, refill_per_sec=1.0, clock=VirtualClock())
    bucket.take(8.0)
    assert bucket.available() == -3.0


async def test_give_back_is_capped_at_capacity() -> None:
    bucket = TokenBucket(capacity=5.0, refill_per_sec=0.0, clock=VirtualClock())
    bucket.take(3.0)  # level 2
    bucket.give_back(10.0)  # would be 12, capped at 5
    assert bucket.available() == 5.0


async def test_refill_is_continuous() -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=100.0, refill_per_sec=10.0, clock=clock)
    bucket.take(100.0)  # empty
    await clock.advance(0.5)
    assert bucket.available() == pytest.approx(5.0)  # 0.5s * 10/s
    await clock.advance(0.5)
    assert bucket.available() == pytest.approx(10.0)


async def test_refill_clamps_at_capacity() -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=10.0, refill_per_sec=100.0, clock=clock)
    bucket.take(10.0)  # empty
    await clock.advance(5.0)  # would refill 500, capped at 10
    assert bucket.available() == 10.0


async def test_refill_from_negative_reaches_zero_exactly_on_time() -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=1000.0, refill_per_sec=100.0, clock=clock)
    bucket.take(1500.0)  # 1000 - 1500 = -500
    assert bucket.available() == pytest.approx(-500.0)
    await clock.advance(4.999)
    assert bucket.available() < 0.0
    await clock.advance(0.001)  # -500 + 5.0s * 100/s = 0
    assert bucket.available() == pytest.approx(0.0, abs=1e-6)


async def test_time_until_is_zero_when_already_available() -> None:
    bucket = TokenBucket(capacity=10.0, refill_per_sec=1.0, clock=VirtualClock())
    assert bucket.time_until(5.0) == 0.0


async def test_time_until_is_inf_when_target_exceeds_capacity() -> None:
    bucket = TokenBucket(capacity=5.0, refill_per_sec=1.0, clock=VirtualClock())
    bucket.take(5.0)
    assert bucket.time_until(6.0) == math.inf  # refill clamps at 5, never reaches 6


async def test_time_until_is_inf_when_rate_is_zero() -> None:
    bucket = TokenBucket(capacity=5.0, refill_per_sec=0.0, clock=VirtualClock())
    bucket.take(5.0)
    assert bucket.time_until(1.0) == math.inf


async def test_one_sleep_of_time_until_always_crosses_the_target() -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=10.0, refill_per_sec=3.0, clock=clock)
    bucket.take(10.0)  # empty
    dt = bucket.time_until(7.0)
    await clock.advance(dt)
    assert bucket.try_take(7.0) is True  # first attempt, no spin


@given(
    capacity=st.floats(
        min_value=0.5, max_value=100.0, allow_nan=False, allow_infinity=False
    ),
    rate=st.floats(
        min_value=0.01, max_value=1000.0, allow_nan=False, allow_infinity=False
    ),
    target=st.floats(
        min_value=0.001, max_value=100.0, allow_nan=False, allow_infinity=False
    ),
    drain=st.floats(
        min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False
    ),
)
@settings(
    max_examples=500,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_time_until_never_spins(
    capacity: float, rate: float, target: float, drain: float
) -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=capacity, refill_per_sec=rate, clock=clock)
    bucket.take(drain)  # possibly negative
    dt = bucket.time_until(target)
    if not math.isfinite(dt):
        return  # unreachable target; correctly reported
    try:
        await clock.advance(dt)
        assert bucket.try_take(target), (
            f"spun: cap={capacity} rate={rate} target={target} drain={drain}"
        )
    finally:
        await clock.aclose()


@pytest.mark.parametrize(
    ("capacity", "rate"),
    [
        (-1.0, 1.0),
        (float("nan"), 1.0),
        (float("inf"), 1.0),
        (1.0, -1.0),
        (1.0, float("inf")),
    ],
)
async def test_construction_rejects_bad_parameters(
    capacity: float, rate: float
) -> None:
    with pytest.raises(ValueError, match="must be finite"):
        TokenBucket(capacity=capacity, refill_per_sec=rate, clock=VirtualClock())


async def test_per_minute_sizes_capacity_below_the_limit() -> None:
    # ADR 0003: refill at the full per-second rate, but capacity is ten seconds of it,
    # so rpm/6 rather than a whole minute's worth.
    bucket = TokenBucket.per_minute(3600, clock=VirtualClock())
    assert bucket.rate == 60.0  # 3600 / 60
    assert bucket.capacity == 600.0  # 60/s * 10s == 3600 / 6
    assert bucket.available() == 600.0


async def test_per_minute_burst_seconds_is_tunable() -> None:
    # A caller who knows the provider uses a rolling window can ask for the full minute.
    bucket = TokenBucket.per_minute(3600, clock=VirtualClock(), burst_seconds=60.0)
    assert bucket.capacity == 3600.0
