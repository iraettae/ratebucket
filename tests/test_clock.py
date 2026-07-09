"""VirtualClock: determinism, stepped advance, cancellation, runaway guards."""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ratebucket import Clock, ClockError, RealClock, VirtualClock

pytestmark = pytest.mark.asyncio


async def test_both_clocks_satisfy_the_protocol() -> None:
    assert isinstance(RealClock(), Clock)
    assert isinstance(VirtualClock(), Clock)


async def test_now_starts_at_zero() -> None:
    assert VirtualClock().now() == 0.0
    assert VirtualClock(start=5.0).now() == 5.0


async def test_advance_moves_now_by_exactly_dt() -> None:
    clock = VirtualClock()
    await clock.advance(1.5)
    assert clock.now() == 1.5
    await clock.advance(0.25)
    assert clock.now() == 1.75


async def test_advance_rejects_negative_and_nonfinite() -> None:
    clock = VirtualClock()
    with pytest.raises(ValueError, match="backwards"):
        await clock.advance(-1.0)
    with pytest.raises(ValueError, match="finite"):
        await clock.advance(float("inf"))


async def test_a_sleeper_wakes_at_its_deadline() -> None:
    clock = VirtualClock()
    woke_at: list[float] = []

    async def sleeper() -> None:
        await clock.sleep(1.0)
        woke_at.append(clock.now())

    _ = clock.spawn(sleeper())
    try:
        await clock.advance(1.0)
        assert woke_at == [1.0]
    finally:
        await clock.aclose()


async def test_a_sleeper_beyond_the_window_stays_asleep() -> None:
    clock = VirtualClock()
    woke = False

    async def sleeper() -> None:
        nonlocal woke
        await clock.sleep(10.0)
        woke = True

    _ = clock.spawn(sleeper())
    try:
        await clock.advance(1.0)
        assert woke is False
    finally:
        await clock.aclose()


async def test_advance_steps_rather_than_jumps() -> None:
    # A worker that wakes at 1.0 and immediately re-sleeps to 2.0 must observe
    # both deadlines, at their true times, inside a single advance(2.0). A clock
    # that jumped straight to 2.0 would report 2.0 for the first wake and schedule
    # the second sleep past the window, losing it.
    clock = VirtualClock()
    seen: list[float] = []

    async def worker() -> None:
        await clock.sleep(1.0)
        seen.append(clock.now())
        await clock.sleep(1.0)
        seen.append(clock.now())

    _ = clock.spawn(worker())
    try:
        await clock.advance(2.0)
        assert seen == [1.0, 2.0]
    finally:
        await clock.aclose()


async def _wake_time_under_deltas(deadline: float, deltas: list[float]) -> list[float]:
    clock = VirtualClock()
    woke_at: list[float] = []

    async def sleeper() -> None:
        await clock.sleep(deadline)
        woke_at.append(clock.now())

    _ = clock.spawn(sleeper())
    try:
        for d in deltas:
            await clock.advance(d)
    finally:
        await clock.aclose()
    return woke_at


async def test_advance_split_equals_advance_whole() -> None:
    # The additivity law that integer nanoseconds buys us: 0.1 + 0.2 != 0.3 in
    # float, yet a sleeper due at 0.3 must fire whether the window is advanced as
    # 0.3, as 0.1 then 0.2, or in tenths.
    for deltas in ([0.3], [0.1, 0.2], [0.15, 0.15], [0.1, 0.1, 0.1]):
        woke_at = await _wake_time_under_deltas(0.3, deltas)
        assert woke_at == [pytest.approx(0.3)], f"deltas={deltas}"


@given(
    chunks=st.lists(
        st.floats(
            min_value=0.001, max_value=2.0, allow_nan=False, allow_infinity=False
        ),
        min_size=1,
        max_size=6,
    ),
    deadline=st.floats(
        min_value=0.001, max_value=3.0, allow_nan=False, allow_infinity=False
    ),
)
@settings(
    max_examples=300,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_additivity_holds_for_any_chopping(
    chunks: list[float], deadline: float
) -> None:
    if sum(chunks) < deadline:
        return  # the window never reaches the deadline; not the case under test
    clock = VirtualClock()
    fired: list[int] = []

    async def sleeper() -> None:
        await clock.sleep(deadline)
        fired.append(clock.now_ns())

    _ = clock.spawn(sleeper())
    try:
        for c in chunks:
            await clock.advance(c)
        assert fired == [round(deadline * 1e9)]
    finally:
        await clock.aclose()


async def test_sleep_zero_is_a_checkpoint() -> None:
    clock = VirtualClock()
    order: list[str] = []

    async def other() -> None:
        order.append("other")

    task = clock.spawn(other())
    order.append("before")
    await clock.sleep(0)  # must yield, letting `other` run
    order.append("after")
    await task
    assert order == ["before", "other", "after"]


async def test_cancelled_sleeper_leaves_no_heap_entry() -> None:
    clock = VirtualClock()

    async def sleeper() -> None:
        await clock.sleep(5.0)

    task = clock.spawn(sleeper())
    await clock.advance(1.0)  # let it park on the heap
    assert len(clock._heap) == 1
    task.cancel()
    await asyncio.sleep(0)  # let the cancellation and the finally run
    assert len(clock._heap) == 0
    await clock.aclose()


async def test_cancelled_waiter_does_not_wedge_advance() -> None:
    clock = VirtualClock()
    grants: list[float] = []

    async def worker(d: float) -> None:
        await clock.sleep(d)
        grants.append(clock.now())

    doomed = clock.spawn(worker(1.0))
    _ = clock.spawn(worker(2.0))
    await clock.advance(0.5)
    doomed.cancel()
    await clock.advance(2.0)
    assert grants == [2.0]
    await clock.aclose()


async def test_a_zeno_schedule_raises_not_hangs() -> None:
    # Deadlines 0.5, 0.75, 0.875, ... converge to 1.0: infinitely many due inside
    # advance(2.0), so the outer step loop must give up rather than walk forever.
    # The cap is lowered so the test trips it in milliseconds instead of at 100k.
    clock = VirtualClock()
    clock.MAX_STEPS = 50

    async def zeno() -> None:
        d = 0.5
        for _ in range(10_000_000):
            await clock.sleep(d)
            d /= 2

    _ = clock.spawn(zeno())
    try:
        with pytest.raises(ClockError, match="did not converge"):
            await clock.advance(2.0)
    finally:
        await clock.aclose()


async def test_a_nonquiescing_coroutine_raises() -> None:
    clock = VirtualClock()
    clock.MAX_YIELDS = 20  # keep the test fast; the guard is what matters

    async def spinner() -> None:
        while True:  # noqa: ASYNC110 - a coroutine that never blocks is exactly what we test
            await asyncio.sleep(0)

    _ = clock.spawn(spinner())
    try:
        with pytest.raises(ClockError, match="quiescence"):
            await clock.advance(1.0)
    finally:
        await clock.aclose()


async def test_aclose_reaps_owned_tasks() -> None:
    clock = VirtualClock()

    async def sleeper() -> None:
        await clock.sleep(100.0)

    task = clock.spawn(sleeper())
    await clock.advance(1.0)
    await clock.aclose()
    assert task.cancelled()


async def test_realclock_now_advances_and_sleep_waits() -> None:
    clock = RealClock()
    t0 = clock.now()
    await clock.sleep(0.01)
    assert clock.now() >= t0
