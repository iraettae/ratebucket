"""The token bucket's permit bound, as a property over random schedules.

A bucket of capacity C refilling at rate R does not hand out at most C permits in a
window; it hands out at most C + R*W. A full bucket fires C at the start of the
window and refills R*W more before it ends, so a window that straddles the moment a
full bucket starts draining sees close to both. With C = RPM and R = RPM/60 that is
nearly 2*RPM in a rolling minute -- which is why the bucket's capacity has to be set
below RPM, not equal to it (see ADR 0003).

The invariant here is the real one: permits(W) <= capacity + rate*W, asserted with a
bare `<=` and no tolerance. It holds because refill is continuous; make it discrete
and the burst at the window boundary breaks it.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ratebucket import TokenBucket, VirtualClock

pytestmark = pytest.mark.asyncio


def _max_in_window(grants_ns: list[int], window_ns: int) -> int:
    """The most grants landing in any window of the given width."""
    if not grants_ns:
        return 0
    g = sorted(grants_ns)
    best = 0
    j = 0
    for i in range(len(g)):
        while g[i] - g[j] >= window_ns:
            j += 1
        best = max(best, i - j + 1)
    return best


@given(
    capacity=st.integers(min_value=1, max_value=20),
    rate=st.floats(
        min_value=0.05, max_value=20.0, allow_nan=False, allow_infinity=False
    ),
    # A schedule of interleaved events: a float advances time, None spawns a worker.
    # Workers must arrive across the run, not all at the start, or the bucket can only
    # be drained once and the boundary burst that continuous refill rules out never
    # gets a chance to appear.
    events=st.lists(
        st.one_of(
            st.floats(
                min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False
            ),
            st.none(),
        ),
        min_size=1,
        max_size=40,
    ),
)
@settings(
    max_examples=300,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_permits_bounded_by_capacity_plus_rate_times_window(
    capacity: int, rate: float, events: list[float | None]
) -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=float(capacity), refill_per_sec=rate, clock=clock)
    grants_ns: list[int] = []

    async def worker() -> None:
        while not bucket.try_take(1.0):
            await clock.sleep(bucket.time_until(1.0))
        grants_ns.append(clock.now_ns())

    try:
        for event in events:
            if event is None:
                _ = clock.spawn(worker())
            else:
                await clock.advance(event)
        for window in (0.5, 1.0, 2.0):
            got = _max_in_window(grants_ns, round(window * 1e9))
            bound = capacity + rate * window
            assert got <= bound, (
                f"granted {got} in a {window}s window, bound {bound} "
                f"(capacity={capacity}, rate={rate})"
            )
    finally:
        await clock.aclose()


async def test_continuous_refill_bounds_the_boundary_burst() -> None:
    # The deterministic proof that continuous refill is load-bearing. A full bucket
    # is drained the instant before a would-be refill boundary, and a second wave of
    # callers arrives the instant after. With continuous refill the second wave gets
    # almost nothing (only rate*dt has accrued), so the window stays within
    # capacity + rate*W. Replace the refill with a discrete once-a-minute dump and the
    # second wave collects a whole fresh capacity, and the same window holds ~2*capacity
    # -- the burst the bound forbids.
    capacity, rate = 10, 1.0
    clock = VirtualClock()
    bucket = TokenBucket(capacity=float(capacity), refill_per_sec=rate, clock=clock)
    grants_ns: list[int] = []

    async def worker() -> None:
        while not bucket.try_take(1.0):
            await clock.sleep(bucket.time_until(1.0))
        grants_ns.append(clock.now_ns())

    try:
        await clock.advance(59.9)  # bucket sits full, untouched
        for _ in range(capacity):  # drain the full bucket just before t=60
            _ = clock.spawn(worker())
        await clock.advance(0.1)  # to t=60.0, the boundary
        for _ in range(capacity):  # a second wave arrives right after
            _ = clock.spawn(worker())
        await clock.advance(0.1)
        window = 0.5
        got = _max_in_window(grants_ns, round(window * 1e9))
        bound = capacity + rate * window
        assert got <= bound, f"granted {got} in {window}s, bound {bound}"
    finally:
        await clock.aclose()
