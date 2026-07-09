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
    capacity=st.integers(min_value=1, max_value=60),
    rate=st.floats(
        min_value=0.05, max_value=50.0, allow_nan=False, allow_infinity=False
    ),
    n_workers=st.integers(min_value=0, max_value=40),
    deltas=st.lists(
        st.floats(min_value=0.0, max_value=5.0, allow_nan=False, allow_infinity=False),
        max_size=12,
    ),
)
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_permits_bounded_by_capacity_plus_rate_times_window(
    capacity: int, rate: float, n_workers: int, deltas: list[float]
) -> None:
    clock = VirtualClock()
    bucket = TokenBucket(capacity=float(capacity), refill_per_sec=rate, clock=clock)
    grants_ns: list[int] = []

    async def worker() -> None:
        while not bucket.try_take(1.0):
            await clock.sleep(bucket.time_until(1.0))
        grants_ns.append(clock.now_ns())

    for _ in range(n_workers):
        _ = clock.spawn(worker())
    try:
        for d in deltas:
            await clock.advance(d)
        window = 1.0
        got = _max_in_window(grants_ns, round(window * 1e9))
        bound = capacity + rate * window
        assert got <= bound, (
            f"granted {got} in a {window}s window, bound {bound} "
            f"(capacity={capacity}, rate={rate}, workers={n_workers})"
        )
    finally:
        await clock.aclose()
