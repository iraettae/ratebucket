"""The token bucket's permit bound, as a property over random schedules.

The server enforces a rate limit of RPM requests per minute. The intuitive bucket
is capacity = RPM refilling at RPM/60 per second, and the intuitive claim is that it
never grants more than RPM permits in any rolling minute. This asserts that claim.
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
        # The bucket must not hand out more than its capacity in any window.
        assert got <= capacity, (
            f"granted {got} in a {window}s window, capacity {capacity} "
            f"(rate={rate}, workers={n_workers})"
        )
    finally:
        await clock.aclose()
