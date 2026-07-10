"""Walk the two headline invariants: the real mechanism, and the naive one it replaces.

`make prove` runs this. It does not edit the library -- WORKLOG.md has the in-place
demonstrations where the actual fix is removed and the real proof-test goes red. This is
the live side-by-side: the same scenario against ratebucket's mechanism and against the
naive version, so you can see exactly what the fix buys.

Exit code is non-zero if the real mechanism ever fails to hold the invariant.
"""

from __future__ import annotations

import asyncio
import sys

from ratebucket import Gate, TokenBucket, VirtualClock


class _DiscreteBucket:
    """The refill ratebucket rejects: top up to capacity in one dump each period, not
    continuously. This is what a once-a-minute timer does, and it bursts at the boundary.
    """

    def __init__(self, capacity: float, rate: float, clock: VirtualClock) -> None:
        self._capacity = capacity
        self._period = capacity / rate  # seconds between dumps (one capacity's worth)
        self._clock = clock
        self._level = capacity
        self._last_dump = clock.now()

    def try_take(self, n: float) -> bool:
        while self._clock.now() - self._last_dump >= self._period:
            self._last_dump += self._period
            self._level = min(self._capacity, self._level + self._capacity)
        if self._level >= n:
            self._level -= n
            return True
        return False


async def _permits_across_the_boundary(bucket: object, clock: VirtualClock) -> int:
    # Drain at 0.99s (bucket full), then again at 1.01s -- a 0.02s window straddling the
    # one-second refill boundary. A continuous bucket accrues only rate*0.02 in between;
    # a discrete one dumps a whole capacity at 1.0s.
    await clock.advance(0.99)
    granted = 0
    while bucket.try_take(1.0):  # type: ignore[attr-defined]
        granted += 1
    await clock.advance(0.02)
    while bucket.try_take(1.0):  # type: ignore[attr-defined]
        granted += 1
    return granted


async def prove_bucket_bound() -> bool:
    capacity, rate, window = 10.0, 10.0, 0.02
    bound = capacity + rate * window  # 10.2: the invariant's ceiling for this window

    clock = VirtualClock()
    real = await _permits_across_the_boundary(TokenBucket(capacity, rate, clock), clock)
    await clock.aclose()

    clock = VirtualClock()
    naive = await _permits_across_the_boundary(
        _DiscreteBucket(capacity, rate, clock), clock
    )
    await clock.aclose()

    print("bucket invariant  permits(W) <= capacity + rate*W")
    print(f"  window 0.02s straddling a refill boundary; ceiling = {bound:g}")
    print(f"  ratebucket (continuous refill): {real} permits  <= {bound:g}   HOLDS")
    print(
        f"  naive (discrete once-per-period): {naive} permits  > {bound:g}   VIOLATED"
    )
    return real <= bound and naive > bound


class _NaiveGate:
    """A gate keyed only on the last 429's reopen time, no epoch. Every 429 closes it
    again and pushes the deadline -- the openai-cookbook behaviour the benchmark shows.
    """

    def __init__(self, clock: VirtualClock) -> None:
        self._clock = clock
        self.open_at = clock.now()
        self.closes = 0

    def throttle(self, retry_after: float, _seen_epoch: int) -> bool:
        self.open_at = self._clock.now() + retry_after
        self.closes += 1
        return True


def prove_gate_single_pause() -> bool:
    clock = VirtualClock()

    real = Gate(clock)
    snapshot = (
        real.epoch
    )  # every worker read the epoch before its request was throttled
    real_closes = sum(real.throttle(20.0, snapshot) for _ in range(50))

    naive = _NaiveGate(clock)
    for _ in range(50):
        naive.throttle(20.0, 0)

    print("gate reaction     fifty concurrent 429s")
    print(
        f"  ratebucket (epoch compare-and-swap): {real_closes} pause, epoch={real.epoch}   HOLDS"
    )
    print(
        f"  naive (last-429 timestamp): {naive.closes} pauses, deadline dragged to t={naive.open_at:g}   VIOLATED"
    )
    return real_closes == 1 and naive.closes == 50


async def main() -> int:
    ok = True
    ok &= await prove_bucket_bound()
    print()
    ok &= prove_gate_single_pause()
    print()
    if ok:
        print("Both invariants hold for ratebucket and fail for the naive version.")
        return 0
    print("A real invariant did not hold; this is a bug.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
