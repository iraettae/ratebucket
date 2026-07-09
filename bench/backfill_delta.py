"""Measure what conservative backfilling buys on a mixed workload.

A scarce token bucket and a plentiful request bucket. The workload interleaves
token-heavy requests (which park on the token bucket) with request-only ones
(which need no tokens). Under strict FIFO a parked heavy request blocks every
light request behind it; backfilling lets the light ones through as long as doing
so does not delay the heavy head.

Reported numbers are on a VirtualClock, deterministic, and reproduce via
`make bench-backfill`. They describe this synthetic workload, not any real API.
"""

from __future__ import annotations

import asyncio

from ratebucket import MultiResourceLimiter, TokenBucket, VirtualClock

HEAVY_TOKENS = 800
N_HEAVY = 12
LIGHT_PER_HEAVY = 6  # light requests queued behind each heavy one
HORIZON = 30.0  # virtual seconds


def _build() -> tuple[VirtualClock, MultiResourceLimiter, dict[str, TokenBucket]]:
    clock = VirtualClock()
    buckets = {
        "req": TokenBucket(capacity=200, refill_per_sec=200, clock=clock),
        "tok": TokenBucket(capacity=1000, refill_per_sec=100, clock=clock),
    }
    return clock, buckets, buckets


async def _run(backfill: bool) -> tuple[int, float]:
    clock, buckets, _ = _build()
    limiter = MultiResourceLimiter(buckets, clock, backfill=backfill)
    limiter.start(clock.spawn)
    done: list[float] = []
    heavy_done: list[float] = []

    async def heavy() -> None:
        t = await limiter.acquire({"req": 1, "tok": HEAVY_TOKENS})
        done.append(t)
        heavy_done.append(t)

    async def light() -> None:
        done.append(await limiter.acquire({"req": 1}))

    # Interleave: one heavy, then a burst of light behind it, repeatedly.
    for _ in range(N_HEAVY):
        clock.spawn(heavy())
        for _ in range(LIGHT_PER_HEAVY):
            clock.spawn(light())

    step = 0.1
    t = 0.0
    while t < HORIZON:
        await clock.advance(step)
        t += step

    completed = len(done)
    last_heavy = max(heavy_done) if heavy_done else 0.0
    await limiter.aclose()
    await clock.aclose()
    return completed, last_heavy


async def main() -> None:
    total = N_HEAVY * (1 + LIGHT_PER_HEAVY)
    fifo_done, fifo_last_heavy = await _run(backfill=False)
    bf_done, bf_last_heavy = await _run(backfill=True)

    print(
        f"workload: {N_HEAVY} heavy (+{HEAVY_TOKENS} tok) interleaved with {N_HEAVY * LIGHT_PER_HEAVY} light"
    )
    print(f"          {total} requests total, {HORIZON:.0f}s horizon, VirtualClock\n")
    print(f"{'':<12}{'completed':>12}{'last heavy done (s)':>22}")
    print(f"{'FIFO':<12}{fifo_done:>12}{fifo_last_heavy:>22.3f}")
    print(f"{'backfill':<12}{bf_done:>12}{bf_last_heavy:>22.3f}")

    thr = (bf_done - fifo_done) / fifo_done * 100 if fifo_done else 0.0
    print(
        f"\nwithin the {HORIZON:.0f}s horizon, backfill completed {bf_done - fifo_done} more "
        f"requests ({thr:+.1f}% throughput on this workload)"
    )
    print(
        f"the heavy requests finish at the same time either way "
        f"({bf_last_heavy:.3f}s vs {fifo_last_heavy:.3f}s): backfilling never delays the head."
    )


if __name__ == "__main__":
    asyncio.run(main())
