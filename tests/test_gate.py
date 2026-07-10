"""Gate: a burst of 429s raises the pool-wide pause once, not once per 429."""

from __future__ import annotations

import pytest

from ratebucket import Gate, VirtualClock

pytestmark = pytest.mark.asyncio


async def test_gate_is_open_by_default() -> None:
    clock = VirtualClock()
    gate = Gate(clock)
    epoch = await gate.pass_()  # returns at once
    assert epoch == 0
    assert clock.now() == 0.0


async def test_throttle_closes_the_gate_until_retry_after() -> None:
    clock = VirtualClock()
    gate = Gate(clock)
    epoch = await gate.pass_()
    assert gate.throttle(20.0, epoch) is True
    assert gate.epoch == 1
    resumed: list[float] = []

    async def waiter() -> None:
        await gate.pass_()
        resumed.append(clock.now())

    _ = clock.spawn(waiter())
    await clock.advance(19.0)
    assert resumed == []
    await clock.advance(1.5)
    assert resumed == [pytest.approx(20.0)]
    await clock.aclose()


async def test_a_stale_throttle_is_ignored() -> None:
    clock = VirtualClock()
    gate = Gate(clock)
    assert gate.throttle(20.0, seen_epoch=0) is True  # first: takes effect
    assert gate.throttle(20.0, seen_epoch=0) is False  # stale epoch: ignored
    assert gate.epoch == 1


async def test_fifty_concurrent_429_produce_single_pause() -> None:
    # Fifty workers pass the open gate, then each gets a 429 (Retry-After 20) sometime
    # in the next three seconds. Only the first closes the gate; the rest carry the now
    # stale epoch and are ignored. So there is one pause, of 20 seconds from the first
    # 429, and all fifty resume together -- not fifty pauses, and not 20 + the spread.
    clock = VirtualClock()
    gate = Gate(clock)
    resumed: list[float] = []

    async def worker(arrival: float) -> None:
        epoch = await gate.pass_()  # everyone passes at epoch 0
        await clock.sleep(arrival)  # the 429 arrives `arrival` seconds later
        gate.throttle(20.0, epoch)
        await gate.pass_()  # wait for the gate to reopen
        resumed.append(clock.now())

    for i in range(50):
        _ = clock.spawn(worker(i * 3.0 / 50))  # 429s spread across [0, 3)
    try:
        await clock.advance(60.0)
        assert gate.epoch == 1  # exactly one throttle took effect
        assert len(resumed) == 50
        assert all(r == pytest.approx(20.0) for r in resumed)  # one 20s pause, together
    finally:
        await clock.aclose()
