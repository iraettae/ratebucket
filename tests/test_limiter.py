"""MultiResourceLimiter: atomic grants, FIFO, cancellation, reservations, backfill."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ratebucket import (
    CostExceedsCapacityError,
    MultiResourceLimiter,
    TokenBucket,
    UnknownResourceError,
    VirtualClock,
)

pytestmark = pytest.mark.asyncio


class Rig:
    """A clock, a started limiter, and helpers to launch and track workers."""

    def __init__(
        self,
        buckets: dict[str, TokenBucket],
        clock: VirtualClock,
        *,
        backfill: bool = False,
    ) -> None:
        self.clock = clock
        self.buckets = buckets
        self.limiter = MultiResourceLimiter(buckets, clock, backfill=backfill)
        self.limiter.start(clock.spawn)
        self.granted: dict[str, float] = {}
        self.cancelled: set[str] = set()
        self.tasks: dict[str, asyncio.Task[Any]] = {}

    def acquire(self, name: str, cost: dict[str, int]) -> None:
        async def worker() -> None:
            try:
                self.granted[name] = await self.limiter.acquire(cost)
            except asyncio.CancelledError:
                self.cancelled.add(name)
                raise

        self.tasks[name] = self.clock.spawn(worker(), name=name)

    async def close(self) -> None:
        await self.limiter.aclose()
        await self.clock.aclose()


def bucket(capacity: float, rate: float, clock: VirtualClock) -> TokenBucket:
    return TokenBucket(capacity=capacity, refill_per_sec=rate, clock=clock)


# --- basic behaviour --------------------------------------------------------


async def test_grants_immediately_when_resources_are_available() -> None:
    clock = VirtualClock()
    rig = Rig({"req": bucket(5, 1, clock)}, clock)
    rig.acquire("a", {"req": 2})
    try:
        await clock.advance(0)
        assert "a" in rig.granted
        assert rig.buckets["req"].available() == 3
    finally:
        await rig.close()


async def test_grants_across_two_dimensions_at_once() -> None:
    clock = VirtualClock()
    rig = Rig({"req": bucket(10, 1, clock), "tok": bucket(1000, 100, clock)}, clock)
    rig.acquire("a", {"req": 1, "tok": 400})
    try:
        await clock.advance(0)
        assert "a" in rig.granted
        assert rig.buckets["req"].available() == 9
        assert rig.buckets["tok"].available() == 600
    finally:
        await rig.close()


async def test_second_request_waits_for_refill() -> None:
    clock = VirtualClock()
    rig = Rig({"req": bucket(1, 1, clock)}, clock)  # 1 token, +1/s
    rig.acquire("a", {"req": 1})  # takes the only token at t=0
    rig.acquire("b", {"req": 1})  # must wait ~1s for a refill
    try:
        await clock.advance(0)
        assert "a" in rig.granted
        assert "b" not in rig.granted
        # time_until rounds the wait up by a nanosecond so one sleep always crosses
        # the threshold, so b is granted at 1s + 1ns rather than exactly 1s.
        await clock.advance(1.001)
        assert rig.granted["b"] == pytest.approx(1.0, abs=1e-6)
    finally:
        await rig.close()


async def test_two_resources_do_not_deadlock() -> None:
    # The scenario two independent buckets would deadlock on: every worker needs
    # both resources, and supply is tight. The single arbiter grants both or neither,
    # so the run drains cleanly instead of wedging.
    clock = VirtualClock()
    rig = Rig({"req": bucket(2, 1, clock), "tok": bucket(20, 10, clock)}, clock)
    for i in range(6):
        rig.acquire(f"w{i}", {"req": 1, "tok": 10})
    try:
        await clock.advance(10.0)
        assert set(rig.granted) == {f"w{i}" for i in range(6)}
        # granted in FIFO order (grant times non-decreasing by launch order)
        times = [rig.granted[f"w{i}"] for i in range(6)]
        assert times == sorted(times)
    finally:
        await rig.close()


async def test_unknown_resource_is_rejected() -> None:
    clock = VirtualClock()
    rig = Rig({"req": bucket(5, 1, clock)}, clock)
    try:
        with pytest.raises(UnknownResourceError):
            await rig.limiter.acquire({"nope": 1})
    finally:
        await rig.close()


# --- proof test: cost larger than capacity fails fast -----------------------


async def test_cost_exceeds_capacity_fails_fast() -> None:
    clock = VirtualClock()
    rig = Rig({"tok": bucket(100, 10, clock)}, clock)
    try:
        # Synchronous raise, before anything is enqueued: it never parks behind a
        # refill that can never arrive.
        with pytest.raises(CostExceedsCapacityError):
            rig.limiter.reserve({"tok": 200})
        with pytest.raises(CostExceedsCapacityError):
            await rig.limiter.acquire({"tok": 200})
    finally:
        await rig.close()


# --- proof test: a cancelled waiter does not wedge the queue -----------------


async def test_cancelled_waiter_removed_from_queue() -> None:
    # A large head parks; a small request queues behind it. Cancelling the head must
    # wake the arbiter at once so the small one is granted now, not when the head's
    # (irrelevant) refill would have elapsed.
    clock = VirtualClock()
    rig = Rig({"req": bucket(10, 1, clock)}, clock)
    rig.buckets["req"].take(10)  # start empty
    rig.acquire("head", {"req": 10})  # needs ~10s of refill
    rig.acquire("small", {"req": 1})  # could be granted after ~1s, but is behind head
    try:
        await clock.advance(1.0)  # one token has refilled; head still can't go
        assert "head" not in rig.granted
        assert "small" not in rig.granted

        rig.tasks["head"].cancel()
        await clock.advance(0)  # cancellation wakes the arbiter; small is head now
        assert "head" in rig.cancelled
        assert rig.granted["small"] == pytest.approx(1.0)  # granted at t=1, not t=10
    finally:
        await rig.close()


# --- proof test: a reservation is released on exception ---------------------


async def test_reservation_released_on_exception() -> None:
    clock = VirtualClock()
    tok = bucket(100, 0, clock)  # no refill, so a leak is plainly visible
    rig = Rig({"tok": tok}, clock)

    async def worker() -> None:
        try:
            async with rig.limiter.reserve({"tok": 30}):
                raise RuntimeError("request blew up before commit")
        except RuntimeError:
            pass

    _ = rig.clock.spawn(worker())
    try:
        await clock.advance(0)
        # The 30 reserved on entry were given back on the exceptional exit.
        assert tok.available() == 100
    finally:
        await rig.close()


# --- proof test: the bucket goes negative when actual exceeds reserved -------


async def test_bucket_goes_negative_when_actual_exceeds_reserved() -> None:
    clock = VirtualClock()
    tok = bucket(50, 0, clock)
    rig = Rig({"tok": tok}, clock)

    async def worker() -> None:
        async with rig.limiter.reserve({"tok": 40}) as reservation:
            reservation.commit({"tok": 60})  # estimate guessed low by 20

    _ = rig.clock.spawn(worker())
    try:
        await clock.advance(0)
        # 50 - 40 reserved - 20 overspend = -10; the overspend is recorded, not clamped.
        assert tok.available() == pytest.approx(-10)
    finally:
        await rig.close()


async def test_reservation_gives_back_when_actual_below_reserved() -> None:
    clock = VirtualClock()
    tok = bucket(100, 0, clock)
    rig = Rig({"tok": tok}, clock)

    async def worker() -> None:
        async with rig.limiter.reserve({"tok": 50}) as reservation:
            reservation.commit({"tok": 20})  # used less than reserved

    _ = rig.clock.spawn(worker())
    try:
        await clock.advance(0)
        assert tok.available() == pytest.approx(80)  # 100 - 20 actually used
    finally:
        await rig.close()


# --- conservative backfilling -----------------------------------------------


async def test_backfill_lets_a_small_request_pass_a_blocked_large_one() -> None:
    # The realistic head-of-line case: a token-heavy request is stuck waiting on the
    # token bucket, while a request behind it needs only a (plentiful) request slot.
    # The small one does not touch the dimension the head is blocked on, so letting
    # it pass cannot delay the head.
    clock = VirtualClock()
    rig = Rig(
        {"req": bucket(100, 100, clock), "tok": bucket(10, 1, clock)},
        clock,
        backfill=True,
    )
    rig.buckets["tok"].take(10)  # tok empty; big waits ~10s on it
    rig.acquire("big", {"req": 1, "tok": 10})  # head, blocked on tok
    rig.acquire("small", {"req": 1})  # needs no tokens; safe to pass
    try:
        await clock.advance(0)
        assert "small" in rig.granted  # backfilled ahead of big
        assert "big" not in rig.granted
    finally:
        await rig.close()


async def test_without_backfill_the_small_request_waits_its_turn() -> None:
    clock = VirtualClock()
    rig = Rig(
        {"req": bucket(100, 100, clock), "tok": bucket(10, 1, clock)},
        clock,
        backfill=False,
    )
    rig.buckets["tok"].take(10)
    rig.acquire("big", {"req": 1, "tok": 10})
    rig.acquire("small", {"req": 1})
    try:
        await clock.advance(0)
        assert "small" not in rig.granted  # strict FIFO: waits behind big
        assert "big" not in rig.granted
    finally:
        await rig.close()


async def test_backfill_refuses_when_it_would_delay_the_head() -> None:
    # Same dimension this time: the head needs the bucket to reach 10, and a request
    # behind it that would take tokens the head is counting on must NOT be backfilled,
    # because doing so pushes the head's start later.
    clock = VirtualClock()
    rig = Rig({"tok": bucket(10, 1, clock)}, clock, backfill=True)
    rig.buckets["tok"].take(10)
    await clock.advance(3.0)  # 3 available; head needs 10, i.e. ~7s more
    rig.acquire("big", {"tok": 10})
    rig.acquire("small", {"tok": 3})  # grantable now, but eats into big's refill
    try:
        await clock.advance(0)
        assert "small" not in rig.granted  # refused: it would push big later
        assert "big" not in rig.granted
    finally:
        await rig.close()


# --- regressions found by adversarial review of the arbiter --------------------


async def test_negative_cost_is_rejected() -> None:
    clock = VirtualClock()
    rig = Rig({"tok": bucket(100, 0, clock)}, clock)
    try:
        with pytest.raises(ValueError, match="non-negative"):
            await rig.limiter.acquire({"tok": -5})
        with pytest.raises(ValueError, match="non-negative"):
            rig.limiter.reserve({"tok": -1})
    finally:
        await rig.close()


async def test_zero_cost_dimension_never_blocks_a_negative_bucket() -> None:
    # A request that needs nothing from a dimension must not be gated by it, even
    # when that bucket has been driven negative by an overspend.
    clock = VirtualClock()
    req = bucket(10, 1, clock)
    tok = bucket(50, 0, clock)
    rig = Rig({"req": req, "tok": tok}, clock)

    async def overspender() -> None:
        async with rig.limiter.reserve({"tok": 40}) as reservation:
            reservation.commit({"tok": 60})

    _ = rig.clock.spawn(overspender())
    await clock.advance(0)
    assert tok.available() < 0

    rig.acquire("z", {"req": 1, "tok": 0})
    try:
        await clock.advance(0)
        assert "z" in rig.granted  # granted despite tok being negative
    finally:
        await rig.close()


async def test_aclose_unblocks_a_pending_waiter() -> None:
    # Closing the limiter while a caller is parked must terminate that caller, not
    # leave it awaiting a future the dead arbiter will never resolve.
    clock = VirtualClock()
    rig = Rig({"req": bucket(1, 0, clock)}, clock)
    rig.buckets["req"].take(1)  # empty, no refill: nothing is grantable
    rig.acquire("stuck", {"req": 1})
    await clock.advance(0)
    assert "stuck" not in rig.granted

    await rig.limiter.aclose()
    await clock.advance(0)
    assert "stuck" in rig.cancelled
    await clock.aclose()


async def test_reservation_released_when_cancelled_in_the_grant_window() -> None:
    # The subtle leak: the arbiter grants (takes the tokens, resolves the waiter's
    # future) and a cancellation reaches the waiter before it resumes, so __aenter__
    # raises and __aexit__ never runs. The tokens must still be given back.
    #
    # Reproduced deterministically by parking a canceller behind the arbiter on the
    # limiter's own wakeup event, so on set() the order is: arbiter grants, canceller
    # cancels, worker throws inside __aenter__. This is a white-box proof of a
    # specific interleaving, hence the reach into a private attribute.
    clock = VirtualClock()
    tok = bucket(100, 0, clock)
    tok.take(100)  # start empty; the grant blocks until we refill
    limiter = MultiResourceLimiter({"tok": tok}, clock)
    limiter.start(clock.spawn)
    outcome: dict[str, str] = {}

    async def worker() -> None:
        try:
            async with limiter.reserve({"tok": 30}) as reservation:
                reservation.commit({"tok": 30})
            outcome["w"] = "completed"
        except asyncio.CancelledError:
            outcome["w"] = "cancelled"
            raise

    wtask = clock.spawn(worker())
    try:
        await clock.advance(0)  # worker parks on the grant; arbiter parks (empty)

        async def canceller() -> None:
            await limiter._wakeup.wait()  # queued behind the arbiter on the same event
            wtask.cancel()

        _ = clock.spawn(canceller())
        await clock.advance(0)

        tok.give_back(30)  # make the grant possible
        limiter._wakeup.set()  # arbiter grants -> canceller cancels -> worker throws
        await clock.advance(0)

        assert outcome["w"] == "cancelled"
        assert tok.available() == 30  # released; a leak would leave it at 0
    finally:
        await limiter.aclose()
        await clock.aclose()


async def test_backfill_candidate_granted_after_it_refills() -> None:
    # With backfill on, a candidate that becomes grantable by refill while the head is
    # still blocked must be granted then, not left until the head's own wait -- which
    # here is infinite, so "left until" would mean forever.
    clock = VirtualClock()
    scarce = bucket(10, 0, clock)  # rate 0: the head never becomes grantable
    other = bucket(10, 1, clock)  # refills 1/s
    scarce.take(10)
    other.take(10)  # both empty
    rig = Rig({"scarce": scarce, "other": other}, clock, backfill=True)
    rig.acquire("head", {"scarce": 5})  # blocked forever
    rig.acquire("cand", {"other": 3})  # grantable after ~3s
    try:
        await clock.advance(0)
        assert "cand" not in rig.granted
        await clock.advance(3.01)
        assert "cand" in rig.granted  # backfilled once its own bucket refilled
        assert "head" not in rig.granted
    finally:
        await rig.close()


@given(
    capacity=st.integers(min_value=1, max_value=100),
    rate=st.floats(
        min_value=0.1, max_value=50.0, allow_nan=False, allow_infinity=False
    ),
    drain=st.integers(min_value=0, max_value=100),
    head=st.integers(min_value=1, max_value=100),
    cand=st.integers(min_value=1, max_value=100),
)
@settings(
    max_examples=250,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_backfill_never_delays_the_head(
    capacity: int, rate: float, drain: int, head: int, cand: int
) -> None:
    # Enabling backfill must never move the head's grant later than the identical
    # run without it. The linear reachability check used to let a candidate through
    # that delayed the head by a nanosecond or two; the exact check must not.
    if head > capacity or cand > capacity:
        return  # oversized costs are rejected, not queued

    async def head_grant_ns(*, backfill: bool) -> int | None:
        clock = VirtualClock()
        b = TokenBucket(capacity=capacity, refill_per_sec=rate, clock=clock)
        b.take(min(drain, capacity))
        limiter = MultiResourceLimiter({"x": b}, clock, backfill=backfill)
        limiter.start(clock.spawn)
        result: dict[str, float] = {}

        async def head_worker() -> None:
            result["head"] = await limiter.acquire({"x": head})

        async def cand_worker() -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await limiter.acquire({"x": cand})

        _ = clock.spawn(head_worker())
        _ = clock.spawn(cand_worker())
        try:
            span = (head / rate + 10.0) if rate > 0 else 10.0
            for _ in range(4):
                await clock.advance(span)
                if "head" in result:
                    break
        finally:
            await limiter.aclose()
            await clock.aclose()
        return round(result["head"] * 1e9) if "head" in result else None

    off = await head_grant_ns(backfill=False)
    on = await head_grant_ns(backfill=True)
    if off is None or on is None:
        return
    assert on <= off, f"backfill delayed the head: {on} ns > {off} ns"


async def test_backfill_does_not_delay_the_head_worked_example() -> None:
    # The exact case a property run turned up (cap=95, rate=1.9, head needs the whole
    # bucket, a unit candidate behind it). Nothing is actually backfilled -- the head
    # needs full capacity -- but without the monotonic head deadline the extra wake
    # backfill mode schedules re-applied the conservative +1ns rounding and pushed the
    # head one nanosecond late. Deterministic, so it locks the fix where the property
    # test only samples it.
    async def head_ns(*, backfill: bool) -> int:
        clock = VirtualClock()
        b = TokenBucket(capacity=95, refill_per_sec=1.9, clock=clock)
        b.take(95)  # empty
        limiter = MultiResourceLimiter({"x": b}, clock, backfill=backfill)
        limiter.start(clock.spawn)
        got: dict[str, float] = {}

        async def head() -> None:
            got["t"] = await limiter.acquire({"x": 95})

        async def cand() -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await limiter.acquire({"x": 1})

        _ = clock.spawn(head())
        _ = clock.spawn(cand())
        try:
            await clock.advance(60.0)
        finally:
            await limiter.aclose()
            await clock.aclose()
        return round(got["t"] * 1e9)

    assert (await head_ns(backfill=True)) == (await head_ns(backfill=False))
