# 1. An injected clock, and virtual time that steps rather than jumps

Date: 2026-07-08
Status: Accepted

## Context

A rate limiter is a state machine whose transitions are driven by the passage of
time. If it reads the wall clock directly, its behaviour can only be tested by
actually waiting, and waiting is both slow and non-deterministic: a test that
sleeps for a second to observe a refill is a test that occasionally fails on a
loaded CI runner and never runs a hundred random schedules in a property test.

Every component therefore takes a `Clock` rather than calling `time.monotonic`.
`RealClock` is the production implementation. `VirtualClock` is a test double the
test drives by hand. The question this record settles is how `VirtualClock`
represents time and what its `advance` operation guarantees, because a subtly
wrong answer makes the whole "deterministic testing" claim false.

## The three things that had to be decided

### 1. The clock is a plain injected object, never installed as the loop's clock

There is a tempting shortcut: patch `loop.time` so that `asyncio.sleep` itself
believes in virtual time. It was rejected. Later stages run a real server on a
real socket in the same event loop, and that loop computes its select() timeouts
from `loop.time()`. Freeze or fast-forward `loop.time` and either the socket
never gets its I/O (the loop thinks no timer is due) or a virtual timeout fires
against a real connection. Virtual time ends exactly where a real socket begins.
The clock is an argument that the limiter reads; it is not a monkeypatch.

A consequence: `advance()` cannot detect quiescence by asking asyncio whether the
loop is idle, because asyncio has no such predicate (unlike trio, whose scheduler
owns the set of runnable tasks and exposes its size). It reads `Task._fut_waiter`
on the tasks it owns instead: `None` or a done future means runnable. That is a
private attribute, so it is read through a guard that raises loudly the day
CPython renames it, rather than silently under-draining.

### 2. Advance steps to each deadline; it does not jump to the end

`advance(dt)` could set `now` straight to `start + dt` and wake everything due.
That is wrong for any state machine with a saturating accumulator, which is
exactly what a token bucket is.

Concretely: a bucket with capacity 1 and rate 1/s, starting empty, and two workers
each wanting one permit. Under stepped time, `advance(1); advance(1)` wakes the
first worker at t=1 (it drains the one token that accrued) and the second at t=2
(one more token accrued). Two permits. Under jump time, a single `advance(2)`
computes the level once at t=2 as `min(capacity=1, rate*2) = 1` — the second
token accrued and was clamped away, because no worker ran at t=1 to take it. One
permit. The result of a schedule would then depend on how it was chopped into
deltas, and a hypothesis property test over random schedules would be meaningless.

So `advance` walks the due deadlines in order, moves `now` to each, wakes exactly
the sleepers there, and drains the loop to quiescence before moving on. There are
two runaway guards, because neither loop has a natural bound: a Zeno schedule
(0.5, 0.25, 0.125, ...) packs infinitely many deadlines into a finite window, and
a coroutine that never blocks never quiesces. Both raise `ClockError`; neither
returns silently.

### 3. Time is integer nanoseconds, so there is no tolerance constant

The obvious `VirtualClock` keeps `now` as a float and computes deadlines as
`now + seconds`. It has a bug that a tolerance constant only hides. A sleeper set
for 0.2s at now=0.1 has deadline `0.1 + 0.2 = 0.30000000000000004`. Then
`advance(0.1); advance(0.2)` reaches a target of `0.30000000000000004` and fires
it, but `advance(0.3)` reaches `0.3` and does not — the additivity that section 2
depends on is broken by one unit in the last place. A `1e-12` slack papers over
it until `now` grows past ~1e4 seconds, where one ulp exceeds the slack again.
That is a tuned constant hiding a representation error, and the project forbids
exactly that.

The fix is to stop representing time as a float. `VirtualClock` counts integer
nanoseconds. Deadlines are integers, `advance` compares integers, and integer
addition is associative, so `advance(a); advance(b)` lands in precisely the same
place as `advance(a + b)`. There is no tolerance constant anywhere in `clock.py`.

The bucket sees the `Clock` protocol, which exposes `now() -> float` (seconds),
so it recovers nanoseconds by `round(now * 1e9)` before doing arithmetic. That
round-trip is exact for any `now` below 2**53 nanoseconds — about 104 days of
continuous virtual time — which was verified empirically across two million
samples. The result is that the bucket's invariant, `permits(W) <= capacity +
rate*W`, holds with a bare `<=` and no epsilon on the assertion. The one rounding
choice that remains, in `time_until`, rounds the wait *up* to the next nanosecond
plus one: rounding up wakes a caller later, which can only grant fewer permits,
so it cannot violate the upper bound, and it guarantees a single sleep crosses
the threshold rather than landing a rounding-error short and spinning.

## Rejected alternatives

- **Patch `loop.time` / freezegun / time-machine.** They patch the wall clock or
  the loop's clock. Both break a real socket sharing the loop. See point 1.
- **Jump advance.** Non-additive for a saturating accumulator. See point 2.
- **Float time with a tolerance constant.** The constant is tuned and stops
  working at large magnitudes. See point 3.
- **`fractions.Fraction` tokens for exactness.** Would make the bucket exact
  everywhere, but it is slow and it was not needed: integer-nanosecond time plus
  upward-rounded `time_until` already makes the invariant hold exactly. Kept in
  reserve if a future dimension needs sub-nanosecond rates.

## Consequences

- The clock's guarantees hold under `VirtualClock`. Under `RealClock`, `round(now
  * 1e9)` loses precision once the machine's uptime is large, and refill is
  approximate. This is honest: the property test certifies the virtual clock, and
  a real rate limiter tolerates sub-microsecond refill error.
- `advance` runs woken coroutines before it returns, so a test's next line reads
  settled state. The arbiter (stage 2) relies on this to observe an enqueue
  before it sleeps.
- The proof is in the tests, not this document: making `advance` jump turns
  `test_advance_steps_rather_than_jumps` red, and clamping the bucket at zero
  turns the negative-balance tests red. Both were demonstrated.
