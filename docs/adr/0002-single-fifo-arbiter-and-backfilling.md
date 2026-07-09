# 2. A single FIFO arbiter, and conservative backfilling for its head-of-line cost

Date: 2026-07-09
Status: Accepted

## Context

The limiter has to satisfy several rate limits at once. A request against an
LLM API costs one request slot and some number of tokens, and both the
requests-per-minute and the tokens-per-minute limit have to hold. A permit must
be granted against every dimension or none: a half-permit that consumed a request
slot but not the tokens is not a smaller permit, it is a corrupt one.

The obvious structures fail:

- **Two independent buckets, acquired in sequence.** `await rpm.acquire(1)` then
  `await tpm.acquire(1340)` deadlocks. One coroutine takes a request slot and waits
  for tokens; another takes tokens and waits for a request slot; neither can finish.
- **Two independent buckets, acquired in a fixed order** (always rpm before tpm).
  No deadlock, but a large request that needs many tokens holds its request slot
  while it waits, and small requests behind it in the token queue never get their
  slot back. Large requests starve.

## Decision

One arbiter coroutine owns a FIFO queue of `(cost, future)`. Callers enqueue and
await; they never touch the buckets. The arbiter looks only at the head of the
queue and, in a single synchronous step with no `await` in the middle, either
finds every bucket has enough and takes from all of them, or takes from none and
sleeps until the scarcest bucket can cover the head.

Because the grant is all-or-nothing and computed in one uninterrupted step, there
is no reachable state where one resource is held while another is awaited. The
deadlock is gone by construction, not by lock ordering.

The price is **head-of-line blocking**: a request for 8000 tokens at the head makes
a request for 200 tokens behind it wait, even when the 200 could be served now.
That is not an inherent cost to be shrugged at, it is a known problem with a known
answer from HPC job schedulers: **conservative backfilling**. A request further
back in the queue may be granted ahead of the head, but only if granting it does
not push the head's computed start any later. Concretely, for a candidate to
backfill, every bucket must, after the candidate takes its tokens now, still refill
to what the head needs within the head's existing wait. A request that needs no
tokens at all (a request-only call behind a token-heavy head) always satisfies this
on the token dimension, which is the common case the optimisation targets.

Backfilling is behind a flag (`backfill=`, default off) rather than always on,
because it trades a simple queue invariant (strict FIFO, trivial to reason about)
for throughput on mixed workloads, and that trade is the caller's to make.

## Measured

`make bench-backfill`, a synthetic mixed workload on the VirtualClock (12
token-heavy requests interleaved with 72 request-only ones, 84 total, 30s horizon):

| | completed in 30s | last heavy request done |
|---|---|---|
| strict FIFO | 28 / 84 | 22.0s |
| backfill | 76 / 84 | 22.0s |

Backfilling clears **+171%** more requests inside the window, and the heavy
requests finish at the **same** time either way. That the heavy tail is unmoved is
the point: conservative backfilling fills the idle gaps with light work and never
delays the head. The number describes this synthetic workload, not any real API.

## Rejected alternatives

- **Independent buckets** (sequential or ordered acquisition): deadlock or
  starvation, above.
- **Greedy backfilling** (serve any grantable request out of order): improves
  throughput more, but delays the head, which reintroduces the starvation the FIFO
  arbiter exists to prevent. The "conservative" qualifier is the whole point.
- **Priority lanes / an anti-starvation coefficient K.** A tunable knob with no
  principled invariant, tuned until the workload of the day looks fair. Conservative
  backfilling has a crisp invariant instead ("never delay the head"), which is why
  it can be tested rather than tuned.

## Consequences

- A cancelled waiter must be pulled out of the queue and the arbiter re-woken, or a
  cancelled head leaves the arbiter computing a refill for a caller that will never
  take it. This is a callback on the waiter's future; `test_cancelled_waiter_removed_from_queue`
  goes red without it.
- A cost larger than a bucket's capacity can never be granted; it is rejected at
  enqueue time rather than parking forever behind an impossible refill.
- The arbiter and its wait-timer are created through the injected clock's `spawn`,
  so the VirtualClock owns them and `advance()` drains the whole grant chain to
  quiescence. Under a real clock they are ordinary tasks.
