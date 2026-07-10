# 4. A hand-written retry policy over a shared gate, not tenacity

Date: 2026-07-09
Status: Accepted

## Context

When a provider answers 429, the right reaction is not this one worker sleeping and
trying again. It is the whole pool easing off, once. A retry library like tenacity
retries inside a single coroutine: it can back that coroutine off with jitter, but it
has no way to tell the other workers that the provider asked everyone to slow down. So
while one coroutine waits out its 429, the other in-flight requests keep hammering the
provider and collecting 429s of their own. The pool-wide reaction has to live outside
the per-worker retry loop.

## Decision

Two pieces, deliberately separate.

**The `Gate` holds the pool-wide pause.** It is a single epoch counter and a reopen
time. A worker reads the epoch before it sends a request and presents that epoch back
if the request is throttled. `throttle` acts only if the epoch has not moved since --
a compare-and-swap on one integer, which is trivial to reason about on a single event
loop and easy to explain at a whiteboard. The first 429 of a burst advances the epoch
and sets the reopen time; every later 429 carrying the now-stale epoch is a no-op. A
burst of fifty 429s therefore produces one pause, at the Retry-After of whichever
landed first, and `test_fifty_concurrent_429_produce_single_pause` goes red (the epoch
climbs to fifty) if the stale-epoch check is removed.

The same shape recurs in stage 6, applying `x-ratelimit-remaining` headers: a header
that arrived on a response to a request sent three seconds ago is a stale observation
and must not overwrite fresher state. Epochs guard both, because it is the same
problem -- acting on an out-of-date view in an asynchronous world.

**`RetryPolicy` drives one worker's attempts** and defers throttling to the gate. It
classifies each failure and acts: a 429 closes the gate and does not consume a retry
(it is backpressure, not a failed attempt); a 5xx or dropped connection sleeps a
jittered backoff; a permanent or fatal failure raises. The backoff is **full jitter**,
`uniform(0, min(cap, base * 2**n))`, not equal jitter (`cap/2 + uniform(0, cap/2)`) or
decorrelated jitter. When many workers back off together, spreading each uniformly
across its whole window minimises the chance they collide again on the retry; Amazon's
"Exponential Backoff And Jitter" measures full jitter as giving the fewest retries. The
randomness is an injected `random.Random`, so a schedule is reproducible in tests.

### The read timeout

`httpx.ReadTimeout` and `httpx.ConnectTimeout` are not the same failure. A connect
timeout means the request never reached the server: safe to retry. A read timeout
means the request was sent and the response did not come back in time -- the server
may have executed it, and for a chat completion may have billed for it, with no
idempotency to lean on. Retrying it risks paying twice.

The default is therefore **not to retry an ambiguous read timeout** (`retry_ambiguous`
defaults to `False`): a read timeout fails the task rather than risking a double
charge. A caller who knows their requests are safe to repeat, or who will send an
`Idempotency-Key` the provider honours (wired in the adapter, stage 6), can set
`retry_ambiguous=True`. Erring toward not-paying-twice is the conservative default for
a library that spends the user's money.

## Rejected alternatives

- **tenacity.** Retries inside one coroutine; cannot coordinate the pool. See above.
- **Equal or decorrelated jitter.** Both leave more clustering than full jitter under a
  synchronised burst, which is exactly the situation a 429 storm creates.
- **A single global backoff timestamp** (what the OpenAI cookbook script does): late
  429s from already-sent requests keep pushing it out, so the pause never settles. The
  epoch is what stops a stale 429 from extending a pause that a fresher one already set.
- **Retrying read timeouts by default.** Simplest, and wrong when each retry may be a
  second charge for a request the first attempt already completed.

## Consequences

- `Outcome` has a distinct `AMBIGUOUS` member for the read-timeout case, one more than
  the five the plan named, so the retry decision for it is explicit rather than hidden
  in a flag on `RETRYABLE`.
- The classifier in the core handles status codes only; the adapter maps the transport's
  own exception types (read vs connect timeout) to outcomes, because only it imports the
  transport.
