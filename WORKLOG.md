# Worklog

A running log of the decisions behind each stage and the surprises that showed up
while building it. The roadmap below is the plan; the notes under each stage are
what actually happened, including the things that went wrong, because those are the
parts worth remembering.

| # | Stage | Planned |
|---|---|---|
| 1 | Skeleton, release pipeline, clock and bucket | done |
| 2 | MultiResourceLimiter: arbiter, Reservation, cancellation | done |
| 3 | Property tests and the bucket invariant | done |
| 4 | Gate with epochs, error classifier, RetryPolicy | done |
| 5 | Fake provider on a real socket | done |
| 6 | Adapters: openai_compat and http_fetch | done |
| 7 | Benchmark against third-party engines | |
| 8 | README, ADRs, 0.1.0 release | |

---

## Stage 1: Skeleton, release pipeline, clock and bucket

### Decisions

- **Name.** `ratebucket` is free on PyPI and TestPyPI (both 404; a known package
  returns 200 on the same probe, so the 404 is real, not a network artefact).
- **Toolchain** pinned to current upstream: hatchling 1.31, ruff 0.15.20, mypy 2.2,
  pytest 9.1, pytest-asyncio 1.4, hypothesis 6.156, gitleaks 8.30. The pins in
  pyproject and the workflows match.
- **Clock: injected, stepped, integer nanoseconds.** Recorded in ADR 0001. The
  short version: virtual time is integer nanoseconds so `advance(a); advance(b)`
  equals `advance(a+b)` exactly, and the bucket invariant holds with no epsilon.
- **Bucket: float level, continuous refill, no lower clamp.** `time_until` rounds
  the wait up so one sleep always crosses the target and the arbiter never spins.

### What went wrong, and what it caught

- **The first CI run was red, and correctly so.** `UV_FROZEN=1` at the job level
  is mutually exclusive with `uv sync --locked`; uv refuses to start. Both are
  individually valid and incompatible together. `UV_LOCKED` is the right one, and
  it is not merely compatible: with a dependency added to pyproject and never
  re-locked, `UV_LOCKED=1 uv run` refuses to start, where `UV_FROZEN` would have
  installed the stale resolution and stayed green. Exactly the kind of mistake that
  getting the pipeline green on an empty repo exists to find.
- **hypothesis found two real bugs in the first bucket.** A capacity-0 bucket had
  `time_until(1)` returning a finite wait for a token it can never hold; and
  `time_until` rounding down in float made a worker wake a hair early, fail, sleep
  ~1ns, and repeat until the step cap tripped. Both were one root cause: the wait
  did not guarantee the token would be takeable on waking. Fixed by returning
  `inf` for unreachable targets and rounding the wait up.
- **The push needed SSH.** The GitHub token in use lacks the `workflow` scope, so
  it refused to create `.github/workflows/*`. The SSH remote has no such restriction.
- **The Zeno test tripped the wrong guard at first.** Float underflow turned the
  halving schedule into a `sleep(0)` spinner (the drain guard) before it exhausted
  the step guard. Lowered the step cap on the instance so it trips the intended
  guard in milliseconds.

### Proof-tests shown red (with the fix removed)

- `test_advance_steps_rather_than_jumps`: jump semantics -> `[2.0]` instead of
  `[1.0, 2.0]`.
- `test_take_drives_the_level_negative` and the negative-refill test: a clamp at
  zero -> `0.0` instead of `-500.0`.

### Deferred by design

- The headline hypothesis property test,
  `test_permits_bounded_by_capacity_plus_rate_times_window`, is stage 3, where it
  arrives with its revert commit and the ADR on capacity below RPM. The bound was
  validated here (6000 examples, three Python versions, bare `<=`), but the test
  as an artefact belongs to stage 3.

---

## Stage 2: MultiResourceLimiter -- arbiter, Reservation, cancellation

### Decisions

- **One FIFO arbiter, atomic all-or-nothing grants.** Recorded in ADR 0002. The
  single arbiter looking only at the head and taking from every bucket or none is
  what makes the two-limit case deadlock-free by construction.
- **Conservative backfilling behind a flag** (`backfill=`, default off). Measured
  on a mixed workload (`make bench-backfill`): +171% requests completed in a 30s
  window versus strict FIFO, with the heavy requests finishing at the same time
  either way -- the head is never delayed.
- **The waiting arbiter is woken through a clock timer, not asyncio.timeout.**
  asyncio.timeout measures real loop time, and the arbiter runs on the injected
  clock, so its refill wait is a clock sleep racing a wakeup Event.

### What went wrong, and what it caught

- **The arbiter's helper tasks have to be clock-owned.** The wait races a clock
  sleep against a wakeup Event; the sleep runs in a spawned task, and the
  VirtualClock only drains tasks it owns. Routing every task the limiter spawns
  through the injected `spawn` (the clock's under test) is what lets `advance()`
  settle the whole grant chain.
- **`time_until` rounds the wait up by a nanosecond**, so a request that needs a
  refill is granted at t+1ns, not exactly at t. The first draft of a test advanced
  by exactly the refill time and read the request as not-yet-granted; the fix was
  in the test, matching the conservative wake.
- **The backfilling test scenarios were written backwards at first.** A single
  bucket where the head needs the full capacity makes *every* take delay the head,
  so backfilling correctly refuses. The real win is multi-dimensional: a request
  that needs none of the scarce dimension slips past. The tests were rewritten to
  the case backfilling actually helps.

### A hard adversarial pass over the arbiter, and six defects it found

After the arbiter passed its own tests, it was attacked from six angles at once --
lost wakeups, cancellation races, reservation accounting under failure, the
backfill invariant, FIFO/atomicity, and clock-drain integration -- each with
runnable repros rather than argument. The core held (no deadlock, no partial take,
strict FIFO, clean drain), but six real defects surfaced, all now fixed with a test
each:

1. A negative cost minted quota above capacity. Rejected at validation.
2. A zero-cost dimension on an overdrafted (negative) bucket wedged the whole
   queue, because `available() >= 0` is false for a negative bucket. A zero
   requirement now never gates a grant.
3. `aclose()` left parked callers awaiting a future the dead arbiter would never
   resolve. It now cancels every pending waiter.
4. Backfill could delay the head by one or two nanoseconds -- the safety check used
   linear reachability while the arbiter's wait rounds up. Both now use the same
   nanosecond arithmetic, and the head's deadline is tracked monotonically so
   re-evaluating it across wakes cannot push it later. The invariant is now exact,
   verified by a property test comparing head grant times with backfill on and off.
5. A cancellation delivered in the grant window -- after the arbiter took the tokens,
   before the caller resumed -- leaked the whole reservation, because `__aenter__`
   raised and `__aexit__` never ran. The enqueue path now gives the grant back on
   that path. This is the reservation leak that is invisible at ten tasks and fatal
   at forty thousand.
6. With backfill on, a candidate that became grantable by refill while the head was
   still blocked was never granted (a lost wakeup, permanent if the head's wait was
   infinite). The arbiter now also wakes when the earliest candidate could refill.

Along the way the bucket's reads were made side-effect-free (the level is computed
from a stored origin rather than accumulated on every read), so reading it at
several instants between two takes no longer drifts.

### Proof-tests shown red (with the fix removed)

Each of the four brief tests, and each of the six regressions above, goes red when
its fix is removed. Demonstrated:

- `test_cost_exceeds_capacity_fails_fast`: drop the capacity check -> `DID NOT RAISE`.
- `test_cancelled_waiter_removed_from_queue`: drop the done-callback -> the request
  behind a cancelled head is granted at t=10 instead of t=1.
- `test_reservation_released_on_exception`: drop the release in `__aexit__` -> the
  bucket stays at 70, not 100.
- `test_bucket_goes_negative_when_actual_exceeds_reserved`: clamp the overspend ->
  the bucket reads 10 instead of -10.
- and the six regression tests (`test_negative_cost_is_rejected`,
  `test_zero_cost_dimension_never_blocks_a_negative_bucket`,
  `test_aclose_unblocks_a_pending_waiter`,
  `test_reservation_released_when_cancelled_in_the_grant_window`,
  `test_backfill_candidate_granted_after_it_refills`,
  `test_backfill_never_delays_the_head`), each red without its fix.

---

## Stage 3: property tests and the bucket invariant

### The invariant, and the naive bound that came before it

The headline property test asserts `permits(W) <= capacity + rate*W` over random
schedules on the VirtualClock. It did not start there. The first version asserted
the intuitive claim -- a bucket of capacity RPM never grants more than RPM in a
rolling minute -- and hypothesis falsified it on the first run, shrinking to
capacity 1, rate 2/s, a one-second window that grants 2: the token the bucket
started with plus one refilled inside the window. That naive commit is kept in
history, with the revert above it explaining the burst, because the reason it is
wrong is the whole reason a bucket's capacity has to be sized below its limit. ADR
0003 has the sizing decision.

### Decisions

- **Capacity below RPM (ADR 0003).** A bucket of capacity C and rate R grants up to
  C + R*W in a window W. With C = RPM and R = RPM/60 a rolling minute holds nearly
  2*RPM, which a fixed-window server rejects. Default the capacity to RPM/6 (ten
  seconds of burst) while keeping the rate at the full RPM/60. **Checkpoint: the
  fraction is the owner's to confirm.**

### What went wrong, and what it caught

- **The property test could not, at first, catch the thing it was meant to prove.**
  With every worker spawned at the start, the bucket is drained once and the
  boundary burst that continuous refill rules out never gets a chance to appear, so
  swapping in a discrete refill left the test green. The schedule was changed to
  spawn workers interleaved with advances, across the run, and a deterministic test
  was added that constructs the burst directly -- a full bucket drained the instant
  before a boundary, a second wave arriving the instant after.

### Proof-tests shown red (with the fix removed)

- The naive commit's `permits <= capacity`: red by construction; hypothesis grants
  2 against a capacity of 1.
- `test_continuous_refill_bounds_the_boundary_burst`: replace continuous refill with
  a discrete once-a-minute dump and the constructed window holds 20 against a bound
  of 10.5 -- the 2*capacity burst. Green on continuous refill.

---

## Stage 4: gate with epochs, error classifier, RetryPolicy

### Decisions

- **A shared Gate for the pool-wide pause; a per-worker RetryPolicy on top (ADR 0004).**
  A 429 is backpressure the whole pool shares, so it is handled by the gate, not by one
  worker backing off alone -- which is precisely what tenacity cannot do.
- **Epochs as a compare-and-swap.** A worker reads the gate's epoch before sending and
  presents it back with a 429; a 429 carrying a stale epoch is ignored. Fifty concurrent
  429s produce one pause. The same guard reappears in stage 6 for stale rate-limit
  headers -- the same "acting on an out-of-date view" problem.
- **Full jitter, injected Random.** `uniform(0, min(cap, base*2**n))`, because it leaves
  the least clustering under a synchronised burst. The randomness is injected so the
  backoff is reproducible.
- **Read timeouts are not retried by default (checkpoint, decided).** A read timeout may
  mean the request was executed and billed; retrying risks paying twice. `retry_ambiguous`
  defaults to False; a caller who sends an Idempotency-Key can opt in. Conservative
  default for a library that spends the user's money.

### Proof-test shown red (with the fix removed)

- `test_fifty_concurrent_429_produce_single_pause`: drop the stale-epoch check in
  `Gate.throttle` and the epoch climbs to fifty -- fifty pauses, not one.

---

## Stage 5: the fake provider on a real socket

### Decisions

- **A real uvicorn server, not a mock transport.** A mock can fake a 429 or a 500,
  but it cannot drop a connection halfway through a response body, and that failure --
  a request that may or may not have been executed -- is the one worth testing. The
  provider runs under uvicorn on an ephemeral port in a background thread.
- **Behaviour chosen per request by a header.** `X-Fake-Behavior` selects the exact
  failure a test wants; with no header the endpoint answers 200 with a usage block and
  `x-ratelimit-*` headers, and enforces its own per-second limits so a load test gets
  real 429s.

### The scenarios (this is also the benchmark's failure script)

429 with Retry-After and without; 500; a connection dropped mid-body; a slow response
(and a client read timeout against it); a 200 with a truncated, unparseable body; and
a real per-second rate limit that 429s past its quota.

### What went wrong, and what it caught

- **The mid-body drop was the one to worry about, and it worked the first time.** A
  StreamingResponse whose generator raises after a partial chunk makes uvicorn close
  the connection without a complete body; httpx sees `RemoteProtocolError: peer closed
  connection without sending complete message body`, which is exactly the real thing.
- **`filterwarnings = ["error"]` had to make room for the server's stack.** uvicorn,
  starlette and websockets emit their own deprecations; those are now ignored by module
  so they do not fail our tests, while everything else still errors.

---

## Stage 6: the adapters, where the core meets httpx

### Decisions

- **Adapters live outside `core/` and are not imported by the top-level package.** So
  httpx stays an optional dependency: `import ratebucket` needs nothing new, and only
  `from ratebucket.adapters.openai_compat import ...` pulls httpx in. The core-purity
  check still passes because the domain words all live in `adapters/`, never in `core/`.
- **The core learned one new verb: `observe`.** A provider's own `x-ratelimit-remaining-*`
  headers are ground truth for a shared quota, so `MultiResourceLimiter.observe` folds
  them back into the buckets. Two rules make it safe. It only ever *lowers* a bucket:
  the provider seeing more room than we do is not licence to burst past our own pacing,
  and a smaller self-imposed capacity must still bind. And it is guarded by an
  observed-at timestamp per dimension, so a header from an earlier request that arrives
  late cannot overwrite a fresher one. The docstrings stay domain-neutral ("an outside
  authority", "an externally reported count") so the word never reaches the core.
- **`classify_httpx` draws the line the core can't.** A connect failure never reached
  the server: retryable. A read timeout or a body dropped mid-stream may have been
  executed and billed: ambiguous, and not retried by default (the stage-4 policy). Only
  the adapter imports httpx, so only the adapter knows `ConnectError` from `ReadTimeout`.
- **`http_fetch` is the proof the core owes nothing to LLMs.** Thirty lines, cost
  `{"requests": 1}`, no token dimension, no usage, no headers to fold. It drives the
  same arbiter, gate and retry policy as the OpenAI client. If the core had leaked a
  single assumption about tokens, this file could not exist.

### The ordering bug I wrote and then caught

The first cut applied the `x-ratelimit` headers inside `_send`, during the request.
But the reservation is committed *after* `execute` returns, and the commit's give-back
would then land on top of the server's correction and undo it. Worse, an early version
*set* the bucket to the server's remaining, which erased a deliberately-small local cap
every time (the server always reports more room than a self-imposed sub-limit). Two
fixes: `observe` only ever lowers, and the adapter carries the observation out of
`_send` and applies it *last*, after the commit, so the provider's number is the final
word. Both are pinned by tests (`..._lower_the_bucket_to_the_servers_remaining`,
`..._generous_response_headers_do_not_raise_the_bucket`).

### The checkpoint: the default token estimate (ADR 0005)

The adapter must reserve a token cost before it knows one, so it estimates. The default
is dependency-free `chars / 4.0`; tiktoken is an optional extra behind a `TokenEstimator`
Protocol. Measured against tiktoken `cl100k_base` (`bench/estimator_error.py`), chars/4
runs ~16% mean absolute error and under-counts token-dense input -- code, numbers,
punctuation -- by up to 45%. That sounds alarming and mostly is not: reserve-and-reconcile
means the estimate only sizes the reservation, and the commit corrects it to the reported
usage, so a miss costs a little transient fairness, never accuracy. 4.0 stays the default
over the marginally-tighter 4.2 because it is the recognisable rule of thumb and biases
slightly high, which is the safe direction for a reservation.

### Proof-tests shown red (with the fix removed)

- `test_stale_ratelimit_headers_ignored`: drop the observed-at guard in
  `MultiResourceLimiter.observe` and a late, stale header drops the bucket below the
  fresher value that already stood.
- `test_a_low_estimate_drives_the_tokens_bucket_negative`: an end-to-end path for the
  overdraft the bucket is built to record -- the reservation under-guesses, the commit
  takes the difference, the bucket goes negative instead of clamping and under-counting.
- `test_a_dropped_connection_surfaces_and_is_not_retried` (real socket): a genuine
  `RemoteProtocolError` is classified ambiguous and reaches the caller once, not retried.
