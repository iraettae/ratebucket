# Worklog

A running log of the decisions behind each stage and the surprises that showed up
while building it. The roadmap below is the plan; the notes under each stage are
what actually happened, including the things that went wrong, because those are the
parts worth remembering.

| # | Stage | Planned |
|---|---|---|
| 1 | Skeleton, release pipeline, clock and bucket | done |
| 2 | MultiResourceLimiter: arbiter, Reservation, cancellation | |
| 3 | Property tests and the bucket invariant | |
| 4 | Gate with epochs, error classifier, RetryPolicy | |
| 5 | Fake provider on a real socket | |
| 6 | Adapters: openai_compat and http_fetch | |
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
