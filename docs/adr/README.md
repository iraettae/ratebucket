# Architecture Decision Records

One file per decision that was not obvious. Each one names the alternatives that were
rejected and why, because a decision without a rejected alternative is not a decision,
it is a default.

Format: [MADR](https://adr.github.io/madr/), trimmed.

| # | Decision | Status |
|---|---|---|
| [0001](0001-injected-clock-and-stepped-time.md) | An injected clock, and virtual time that steps rather than jumps | Accepted |

Planned, each written when the code that motivates it exists:

| Decision | Stage |
|---|---|
| Bucket capacity below RPM: why a full bucket of `C = RPM` yields `2 * RPM` in a sliding window | 3 |
| One FIFO arbiter instead of independent buckets: deadlock, starvation, conservative backfilling | 2 |
| A hand-written RetryPolicy instead of tenacity: retrying inside one coroutine cannot tell the pool to slow down | 4 |
