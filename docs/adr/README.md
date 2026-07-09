# Architecture Decision Records

One file per decision that was not obvious. Each one names the alternatives that were
rejected and why, because a decision without a rejected alternative is not a decision,
it is a default.

Format: [MADR](https://adr.github.io/madr/), trimmed.

| # | Decision | Status |
|---|---|---|
| [0001](0001-injected-clock-and-stepped-time.md) | An injected clock, and virtual time that steps rather than jumps | Accepted |
| [0002](0002-single-fifo-arbiter-and-backfilling.md) | A single FIFO arbiter, and conservative backfilling for its head-of-line cost | Accepted |
| [0003](0003-bucket-capacity-below-rpm.md) | A bucket's capacity is set below the rate limit, not equal to it | Accepted |

Planned, each written when the code that motivates it exists:

| Decision | Stage |
|---|---|
| A hand-written RetryPolicy instead of tenacity: retrying inside one coroutine cannot tell the pool to slow down | 4 |
