# 5. The default token estimate is a dependency-free heuristic, not a tokenizer

Date: 2026-07-10
Status: Accepted

## Context

The token limit is enforced per token, but a request's token cost is not known until
the provider answers with a `usage` block. To reserve against the tokens-per-minute
bucket *before* sending, the adapter needs an estimate of the cost up front. Two ways
to get one:

- An exact tokenizer (tiktoken) run on the prompt. Accurate for OpenAI's own models,
  but it is a compiled dependency that pulls in a Rust extension and downloads encoding
  files, and it is only *exact* for the models whose encodings it ships -- against the
  OpenAI-*compatible* providers this adapter targets (local servers, proxies, other
  vendors) it is itself an approximation.
- A cheap heuristic: characters divided by a constant. No dependency, wrong by some
  margin, and the question is whether the margin matters.

It matters less than it first appears, because of reserve-and-reconcile (see the
`Reservation` and `MultiResourceLimiter` docstrings). The estimate only *sizes the
reservation*. When the response arrives, the reservation is committed to the actual
`usage.total_tokens`, and the bucket is adjusted by the difference: an over-estimate
gives tokens back, an under-estimate drives the bucket negative, which the bucket is
built to record rather than clamp away (ADR 0001). So the estimate never corrupts the
count. Its only cost is transient: an over-estimate holds a little too much quota out
of everyone else's reach until the commit; an under-estimate lets one request's
overshoot briefly overdraw the shared bucket.

## Decision

Default to a dependency-free `HeuristicEstimator` of `chars / 4.0`, exposed behind a
`TokenEstimator` Protocol so an exact tokenizer can be supplied instead. tiktoken is an
optional extra (`pip install ratebucket[tiktoken]`), never a hard dependency. The
divisor is a constructor argument (`chars_per_token`).

The constant is **4.0**, the familiar "about four characters per token" rule of thumb.
Measured against tiktoken `cl100k_base` over a spread of English chat prompts
(reproducible with `bench/estimator_error.py`):

    chars/3.8: mean signed  +9.3%, mean abs 19.5%, max abs 42.4%
    chars/4.0: mean signed  +4.0%, mean abs 16.5%, max abs 45.5%
    chars/4.2: mean signed  -0.7%, mean abs 14.0%, max abs 48.5%

`chars/4.2` is marginally better-centred on this corpus, but the edge (2 points of mean
absolute error) is well inside the noise of a sixteen-prompt sample and is bought by
moving the *signed* bias slightly negative. For a reservation, a small positive bias is
the safe direction -- reserving a little too much never overshoots the provider's limit,
while under-reserving is what lets a burst slip through. `chars/4.0`'s +4% mean signed
bias is that safe lean, and 4.0 is the number every reader already recognises. The
per-prompt spread (up to ~45% either way) dwarfs the choice of divisor regardless, which
is the real message: the constant barely matters; reconciliation is what makes the
estimate safe to be wrong.

## Rejected alternatives

- **tiktoken as a hard dependency.** A heavy, compiled dependency made mandatory to buy
  accuracy that reconciliation already makes optional, and it is not even exact for the
  non-OpenAI providers this adapter is meant to also serve.
- **`chars/4.2` (or a fitted constant) as the default.** Marginally tighter on the
  sample, within its noise, and it trades away the conservative positive bias. Not worth
  giving up the recognisable 4.0 and the safe lean for a fit to sixteen prompts.
- **No prompt estimate -- reserve `max_tokens` alone.** Ignores the prompt's own cost,
  which systematically under-reserves on long prompts (exactly where the token budget is
  tightest) and leans on reconciliation to clean up every single request instead of only
  the mis-estimated tail.

## Consequences

- The dangerous direction is under-estimation on token-dense input -- code, numbers,
  punctuation, where a character count sees few characters but the tokenizer emits many
  (the worst case in the corpus, a list of digits, is under-counted by 45%). That is the
  direction that drives the shared bucket negative and can briefly let a workload
  overspend a shared quota. It is repaired on commit, and further corrected by the
  provider's own `x-ratelimit-*` headers via `MultiResourceLimiter.observe`; but a
  workload dominated by code or JSON should pass a lower `chars_per_token`, or install
  the `tiktoken` extra and supply an exact estimator.
- The Protocol keeps the exact path a drop-in: a caller wires their own tokenizer once,
  and the reserve-commit-observe machinery is unchanged.
- The benchmark is committed, so the numbers above are reproducible and this decision can
  be re-litigated against a larger or domain-specific corpus rather than argued from
  rules of thumb.
