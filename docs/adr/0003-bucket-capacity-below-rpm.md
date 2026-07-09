# 3. A bucket's capacity is set below the rate limit, not equal to it

Date: 2026-07-09
Status: Accepted

## Context

The property test `test_permits_bounded_by_capacity_plus_rate_times_window` asserts
the bound a token bucket actually obeys:

    permits(W) <= capacity + rate * W

A bucket of capacity `C` and refill rate `R` grants up to `C + R*W` permits in a
window of width `W`: the full `C` it starts with, plus `R*W` more as it refills
during the window. hypothesis found the smallest case that shows it -- capacity 1,
rate 2/s, a one-second window grants 2 -- and the previous commit's naive bound
(`permits <= capacity`) is red in the history to mark it.

This matters because of a mismatch in how "the limit" is counted. A provider that
advertises **RPM requests per minute** may enforce it as a **fixed window**: it
counts requests in `[00:00, 01:00)`, resets at the minute, counts again in
`[01:00, 02:00)`. A token bucket enforces a **rolling** window. If you size the
bucket at `capacity = RPM`, `rate = RPM/60`, then a rolling minute can contain up to
`RPM + (RPM/60)*60 = 2*RPM` permits: the bucket empties its full `RPM` at, say,
00:59, and refills another `RPM` by 01:59, and the rolling minute `[00:59, 01:59)`
straddles both. Against a fixed-window server that is a burst of nearly double the
limit, and the server answers with 429s.

## Decision

Size the bucket's capacity **below** the advertised limit, defaulting to **one sixth
of it** -- `capacity = RPM/6`, i.e. about ten seconds of burst -- while keeping the
refill rate at the full `RPM/60`. The rolling-minute worst case is then
`RPM/6 + RPM = 7/6 * RPM`, and the depth of any burst is bounded by `capacity`, which
is now ten seconds' worth rather than a full minute's.

The rate stays at the real limit so throughput is not sacrificed; only the *burst
depth* is capped. Ten seconds is a round, defensible default: long enough to absorb
normal concurrency, short enough that a fixed-window server never sees close to a
double-limit spike. Capacity is a constructor argument, so a caller who knows their
provider uses a rolling window (or who wants more burst) can set it as high as `RPM`.

## Rejected alternatives

- **capacity = RPM.** Allows the `2*RPM` rolling-minute burst above. Fine against a
  rolling-window server, wrong against a fixed-window one, and you rarely know which
  you face.
- **capacity = 1 (no burst).** Serialises everything to one request per `1/rate`
  seconds. Safe but throws away all the concurrency the limit actually permits, and
  turns a batch into a slow trickle.
- **A second, outer fixed-window counter.** Correct, but it is a whole extra
  mechanism (and its own reset-boundary edge cases) to buy what a smaller capacity
  buys for free. Out of scope for v0.1.

## Consequences

- The invariant the property test proves is `permits(W) <= capacity + rate*W`, and it
  holds only because the refill is continuous. Replace continuous refill with a
  discrete once-per-minute top-up and the burst at the window boundary breaks it;
  that is exactly what the test catches when the fix is removed.
- The default is a recommendation encoded where buckets are built from a per-minute
  limit; it is not enforced. A caller is free to size capacity up to the full limit.
