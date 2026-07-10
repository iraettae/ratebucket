# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-10

The first release with a library in it: the resource arbiter, the pool-wide gate, the
retry policy, and two adapters over a domain-free core.

### Added

- `TokenBucket` -- one quota dimension with continuous lazy refill, a level that may go
  negative to record an over-spend, and a spin-free `time_until`. `TokenBucket.per_minute`
  sizes capacity below the limit (ADR 0003).
- `MultiResourceLimiter` -- a single arbiter coroutine granting the FIFO head across every
  dimension atomically, so the two-limit case is deadlock-free by construction.
  `Reservation` reserves by an upper bound and reconciles against actual usage; conservative
  backfilling addresses head-of-line blocking, behind a flag.
- `Gate` -- the pool-wide reaction to 429, with an epoch so a burst of 429s raises one pause.
- `RetryPolicy`, `classify_status`, and the `Outcome` taxonomy: full-jitter backoff, an
  injected `Random`, throttling deferred to the gate, ambiguous failures not retried by
  default.
- `Clock` protocol with `RealClock` and an integer-nanosecond `VirtualClock` for
  deterministic tests.
- Adapters over the core: `openai_compat.OpenAICompatClient` (reserve-and-reconcile, a
  `TokenEstimator` protocol with a dependency-free default, and a staleness-guarded fold of
  the provider's `x-ratelimit-*` headers) and the thirty-line `http_fetch.HttpFetcher`.
- Optional extras: `openai` (httpx + pydantic) and `tiktoken`.
- `make bench` -- the four-engine benchmark against the fake provider, with committed
  `bench/results.json` and `bench/timeline.png`; `make prove` -- the two headline invariants
  against the naive versions they replace.
- Five ADRs, a fake provider on a real socket, and the full proof-test suite.

## [0.0.1] - 2026-07-08

The release pipeline, published before there is anything to release. This version
exists to prove that building, checking and publishing work end to end, on a real
index, with no API token anywhere. It contains no library.

### Added

- Packaging with uv and hatchling, `py.typed`, src-layout.
- `pyproject.toml` as the only configuration file.
- ruff (linter and formatter), mypy `--strict`, pytest.
- CI: lint, a mypy matrix over 3.11/3.12/3.13, a pytest matrix, and a build job
  that fails if `py.typed` is missing from the wheel.
- A secret scan over every commit, not just the tip.
- Trusted Publishing to TestPyPI, and to PyPI behind a human approval gate.

[Unreleased]: https://github.com/iraettae/ratebucket/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/iraettae/ratebucket/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/iraettae/ratebucket/releases/tag/v0.0.1
