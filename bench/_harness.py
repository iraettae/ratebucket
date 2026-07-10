"""Shared scaffolding for the benchmark: the workload, the result shape, the plot.

Every engine runs the same workload against the same fake provider (the one from the
repository, in tests/), and the provider records the arrival time and status of every
request it serves. That server-side log is the timeline: it is the one vantage point all
four engines share, so their behaviour is compared on identical evidence rather than on
each engine's own self-reported counters.

Nothing here is an engine. The rate-limiting engines are third-party (openai-cookbook,
LiteLLM, aiometer+tenacity) or the library under test; benchmarking against code you
wrote yourself is a rigged fight, and reads as one.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

# The fake provider lives with the tests; the benchmark drives the same one. Put tests/
# on the path first, then re-export it, so callers get the provider from `_harness` and
# never have to repeat this ordering-sensitive dance.
_TESTS = Path(__file__).resolve().parent.parent / "tests"
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from fake_provider import FakeProvider, running  # noqa: E402 - needs tests/ on sys.path

__all__ = ["ENDPOINT", "Config", "EngineResult", "FakeProvider", "normalize", "running"]

ENDPOINT = "/v1/chat/completions"

# Status codes the provider records; -1 is a body dropped mid-stream.
OK = 200
RATE_LIMITED = 429
SERVER_ERROR = 500
DROP = -1


@dataclass(frozen=True)
class Config:
    """The one workload every engine runs. Small numbers on purpose: this is a fake
    provider on localhost, and the point is the shape of each engine's reaction to 429s,
    not a throughput record.

    The provider enforces a per-second request window. An engine that paces under it
    flows steadily and earns only the occasional boundary 429; an engine that bursts a
    whole allowance at once earns a storm of them. That is the whole story, and it is the
    cookbook script's undoing: its client-side limiter refills to a full minute of
    capacity and dumps it, so it bursts, storms, and then its one global pause -- held by
    the timestamp of the *last* 429 seen -- is dragged forward by every late 429 from a
    request that was already in flight. On the timeline that is a run of dead air; the
    epoch in ratebucket's gate is exactly what stops one 429 from extending a pause meant
    for another.

    Injected faults (`fail_every`, `drop_every`) stay available for a noisier run but
    default off, so the clean comparison is about the 429s alone.
    """

    n_requests: int = 150
    max_tokens: int = 16
    provider_rps: int = 45  # provider's per-second request window
    provider_tps: int = 1_000_000
    fail_every: int = 0
    drop_every: int = 0
    # Exogenous throttling (429 everything for a window on a period) is available but off
    # by default: honouring a 1s Retry-After against a sub-second recurring block punishes
    # every pool-wide limiter equally and drowns out the burst-vs-pace story.
    throttle_period_s: float = 0.0
    throttle_duration_s: float = 0.0
    # What each engine is *told* the provider allows. Equal for all, so the comparison is
    # of reactions, not of who guessed the limit. A hair under provider_rps, so a paced
    # engine mostly stays under and a bursting one still overruns badly.
    declared_rps: float = 42.0
    declared_tpm: int = 1_000_000

    @property
    def declared_rpm(self) -> float:
        return self.declared_rps * 60.0

    def payload(self) -> dict[str, object]:
        return {
            "model": "fake-1",
            "messages": [{"role": "user", "content": "benchmark request, please echo"}],
            "max_tokens": self.max_tokens,
        }


@dataclass
class EngineResult:
    """One engine's run, reduced to what the timeline and the table need."""

    name: str
    wall_clock_s: float
    events: list[tuple[float, int]]  # (seconds since run start, status)
    pauses: list[tuple[float, float]] = field(default_factory=list)  # (start, end)
    note: str = ""

    @property
    def sent(self) -> int:
        return len(self.events)

    @property
    def ok(self) -> int:
        return sum(1 for _, s in self.events if s == OK)

    @property
    def rate_limited(self) -> int:
        return sum(1 for _, s in self.events if s == RATE_LIMITED)

    @property
    def server_errors(self) -> int:
        return sum(1 for _, s in self.events if s == SERVER_ERROR)

    @property
    def drops(self) -> int:
        return sum(1 for _, s in self.events if s == DROP)

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "wall_clock_s": round(self.wall_clock_s, 3),
            "requests_sent": self.sent,
            "succeeded": self.ok,
            "rate_limited_429": self.rate_limited,
            "server_errors_500": self.server_errors,
            "dropped_bodies": self.drops,
            "gate_pauses": len(self.pauses),
            "note": self.note,
        }


def normalize(events: list[tuple[float, int]], t0: float) -> list[tuple[float, int]]:
    return [(t - t0, s) for t, s in events]
