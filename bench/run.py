"""Run the workload through each engine against a fresh fake provider, then report.

    uv run --with aiometer --with tenacity --with litellm --with aiohttp \
           --with tiktoken --with matplotlib python bench/run.py

`make bench` wraps that. Pass engine names as arguments to run a subset, e.g.
`python bench/run.py ratebucket aiometer` while iterating. Outputs bench/results.json and
bench/timeline.png. Every number is against the fake provider in this repository on
localhost -- not a production provider, and not a throughput record.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import engines
from _harness import Config, EngineResult, FakeProvider, normalize, running

_HERE = Path(__file__).resolve().parent

ENGINES = {
    "ratebucket": engines.run_ratebucket,
    "aiometer": engines.run_aiometer_tenacity,
    "cookbook": engines.run_cookbook,
    "litellm": engines.run_litellm,
}
DISPLAY = {
    "ratebucket": "ratebucket",
    "aiometer": "aiometer+tenacity",
    "cookbook": "openai-cookbook",
    "litellm": "litellm-router",
}


def run_one(key: str, config: Config) -> EngineResult:
    provider = FakeProvider(
        rps_limit=config.provider_rps,
        tps_limit=config.provider_tps,
        fail_every=config.fail_every,
        drop_every=config.drop_every,
        throttle_period_s=config.throttle_period_s,
        throttle_duration_s=config.throttle_duration_s,
    )
    with running(provider) as base_url:
        t0 = time.perf_counter()
        pauses = asyncio.run(ENGINES[key](base_url, config))
        wall = time.perf_counter() - t0
    return EngineResult(
        name=DISPLAY[key],
        wall_clock_s=wall,
        events=normalize(provider.events, t0),
        pauses=[(s - t0, e - t0) for s, e in pauses],
    )


def write_results(config: Config, results: list[EngineResult]) -> None:
    out = {
        "disclaimer": (
            "Fake provider from this repository (tests/fake_provider.py) on localhost. "
            "Not a production provider; wall-clock is dominated by each engine's reaction "
            "to the provider's rate-limit 429s, not by network or model latency."
        ),
        "config": {
            "n_requests": config.n_requests,
            "provider_rps_limit": config.provider_rps,
            "provider_tps_limit": config.provider_tps,
            "provider_fail_every": config.fail_every,
            "provider_drop_every": config.drop_every,
            "throttle_period_s": config.throttle_period_s,
            "throttle_duration_s": config.throttle_duration_s,
            "declared_rps": config.declared_rps,
            "declared_tpm": config.declared_tpm,
        },
        "engines": [r.as_dict() for r in results],
    }
    (_HERE / "results.json").write_text(json.dumps(out, indent=2) + "\n")


def plot(config: Config, results: list[EngineResult]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    color = {200: "#2e7d32", 429: "#c62828", 500: "#f9a825", -1: "#1a1a1a"}
    tmax = max((t for r in results for t, _ in r.events), default=1.0)

    fig, axes = plt.subplots(
        len(results), 1, figsize=(11, 1.9 * len(results)), sharex=True, squeeze=False
    )
    for ax, r in zip((a[0] for a in axes), results, strict=True):
        for start, end in r.pauses:
            ax.axvspan(start, end, color="0.86", zorder=0)
        ax.scatter(
            [t for t, _ in r.events],
            [1] * len(r.events),
            c=[color.get(s, "#888") for _, s in r.events],
            s=9,
            zorder=2,
        )
        ax.set_yticks([])
        ax.set_ylabel(r.name, rotation=0, ha="right", va="center", fontsize=9)
        ax.set_xlim(-0.5, tmax * 1.02 + 0.5)
        ax.text(
            0.995,
            0.08,
            f"{r.wall_clock_s:.1f}s   ok {r.ok}/{config.n_requests}   "
            f"429 {r.rate_limited}   sent {r.sent}",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=8,
        )
    axes[-1][0].set_xlabel("seconds since start")
    axes[0][0].legend(
        handles=[
            Patch(color=color[200], label="200"),
            Patch(color=color[429], label="429"),
            Patch(color=color[500], label="500"),
            Patch(color="0.86", label="gate pause"),
        ],
        loc="upper right",
        fontsize=7,
        ncol=4,
    )
    fig.suptitle(
        "Same workload, same fake provider: each engine's reaction to 429s", fontsize=11
    )
    fig.tight_layout()
    fig.savefig(_HERE / "timeline.png", dpi=130)


def main() -> None:
    wanted = [a for a in sys.argv[1:] if a in ENGINES] or list(ENGINES)
    config = Config()
    results: list[EngineResult] = []
    for key in wanted:
        print(f"running {DISPLAY[key]} ...", flush=True)
        r = run_one(key, config)
        results.append(r)
        print(
            f"  {r.name}: {r.wall_clock_s:.1f}s  sent {r.sent}  ok {r.ok}  "
            f"429 {r.rate_limited}  500 {r.server_errors}  drop {r.drops}  "
            f"pauses {len(r.pauses)}",
            flush=True,
        )
    write_results(config, results)
    plot(config, results)
    print(f"\nwrote {_HERE / 'results.json'} and {_HERE / 'timeline.png'}")


if __name__ == "__main__":
    main()
