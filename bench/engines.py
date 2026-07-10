"""One driver per engine. Each sends the same workload; none of them is written here.

- ratebucket: the library under test, wired the way its README shows.
- aiometer + tenacity: two respected libraries composed the common way -- aiometer paces
  the launch rate, tenacity retries each request. The pathology this exposes: a 429 is
  handled by one coroutine backing off alone while the rest keep hitting the server.
- openai-cookbook: the vendored reference script, run unmodified (bench/vendor/).
- LiteLLM Router: the production router, pointed at the fake provider as a custom model.

Engine imports are lazy, so importing this module needs only the engine you call. The
heavy dependencies come in through `uv run --with ...` in `make bench`.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from _harness import ENDPOINT, Config

if TYPE_CHECKING:
    from ratebucket import Gate, VirtualClock

_VENDOR = Path(__file__).resolve().parent / "vendor"


# --- ratebucket -------------------------------------------------------------


async def run_ratebucket(base_url: str, config: Config) -> list[tuple[float, float]]:
    from ratebucket import (
        Gate,
        MultiResourceLimiter,
        RealClock,
        RetryPolicy,
        TokenBucket,
    )
    from ratebucket.adapters.openai_compat import OpenAICompatClient

    clock = RealClock()
    rps, tpm = config.declared_rps, config.declared_tpm
    # Pace at the declared rate with a small burst (capacity well below the per-second
    # window), so ratebucket avoids the rate-limit 429s and meets only the exogenous
    # ones -- which the gate then collapses to a single pause per epoch.
    buckets = {
        "requests": TokenBucket(max(2.0, rps * 0.25), rps, clock),
        "tokens": TokenBucket(tpm / 6, tpm / 60, clock),
    }
    limiter = MultiResourceLimiter(buckets, clock)
    limiter.start()
    gate = Gate(clock)
    retry = RetryPolicy(
        clock, random.Random(0), max_attempts=10, base_delay=0.1, max_delay=2.0
    )
    pauses: list[tuple[float, float]] = []
    messages = config.payload()["messages"]

    stop = asyncio.Event()
    watcher = asyncio.create_task(_watch_gate(gate, clock, pauses, stop))
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as http:
            client = OpenAICompatClient(
                http, limiter, gate, retry, clock, model="fake-1"
            )
            await asyncio.gather(
                *(
                    client.complete(messages, max_tokens=config.max_tokens)
                    for _ in range(config.n_requests)
                ),
                return_exceptions=True,
            )
    finally:
        stop.set()
        await watcher
        await limiter.aclose()
    return pauses


async def _watch_gate(
    gate: Gate,
    clock: VirtualClock,
    pauses: list[tuple[float, float]],
    stop: asyncio.Event,
) -> None:
    # One pause per epoch: the gate collapses concurrent 429s into a single closure, so
    # a new epoch is a new pause. Record it in wall-clock (perf_counter) for the plot.
    seen = gate.epoch
    while not stop.is_set():
        epoch = gate.epoch
        if epoch != seen:
            seen = epoch
            remaining = gate.open_at - clock.now()
            if remaining > 0:
                start = time.perf_counter()
                pauses.append((start, start + remaining))
        await asyncio.sleep(0.004)


# --- aiometer + tenacity ----------------------------------------------------


async def run_aiometer_tenacity(
    base_url: str, config: Config
) -> list[tuple[float, float]]:
    import aiometer
    import tenacity

    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as http:

        @tenacity.retry(
            retry=tenacity.retry_if_exception_type(
                (httpx.HTTPStatusError, httpx.TransportError)
            ),
            wait=tenacity.wait_exponential_jitter(initial=0.1, max=2.0),
            stop=tenacity.stop_after_attempt(10),
            reraise=True,
        )
        async def one() -> None:
            response = await http.post(ENDPOINT, json=config.payload())
            response.raise_for_status()

        async def attempt() -> None:
            with contextlib.suppress(Exception):  # gave up after its own retries
                await one()

        await aiometer.run_all(
            [attempt for _ in range(config.n_requests)],
            max_at_once=64,
            max_per_second=config.declared_rpm / 60,
        )
    return []


# --- openai-cookbook (vendored, unmodified) ---------------------------------


def _write_cookbook_requests(config: Config) -> tuple[Path, Path]:
    # Sync setup: the cookbook reads its requests from a jsonl file and writes results
    # to another. Done outside the async run so the timed region holds only the work.
    tmp = Path(__file__).resolve().parent / ".bench_tmp"
    tmp.mkdir(exist_ok=True)
    req_file = tmp / "cookbook_requests.jsonl"
    save_file = tmp / "cookbook_results.jsonl"
    payload = config.payload()
    lines = [
        json.dumps(dict(payload, metadata={"i": i})) for i in range(config.n_requests)
    ]
    req_file.write_text("\n".join(lines))
    save_file.unlink(missing_ok=True)
    return req_file, save_file


async def run_cookbook(base_url: str, config: Config) -> list[tuple[float, float]]:
    spec = importlib.util.spec_from_file_location(
        "cookbook_app", _VENDOR / "api_request_parallel_processor.py"
    )
    assert spec is not None
    assert spec.loader is not None
    cookbook = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cookbook)

    # The one adaptation for a localhost target: the script's URL parser hardcodes
    # `^https://`, so it cannot name the endpoint of an http:// test server. Override it
    # with the same "suffix after /vN/" rule, scheme-agnostic. Its throttling, retry and
    # pause logic -- the subject of the benchmark -- runs verbatim.
    cookbook.api_endpoint_from_url = lambda url: re.search(r"/v\d+/(.+)$", url)[1]

    req_file, save_file = _write_cookbook_requests(config)
    await cookbook.process_api_requests_from_file(
        requests_filepath=str(req_file),
        save_filepath=str(save_file),
        request_url=base_url + ENDPOINT,
        api_key="fake-key",
        max_requests_per_minute=float(config.declared_rpm),
        max_tokens_per_minute=float(config.declared_tpm),
        token_encoding_name="cl100k_base",
        max_attempts=10,
        logging_level=logging.ERROR,
    )
    return []


# --- LiteLLM Router ---------------------------------------------------------


async def run_litellm(base_url: str, config: Config) -> list[tuple[float, float]]:
    import litellm
    from litellm import Router

    litellm.suppress_debug_info = True
    logging.getLogger("LiteLLM").setLevel(logging.ERROR)

    router = Router(
        model_list=[
            {
                "model_name": "fake-1",
                "litellm_params": {
                    "model": "openai/fake-1",
                    "api_base": base_url + "/v1",
                    "api_key": "fake-key",
                    "rpm": int(
                        config.declared_rpm
                    ),  # let the Router pace this deployment
                },
            }
        ],
        num_retries=20,
        retry_after=1,
        routing_strategy="usage-based-routing",
        allowed_fails=config.n_requests,  # a single deployment cannot be cooled out
        cooldown_time=0.0,
    )
    messages = config.payload()["messages"]

    async def one() -> None:
        with contextlib.suppress(Exception):  # exhausted the router's retries
            await router.acompletion(
                model="fake-1", messages=messages, max_tokens=config.max_tokens
            )

    await asyncio.gather(*(one() for _ in range(config.n_requests)))
    return []
