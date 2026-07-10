"""The adapters against the fake provider over a real socket.

The deterministic logic is covered in test_adapters.py with scripted replies; these
prove the other half -- that the adapter actually speaks httpx to a listening server,
reconciles against a real `usage` block, and that `classify_httpx` fires on genuine
httpx exceptions and not only constructed ones.
"""

from __future__ import annotations

import asyncio
import random

import httpx
import pytest

from fake_provider import FakeProvider
from ratebucket import (
    Gate,
    MultiResourceLimiter,
    RealClock,
    RetryPolicy,
    TokenBucket,
)
from ratebucket.adapters.http_fetch import HttpFetcher
from ratebucket.adapters.openai_compat import OpenAICompatClient

pytestmark = pytest.mark.asyncio


def _limiter(clock: RealClock, **buckets: TokenBucket) -> MultiResourceLimiter:
    limiter = MultiResourceLimiter(buckets, clock)
    limiter.start()  # real create_task under the real loop
    return limiter


async def test_openai_client_completes_over_a_real_socket(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, prov = provider
    clock = RealClock()
    limiter = _limiter(
        clock,
        requests=TokenBucket(1000.0, 1000.0, clock),
        tokens=TokenBucket(1_000_000.0, 1_000_000.0, clock),
    )
    gate = Gate(clock)
    retry = RetryPolicy(clock, random.Random(0), base_delay=0.01, max_delay=0.1)
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as http:
            client = OpenAICompatClient(
                http, limiter, gate, retry, clock, model="fake-1"
            )
            data = await client.complete(
                [{"role": "user", "content": "hello there"}], max_tokens=16
            )
    finally:
        await limiter.aclose()
    assert data["usage"]["total_tokens"] == 24  # provider returns max_tokens + 8
    assert prov.seen == 1


async def test_http_fetcher_gets_a_real_response(
    provider: tuple[str, FakeProvider],
) -> None:
    # The proof that the core owes nothing to LLMs: a fetcher with only a requests
    # dimension, no tokens, drives the same arbiter and gate.
    base_url, _prov = provider
    clock = RealClock()
    limiter = _limiter(clock, requests=TokenBucket(1000.0, 1000.0, clock))
    gate = Gate(clock)
    retry = RetryPolicy(clock, random.Random(0), base_delay=0.01)
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as http:
            fetcher = HttpFetcher(http, limiter, gate, retry, clock)
            resp = await fetcher.fetch(
                "POST", "/v1/chat/completions", json={"max_tokens": 8}
            )
    finally:
        await limiter.aclose()
    assert resp.status_code == 200


async def test_a_dropped_connection_surfaces_and_is_not_retried(
    provider: tuple[str, FakeProvider],
) -> None:
    # A body dropped mid-stream is a real RemoteProtocolError, not a constructed one;
    # classify_httpx must call it ambiguous, which the default policy does not retry,
    # so it surfaces once rather than silently paying twice.
    base_url, prov = provider
    clock = RealClock()
    limiter = _limiter(clock, requests=TokenBucket(1000.0, 1000.0, clock))
    gate = Gate(clock)
    retry = RetryPolicy(clock, random.Random(0), base_delay=0.01)
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as http:
            fetcher = HttpFetcher(http, limiter, gate, retry, clock)
            with pytest.raises(httpx.RemoteProtocolError):
                await fetcher.fetch(
                    "POST",
                    "/v1/chat/completions",
                    json={},
                    headers={"x-fake-behavior": "drop"},
                )
    finally:
        await limiter.aclose()
    assert prov.seen == 1  # reached the server once, and was not retried


async def test_the_client_backs_off_when_the_provider_throttles(
    limited_provider: tuple[str, FakeProvider],
) -> None:
    # The provider allows five requests a second. Fire ten concurrently through one
    # shared gate: the ones that earn a 429 defer to the gate, wait out its short
    # retry-after, and complete on a retry -- so all ten succeed, none raises.
    base_url, prov = limited_provider
    clock = RealClock()
    limiter = _limiter(
        clock,
        requests=TokenBucket(1000.0, 1000.0, clock),
        tokens=TokenBucket(1_000_000.0, 1_000_000.0, clock),
    )
    gate = Gate(clock)
    retry = RetryPolicy(clock, random.Random(0), base_delay=0.02, max_delay=0.2)
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as http:
            client = OpenAICompatClient(
                http, limiter, gate, retry, clock, model="fake-1"
            )
            results = await asyncio.gather(
                *(
                    client.complete([{"role": "user", "content": "hi"}], max_tokens=8)
                    for _ in range(10)
                )
            )
    finally:
        await limiter.aclose()
    assert len(results) == 10
    assert all(r["usage"]["total_tokens"] == 16 for r in results)
    assert prov.seen >= 10  # some requests took a throttled attempt before landing
