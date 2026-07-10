"""The fake provider produces each failure over a real socket, not a mock transport."""

from __future__ import annotations

import json
import time

import httpx
import pytest

from fake_provider import FakeProvider

pytestmark = pytest.mark.asyncio


async def test_normal_completion_carries_usage_and_ratelimit_headers(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        r = await client.post("/v1/chat/completions", json={"max_tokens": 16})
    assert r.status_code == 200
    assert r.json()["usage"]["total_tokens"] == 24
    assert "x-ratelimit-remaining-requests" in r.headers
    assert "x-ratelimit-remaining-tokens" in r.headers


async def test_throttle_is_429_with_retry_after(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        r = await client.post(
            "/v1/chat/completions", json={}, headers={"x-fake-behavior": "throttle"}
        )
    assert r.status_code == 429
    assert r.headers["retry-after"] == "20"


async def test_throttle_without_retry_after(provider: tuple[str, FakeProvider]) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        r = await client.post(
            "/v1/chat/completions",
            json={},
            headers={"x-fake-behavior": "throttle-no-retry-after"},
        )
    assert r.status_code == 429
    assert "retry-after" not in r.headers


async def test_server_error_is_500(provider: tuple[str, FakeProvider]) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        r = await client.post(
            "/v1/chat/completions", json={}, headers={"x-fake-behavior": "server-error"}
        )
    assert r.status_code == 500


async def test_malformed_body_is_200_but_unparseable(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        r = await client.post(
            "/v1/chat/completions", json={}, headers={"x-fake-behavior": "malformed"}
        )
    assert r.status_code == 200
    with pytest.raises(json.JSONDecodeError):
        r.json()


async def test_slow_response_takes_the_delay(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        start = time.monotonic()
        r = await client.post(
            "/v1/chat/completions", json={}, headers={"x-fake-behavior": "slow:0.4"}
        )
    assert r.status_code == 200
    assert time.monotonic() - start >= 0.4


async def test_read_timeout_when_the_response_is_slower_than_the_deadline(
    provider: tuple[str, FakeProvider],
) -> None:
    base_url, _ = provider
    async with httpx.AsyncClient(
        base_url=base_url, timeout=httpx.Timeout(5.0, read=0.2)
    ) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.post(
                "/v1/chat/completions", json={}, headers={"x-fake-behavior": "slow:1.0"}
            )


async def test_connection_dropped_mid_body_raises_remote_protocol_error(
    provider: tuple[str, FakeProvider],
) -> None:
    # The scenario a mock transport cannot produce: headers and a partial body arrive,
    # then the socket closes. The request may or may not have been executed.
    base_url, _ = provider
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        with pytest.raises(httpx.RemoteProtocolError):
            await client.post(
                "/v1/chat/completions", json={}, headers={"x-fake-behavior": "drop"}
            )


async def test_the_provider_enforces_its_own_rate_limit(
    limited_provider: tuple[str, FakeProvider],
) -> None:
    # Five requests a second are allowed; the sixth in the same second is throttled.
    base_url, _ = limited_provider
    codes: list[int] = []
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        for _ in range(7):
            r = await client.post("/v1/chat/completions", json={"max_tokens": 8})
            codes.append(r.status_code)
    assert codes[:5] == [200, 200, 200, 200, 200]
    assert 429 in codes[5:]
