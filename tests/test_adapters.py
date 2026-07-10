"""The adapters, driven on the virtual clock with scripted replies.

Everything here is deterministic: the http client is a stand-in that returns canned
`httpx.Response` objects (or raises canned httpx exceptions), and time is the virtual
clock, so a reservation, a reconciliation, a throttle deferred to the gate, and a stale
header all play out without a socket or a wall-clock sleep. The real-socket path lives
in test_adapters_live.py.
"""

from __future__ import annotations

import random
from typing import Any

import httpx
import pytest

from ratebucket import (
    Gate,
    MultiResourceLimiter,
    Outcome,
    RetryPolicy,
    TokenBucket,
    VirtualClock,
)
from ratebucket.adapters._httpx import classify_httpx, parse_retry_after
from ratebucket.adapters.openai_compat import HeuristicEstimator, OpenAICompatClient

pytestmark = pytest.mark.asyncio

_ENDPOINT = "http://fake.test/v1/chat/completions"


# --- the httpx classifier ---------------------------------------------------


def _status_error(
    code: int, headers: dict[str, str] | None = None
) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", _ENDPOINT)
    response = httpx.Response(code, headers=headers or {}, request=request)
    return httpx.HTTPStatusError(f"{code}", request=request, response=response)


async def test_connect_failures_are_retryable_they_never_reached_the_server() -> None:
    for exc in (httpx.ConnectTimeout("t"), httpx.ConnectError("t")):
        assert classify_httpx(exc, now=0.0) == (Outcome.RETRYABLE, None)


async def test_read_timeout_and_dropped_body_are_ambiguous() -> None:
    # Either may have been executed and billed; the classifier must not call them
    # cleanly retryable, or the default policy would risk paying twice.
    for exc in (
        httpx.ReadTimeout("t"),
        httpx.RemoteProtocolError("t"),
        httpx.ReadError("t"),
    ):
        assert classify_httpx(exc, now=0.0) == (Outcome.AMBIGUOUS, None)


async def test_status_errors_map_by_code_and_carry_retry_after() -> None:
    assert classify_httpx(_status_error(429, {"retry-after": "20"}), now=0.0) == (
        Outcome.THROTTLED,
        20.0,
    )
    assert classify_httpx(_status_error(500), now=0.0) == (Outcome.RETRYABLE, None)
    assert classify_httpx(_status_error(400), now=0.0) == (Outcome.PERMANENT, None)
    assert classify_httpx(_status_error(401), now=0.0) == (Outcome.FATAL, None)


async def test_a_non_transport_exception_is_permanent_not_retried() -> None:
    # A bug in response handling (a KeyError, say) is this request's fault, not a fault
    # to hammer the provider over.
    assert classify_httpx(KeyError("usage"), now=0.0) == (Outcome.PERMANENT, None)


async def test_retry_after_parses_seconds_and_http_dates() -> None:
    assert parse_retry_after("30", now=1000.0) == 30.0
    # An HTTP-date is measured against the clock: Thu, 01 Jan 1970 00:00:40 GMT is 40s.
    assert parse_retry_after("Thu, 01 Jan 1970 00:00:40 GMT", now=10.0) == 30.0
    assert parse_retry_after(None, now=0.0) is None
    assert parse_retry_after("garbage", now=0.0) is None


# --- the default estimator --------------------------------------------------


async def test_heuristic_estimator_is_about_four_characters_a_token() -> None:
    est = HeuristicEstimator()
    assert est.estimate([{"role": "user", "content": "x" * 40}]) == 10


async def test_heuristic_estimator_never_returns_zero() -> None:
    # An empty prompt still reserves at least one token, so the request has a cost.
    assert HeuristicEstimator().estimate([{"role": "user", "content": ""}]) == 1


# --- a scripted http client and the adapter stack ---------------------------


class ScriptedHttp:
    """Stands in for httpx.AsyncClient: each call consumes the next scripted reply,
    returning a canned response or raising a canned exception."""

    def __init__(self, script: list[httpx.Response | Exception]) -> None:
        self._script = list(script)
        self.calls = 0
        self.sent: list[Any] = []

    async def post(self, url: str, *, json: Any = None) -> httpx.Response:
        self.calls += 1
        self.sent.append(json)
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        return await self.post(url, json=kwargs.get("json"))


def _response(
    status: int, *, body: dict[str, Any], headers: dict[str, str] | None = None
) -> httpx.Response:
    request = httpx.Request("POST", _ENDPOINT)
    return httpx.Response(status, json=body, headers=headers or {}, request=request)


def _completion(total_tokens: int) -> dict[str, Any]:
    return {
        "id": "chatcmpl-fake",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}],
        "usage": {"total_tokens": total_tokens},
    }


def _stack(
    clock: VirtualClock,
    http: ScriptedHttp,
    *,
    tok_cap: float = 1000.0,
    req_cap: float = 100.0,
    **policy: Any,
) -> tuple[OpenAICompatClient, dict[str, TokenBucket], Gate]:
    buckets = {
        "requests": TokenBucket(req_cap, 0.0, clock),  # rate 0: levels stay exact
        "tokens": TokenBucket(tok_cap, 0.0, clock),
    }
    limiter = MultiResourceLimiter(buckets, clock)
    limiter.start(clock.spawn)
    gate = Gate(clock)
    retry = RetryPolicy(clock, random.Random(0), **policy)
    client = OpenAICompatClient(http, limiter, gate, retry, clock, model="fake-1")  # type: ignore[arg-type]
    return client, buckets, gate


async def _drive(clock: VirtualClock, coro: Any) -> dict[str, Any]:
    box: dict[str, Any] = {}

    async def go() -> None:
        try:
            box["value"] = await coro
        except Exception as exc:  # captured for the assertions, like the retry driver
            box["error"] = exc

    _ = clock.spawn(go())
    await clock.advance(600.0)
    await clock.aclose()
    return box


_PROMPT = [{"role": "user", "content": "x" * 40}]  # 40 chars -> estimate 10 tokens


async def test_complete_reconciles_the_reservation_down_to_reported_usage() -> None:
    clock = VirtualClock()
    http = ScriptedHttp([_response(200, body=_completion(50))])  # no ratelimit headers
    client, buckets, _ = _stack(clock, http, tok_cap=1000.0)
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert "error" not in box
    # Reserved 10+100=110 tokens, used 50: the 60 over-reserved come back.
    assert buckets["tokens"].available() == 950.0
    assert buckets["requests"].available() == 99.0


async def test_a_low_estimate_drives_the_tokens_bucket_negative() -> None:
    clock = VirtualClock()
    http = ScriptedHttp([_response(200, body=_completion(500))])
    client, buckets, _ = _stack(clock, http, tok_cap=200.0)
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert "error" not in box
    # Reserved 110 (200 -> 90), actually used 500: the bucket records the 300 overdraft
    # rather than clamping it away and under-counting the next window.
    assert buckets["tokens"].available() == -300.0


async def test_an_ambiguous_failure_releases_the_whole_reservation() -> None:
    clock = VirtualClock()
    http = ScriptedHttp([httpx.ReadTimeout("read timed out")])
    client, buckets, _ = _stack(
        clock, http, tok_cap=1000.0
    )  # default: ambiguous not retried
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert isinstance(box["error"], httpx.ReadTimeout)
    # The block exited without committing, so the reservation is handed back in full.
    # (If the request had in fact been billed, the bucket is now optimistic until the
    # next response's headers correct it -- the documented desync.)
    assert buckets["tokens"].available() == 1000.0
    assert buckets["requests"].available() == 100.0


async def test_a_throttle_defers_to_the_gate_then_succeeds() -> None:
    clock = VirtualClock()
    http = ScriptedHttp(
        [
            _response(
                429,
                body={"error": {"message": "rate limit"}},
                headers={"retry-after": "5"},
            ),
            _response(200, body=_completion(50)),
        ]
    )
    client, _buckets, gate = _stack(clock, http, tok_cap=1000.0, max_attempts=3)
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert "error" not in box
    assert http.calls == 2  # the 429 waited out the gate, then the retry landed
    assert gate.epoch >= 1  # the gate did close


async def test_response_headers_lower_the_bucket_to_the_servers_remaining() -> None:
    clock = VirtualClock()
    http = ScriptedHttp(
        [
            _response(
                200,
                body=_completion(50),
                headers={
                    "x-ratelimit-remaining-tokens": "42",
                    "x-ratelimit-remaining-requests": "7",
                },
            )
        ]
    )
    client, buckets, _ = _stack(clock, http, tok_cap=1000.0, req_cap=100.0)
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert "error" not in box
    # The provider says less is left than our local count; applied last (after commit),
    # it lowers the buckets to match so we back off before earning a 429.
    assert buckets["tokens"].available() == 42.0
    assert buckets["requests"].available() == 7.0


async def test_generous_response_headers_do_not_raise_the_bucket() -> None:
    clock = VirtualClock()
    http = ScriptedHttp(
        [
            _response(
                200,
                body=_completion(50),
                headers={"x-ratelimit-remaining-tokens": "900000"},
            )
        ]
    )
    client, buckets, _ = _stack(clock, http, tok_cap=1000.0)
    box = await _drive(clock, client.complete(_PROMPT, max_tokens=100))
    assert "error" not in box
    # The provider reports far more room than our small local cap; observe must not
    # raise us, or the self-imposed limit would not bind. Stays at the post-commit 950.
    assert buckets["tokens"].available() == 950.0


async def test_stale_ratelimit_headers_ignored() -> None:
    # Responses race. A header from a request sent earlier can arrive after a newer one
    # has already been applied; taking the stale one would drop the bucket below what
    # the fresher observation established. The observed-at timestamp guards against it.
    clock = VirtualClock()
    tokens = TokenBucket(1000.0, 0.0, clock)
    limiter = MultiResourceLimiter({"tokens": tokens}, clock)

    limiter.observe({"tokens": 900.0}, observed_ns=5_000)  # fresh: 900 remain
    assert tokens.available() == 900.0

    limiter.observe({"tokens": 100.0}, observed_ns=3_000)  # older send-time: ignored
    assert tokens.available() == 900.0

    limiter.observe({"tokens": 50.0}, observed_ns=9_000)  # genuinely newer: applied
    assert tokens.available() == 50.0


async def test_observe_ignores_dimensions_the_limiter_does_not_model() -> None:
    clock = VirtualClock()
    tokens = TokenBucket(1000.0, 0.0, clock)
    limiter = MultiResourceLimiter({"tokens": tokens}, clock)
    limiter.observe(
        {"images": 3.0}, observed_ns=1
    )  # unknown dimension: quietly skipped
    assert tokens.available() == 1000.0
