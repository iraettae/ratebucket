"""An OpenAI-compatible provider that fails in the specific ways a real one does.

It runs on a real socket under uvicorn, on purpose: a mock transport can fake a 429
or a 500, but it cannot drop a connection halfway through the response body, and that
-- a request that may or may not have been executed -- is the failure worth testing.

Behaviour is chosen per request by an `X-Fake-Behavior` header so a test can ask for
exactly the failure it means to exercise; with no header the endpoint answers 200 with
a realistic usage block and `x-ratelimit-*` headers, and enforces its own requests- and
tokens-per-minute limits so a load test gets real 429s.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route


class _Window:
    """A fixed one-second window counter, so the provider throttles under load."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._start = time.monotonic()
        self._used = 0

    def take(self, n: int) -> bool:
        now = time.monotonic()
        if now - self._start >= 1.0:
            self._start = now
            self._used = 0
        if self._used + n > self._limit:
            return False
        self._used += n
        return True

    def remaining(self) -> int:
        return max(0, self._limit - self._used)


class FakeProvider:
    """Builds the Starlette app. One instance per test keeps the counters isolated."""

    def __init__(
        self,
        *,
        rps_limit: int = 1_000_000,
        tps_limit: int = 1_000_000_000,
        fail_every: int = 0,
        drop_every: int = 0,
        throttle_period_s: float = 0.0,
        throttle_duration_s: float = 0.0,
    ) -> None:
        self._requests = _Window(rps_limit)
        self._tokens = _Window(tps_limit)
        self.seen = 0
        # Off in the tests (all zero); the benchmark turns them on for a load run that
        # sees occasional 500s and dropped bodies alongside the rate-limit 429s.
        self._fail_every = fail_every
        self._drop_every = drop_every
        # Exogenous throttling: 429 everything for `throttle_duration_s` on each
        # `throttle_period_s` cycle, a shared quota briefly exhausted from outside.
        self._throttle_period_s = throttle_period_s
        self._throttle_duration_s = throttle_duration_s
        self._started = time.perf_counter()
        # (arrival time, status) per request on the load path, for the benchmark
        # timeline. -1 marks a body dropped mid-stream. Unused by the tests.
        self.events: list[tuple[float, int]] = []

    def _record(self, status: int) -> None:
        self.events.append((time.perf_counter(), status))

    def _in_throttle_window(self) -> bool:
        if self._throttle_period_s <= 0:
            return False
        phase = (time.perf_counter() - self._started) % self._throttle_period_s
        return phase < self._throttle_duration_s

    def build(self) -> Starlette:
        return Starlette(
            routes=[Route("/v1/chat/completions", self._complete, methods=["POST"])]
        )

    async def _complete(self, request: Request) -> Response:
        self.seen += 1
        behavior = request.headers.get("x-fake-behavior", "ok")

        if behavior == "throttle":
            return JSONResponse(
                {"error": {"message": "rate limit", "type": "rate_limit_error"}},
                status_code=429,
                headers={"retry-after": "20"},
            )
        if behavior == "throttle-no-retry-after":
            return JSONResponse({"error": {"message": "rate limit"}}, status_code=429)
        if behavior == "server-error":
            return JSONResponse({"error": {"message": "internal"}}, status_code=500)
        if behavior == "malformed":
            # A 200 whose body is not the JSON the client will try to parse.
            return Response(
                b'{"id": "chatcmpl", "choices": [', media_type="application/json"
            )
        if behavior.startswith("slow:"):
            await asyncio.sleep(float(behavior.split(":", 1)[1]))
        if behavior == "drop":
            return StreamingResponse(
                self._drop_mid_body(), media_type="application/json"
            )

        # The load-limit path: enforce the provider's own quotas and 429 past them.
        body = await request.json()
        tokens = int(body.get("max_tokens", 16)) + 8

        # Exogenous throttling: a shared quota briefly exhausted, 429 no client could
        # have paced around. Checked before the rate window so it 429s even a paced one.
        if self._in_throttle_window():
            self._record(429)
            return JSONResponse(
                {"error": {"message": "rate limit"}},
                status_code=429,
                headers={"retry-after": "1"},
            )

        # Deterministic fault injection for the benchmark (both knobs zero in tests).
        if self._fail_every and self.seen % self._fail_every == 0:
            self._record(500)
            return JSONResponse({"error": {"message": "internal"}}, status_code=500)
        if self._drop_every and self.seen % self._drop_every == 0:
            self._record(-1)  # the client will see a body dropped mid-stream
            return StreamingResponse(
                self._drop_mid_body(), media_type="application/json"
            )

        if not self._requests.take(1) or not self._tokens.take(tokens):
            self._record(429)
            return JSONResponse(
                {"error": {"message": "rate limit"}},
                status_code=429,
                headers={"retry-after": "1"},
            )
        self._record(200)
        return JSONResponse(
            _completion(tokens),
            headers={
                "x-ratelimit-remaining-requests": str(self._requests.remaining()),
                "x-ratelimit-remaining-tokens": str(self._tokens.remaining()),
            },
        )

    async def _drop_mid_body(self) -> AsyncIterator[bytes]:
        # Headers and a partial body go out, then the generator raises. uvicorn closes
        # the connection without a complete body, so the client sees a truncated read --
        # the request may or may not have been "executed". This is the yak.
        yield b'{"id": "chatcmpl-fake", "object": "chat.completion", "choices": [{"index": 0, '
        raise RuntimeError("connection dropped mid-body")


def _completion(tokens: int) -> dict[str, Any]:
    return {
        "id": "chatcmpl-fake",
        "object": "chat.completion",
        "model": "fake-1",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 8,
            "completion_tokens": tokens - 8,
            "total_tokens": tokens,
        },
    }


class RunningServer:
    """Runs a Starlette app under uvicorn on an ephemeral port, in a background thread."""

    def __init__(self, app: Starlette) -> None:
        config = uvicorn.Config(
            app, host="127.0.0.1", port=0, log_level="critical", loop="asyncio"
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> str:
        self._thread.start()
        deadline = time.monotonic() + 10.0
        while not self._server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self._server.started:  # pragma: no cover - startup failure
            raise RuntimeError("fake provider did not start")
        # The bound socket's real port, since we asked for 0.
        port = self._server.servers[0].sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10.0)


@contextlib.contextmanager
def running(provider: FakeProvider) -> Iterator[str]:
    with RunningServer(provider.build()) as base_url:
        yield base_url
