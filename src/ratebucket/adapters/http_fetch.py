"""A plain HTTP fetcher on the same core, to show the core owes nothing to LLMs.

Its only cost is one request; there is no token dimension, no usage to reconcile, no
provider counters to fold back. The same arbiter, gate, and retry policy that pace an
OpenAI-compatible client here pace ordinary GETs against any host that rate-limits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx

    from ratebucket.core.clock import Clock
    from ratebucket.core.gate import Gate
    from ratebucket.core.limiter import MultiResourceLimiter
    from ratebucket.core.retry import Outcome, RetryPolicy

from ratebucket.adapters._httpx import classify_httpx

__all__ = ["HttpFetcher"]


class HttpFetcher:
    """Rate-limited HTTP requests whose only cost is `{requests_dimension: 1}`."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        limiter: MultiResourceLimiter,
        gate: Gate,
        retry: RetryPolicy,
        clock: Clock,
        *,
        requests_dimension: str = "requests",
    ) -> None:
        self._http = http
        self._limiter = limiter
        self._gate = gate
        self._retry = retry
        self._clock = clock
        self._cost = {requests_dimension: 1}

    async def fetch(self, method: str, url: str, **kwargs: Any) -> httpx.Response:  # noqa: ANN401 - forwarded verbatim to httpx.AsyncClient.request
        """Send one request under the limiter, retrying transient failures."""
        async with self._limiter.reserve(self._cost) as reservation:
            response = await self._retry.execute(
                lambda _epoch: self._send(method, url, **kwargs),
                self._classify,
                gate=self._gate,
            )
            reservation.commit(self._cost)
            return response

    def _classify(self, exc: Exception) -> tuple[Outcome, float | None]:
        return classify_httpx(exc, now=self._clock.now())

    async def _send(self, method: str, url: str, **kwargs: Any) -> httpx.Response:  # noqa: ANN401 - forwarded verbatim to httpx.AsyncClient.request
        response = await self._http.request(method, url, **kwargs)
        response.raise_for_status()
        return response
