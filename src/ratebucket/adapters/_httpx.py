"""Mapping httpx's transport failures to the core's outcomes.

The distinction the core cannot make on its own: a connect failure never reached the
server and is safe to retry, while a read timeout or a body dropped mid-stream may have
been executed -- and, for a paid API, billed -- so it is ambiguous and not retried by
default. Only the adapter imports httpx, so only the adapter knows these types.
"""

from __future__ import annotations

from email.utils import parsedate_to_datetime

import httpx

from ratebucket.core.retry import Outcome, classify_status


def parse_retry_after(value: str | None, *, now: float) -> float | None:
    """Parse a Retry-After header: either delta-seconds or an HTTP-date."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - now)


def classify_httpx(exc: Exception, *, now: float) -> tuple[Outcome, float | None]:
    """Map an httpx exception (or an HTTP error response) to an outcome."""
    if isinstance(exc, httpx.HTTPStatusError):
        retry_after = parse_retry_after(
            exc.response.headers.get("retry-after"), now=now
        )
        return classify_status(exc.response.status_code), retry_after
    if isinstance(exc, httpx.ConnectTimeout | httpx.ConnectError):
        return Outcome.RETRYABLE, None  # never reached the server
    if isinstance(exc, httpx.ReadTimeout | httpx.RemoteProtocolError | httpx.ReadError):
        return (
            Outcome.AMBIGUOUS,
            None,
        )  # may have been executed; do not retry by default
    if isinstance(exc, httpx.TransportError):
        return Outcome.RETRYABLE, None  # pool/write timeouts: the request did not land
    return Outcome.PERMANENT, None  # not a transport failure; surface it, do not retry
