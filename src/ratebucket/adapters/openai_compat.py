"""An OpenAI-compatible chat-completions client built on the domain-free core.

What the adapter adds over the core is exactly the three things the core refuses to
know about:

*httpx's failure modes.* A connect timeout never reached the server and is safe to
retry; a read timeout or a body dropped mid-stream may have been executed and billed,
so it is ambiguous and, by the stage-4 policy, not retried by default. `classify_httpx`
draws that line; the core only sees the resulting outcome.

*Token cost.* A request's token cost is not known until the provider answers, so the
adapter reserves an upper bound -- an estimate of the prompt plus `max_tokens` -- and
reconciles against the `usage` the response reports. An estimate that misses does not
corrupt the count: the reservation is a guess, `usage` is the truth, and the bucket is
adjusted by the difference on commit (downward, or negative if the estimate came in low).

*The provider's own counters.* Every response carries `x-ratelimit-remaining-*` headers
stating what the provider thinks is left. The adapter folds those back into the buckets
through `MultiResourceLimiter.observe`, which keeps the newest and drops any a race
delivered out of order.

Limitations (all consequences of not being able to see the future):

- The reservation blocks its full upper bound for the length of the request. A prompt
  that reserves 4000 tokens and uses 20 has kept 3980 tokens out of everyone else's
  reach until it commits. That is the price of never over-granting; a tighter estimator
  (the `tiktoken` extra) narrows it.
- An ambiguous failure releases the whole reservation. If a read timeout fired after the
  provider had already executed and billed the request, the release hands back tokens
  that were in fact spent, and the bucket is optimistic until the next response's
  `x-ratelimit-*` headers correct it. Retrying ambiguous failures would risk paying
  twice instead; the default chooses the desync it can later repair over the double bill.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import httpx

    from ratebucket.core.clock import Clock
    from ratebucket.core.gate import Gate
    from ratebucket.core.limiter import MultiResourceLimiter
    from ratebucket.core.retry import Outcome, RetryPolicy

from ratebucket.adapters._httpx import classify_httpx

__all__ = [
    "HeuristicEstimator",
    "OpenAICompatClient",
    "TokenEstimator",
]

_NANOS_PER_SECOND = 1_000_000_000


class TokenEstimator(Protocol):
    """Estimates the prompt tokens of a message list before the request is sent.

    The estimate only sizes the reservation, which is reconciled against the provider's
    reported usage afterwards, so a miss makes the reservation temporarily too large or
    too small -- it never corrupts the count.
    """

    def estimate(self, messages: Sequence[Mapping[str, Any]]) -> int: ...


class HeuristicEstimator:
    """The default estimator: roughly four characters per token, and no dependency.

    It is deliberately crude. Measured against tiktoken's `cl100k_base` over a spread of
    English chat prompts (bench/estimator_error.py) its mean absolute error is about 16%,
    and it can under-count token-dense input -- code, numbers, punctuation -- by as much
    as ~45%. Reserve-and-reconcile absorbs the miss: the estimate only sizes the
    reservation, and the commit corrects it to the reported usage, so an error costs a
    little transient fairness, never accuracy. The default constant is 4.0, the familiar
    rule of thumb, which biases the estimate very slightly high on prose (about +4%) --
    the safe direction for a reservation. Pass `chars_per_token` to retune it, or install
    the `tiktoken` extra and supply an exact `TokenEstimator` when the reservation must be
    tight rather than merely safe. See docs/adr/0005.
    """

    def __init__(self, *, chars_per_token: float = 4.0) -> None:
        if chars_per_token <= 0:
            raise ValueError("chars_per_token must be positive")
        self._chars_per_token = chars_per_token

    def estimate(self, messages: Sequence[Mapping[str, Any]]) -> int:
        chars = 0
        for message in messages:
            content = message.get("content", "")
            if isinstance(content, str):
                chars += len(content)
        return max(1, round(chars / self._chars_per_token))


class OpenAICompatClient:
    """Chat completions over a limiter, a shared gate, and a retry policy.

    Every request reserves `1` request and an estimate of its token cost, runs under the
    retry policy (which defers 429s to the pool-wide gate), commits the actual usage the
    provider reports, and folds the provider's `x-ratelimit-*` headers back into the
    buckets. The limiter and gate are shared across clients on purpose: one pool, one
    arbiter, one pause.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        limiter: MultiResourceLimiter,
        gate: Gate,
        retry: RetryPolicy,
        clock: Clock,
        *,
        model: str,
        estimator: TokenEstimator | None = None,
        requests_dimension: str = "requests",
        tokens_dimension: str = "tokens",
        endpoint: str = "/v1/chat/completions",
    ) -> None:
        self._http = http
        self._limiter = limiter
        self._gate = gate
        self._retry = retry
        self._clock = clock
        self._model = model
        self._estimator = estimator or HeuristicEstimator()
        self._req_dim = requests_dimension
        self._tok_dim = tokens_dimension
        self._endpoint = endpoint

    async def complete(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        max_tokens: int,
        **params: Any,  # noqa: ANN401 - extra provider params pass verbatim into the JSON body
    ) -> dict[str, Any]:
        """Send one chat completion, reserving up front and reconciling on the reply.

        The reservation covers the whole retry sequence: while the policy backs off or
        the gate holds a pause, the tokens stay reserved, because the request is still
        in flight. On success the reservation is committed to the actual usage; on
        exhaustion or a non-retried failure the whole reservation is released.
        """
        prompt_estimate = self._estimator.estimate(messages)
        upper_bound = prompt_estimate + max_tokens
        reserve = {self._req_dim: 1, self._tok_dim: upper_bound}
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": list(messages),
            "max_tokens": max_tokens,
            **params,
        }
        async with self._limiter.reserve(reserve) as reservation:
            data, observation = await self._retry.execute(
                lambda _epoch: self._send(payload),
                self._classify,
                gate=self._gate,
            )
            usage = data.get("usage") or {}
            total = _as_int(usage.get("total_tokens"), default=upper_bound)
            reservation.commit({self._req_dim: 1, self._tok_dim: total})
            # Fold the provider's own counters in last, after the reservation has been
            # reconciled, so its number is the final word and a give-back cannot undo it.
            if observation is not None:
                remaining, sent_ns = observation
                self._limiter.observe(remaining, observed_ns=sent_ns)
            return data

    def _classify(self, exc: Exception) -> tuple[Outcome, float | None]:
        # `now` is read here, at classification time, so a Retry-After given as an
        # HTTP-date is measured against the clock the rest of the stack runs on.
        return classify_httpx(exc, now=self._clock.now())

    async def _send(
        self, payload: dict[str, Any]
    ) -> tuple[dict[str, Any], tuple[dict[str, float], int] | None]:
        # The instant the request leaves is the instant the response's remaining-counts
        # describe, so it is stamped before the await and carried out with the reply for
        # `complete` to apply once the reservation has been reconciled.
        sent_ns = round(self._clock.now() * _NANOS_PER_SECOND)
        response = await self._http.post(self._endpoint, json=payload)
        response.raise_for_status()  # 4xx/5xx -> HTTPStatusError, classified by _classify
        data: dict[str, Any] = response.json()
        remaining = self._read_ratelimit_headers(response.headers)
        return data, (remaining, sent_ns) if remaining else None

    def _read_ratelimit_headers(self, headers: httpx.Headers) -> dict[str, float]:
        remaining: dict[str, float] = {}
        raw_requests = headers.get("x-ratelimit-remaining-requests")
        raw_tokens = headers.get("x-ratelimit-remaining-tokens")
        if raw_requests is not None and raw_requests.lstrip("-").isdigit():
            remaining[self._req_dim] = float(raw_requests)
        if raw_tokens is not None and raw_tokens.lstrip("-").isdigit():
            remaining[self._tok_dim] = float(raw_tokens)
        return remaining


def _as_int(value: object, *, default: int) -> int:
    if isinstance(
        value, bool
    ):  # bool is an int subclass; a usage count is never a bool
        return default
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default
