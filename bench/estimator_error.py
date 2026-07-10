"""Measure the heuristic estimator (chars/N) against tiktoken's cl100k_base.

Reproduces the numbers in docs/adr/0005. Run with the tiktoken extra:

    uv run --extra tiktoken python bench/estimator_error.py

Reports, over a spread of representative English chat prompts, the signed and absolute
relative error of chars/N for several N, then the worst per-prompt offenders for the
default N=4.0 so the drift has a face: token-dense input (numbers, punctuation, code).
"""

from __future__ import annotations

import statistics

import tiktoken

from ratebucket.adapters.openai_compat import HeuristicEstimator

_ENC = tiktoken.get_encoding("cl100k_base")

# Realistic chat-prompt shapes: short questions, prose, instructions, code, lists, SQL,
# a regex ask, and a longer multi-paragraph question. Not chosen to flatter chars/4 --
# the code and punctuation-heavy entries are exactly where a character count drifts.
CORPUS = [
    "What's the capital of France?",
    "Summarize the following article in three bullet points.",
    "Translate 'good morning' into Japanese and give the romaji.",
    "Write a haiku about autumn rain.",
    "Explain the difference between a process and a thread to a beginner.",
    "Here is a Python function:\n\ndef add(a, b):\n    return a + b\n\nAdd type hints and a docstring.",
    "Given the list [3, 1, 4, 1, 5, 9, 2, 6], sort it and remove duplicates.",
    "You are a helpful assistant. Answer concisely and cite sources when possible.",
    "Draft a polite email declining a meeting invitation for next Tuesday at 3pm.",
    "def fib(n):\n    if n < 2:\n        return n\n    return fib(n-1) + fib(n-2)\n# Make this iterative and O(n).",
    "Compare REST and GraphQL for a mobile app backend. Cover caching, over-fetching, and versioning.",
    "List five ideas for a weekend trip within 200 miles of Seattle, with a one-line reason each.",
    "Rewrite this sentence to be more formal: 'hey can you send me that thing when you get a sec'",
    (
        "I'm building a rate limiter for an API that enforces both requests-per-minute "
        "and tokens-per-minute at the same time. If I use two independent token buckets "
        "and acquire from each in turn, can I deadlock? Walk through a concrete "
        "interleaving, then explain how a single arbiter that grants across both "
        "dimensions atomically avoids it."
    ),
    "SELECT user_id, COUNT(*) FROM events WHERE ts > now() - interval '1 day' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;",
    "Give me a regex that matches an ISO-8601 timestamp with an optional timezone offset.",
]


def _estimate(text: str, chars_per_token: float) -> int:
    est = HeuristicEstimator(chars_per_token=chars_per_token)
    return est.estimate([{"role": "user", "content": text}])


def main() -> None:
    for n in (3.5, 3.8, 4.0, 4.2):
        signed = [
            (_estimate(t, n) - len(_ENC.encode(t))) / len(_ENC.encode(t))
            for t in CORPUS
        ]
        abs_err = [abs(e) for e in signed]
        within = sum(e <= 0.15 for e in abs_err)
        print(
            f"chars/{n}: mean signed {statistics.mean(signed):+.1%}, "
            f"mean abs {statistics.mean(abs_err):.1%}, "
            f"median abs {statistics.median(abs_err):.1%}, "
            f"max abs {max(abs_err):.1%}, within-15% {within}/{len(abs_err)}"
        )

    print("\nchars/4 per-prompt, worst first:")
    rows = [
        (
            abs(_estimate(t, 4.0) - len(_ENC.encode(t))) / len(_ENC.encode(t)),
            _estimate(t, 4.0),
            len(_ENC.encode(t)),
            t[:48],
        )
        for t in CORPUS
    ]
    for rel, est, actual, sample in sorted(rows, reverse=True)[:6]:
        print(f"  {rel:>5.0%}  est={est:>4} actual={actual:>4}  {sample!r}")


if __name__ == "__main__":
    main()
