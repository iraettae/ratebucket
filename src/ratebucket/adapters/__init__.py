"""Adapters over the domain-free core. Each is a first user of the arbiter, gate and
retry policy; none of them is imported by the top-level package, so httpx stays an
optional dependency you pull in through the `openai` extra.
"""

__all__: list[str] = []
