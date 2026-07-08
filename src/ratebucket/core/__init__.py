"""Domain-free core: clocks, buckets, the arbiter, the gate, the retry policy.

Nothing in this package knows what it is rate-limiting. That is not modesty, it
is the load-bearing constraint: `make core-purity` greps this directory and
fails the build if it finds the domain leaking in.
"""

__all__: list[str] = []
