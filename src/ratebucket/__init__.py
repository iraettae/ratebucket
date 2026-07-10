"""An async resource arbiter for APIs with several simultaneous rate limits."""

from ratebucket.core.clock import Clock, ClockError, RealClock, VirtualClock
from ratebucket.core.gate import Gate
from ratebucket.core.limiter import (
    CostExceedsCapacityError,
    LimiterError,
    MultiResourceLimiter,
    Reservation,
    TokenBucket,
    UnknownResourceError,
)
from ratebucket.core.retry import Outcome, RetryError, RetryPolicy, classify_status

__version__ = "0.0.1"

__all__ = [
    "Clock",
    "ClockError",
    "CostExceedsCapacityError",
    "Gate",
    "LimiterError",
    "MultiResourceLimiter",
    "Outcome",
    "RealClock",
    "Reservation",
    "RetryError",
    "RetryPolicy",
    "TokenBucket",
    "UnknownResourceError",
    "VirtualClock",
    "__version__",
    "classify_status",
]
