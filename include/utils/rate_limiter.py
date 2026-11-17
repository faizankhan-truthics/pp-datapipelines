"""Thread-safe token-bucket rate limiter shared by hooks that call rate-limited APIs.

Each Airflow task runs in its own worker process, so a limiter registered here caps
throughput per task instance (e.g. per Solidgate account, per Stripe operator run) —
not globally across the whole deployment. Within a task, all threads that call
``get_rate_limiter`` with the same ``name`` share one bucket (e.g. Stripe's
per-day worker threads), since limiters are cached as process-wide singletons.
"""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """Caps throughput to ``rate`` calls per second, shared across threads.

    Token bucket: refills continuously at ``rate`` tokens/sec up to a burst
    ceiling of ``rate`` tokens. Callers block in ``acquire()`` only once the
    instantaneous rate would exceed the cap.
    """

    def __init__(self, rate: float) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate!r}")
        self.rate = rate
        # Burst capacity is at least 1 token so rates below 1/sec (e.g. "once every
        # 10 seconds") can still ever accumulate a full token instead of blocking forever.
        self._capacity = max(rate, 1.0)
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._last_refill = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self.rate)
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self.rate
            time.sleep(wait)


_limiters: dict[str, RateLimiter] = {}
_limiters_lock = threading.Lock()


def get_rate_limiter(name: str, rate: float) -> RateLimiter:
    """Return the process-wide singleton limiter registered under ``name``.

    The limiter is created on first use with the given ``rate``; later calls
    with the same ``name`` return the same instance regardless of ``rate``.
    """
    with _limiters_lock:
        limiter = _limiters.get(name)
        if limiter is None:
            limiter = RateLimiter(rate)
            _limiters[name] = limiter
        return limiter
