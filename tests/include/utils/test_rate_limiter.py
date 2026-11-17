"""Unit tests for the shared token-bucket rate limiter
(include/utils/rate_limiter.py), used by every hook that calls a rate-limited
API (Stripe, Solidgate, Zendesk, ...).

Timing is driven entirely through monkeypatched ``time.monotonic``/``time.sleep``
so these tests are deterministic and instant -- no real waiting.
"""

from __future__ import annotations

import pytest

import include.utils.rate_limiter as rate_limiter_module
from include.utils.rate_limiter import RateLimiter, get_rate_limiter


class _FakeClock:
    """Lets a test move ``time.monotonic()`` forward explicitly, and records
    every ``time.sleep()`` call instead of actually sleeping."""

    def __init__(self, start: float = 0.0):
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> _FakeClock:
    fake = _FakeClock()
    monkeypatch.setattr(rate_limiter_module.time, "monotonic", fake.monotonic)
    monkeypatch.setattr(rate_limiter_module.time, "sleep", fake.sleep)
    return fake


class TestRateLimiterConstruction:
    def test_rejects_non_positive_rate(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            RateLimiter(0)

    def test_rejects_negative_rate(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            RateLimiter(-5)

    def test_burst_capacity_is_at_least_one_token_for_sub_1_rates(self, clock):
        # A rate like "1 call per 10 seconds" (rate=0.1) must still start with a
        # full token available, or the very first acquire() would block forever
        # waiting to accumulate from a capacity below 1.
        limiter = RateLimiter(0.1)

        limiter.acquire()

        assert clock.sleeps == []

    def test_burst_capacity_matches_rate_when_rate_is_at_least_1(self):
        limiter = RateLimiter(5)

        assert limiter._capacity == 5


class TestAcquire:
    def test_first_acquire_never_blocks(self, clock):
        limiter = RateLimiter(10)

        limiter.acquire()

        assert clock.sleeps == []

    def test_consumes_one_token_per_call_without_sleeping_while_tokens_remain(
        self, clock
    ):
        limiter = RateLimiter(5)  # capacity = 5 tokens available immediately

        for _ in range(5):
            limiter.acquire()

        assert clock.sleeps == []

    def test_blocks_once_the_burst_is_exhausted(self, clock):
        limiter = RateLimiter(2)  # capacity = 2

        limiter.acquire()
        limiter.acquire()  # bucket now empty, no time has passed
        limiter.acquire()  # must wait for a token to refill

        assert len(clock.sleeps) == 1
        # At rate=2/sec, refilling 1 token from empty takes 0.5s.
        assert clock.sleeps[0] == pytest.approx(0.5)

    def test_tokens_refill_over_elapsed_time(self, clock):
        limiter = RateLimiter(10)
        limiter.acquire()  # 9 tokens left
        limiter.acquire()  # 8 tokens left

        clock.advance(1.0)  # +10 tokens worth of time, capped at capacity (10)

        limiter.acquire()  # should not need to block -- bucket refilled to the cap

        assert clock.sleeps == []

    def test_refill_is_capped_at_capacity_not_unbounded(self, clock):
        limiter = RateLimiter(2)  # capacity = 2
        limiter.acquire()
        limiter.acquire()  # bucket empty

        clock.advance(1000)  # huge gap -- refill must cap at capacity, not overflow

        limiter.acquire()

        assert limiter._tokens <= limiter._capacity

    def test_each_acquire_call_leaves_tokens_non_negative(self, clock):
        limiter = RateLimiter(1)

        limiter.acquire()

        assert limiter._tokens >= 0


class TestGetRateLimiter:
    def test_same_name_returns_the_same_singleton_instance(self, monkeypatch):
        monkeypatch.setattr(rate_limiter_module, "_limiters", {})

        first = get_rate_limiter("stripe", 10)
        second = get_rate_limiter("stripe", 10)

        assert first is second

    def test_different_names_return_different_instances(self, monkeypatch):
        monkeypatch.setattr(rate_limiter_module, "_limiters", {})

        stripe_limiter = get_rate_limiter("stripe", 10)
        solidgate_limiter = get_rate_limiter("solidgate", 5)

        assert stripe_limiter is not solidgate_limiter

    def test_rate_argument_is_ignored_once_the_limiter_already_exists(
        self, monkeypatch
    ):
        """Documents real behavior: the first call's rate wins forever for a given
        name, even if a later call passes a different rate -- there is no way to
        reconfigure an already-registered limiter."""
        monkeypatch.setattr(rate_limiter_module, "_limiters", {})

        first = get_rate_limiter("zendesk", 3)
        second = get_rate_limiter("zendesk", 999)

        assert second is first
        assert second.rate == 3
