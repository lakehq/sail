"""RetryPolicy configuration and the Tenacity policies built from it."""

import math
import random
from collections.abc import Callable
from dataclasses import dataclass, field

from tenacity import AsyncRetrying, RetryCallState, Retrying, retry_if_exception, stop_after_attempt, stop_before_delay
from tenacity.stop import stop_base

from typesafe_sdk._core.config import resolve_timeout
from typesafe_sdk._core.errors import TypeSafeAPIConnectionError, TypeSafeAPIError, TypeSafeAPITimeoutError, TypeSafeError, parse_retry_after

# `build_tenacity`/`build_tenacity_async` are internal build seams and stay out of the public docs.
__all__ = ["RetryPolicy"]


def _retry_after(state: RetryCallState) -> float | None:
    error = state.outcome.exception() if state.outcome is not None else None
    if isinstance(error, TypeSafeAPIError):
        delay = parse_retry_after(error.headers)
        if delay is not None:
            return delay / 1000
    return None


def _backoff(attempt: int, initial: float, maximum: float, jitter: float) -> float:
    if initial == 0 or maximum == 0:
        return 0.0
    exponent = attempt - 1
    exponential = maximum if exponent >= math.log2(maximum) - math.log2(initial) else math.ldexp(initial, exponent)
    delay = exponential * (1 - random.random() * jitter)  # noqa: S311 - Backoff jitter, not cryptography.
    return min(exponential, round(delay, 3))


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for SDK retry behavior.

    Examples:
        ```python
        from typesafe_sdk import RetryPolicy, TypeSafeClient

        client = TypeSafeClient(
            retry=RetryPolicy(
                max_retries=3, timeout=10.0, http_statuses={429, 500, 502, 503, 504}
            )
        )
        ```
    """

    max_retries: int = 2
    """Maximum retries after the initial attempt; `0` disables retries."""

    backoff_initial: float = 0.5
    """First backoff delay in seconds, doubled each attempt up to `backoff_max`; zero disables backoff."""

    backoff_max: float = 5.0
    """Maximum backoff delay in seconds; zero disables backoff."""

    backoff_jitter: float = 0.25
    """Fraction of each backoff delay randomly subtracted, between 0 and 1."""

    http_statuses: set[int] = field(default_factory=lambda: {408, 429, *range(500, 600)})
    """HTTP status codes that are retried."""

    respect_retry_after: bool = True
    """Whether to honor `Retry-After` and `retry-after-ms` response headers."""

    api_connection_error: bool = True
    """Whether to retry `TypeSafeAPIConnectionError`, raised when the request cannot reach or read from the server."""

    api_timeout_error: bool = True
    """Whether to retry `TypeSafeAPITimeoutError`, raised when the request exceeds its timeout."""

    exceptions: set[type[BaseException]] = field(default_factory=set)
    """Additional exception types that trigger a retry, on top of the built-in rules."""

    predicate: Callable[[BaseException], bool] | None = None
    """An optional predicate called with the raised exception; returning `True` triggers a retry in addition to the other rules."""

    timeout: float | None = 30.0
    """Total retry budget in seconds per SDK call, including the initial attempt and delays; `None` disables the limit.

    Stops before a retry whose delay would reach or exceed the budget, re-raising the last error.
    """

    def __post_init__(self) -> None:
        """Validate retry counts, delays, jitter, and the optional retry timeout."""
        if not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise TypeSafeError("max_retries must be a non-negative integer.")
        for name, value in (("backoff_initial", self.backoff_initial), ("backoff_max", self.backoff_max)):
            if not math.isfinite(value) or value < 0:
                raise TypeSafeError(f"{name} must be a non-negative, finite number of seconds.")
        if not 0 <= self.backoff_jitter <= 1:
            raise TypeSafeError("backoff_jitter must be between zero and one.")
        if self.timeout is not None:
            resolve_timeout(self.timeout)

    def _retryable(self, error: BaseException) -> bool:
        if isinstance(error, TypeSafeAPITimeoutError):
            builtin = self.api_timeout_error
        elif isinstance(error, TypeSafeAPIConnectionError):
            builtin = self.api_connection_error
        elif isinstance(error, TypeSafeAPIError):
            builtin = error.status in self.http_statuses
        else:
            builtin = False
        return builtin or isinstance(error, tuple(self.exceptions)) or (self.predicate is not None and self.predicate(error))

    def _wait(self, state: RetryCallState) -> float:
        if self.respect_retry_after:
            delay = _retry_after(state)
            if delay is not None:
                return delay
        return _backoff(state.attempt_number, self.backoff_initial, self.backoff_max, self.backoff_jitter)

    def _stop(self) -> stop_base:
        attempts = stop_after_attempt(self.max_retries + 1)
        return attempts if self.timeout is None else attempts | stop_before_delay(self.timeout)

    def _build_tenacity(self) -> Retrying:
        return Retrying(stop=self._stop(), wait=self._wait, retry=retry_if_exception(self._retryable), reraise=True)

    def _build_tenacity_async(self) -> AsyncRetrying:
        return AsyncRetrying(stop=self._stop(), wait=self._wait, retry=retry_if_exception(self._retryable), reraise=True)


def build_tenacity(policy: "RetryPolicy | None") -> Retrying:
    """Build the synchronous Tenacity policy, falling back to default retry behavior when `policy` is None."""
    return (policy or RetryPolicy())._build_tenacity()  # noqa: SLF001 - internal build seam.


def build_tenacity_async(policy: "RetryPolicy | None") -> AsyncRetrying:
    """Build the asynchronous Tenacity policy, falling back to default retry behavior when `policy` is None."""
    return (policy or RetryPolicy())._build_tenacity_async()  # noqa: SLF001 - internal build seam.
