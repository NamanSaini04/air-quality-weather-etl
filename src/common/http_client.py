"""Resilient HTTP client shared by every extractor.

Responsibilities (and only these):
  * timeouts            - never wait forever on a hung API
  * retries + backoff   - retry only errors that can succeed on a retry
  * Retry-After         - obey the server when it tells us how long to wait
  * client-side throttle- stay under the provider's rate limit
  * typed errors        - callers decide what to do based on the error type

Retry policy:
  RETRY      : timeouts, connection errors, HTTP 429, 500, 502, 503, 504
  DON'T RETRY: other 4xx (bad request, auth, not found) - retrying cannot fix them
"""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


# ---------------------------------------------------------------- errors
class ApiError(Exception):
    """Base class. `response` is attached when the server did answer."""

    def __init__(self, message: str, response: "ApiResponse | None" = None):
        super().__init__(message)
        self.response = response


class PermanentApiError(ApiError):
    """Request is wrong (4xx, or API-level error). Retrying will not help."""


class RetriesExhaustedError(ApiError):
    """Transient errors kept happening until we ran out of attempts."""


class InvalidResponseError(ApiError):
    """Server returned 2xx but the body is not valid JSON."""


class SchemaError(ApiError):
    """Valid JSON, but not the shape we expect (e.g. a field disappeared)."""


# ---------------------------------------------------------------- response
@dataclass
class ApiResponse:
    url: str
    status_code: int
    body: bytes
    data: Any | None = None          # parsed JSON, None if parsing failed
    attempts: int = 1

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


# ---------------------------------------------------------------- throttle
class RateLimiter:
    """Enforces a minimum gap between requests.

    Open-Meteo allows 600/min -> 0.1 s gap. We default higher to be polite.
    Clock and sleep are injectable so tests run instantly.
    """

    def __init__(self, min_interval_s: float,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.min_interval_s = min_interval_s
        self._clock, self._sleep = clock, sleep
        self._last: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last is not None:
            gap = self.min_interval_s - (now - self._last)
            if gap > 0:
                self._sleep(gap)
                now += gap
        self._last = now


# ---------------------------------------------------------------- client
@dataclass
class RetryPolicy:
    max_attempts: int = 4
    base_delay_s: float = 1.0
    max_delay_s: float = 30.0
    jitter_s: float = 0.5

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        """attempt is 1-based. Exponential: 1, 2, 4, 8 ... + random jitter."""
        if retry_after is not None:
            return min(retry_after, self.max_delay_s)
        backoff = self.base_delay_s * (2 ** (attempt - 1))
        return min(backoff + random.uniform(0, self.jitter_s), self.max_delay_s)


@dataclass
class ResilientHttpClient:
    timeout_s: float = 20.0
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    rate_limiter: RateLimiter | None = None
    headers: dict[str, str] = field(default_factory=dict)
    transport: httpx.BaseTransport | None = None   # tests inject httpx.MockTransport
    sleep: Callable[[float], None] = time.sleep

    def __post_init__(self):
        self._client = httpx.Client(
            timeout=self.timeout_s,
            headers={"User-Agent": "air-quality-weather-etl/0.1", **self.headers},
            transport=self.transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def get_json(self, url: str, params: dict | None = None) -> ApiResponse:
        last_error = "unknown"
        for attempt in range(1, self.retry.max_attempts + 1):
            if self.rate_limiter:
                self.rate_limiter.wait()
            retry_after = None
            try:
                resp = self._client.get(url, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                api_resp = ApiResponse(str(resp.url), resp.status_code, resp.content,
                                       attempts=attempt)
                if resp.status_code in RETRYABLE_STATUS:
                    last_error = f"HTTP {resp.status_code}"
                    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                elif resp.status_code >= 400:
                    api_resp.data = _try_json(resp.content)
                    raise PermanentApiError(
                        f"HTTP {resp.status_code} from {url}: {_reason(api_resp)}", api_resp)
                else:
                    api_resp.data = _try_json(resp.content)
                    if api_resp.data is None:
                        raise InvalidResponseError(f"non-JSON body from {url}", api_resp)
                    return api_resp

            if attempt < self.retry.max_attempts:
                delay = self.retry.delay(attempt, retry_after)
                log.warning("attempt %d/%d failed for %s (%s); retrying in %.1fs",
                            attempt, self.retry.max_attempts, url, last_error, delay)
                self.sleep(delay)

        raise RetriesExhaustedError(
            f"{url} failed after {self.retry.max_attempts} attempts: {last_error}")


def _try_json(body: bytes) -> Any | None:
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None          # HTTP-date form is rare for APIs; fall back to backoff


def _reason(resp: ApiResponse) -> str:
    if isinstance(resp.data, dict) and resp.data.get("reason"):
        return str(resp.data["reason"])
    return resp.text[:200]
