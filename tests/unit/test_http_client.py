"""Unit tests for the resilient HTTP client: retries, backoff, error types, throttling."""
import httpx
import pytest

from src.common.http_client import (InvalidResponseError, PermanentApiError,
                                    RateLimiter, ResilientHttpClient,
                                    RetriesExhaustedError, RetryPolicy)

URL = "https://api.example.test/v1/x"


def make_client(responses, max_attempts=4):
    """responses: list of httpx.Response or Exception, returned in order."""
    calls, sleeps = [], []

    def handler(request):
        calls.append(request)
        item = responses[min(len(calls) - 1, len(responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    client = ResilientHttpClient(
        transport=httpx.MockTransport(handler),
        retry=RetryPolicy(max_attempts=max_attempts, base_delay_s=1, jitter_s=0),
        sleep=sleeps.append)
    return client, calls, sleeps


def test_success_first_try():
    client, calls, sleeps = make_client([httpx.Response(200, json={"a": 1})])
    resp = client.get_json(URL)
    assert resp.data == {"a": 1} and resp.attempts == 1
    assert len(calls) == 1 and sleeps == []


def test_retries_5xx_with_exponential_backoff_then_succeeds():
    client, calls, sleeps = make_client([
        httpx.Response(503), httpx.Response(502), httpx.Response(200, json={"ok": True})])
    resp = client.get_json(URL)
    assert resp.data == {"ok": True} and resp.attempts == 3
    assert sleeps == [1, 2]          # 1s, then 2s (no jitter in test)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_every_retryable_status_is_retried(status):
    client, calls, _ = make_client([httpx.Response(status), httpx.Response(200, json={})])
    assert client.get_json(URL).attempts == 2 and len(calls) == 2


def test_429_respects_retry_after_header():
    client, _, sleeps = make_client([
        httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200, json={})])
    client.get_json(URL)
    assert sleeps == [7]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_4xx_is_not_retried(status):
    client, calls, sleeps = make_client(
        [httpx.Response(status, json={"error": True, "reason": "bad latitude"})])
    with pytest.raises(PermanentApiError, match="bad latitude") as info:
        client.get_json(URL)
    assert len(calls) == 1 and sleeps == []
    assert info.value.response.status_code == status     # body kept for raw storage


def test_timeouts_exhaust_retries():
    client, calls, sleeps = make_client([httpx.ReadTimeout("slow")], max_attempts=3)
    with pytest.raises(RetriesExhaustedError, match="ReadTimeout"):
        client.get_json(URL)
    assert len(calls) == 3 and sleeps == [1, 2]   # no sleep after the final attempt


def test_connection_error_is_retried():
    client, calls, _ = make_client([httpx.ConnectError("refused"),
                                    httpx.Response(200, json={})])
    assert client.get_json(URL).attempts == 2


def test_invalid_json_raises_and_keeps_body():
    client, _, _ = make_client([httpx.Response(200, content=b"<html>oops</html>")])
    with pytest.raises(InvalidResponseError) as info:
        client.get_json(URL)
    assert "oops" in info.value.response.text


def test_backoff_is_capped():
    policy = RetryPolicy(base_delay_s=1, max_delay_s=5, jitter_s=0)
    assert [policy.delay(a) for a in range(1, 6)] == [1, 2, 4, 5, 5]
    assert policy.delay(1, retry_after=120) == 5


def test_rate_limiter_enforces_min_gap():
    now = [100.0]
    sleeps = []
    limiter = RateLimiter(0.5, clock=lambda: now[0], sleep=sleeps.append)
    limiter.wait()                 # first call: no wait
    now[0] += 0.1
    limiter.wait()                 # 0.1s later: must wait 0.4s
    now[0] += 2.0
    limiter.wait()                 # long gap: no wait
    assert sleeps == [pytest.approx(0.4)]
