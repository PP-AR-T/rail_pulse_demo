import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import poller  # noqa: E402


class FakeResponse:
    def __init__(self, status=200, body="", headers=None, payload=None):
        self.status_code = status
        self.text = body
        self.headers = headers or {}
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def token_response(valid_until="2026-10-12T13:59:50+00:00", token="access-1"):
    return FakeResponse(200, payload={"token": token, "validUntil": valid_until})


def test_landing_path_uses_year_month_day_folders_and_utc_time():
    when = datetime(2026, 10, 12, 14, 39, 50, tzinfo=UTC)
    assert poller.landing_path("/Volumes/c/s/v/rtt/bmh", when, "BMH") == (
        "/Volumes/c/s/v/rtt/bmh/2026/10/12/bmh_departures_14_39_50.json"
    )


def test_landing_path_converts_other_time_zones_to_utc():
    from datetime import timedelta, timezone

    bst = timezone(timedelta(hours=1))
    when = datetime(2026, 10, 12, 15, 0, 0, tzinfo=bst)
    assert poller.landing_path("/r", when).endswith("/2026/10/12/bmh_departures_14_00_00.json")


def test_token_cache_reuses_token_until_near_expiry():
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return token_response()

    cache = poller.TokenCache("https://x", "refresh", margin_s=120, http_get=fake_get)
    early = datetime(2026, 10, 12, 13, 40, tzinfo=UTC)
    assert cache.get(early) == "access-1"
    assert cache.get(early) == "access-1"
    assert len(calls) == 1
    near_expiry = datetime(2026, 10, 12, 13, 58, tzinfo=UTC)  # inside the 120 s margin
    cache.get(near_expiry)
    assert len(calls) == 2


def test_token_cache_sends_refresh_token_as_bearer():
    seen = {}

    def fake_get(url, headers=None, **kwargs):
        seen.update(url=url, headers=headers)
        return token_response()

    poller.TokenCache("https://x", "refresh-abc", http_get=fake_get).get(
        datetime(2026, 10, 12, 13, 40, tzinfo=UTC)
    )
    assert seen["url"] == "https://x/api/get_access_token"
    assert seen["headers"] == {"Authorization": "Bearer refresh-abc"}


def make_cache():
    return poller.TokenCache("https://x", "refresh", http_get=lambda *a, **k: token_response())


NOW = datetime(2026, 10, 12, 13, 40, 0, tzinfo=UTC)


def test_poll_once_writes_envelope_with_body_exactly_as_received(tmp_path):
    body = '{"services":[{"a":1}]}'
    resp = FakeResponse(200, body, {"X-RateLimit-Remaining-Minute": "9"})
    result = poller.poll_once(
        make_cache(), poller.LocalWriter(), now=NOW, root=str(tmp_path), base_url="https://x",
        http_get=lambda *a, **k: resp,
    )
    written = Path(result["path"])
    assert written.parent == tmp_path / "2026" / "10" / "12"
    env = json.loads(written.read_text())
    assert env["http_status"] == 200
    assert env["body"] == body
    assert env["rate_limits"] == {"x-ratelimit-remaining-minute": "9"}
    assert env["error"] is None
    assert env["request"]["params"] == {"code": "BMH", "timeWindow": 60}


def test_poll_once_keeps_a_204_as_data(tmp_path):
    result = poller.poll_once(
        make_cache(), poller.LocalWriter(), now=NOW, root=str(tmp_path), base_url="https://x",
        http_get=lambda *a, **k: FakeResponse(204, ""),
    )
    env = json.loads(Path(result["path"]).read_text())
    assert env["http_status"] == 204
    assert env["body"] == ""


def test_poll_once_saves_network_errors_instead_of_raising(tmp_path):
    def boom(*a, **k):
        raise requests.ConnectionError("down")

    result = poller.poll_once(
        make_cache(), poller.LocalWriter(), now=NOW, root=str(tmp_path), base_url="https://x",
        http_get=boom,
    )
    env = json.loads(Path(result["path"]).read_text())
    assert env["http_status"] is None
    assert "down" in env["error"]


def test_poll_once_saves_token_failure_as_an_error_file(tmp_path):
    bad = poller.TokenCache(
        "https://x", "refresh", http_get=lambda *a, **k: FakeResponse(401, "no")
    )
    result = poller.poll_once(
        bad, poller.LocalWriter(), now=NOW, root=str(tmp_path), base_url="https://x",
        http_get=lambda *a, **k: pytest.fail("should not call the API without a token"),
    )
    env = json.loads(Path(result["path"]).read_text())
    assert env["http_status"] is None
    assert "401" in env["error"]


def test_poll_once_reports_retry_after_on_429(tmp_path):
    resp = FakeResponse(429, "slow down", {"Retry-After": "30"})
    result = poller.poll_once(
        make_cache(), poller.LocalWriter(), now=NOW, root=str(tmp_path), base_url="https://x",
        http_get=lambda *a, **k: resp,
    )
    assert result["http_status"] == 429
    assert result["retry_after"] == "30"
