"""Poll Realtime Trains for departures from one station and land each raw response as a file.

Landing layout:  <root>/YYYY/MM/DD/<station>_departures_HH_MM_SS.json   (all times UTC)
Each file is an "envelope": when we asked, what we asked, the HTTP status, the rate-limit
headers, any error, and the response body exactly as received. Failures are saved too.
"""

import io
import json
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

BASE_URL = os.environ.get("BASE_URL", "https://data.rtt.io").rstrip("/")
REFRESH_TOKEN = os.environ.get("TOKEN", "")
LANDING_DIR = os.environ.get("LANDING_DIR", "/Volumes/rail_pulse/bronze/landing/rtt/bmh")
STATION = "BMH"
WINDOW_MINUTES = 60
INTERVAL_S = 120
TOKEN_MARGIN_S = 120
HTTP_TIMEOUT_S = 20


def landing_path(root: str, when: datetime, station: str = STATION) -> str:
    """Return <root>/YYYY/MM/DD/<station>_departures_HH_MM_SS.json for a UTC time."""
    when = when.astimezone(UTC)
    return (
        f"{root.rstrip('/')}/{when:%Y}/{when:%m}/{when:%d}/"
        f"{station.lower()}_departures_{when:%H_%M_%S}.json"
    )


class TokenCache:
    """Hold a short-lived access token and renew it shortly before it expires."""

    def __init__(self, base_url, refresh_token, margin_s=TOKEN_MARGIN_S, http_get=requests.get):
        self._base_url = base_url
        self._refresh_token = refresh_token
        self._margin = timedelta(seconds=margin_s)
        self._get = http_get
        self._token = None
        self._valid_until = None

    def get(self, now: datetime | None = None) -> str:
        now = now or datetime.now(UTC)
        if self._token and now < self._valid_until - self._margin:
            return self._token
        r = self._get(
            f"{self._base_url}/api/get_access_token",
            headers={"Authorization": f"Bearer {self._refresh_token}"},
            timeout=HTTP_TIMEOUT_S,
        )
        r.raise_for_status()
        data = r.json()
        self._token = data["token"]
        self._valid_until = datetime.fromisoformat(data["validUntil"])
        return self._token

    def invalidate(self) -> None:
        self._token = None


def build_envelope(fetched_at: datetime, request: dict, response=None, error=None) -> dict:
    headers = {}
    if response is not None:
        headers = {
            k.lower(): v
            for k, v in response.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"
        }
    return {
        "fetched_at": fetched_at.astimezone(UTC).isoformat(),
        "request": request,
        "http_status": None if response is None else response.status_code,
        "rate_limits": headers,
        "error": error,
        "body": "" if response is None else response.text,
    }


class LocalWriter:
    """Write files to a local folder (used by tests)."""

    def write(self, path: str, data: bytes) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


class VolumeWriter:
    """Write files to a Unity Catalog Volume with the Databricks SDK."""

    def __init__(self):
        from databricks.sdk import WorkspaceClient

        self._w = WorkspaceClient()
        self._dirs: set[str] = set()

    def write(self, path: str, data: bytes) -> None:
        parent = path.rsplit("/", 1)[0]
        if parent not in self._dirs:
            self._w.files.create_directory(parent)
            self._dirs.add(parent)
        self._w.files.upload(path, io.BytesIO(data), overwrite=False)


def poll_once(
    cache,
    writer,
    *,
    now=None,
    station=STATION,
    window=WINDOW_MINUTES,
    root=LANDING_DIR,
    base_url=BASE_URL,
    http_get=requests.get,
) -> dict:
    fetched_at = now or datetime.now(UTC)
    params = {"code": station, "timeWindow": window}
    request = {"path": "/gb-nr/location", "params": params}
    response, error = None, None
    try:
        token = cache.get(fetched_at)
        response = http_get(
            f"{base_url}{request['path']}",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT_S,
        )
        if response.status_code == 401:
            cache.invalidate()
    except (requests.RequestException, ValueError, KeyError) as e:
        error = repr(e)
    envelope = build_envelope(fetched_at, request, response, error)
    path = landing_path(root, fetched_at, station)
    writer.write(path, json.dumps(envelope).encode("utf-8"))
    return {
        "fetched_at": envelope["fetched_at"],
        "http_status": envelope["http_status"],
        "error": error,
        "path": path,
        "retry_after": envelope["rate_limits"].get("retry-after"),
        "body": envelope["body"],
    }


def run_forever(stop: threading.Event, status: dict, cache, writer, interval_s=INTERVAL_S) -> None:
    """Poll until stopped. Never dies on one bad call; the status shows what went wrong."""
    while not stop.is_set():
        wait = interval_s
        try:
            result = poll_once(cache, writer)
            status.update(
                last_call=result["fetched_at"],
                last_status=result["http_status"],
                last_error=result["error"],
                last_path=result["path"],
                last_body=result["body"],
            )
            status["files_written"] = status.get("files_written", 0) + 1
            if result["http_status"] == 429:
                wait = max(interval_s, int(result["retry_after"] or 60))
        except Exception as e:  # keep the loop alive; the status page shows the error
            status["last_error"] = repr(e)
        stop.wait(wait)
