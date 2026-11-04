"""Fake Open-Meteo responses, shaped like the documented API.

NOTE: built from Open-Meteo's documented response format, not captured from the
live API (the build environment had no network access). Phase 2's live check
(`python main.py extract-weather --cities london`) is what confirms the real shape.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import httpx

UNITS = {
    "time": "iso8601", "temperature_2m": "°C", "relative_humidity_2m": "%",
    "pressure_msl": "hPa", "precipitation": "mm", "rain": "mm",
    "wind_speed_10m": "km/h", "wind_direction_10m": "°", "weather_code": "wmo code",
}


def open_meteo_payload(lat: float, lon: float, start: date, end: date,
                       generation_ms: float = 0.5) -> dict:
    hours = int(((end - start).days + 1) * 24)
    t0 = datetime(start.year, start.month, start.day)
    times = [(t0 + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]
    return {
        "latitude": lat, "longitude": lon,
        "generationtime_ms": generation_ms,
        "utc_offset_seconds": 0, "timezone": "GMT", "timezone_abbreviation": "GMT",
        "elevation": 20.0,
        "hourly_units": dict(UNITS),
        "hourly": {
            "time": times,
            "temperature_2m": [10.0 + (h % 24) * 0.2 for h in range(hours)],
            "relative_humidity_2m": [80] * hours,
            "pressure_msl": [1012.3] * hours,
            "precipitation": [0.0] * hours,
            "rain": [0.0] * hours,
            "wind_speed_10m": [12.5] * hours,
            "wind_direction_10m": [220] * hours,
            "weather_code": [3] * hours,
        },
    }


class FakeOpenMeteo:
    """Callable for httpx.MockTransport. Records every request it receives.

    behaviour: {latitude_as_str: list of actions}, consumed one per request.
      action = 'ok' | 'ok_new_gen' | int status | 'timeout' | 'bad_json' | callable(payload)->payload
    Default action is 'ok'.
    """

    def __init__(self, behaviour: dict[str, list] | None = None):
        self.behaviour = {k: list(v) for k, v in (behaviour or {}).items()}
        self.requests: list[httpx.Request] = []
        self._gen = 0.5

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        q = request.url.params
        queue = self.behaviour.get(q["latitude"], [])
        action = queue.pop(0) if queue else "ok"

        if action == "timeout":
            raise httpx.ReadTimeout("simulated timeout", request=request)
        if isinstance(action, int):
            body = {"error": True, "reason": f"simulated {action}"}
            headers = {"Retry-After": "7"} if action == 429 else {}
            return httpx.Response(action, json=body, headers=headers)
        if action == "bad_json":
            return httpx.Response(200, content=b"<html>Gateway error</html>")

        if action == "ok_new_gen":
            self._gen += 1.0            # same data, different server timing
        payload = open_meteo_payload(float(q["latitude"]), float(q["longitude"]),
                                     date.fromisoformat(q["start_date"]),
                                     date.fromisoformat(q["end_date"]), self._gen)
        if callable(action):
            payload = action(payload)
        return httpx.Response(200, content=json.dumps(payload).encode())

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)
