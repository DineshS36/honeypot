import json
import subprocess
import time

import requests


import pytest

BASE_URL = "http://localhost:8000"


def ensure_api_running() -> None:
    try:
        response = requests.get(
            f"{BASE_URL}/health",
            timeout=1,
        )
        if response.ok:
            return
    except requests.RequestException:
        pass

    pytest.skip("Live API container is not running on http://localhost:8000")



def test_health() -> None:
    ensure_api_running()

    response = requests.get(
        f"{BASE_URL}/health",
        timeout=5,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_events_endpoint() -> None:
    ensure_api_running()

    response = requests.get(
        f"{BASE_URL}/events",
        params={"limit": 10},
        timeout=5,
    )

    assert response.status_code == 200

    body = response.json()

    assert isinstance(body, list)


def test_stream_endpoint() -> None:
    ensure_api_running()

    with requests.get(
        f"{BASE_URL}/events/stream",
        stream=True,
        timeout=5,
    ) as response:

        assert response.status_code == 200

        assert (
            response.headers["content-type"]
            .startswith("text/event-stream")
        )