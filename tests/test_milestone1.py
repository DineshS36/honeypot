import json
import subprocess
import time

import requests


BASE_URL = "http://localhost:8000"


def wait_for_api(timeout: int = 30) -> None:
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(
                f"{BASE_URL}/health",
                timeout=2,
            )

            if response.ok:
                return

        except requests.RequestException:
            pass

        time.sleep(1)

    raise RuntimeError("API did not become ready")


def test_health() -> None:
    wait_for_api()

    response = requests.get(
        f"{BASE_URL}/health",
        timeout=5,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_events_endpoint() -> None:
    wait_for_api()

    response = requests.get(
        f"{BASE_URL}/events",
        params={"limit": 10},
        timeout=5,
    )

    assert response.status_code == 200

    body = response.json()

    assert isinstance(body, list)


def test_stream_endpoint() -> None:
    wait_for_api()

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