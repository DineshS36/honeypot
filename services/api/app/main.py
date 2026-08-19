from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from shared.event_schema import TelemetryEvent

from .config import NORMALIZED_EVENTS_FILE
from .db import init_db
from .repository import EventRepository
from .sse import EventBroadcaster


repository = EventRepository()
broadcaster = EventBroadcaster()


async def ingestion_loop() -> None:
    NORMALIZED_EVENTS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    NORMALIZED_EVENTS_FILE.touch(exist_ok=True)

    offset = 0

    while True:
        try:
            with NORMALIZED_EVENTS_FILE.open(
                "r",
                encoding="utf-8",
            ) as handle:

                handle.seek(offset)

                while True:
                    line_start = handle.tell()
                    line = handle.readline()

                    if not line:
                        break

                    # Incomplete line check: wait for producer to complete writing line
                    if not (line.endswith("\n") or line.endswith("\r")):
                        offset = line_start
                        break

                    offset = handle.tell()

                    if not line.strip():
                        continue

                    event = TelemetryEvent.model_validate_json(line)

                    repository.insert(event)

                    await broadcaster.publish(event)

        except Exception as exc:
            print(
                f"[api] ingestion failure: {exc}",
                flush=True,
            )

        await asyncio.sleep(0.25)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    task = asyncio.create_task(
        ingestion_loop()
    )

    try:
        yield
    finally:
        task.cancel()


app = FastAPI(
    title="Honeypot Telemetry API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "telemetry-api",
    }


@app.get("/events")
def get_events(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
    ),
) -> list[TelemetryEvent]:

    return repository.list_events(limit)


@app.get("/events/stream")
async def event_stream():

    async def generator():
        async for event in broadcaster.stream():
            payload = event.model_dump_json()

            yield f"event: telemetry\ndata: {payload}\n\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/events/{event_id}")
def get_event(event_id: str):
    event = repository.get_by_id(event_id)
    if event:
        return event

    raise HTTPException(
        status_code=404,
        detail="Event not found",
    )
