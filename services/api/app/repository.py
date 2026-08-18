from __future__ import annotations

import json
from datetime import datetime

from shared.event_schema import TelemetryEvent

from .db import connect


class EventRepository:

    def insert(self, event: TelemetryEvent) -> None:
        with connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO events (
                    event_id,
                    timestamp,
                    source,
                    event_type,
                    host_id,
                    sensor_id,
                    payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.source.value,
                    event.event_type.value,
                    event.host_id,
                    event.sensor_id,
                    event.model_dump_json(),
                ),
            )

            conn.commit()

    def list_events(
        self,
        limit: int = 100,
    ) -> list[TelemetryEvent]:

        with connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM events
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            TelemetryEvent.model_validate_json(row["payload_json"])
            for row in rows
        ]