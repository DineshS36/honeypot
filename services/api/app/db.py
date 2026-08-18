from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import DATABASE_PATH


def connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        DATABASE_PATH,
        check_same_thread=False,
    )

    connection.row_factory = sqlite3.Row

    return connection


def init_db() -> None:
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                host_id TEXT NOT NULL,
                sensor_id TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(timestamp)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_source
            ON events(source)
            """
        )

        conn.commit()