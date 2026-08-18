from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from shared.event_schema import (
    EventSource,
    EventType,
    NetworkMetadata,
    Protocol,
    TelemetryEvent,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("normalizer")

QUEUE_DIR = Path(os.getenv("QUEUE_DIR", "/shared/events"))
RAW_FILE = QUEUE_DIR / "raw.jsonl"
NORMALIZED_FILE = QUEUE_DIR / "normalized.jsonl"
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.25"))

CANONICAL_TOP_LEVEL_KEYS = {
    "event_id",
    "timestamp",
    "source",
    "event_type",
    "host_id",
    "sensor_id",
    "network",
    "process_name",
    "process_pid",
    "file_path",
    "command",
    "attributes",
}


def parse_timestamp(ts_val: Any) -> datetime:
    """Parse Unix float/int timestamp or ISO datetime string into UTC datetime."""
    if isinstance(ts_val, (int, float)):
        return datetime.fromtimestamp(ts_val, tz=timezone.utc)
    if isinstance(ts_val, str) and ts_val.strip():
        try:
            dt = datetime.fromisoformat(ts_val.strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def parse_protocol(proto_val: Any) -> Protocol | None:
    """Convert raw protocol string into canonical Protocol enum."""
    if not proto_val:
        return None
    val_str = str(proto_val).lower().strip()
    if val_str == "tcp":
        return Protocol.TCP
    elif val_str == "udp":
        return Protocol.UDP
    elif val_str == "icmp":
        return Protocol.ICMP
    else:
        return Protocol.OTHER


def normalize_event(raw_data: dict[str, Any]) -> TelemetryEvent:
    """
    Construct canonical TelemetryEvent model from raw event dict.
    Preserves raw_id mapping to event_id if event_id is absent.
    Preserves unexpected raw fields into attributes without silent data loss.
    """
    # 1. Determine event_id
    event_id = raw_data.get("event_id")
    if not event_id:
        event_id = raw_data.get("raw_id")
    if not event_id:
        event_id = str(uuid.uuid4())
    else:
        event_id = str(event_id)

    # 2. Parse timestamp
    ts = parse_timestamp(raw_data.get("timestamp"))

    # 3. Source & Event Type
    source_val = raw_data.get("source", EventSource.NETWORK_SENSOR.value)
    source = EventSource(source_val)

    event_type_val = raw_data.get("event_type", EventType.NETWORK_ACTIVITY.value)
    event_type = EventType(event_type_val)

    # 4. Host ID & Sensor ID
    host_id = raw_data.get("host_id")
    sensor_id = raw_data.get("sensor_id")

    # 5. Network Metadata
    network: NetworkMetadata | None = None
    raw_net = raw_data.get("network")
    if isinstance(raw_net, dict):
        network = NetworkMetadata(
            src_ip=raw_net.get("src_ip"),
            dst_ip=raw_net.get("dst_ip"),
            src_port=raw_net.get("src_port"),
            dst_port=raw_net.get("dst_port"),
            protocol=parse_protocol(raw_net.get("protocol")),
            payload_size=raw_net.get("payload_size", 0),
            interface=raw_net.get("interface"),
        )

    # 6. Attributes & Extra Fields Preservation
    attributes = dict(raw_data.get("attributes", {})) if isinstance(raw_data.get("attributes"), dict) else {}

    # Preserve any unmapped top-level keys into attributes to prevent data loss
    for key, val in raw_data.items():
        if key not in CANONICAL_TOP_LEVEL_KEYS:
            if key not in attributes:
                attributes[key] = val

    # 7. Construct final dict and validate against TelemetryEvent boundary
    canonical_dict = {
        "event_id": event_id,
        "timestamp": ts,
        "source": source,
        "event_type": event_type,
        "host_id": host_id,
        "sensor_id": sensor_id,
        "network": network,
        "process_name": raw_data.get("process_name"),
        "process_pid": raw_data.get("process_pid"),
        "file_path": raw_data.get("file_path"),
        "command": raw_data.get("command"),
        "attributes": attributes,
    }

    return TelemetryEvent.model_validate(canonical_dict)



def append_normalized_event(event: TelemetryEvent) -> None:
    """Append validated TelemetryEvent to normalized.jsonl line-by-line with flush."""
    NORMALIZED_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NORMALIZED_FILE.open("a", encoding="utf-8") as handle:
        handle.write(event.model_dump_json() + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def run_normalizer() -> None:
    """Incremental normalizer loop consuming raw.jsonl and emitting normalized.jsonl."""
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Starting Normalizer: input={RAW_FILE}, output={NORMALIZED_FILE}")

    cursor = 0

    while True:
        if not RAW_FILE.exists():
            time.sleep(POLL_INTERVAL)
            continue

        try:
            file_size = RAW_FILE.stat().st_size
            if file_size < cursor:
                logger.warning(f"File truncated detected: cursor={cursor} > size={file_size}. Resetting cursor to 0.")
                cursor = 0

            with RAW_FILE.open("r", encoding="utf-8") as handle:
                handle.seek(cursor)

                while True:
                    line_start = handle.tell()
                    line = handle.readline()

                    if not line:
                        # EOF reached
                        cursor = line_start
                        break

                    # Incomplete line check: writer has not finished writing newline
                    if not (line.endswith("\n") or line.endswith("\r")):
                        # Reset cursor to start of partial line and wait for producer
                        cursor = line_start
                        break

                    cursor = handle.tell()
                    raw_str = line.strip()

                    if not raw_str:
                        # Blank line, ignore silently
                        continue

                    # 1. JSON parsing phase
                    try:
                        raw_data = json.loads(raw_str)
                    except json.JSONDecodeError as err:
                        logger.error(f"REJECTED malformed JSON [line offset {line_start}]: {err} | Raw content: {raw_str!r}")
                        continue

                    if not isinstance(raw_data, dict):
                        logger.error(f"REJECTED non-object JSON [line offset {line_start}]: expected dict, got {type(raw_data).__name__}")
                        continue

                    # 2. Schema normalization & validation phase
                    try:
                        event = normalize_event(raw_data)
                    except (ValidationError, Exception) as err:
                        logger.error(f"REJECTED schema invalid event [line offset {line_start}]: {err}")
                        continue

                    # 3. Output write phase
                    try:
                        append_normalized_event(event)
                        logger.info(f"NORMALIZED event_id={event.event_id} source={event.source.value} type={event.event_type.value}")
                    except Exception as err:
                        logger.error(f"Failed writing normalized event [event_id={event.event_id}]: {err}")

        except Exception as exc:
            logger.error(f"Unexpected normalizer loop error: {exc}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_normalizer()
