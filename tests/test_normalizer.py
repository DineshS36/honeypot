import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.normalizer.main import (
    append_normalized_event,
    normalize_event,
    parse_protocol,
    parse_timestamp,
)
from shared.event_schema import EventSource, EventType, Protocol, TelemetryEvent


def test_parse_timestamp():
    # Unix timestamp
    ts_float = 1700000000.5
    dt1 = parse_timestamp(ts_float)
    assert dt1.tzinfo == timezone.utc
    assert dt1.timestamp() == ts_float

    # ISO string
    iso_str = "2026-08-18T23:00:00+00:00"
    dt2 = parse_timestamp(iso_str)
    assert dt2.isoformat() == "2026-08-18T23:00:00+00:00"


def test_parse_protocol():
    assert parse_protocol("tcp") == Protocol.TCP
    assert parse_protocol("UDP") == Protocol.UDP
    assert parse_protocol("icmp") == Protocol.ICMP
    assert parse_protocol("unknown") == Protocol.OTHER
    assert parse_protocol(None) is None


def test_normalize_event_valid_with_raw_id():
    raw_id = str(uuid.uuid4())
    raw_event = {
        "raw_id": raw_id,
        "timestamp": 1700000000.0,
        "source": "network_sensor",
        "event_type": "network_activity",
        "host_id": "test-host",
        "sensor_id": "test-sensor",
        "network": {
            "src_ip": "192.168.1.10",
            "dst_ip": "10.0.0.1",
            "src_port": 12345,
            "dst_port": 80,
            "protocol": "tcp",
            "payload_size": 64,
            "interface": "eth0",
        },
        "attributes": {
            "packet_hash": "abc123hash",
        },
        "unexpected_extra_field": "extra_val",
    }

    event = normalize_event(raw_event)
    assert isinstance(event, TelemetryEvent)
    assert event.event_id == raw_id
    assert event.source == EventSource.NETWORK_SENSOR
    assert event.event_type == EventType.NETWORK_ACTIVITY
    assert event.host_id == "test-host"
    assert event.sensor_id == "test-sensor"
    assert event.network is not None
    assert str(event.network.src_ip) == "192.168.1.10"
    assert str(event.network.dst_ip) == "10.0.0.1"
    assert event.network.src_port == 12345
    assert event.network.dst_port == 80
    assert event.network.protocol == Protocol.TCP

    # Verify no silent data loss: unexpected extra field stored in attributes
    assert event.attributes.get("packet_hash") == "abc123hash"
    assert event.attributes.get("unexpected_extra_field") == "extra_val"


def test_normalize_event_preserves_existing_event_id():
    existing_id = "custom-event-id-999"
    raw_event = {
        "event_id": existing_id,
        "raw_id": "different-raw-id",
        "source": "host_sensor",
        "event_type": "process_start",
        "host_id": "host-1",
        "sensor_id": "sensor-1",
    }

    event = normalize_event(raw_event)
    assert event.event_id == existing_id


def test_append_and_verify_jsonl(tmp_path, monkeypatch):
    norm_file = tmp_path / "normalized.jsonl"
    monkeypatch.setattr("services.normalizer.main.NORMALIZED_FILE", norm_file)

    event = TelemetryEvent(
        event_id="evt-1",
        timestamp=datetime.now(timezone.utc),
        source=EventSource.NETWORK_SENSOR,
        event_type=EventType.NETWORK_ACTIVITY,
        host_id="h1",
        sensor_id="s1",
    )

    append_normalized_event(event)

    assert norm_file.exists()
    lines = norm_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1

    parsed = TelemetryEvent.model_validate_json(lines[0])
    assert parsed.event_id == "evt-1"


def test_incremental_processing_and_malformed_recovery(tmp_path, monkeypatch):
    """
    Tests incremental cursor reading, malformed JSON recovery,
    schema-invalid skipping, blank lines, and incomplete line handling.
    """
    raw_file = tmp_path / "raw.jsonl"
    norm_file = tmp_path / "normalized.jsonl"

    monkeypatch.setattr("services.normalizer.main.RAW_FILE", raw_file)
    monkeypatch.setattr("services.normalizer.main.NORMALIZED_FILE", norm_file)
    monkeypatch.setattr("services.normalizer.main.QUEUE_DIR", tmp_path)

    # 1. Write mixed stream: valid event, blank line, malformed JSON, schema invalid, valid event
    valid1 = {
        "raw_id": "valid-1",
        "timestamp": 1700000001.0,
        "source": "network_sensor",
        "event_type": "network_activity",
        "host_id": "h1",
        "sensor_id": "s1",
    }
    valid2 = {
        "raw_id": "valid-2",
        "timestamp": 1700000002.0,
        "source": "network_sensor",
        "event_type": "network_activity",
        "host_id": "h1",
        "sensor_id": "s1",
    }

    content_batch_1 = (
        json.dumps(valid1) + "\n"
        + "\n"  # blank line
        + "THIS_IS_NOT_VALID_JSON\n"  # malformed JSON
        + json.dumps({"source": "invalid_source_enum"}) + "\n"  # missing required host_id/sensor_id
        + json.dumps(valid2) + "\n"
    )

    raw_file.write_text(content_batch_1, encoding="utf-8")

    # Run normalizer iteration logic manually for batch 1
    cursor = 0
    with raw_file.open("r", encoding="utf-8") as handle:
        handle.seek(cursor)
        while True:
            line_start = handle.tell()
            line = handle.readline()
            if not line:
                cursor = line_start
                break
            if not (line.endswith("\n") or line.endswith("\r")):
                cursor = line_start
                break

            cursor = handle.tell()
            raw_str = line.strip()
            if not raw_str:
                continue

            try:
                raw_data = json.loads(raw_str)
            except json.JSONDecodeError:
                continue

            if not isinstance(raw_data, dict):
                continue

            try:
                evt = normalize_event(raw_data)
                append_normalized_event(evt)
            except Exception:
                continue

    # Verify batch 1 results: only valid-1 and valid-2 should be in normalized.jsonl
    assert norm_file.exists()
    lines = norm_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2

    e1 = TelemetryEvent.model_validate_json(lines[0])
    e2 = TelemetryEvent.model_validate_json(lines[1])
    assert e1.event_id == "valid-1"
    assert e2.event_id == "valid-2"

    # 2. Test incremental append (batch 2) without reprocessing batch 1
    valid3 = {
        "raw_id": "valid-3",
        "timestamp": 1700000003.0,
        "source": "network_sensor",
        "event_type": "network_activity",
        "host_id": "h1",
        "sensor_id": "s1",
    }

    with raw_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(valid3) + "\n")

    # Resume from cursor
    with raw_file.open("r", encoding="utf-8") as handle:
        handle.seek(cursor)
        while True:
            line_start = handle.tell()
            line = handle.readline()
            if not line:
                cursor = line_start
                break
            if not (line.endswith("\n") or line.endswith("\r")):
                cursor = line_start
                break
            cursor = handle.tell()
            raw_str = line.strip()
            if not raw_str:
                continue
            try:
                raw_data = json.loads(raw_str)
                evt = normalize_event(raw_data)
                append_normalized_event(evt)
            except Exception:
                continue

    lines = norm_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 3
    e3 = TelemetryEvent.model_validate_json(lines[2])
    assert e3.event_id == "valid-3"


def test_incomplete_line_buffering(tmp_path, monkeypatch):
    """
    Tests that a line without a newline at EOF is deferred and not treated as malformed JSON.
    """
    raw_file = tmp_path / "raw.jsonl"

    # Write partial JSON line without newline
    raw_file.write_text('{"raw_id":"partial-1", "event_type":"net', encoding="utf-8")

    cursor = 0
    line_read = ""
    is_incomplete = False

    with raw_file.open("r", encoding="utf-8") as handle:
        handle.seek(cursor)
        line_start = handle.tell()
        line = handle.readline()

        if line and not (line.endswith("\n") or line.endswith("\r")):
            # Deferred!
            is_incomplete = True
            cursor = line_start

    assert is_incomplete is True
    assert cursor == 0

    # Now simulate producer completing the line
    with raw_file.open("a", encoding="utf-8") as handle:
        handle.write('work"}\n')

    # Read again from cursor 0
    with raw_file.open("r", encoding="utf-8") as handle:
        handle.seek(cursor)
        line = handle.readline()
        assert line.endswith("\n")
        data = json.loads(line.strip())
        assert data["raw_id"] == "partial-1"
