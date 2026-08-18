from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress


class EventSource(str, Enum):
    NETWORK_SENSOR = "network_sensor"
    HOST_SENSOR = "host_sensor"
    DECOY = "decoy"


class EventType(str, Enum):
    NETWORK_ACTIVITY = "network_activity"
    PROCESS_START = "process_start"
    FILE_MODIFICATION = "file_modification"
    COMMAND_EXECUTION = "command_execution"


class Protocol(str, Enum):
    TCP = "tcp"
    UDP = "udp"
    ICMP = "icmp"
    OTHER = "other"


class NetworkMetadata(BaseModel):
    src_ip: IPvAnyAddress | None = None
    dst_ip: IPvAnyAddress | None = None

    src_port: int | None = Field(default=None, ge=0, le=65535)
    dst_port: int | None = Field(default=None, ge=0, le=65535)

    protocol: Protocol | None = None

    payload_size: int = Field(default=0, ge=0)

    interface: str | None = None


class TelemetryEvent(BaseModel):
    """
    Canonical telemetry contract.

    Every current and future telemetry source MUST emit this model.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: str
    timestamp: datetime

    source: EventSource
    event_type: EventType

    host_id: str
    sensor_id: str

    network: NetworkMetadata | None = None

    process_name: str | None = None
    process_pid: int | None = None

    file_path: str | None = None
    command: str | None = None

    attributes: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(timezone.utc)