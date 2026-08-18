from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
import time
import uuid
from pathlib import Path

from scapy.all import AsyncSniffer, IP, IPv6, TCP, UDP


QUEUE_DIR = Path(os.getenv("QUEUE_DIR", "/shared/events"))
RAW_FILE = QUEUE_DIR / "raw.jsonl"

INTERFACE = os.getenv("SENSOR_INTERFACE", "any")
HOST_ID = os.getenv("HOST_ID", socket.gethostname())
SENSOR_ID = os.getenv("SENSOR_ID", "network-sensor-01")


def protocol_for_packet(packet) -> str:
    if TCP in packet:
        return "tcp"

    if UDP in packet:
        return "udp"

    return "other"


def extract_packet(packet) -> dict:
    src_ip = None
    dst_ip = None

    if IP in packet:
        src_ip = packet[IP].src
        dst_ip = packet[IP].dst

    elif IPv6 in packet:
        src_ip = packet[IPv6].src
        dst_ip = packet[IPv6].dst

    src_port = None
    dst_port = None

    if TCP in packet:
        src_port = packet[TCP].sport
        dst_port = packet[TCP].dport

    elif UDP in packet:
        src_port = packet[UDP].sport
        dst_port = packet[UDP].dport

    payload_size = max(len(bytes(packet)) - 40, 0)

    return {
        "raw_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "source": "network_sensor",
        "event_type": "network_activity",
        "host_id": HOST_ID,
        "sensor_id": SENSOR_ID,
        "network": {
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": protocol_for_packet(packet),
            "payload_size": payload_size,
            "interface": INTERFACE,
        },
        "attributes": {
            "packet_hash": hashlib.sha256(bytes(packet)).hexdigest(),
        },
    }


def append_raw(event: dict) -> None:
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)

    temporary = RAW_FILE.with_suffix(".tmp")

    with temporary.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    temporary.replace(RAW_FILE)


def handle_packet(packet) -> None:
    try:
        event = extract_packet(packet)

        if not event["network"]["src_ip"]:
            return

        append_raw(event)

    except Exception as exc:
        print(
            f"[sensor] packet processing failure: {exc}",
            file=sys.stderr,
            flush=True,
        )


def main() -> None:
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)

    print(
        f"[sensor] starting interface={INTERFACE} "
        f"host={HOST_ID} sensor={SENSOR_ID}",
        flush=True,
    )

    sniffer = AsyncSniffer(
        iface=None if INTERFACE == "any" else INTERFACE,
        prn=handle_packet,
        store=False,
    )

    try:
        sniffer.start()

        while True:
            time.sleep(5)

    except KeyboardInterrupt:
        print("[sensor] shutting down", flush=True)

    except Exception as exc:
        print(f"[sensor] fatal failure: {exc}", file=sys.stderr)

        try:
            sniffer.stop()
        except Exception:
            pass

        raise


if __name__ == "__main__":
    main()