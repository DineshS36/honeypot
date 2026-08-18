from pathlib import Path


BASE_DIR = Path("/data")

DATABASE_PATH = BASE_DIR / "telemetry.db"

NORMALIZED_EVENTS_FILE = Path("/shared/events/normalized.jsonl")