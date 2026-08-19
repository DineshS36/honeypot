# Milestone 1 Documentary

## 1) Milestone Goal
Milestone 1 delivers a working end-to-end telemetry pipeline for the honeypot platform:
- capture raw network events,
- normalize them into a strict canonical schema,
- ingest/store them in SQLite,
- expose query + live stream APIs.

## 2) What Was Built

### A. Shared Canonical Contract
**File:** `/home/runner/work/honeypot/honeypot/shared/event_schema.py`
- Defined a strict `TelemetryEvent` model (Pydantic) used as the system boundary.
- Added enums for `EventSource`, `EventType`, and `Protocol`.
- Added optional structured `NetworkMetadata`.
- Enforced `extra="forbid"` so persisted API events remain canonical.

### B. Sensor Service (Raw Producer)
**File:** `/home/runner/work/honeypot/honeypot/services/sensor/main.py`
- Captures packets using Scapy async sniffer.
- Extracts IP/port/protocol metadata.
- Creates raw event JSON with IDs, host/sensor identity, and packet hash.
- Appends newline-delimited JSON records to `raw.jsonl` with flush + `fsync`.

### C. Normalizer Service (Canonicalizer)
**File:** `/home/runner/work/honeypot/honeypot/services/normalizer/main.py`
- Polls `raw.jsonl` incrementally using cursor-based reads.
- Parses raw JSON defensively (blank/malformed/non-object handling).
- Normalizes timestamp/protocol and maps IDs (`event_id` fallback chain).
- Preserves unknown top-level fields into `attributes` (no silent data loss).
- Validates output against `TelemetryEvent`.
- Appends validated events to `normalized.jsonl` with flush + `fsync`.

### D. API Service (Storage + Access)
**Files:**
- `/home/runner/work/honeypot/honeypot/services/api/app/main.py`
- `/home/runner/work/honeypot/honeypot/services/api/app/db.py`
- `/home/runner/work/honeypot/honeypot/services/api/app/repository.py`
- `/home/runner/work/honeypot/honeypot/services/api/app/sse.py`

Delivered:
- FastAPI app with health endpoint: `GET /health`.
- Event listing endpoint: `GET /events?limit=`.
- Event-by-id endpoint: `GET /events/{event_id}`.
- Live stream endpoint (SSE): `GET /events/stream`.
- Background ingestion loop from `normalized.jsonl` into SQLite (`events` table).
- In-memory broadcaster for real-time event fan-out to SSE clients.

### E. Containerized Runtime
**File:** `/home/runner/work/honeypot/honeypot/docker-compose.yml`
- Three services wired together: `sensor` -> `normalizer` -> `api`.
- Shared volume for queue files (`raw.jsonl`, `normalized.jsonl`).
- Persistent volume for SQLite DB data.
- Environment-driven host/sensor/interface configuration.

### F. Tests for Milestone 1
**Files:**
- `/home/runner/work/honeypot/honeypot/tests/test_normalizer.py`
- `/home/runner/work/honeypot/honeypot/tests/test_milestone1.py`

Coverage includes:
- timestamp and protocol normalization,
- event ID behavior,
- attribute preservation,
- append/write correctness,
- incremental cursor processing,
- malformed JSON recovery,
- incomplete-line buffering,
- live API endpoint behavior (when API is running).

## 3) How It Works (Execution Workflow)
1. **Sensor** sniffs packets and writes one JSON event per line to `raw.jsonl`.
2. **Normalizer** tails `raw.jsonl`, converts raw events to canonical `TelemetryEvent`, and writes to `normalized.jsonl`.
3. **API ingestion loop** tails `normalized.jsonl`, validates/parses each line, inserts into SQLite (dedupe by `event_id` via primary key), and publishes to SSE.
4. **Clients** consume:
   - latest events via `GET /events`,
   - specific event via `GET /events/{event_id}`,
   - live stream via `GET /events/stream`.

## 4) Issues Faced During Milestone 1 and Fixes Applied
The code history indicates five major flaws were addressed:

1. **Unsafe queue write behavior in sensor**
   - Earlier approach used temp-file replacement for append flows.
   - **Fix:** direct append to `raw.jsonl` with `flush + fsync` for stable producer semantics.

2. **Normalizer produced invalid schema defaults when IDs missing**
   - Missing `host_id` / `sensor_id` could break strict model validation.
   - **Fix:** fallback to environment defaults (`HOST_ID`, `SENSOR_ID`) during normalization.

3. **API ingestion risk with partial lines**
   - Reader could consume an incomplete line written by producer.
   - **Fix:** explicit incomplete-line detection and cursor rollback to `line_start`.

4. **Inefficient event lookup endpoint**
   - `GET /events/{event_id}` previously scanned list output.
   - **Fix:** repository-level `get_by_id` query for direct DB lookup.

5. **Brittle live integration tests**
   - Tests failed hard when API container was unavailable.
   - **Fix:** replaced readiness wait/fail with graceful `pytest.skip` when API is not running.

## 5) Current Milestone-1 Status
- Core pipeline is implemented and integrated.
- Canonical schema boundary is enforced.
- Incremental file-based ingestion across services is in place.
- API retrieval and live streaming are available.
- Test suite includes unit-level and live endpoint checks with practical resilience.

## 6) Known Boundaries at Milestone 1
- Queueing uses JSONL files (not message broker backed).
- In-memory SSE broadcaster is process-local.
- Primary implemented telemetry source is network sensor flow.
- Robust production concerns (auth, multi-node stream fan-out, advanced retries/observability) are future-stage work.
