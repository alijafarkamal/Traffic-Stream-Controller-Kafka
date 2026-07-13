# Traffic Stream Controller (Kafka)

A local teaching demo that simulates vehicle telemetry, streams it through Apache Kafka, classifies each event with **speed-threshold rules (default) or optional Gemini**, and exposes a Streamlit control panel over SQLite/CSV.

## Problem

Traffic operations need a mental model for ingest → stream → classify → dashboard. This project is a single-machine pipeline that makes those stages concrete with Kafka, a rule/LLM classifier, and a UI that starts/stops producer and consumer processes.

## Architecture / approach

```
producer.py  →  Kafka topic `traffic-events` (localhost:9092, 1 partition)
                      ↓
                 consumer.py
                      ├─ classify: mock speed bands | optional Gemini | dry-run
                      ├─ SQLite `traffic_events.db`
                      └─ CSV `traffic_events_log.csv`
streamlit_app.py  →  subprocess start/stop via `.pids/` + log tails + SQLite KPIs
```

**Mock thresholds (default):** speed &lt; 5 → accident; &lt; 20 → congested; 20–80 → normal; &gt; 80 → anomaly.

**Optional AI:** `USE_REAL_GEMINI=1` + `GEMINI_API_KEY` → `google.genai` model `gemini-2.5-flash` (falls back to mock on errors/429).

Event payload: `{vehicle_id, speed_kmh, location:{lat,lon}, timestamp_utc}` (random points near fixed SF coords).

## Key engineering decisions

| Decision | Why (as implemented) |
|----------|----------------------|
| Mock classification by default | Zero API cost/rate limits for demos |
| Synchronous `future.get()` per Kafka send | Simple visibility; not a high-throughput design |
| Streamlit as process orchestrator | UI writes `.env`, starts/stops PIDs, tails logs—no Flink/Kafka Streams |
| SQLite + CSV dual write | Easy local inspection |
| Retry with backoff on producer connect | Tolerate slow Kafka Docker startup |

## Tech stack

From `requirements.txt`:

| Package | Version |
|---------|---------|
| kafka-python | 2.0.2 |
| streamlit | 1.28.0 |
| pandas | 2.2.3 |
| python-dotenv | 1.0.1 |
| google-genai | unpinned |

Also present: legacy Flask UI (`gui.py` + `templates/`)—Flask is **not** listed in requirements. Kafka via Confluent Docker images (ZK + broker `7.6.0`) as documented in README run steps.

## How to run

1. Start Zookeeper + Kafka (Docker) and create topic `traffic-events` (see prior run instructions in repo history / Docker `kafka-topics --create`).
2. `pip install -r requirements.txt`
3. `streamlit run streamlit_app.py` → Start Producer / Start Consumer from the UI.

Env knobs: `EVENTS_PER_SECOND` (default 10), `USE_REAL_GEMINI`, `DRY_RUN`, `CSV_LOG_ENABLED`, `VERBOSE`, `GEMINI_API_KEY`.

## Limitations / what I'd improve

- Not a city-scale system: default **10** events/sec (UI max 1000), single partition, sync produce—do not claim “thousands/sec” without measurement.
- Events are simulated, not live sensors; “accident” labels are rule/LLM tags on synthetic speeds.
- Error hints mention `docker-compose` but no compose file ships—add one or fix messages.
- Flask GUI is secondary and under-documented; center Streamlit or remove Flask.
- Add consumer lag metrics, async produce batching, and multi-partition demos for a stronger systems lab.
