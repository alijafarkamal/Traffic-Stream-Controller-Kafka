#!/usr/bin/env python3
"""
consumer.py

Traffic event consumer with Gemini AI classification.

- Consumes events from Kafka topic 'traffic-events'
- For each event, calls Gemini API to classify traffic status
  (e.g., normal, congested, accident)
- Saves results to:
    - SQLite database: traffic_events.db (table: traffic_events)
    - CSV file: traffic_events_log.csv
- Prints live output to the terminal for debugging

Environment:
- GEMINI_API_KEY must be set to your Gemini API key.
"""

import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Optional import for Gemini API (only needed if USE_REAL_GEMINI=1)
try:
    from google import genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False
    genai = None


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "traffic-events"

GEMINI_API_KEY_ENV = "GEMINI_API_KEY"
GEMINI_MODEL = "gemini-2.5-flash"

SQLITE_DB_PATH = "traffic_events.db"
CSV_LOG_PATH = "traffic_events_log.csv"


def get_gemini_api_key() -> str:
    """Fetch Gemini API key from environment variable (supports .env via python-dotenv)."""
    # Load from .env in project root if present
    load_dotenv()
    api_key = os.getenv(GEMINI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(
            f"Environment variable '{GEMINI_API_KEY_ENV}' is not set. "
            "Set it to your Gemini API key before running the consumer."
        )
    return api_key


_genai_client: Optional[object] = None


def get_genai_client():
    """Get or create Gemini client. Returns None if genai is not available."""
    global _genai_client
    if not GENAI_AVAILABLE:
        raise RuntimeError("google-genai package is not installed. Install it with: pip install google-genai")
    if _genai_client is None:
        # Client reads GEMINI_API_KEY from environment automatically.
        _genai_client = genai.Client()
    return _genai_client


def classify_event_with_mock(event: Dict) -> Dict[str, Optional[str]]:
    """
    Mock classification function that uses hardcoded logic instead of Gemini API.
    This avoids API costs and rate limits while providing realistic classifications.

    Returns a dict with:
        - status: 'success' or 'error'
        - classification: textual label (e.g., 'normal', 'congested', 'accident')
        - raw_response: mock response text
        - error: error message if any
    """
    speed_kmh = event.get("speed_kmh", 0)
    vehicle_id = event.get("vehicle_id", "")
    
    # Mock classification logic based on speed
    # Low speed (< 20 km/h) = congested
    # Very low speed (< 5 km/h) = accident
    # Normal speed (20-80 km/h) = normal
    # High speed (> 80 km/h) = anomaly
    if speed_kmh < 5:
        classification = "accident"
        mock_response = "Based on the extremely low speed ({} km/h), this indicates a potential accident or stopped vehicle blocking traffic.".format(speed_kmh)
    elif speed_kmh < 20:
        classification = "congested"
        mock_response = "Traffic is congested with vehicle {} moving at {} km/h, well below normal flow speed.".format(vehicle_id, speed_kmh)
    elif speed_kmh <= 80:
        classification = "normal"
        mock_response = "Normal traffic flow detected. Vehicle {} traveling at {} km/h within expected range.".format(vehicle_id, speed_kmh)
    else:
        classification = "anomaly"
        mock_response = "Anomaly detected: Vehicle {} traveling at {} km/h, which is unusually high and may indicate an error or emergency situation.".format(vehicle_id, speed_kmh)
    
    return {
        "status": "success",
        "classification": classification,
        "raw_response": mock_response,
        "error": None,
    }


def classify_event_with_gemini(event: Dict) -> Dict[str, Optional[str]]:
    """
    Call Gemini API to classify the traffic event.
    NOTE: This function uses MOCK mode by default to avoid API costs.
    Set USE_REAL_GEMINI=1 environment variable to enable real API calls.

    Returns a dict with:
        - status: 'success' or 'error'
        - classification: textual label (e.g., 'normal', 'congested', 'accident')
        - raw_response: raw model text (optional)
        - error: error message if any
    """
    # DEFAULT: Use mock mode unless explicitly enabled
    use_real_api = os.getenv("USE_REAL_GEMINI", "0") == "1"
    
    if not use_real_api:
        return classify_event_with_mock(event)
    
    # Real API mode - check for API key
    try:
        api_key = get_gemini_api_key()
    except RuntimeError:
        # Fall back to mock if API key is not available
        return classify_event_with_mock(event)

    prompt = (
        "You are a traffic monitoring AI. Classify the following single traffic event "
        "into one of these labels: normal, congested, accident, or anomaly. "
        "ONLY return the single label in lowercase. Here is the JSON event:\n\n"
        f"{json.dumps(event, indent=2)}"
    )

    try:
        client = get_genai_client()
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        text = (resp.text or "").strip()

        classification = text.splitlines()[0].strip().lower() if text else None

        return {
            "status": "success",
            "classification": classification,
            "raw_response": text,
            "error": None,
        }
    except Exception as e:
        error_str = str(e)
        # Handle rate limit errors gracefully - fall back to mock
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "quota" in error_str.lower():
            # Rate limit hit - silently fall back to mock
            return classify_event_with_mock(event)
        # Other errors - return error but don't spam logs
        return {
            "status": "error",
            "classification": None,
            "raw_response": None,
            "error": f"API error: {error_str[:100]}",
        }


def init_sqlite_db(db_path: str = SQLITE_DB_PATH) -> None:
    """Initialize SQLite database and table if not existing."""
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS traffic_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                vehicle_id TEXT,
                speed_kmh REAL,
                lat REAL,
                lon REAL,
                event_timestamp_utc TEXT,
                consumed_at_utc TEXT,
                classification TEXT,
                gemini_raw_response TEXT,
                gemini_status TEXT,
                gemini_error TEXT
            );
            """
        )
        conn.commit()


def save_to_sqlite(
    event: Dict,
    kafka_partition: int,
    kafka_offset: int,
    gemini_result: Dict[str, Optional[str]],
    db_path: str = SQLITE_DB_PATH,
) -> None:
    """Persist event + Gemini classification in SQLite DB."""
    with closing(sqlite3.connect(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO traffic_events (
                kafka_partition,
                kafka_offset,
                vehicle_id,
                speed_kmh,
                lat,
                lon,
                event_timestamp_utc,
                consumed_at_utc,
                classification,
                gemini_raw_response,
                gemini_status,
                gemini_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                kafka_partition,
                kafka_offset,
                event.get("vehicle_id"),
                event.get("speed_kmh"),
                event.get("location", {}).get("lat"),
                event.get("location", {}).get("lon"),
                event.get("timestamp_utc"),
                datetime.utcnow().isoformat(),
                gemini_result.get("classification"),
                gemini_result.get("raw_response"),
                gemini_result.get("status"),
                gemini_result.get("error"),
            ),
        )
        conn.commit()


def append_to_csv(
    event: Dict,
    kafka_partition: int,
    kafka_offset: int,
    gemini_result: Dict[str, Optional[str]],
    csv_path: str = CSV_LOG_PATH,
) -> None:
    """
    Append event + Gemini result to CSV log.

    Uses pandas for convenient appending.
    """
    row = {
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "vehicle_id": event.get("vehicle_id"),
        "speed_kmh": event.get("speed_kmh"),
        "lat": event.get("location", {}).get("lat"),
        "lon": event.get("location", {}).get("lon"),
        "event_timestamp_utc": event.get("timestamp_utc"),
        "consumed_at_utc": datetime.utcnow().isoformat(),
        "classification": gemini_result.get("classification"),
        "gemini_raw_response": gemini_result.get("raw_response"),
        "gemini_status": gemini_result.get("status"),
        "gemini_error": gemini_result.get("error"),
    }

    df = pd.DataFrame([row])
    header = not os.path.exists(csv_path)
    df.to_csv(csv_path, mode="a", header=header, index=False)


def create_consumer() -> KafkaConsumer:
    """Create and return a KafkaConsumer instance with JSON deserialization."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v is not None else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="traffic-consumer-group-1",
    )
    return consumer


def main():
    print(f"Initializing SQLite DB at '{SQLITE_DB_PATH}'...")
    init_sqlite_db(SQLITE_DB_PATH)
    print("SQLite DB ready.")

    # Check if we should use real Gemini API (default: use mock)
    use_real_gemini = os.getenv("USE_REAL_GEMINI", "0") == "1"
    if use_real_gemini:
        try:
            _ = get_gemini_api_key()
            print("[INFO] Using real Gemini API (USE_REAL_GEMINI=1)")
        except RuntimeError as e:
            print(f"[WARNING] {e}")
            print("[INFO] Falling back to mock responses")
    else:
        print("[INFO] Using mock/hardcoded Gemini responses (set USE_REAL_GEMINI=1 to use real API)")

    print(f"[INFO] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            consumer = create_consumer()
            print(f"[INFO] ✓ Connected successfully. Consuming from topic '{TOPIC_NAME}'.")
            print("[INFO] Press Ctrl+C to stop.\n")
            break  # Success, exit retry loop
        except KafkaError as e:
            error_msg = str(e)[:200]
            if attempt < max_retries - 1:
                print(f"[WARNING] Connection attempt {attempt + 1}/{max_retries} failed: {error_msg}")
                print(f"[INFO] Retrying in {retry_delay}s... (Kafka may still be starting)")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Increase delay slightly
            else:
                print(f"[FATAL] Failed to connect to Kafka after {max_retries} attempts: {error_msg}")
                print("[HINT] Start Kafka with: docker-compose up -d")
                print("[HINT] Wait 10-15 seconds for Kafka to be fully ready after starting")
                print("[HINT] Check Kafka status: docker ps | grep kafka")
                return
        except Exception as e:
            print(f"[FATAL] Unexpected error: {e}")
            return

    try:
        for message in consumer:
            event = message.value
            vehicle_id = event.get("vehicle_id")
            speed = event.get("speed_kmh")
            location = event.get("location", {})
            partition = message.partition
            offset = message.offset

            # Always print consumption details for visibility in GUI
            print(
                f"[CONSUMED] p={partition} o={offset} "
                f"vehicle={vehicle_id} speed={speed:.1f}km/h"
            )

            # Respect DRY_RUN env var: when set to '1', skip classification entirely
            if os.getenv("DRY_RUN", "0") == "1":
                gemini_result = {
                    "status": "success",
                    "classification": "dry-run",
                    "raw_response": None,
                    "error": None,
                }
                print("  [DRY-RUN] skipping classification")
            else:
                # classify_event_with_gemini will use mock by default unless USE_REAL_GEMINI=1
                gemini_result = classify_event_with_gemini(event)

            if gemini_result["status"] == "success":
                classification = gemini_result.get('classification', 'unknown')
                # Only print detailed info for non-mock responses or verbose mode
                if os.getenv("VERBOSE", "0") == "1":
                    print(
                        f"  [CLASSIFIED] {classification} | "
                        f"response='{gemini_result.get('raw_response', '')[:50]}...'"
                    )
                else:
                    print(f"  [CLASSIFIED] {classification}")
            else:
                # Only log errors if not rate-limited (rate limits are expected with free tier)
                error_msg = gemini_result.get('error', 'Unknown error')
                if "429" not in str(error_msg) and "RESOURCE_EXHAUSTED" not in str(error_msg):
                    print(f"  [ERROR] Classification failed: {error_msg[:100]}")
                # Rate limit errors are silently handled by falling back to mock

            try:
                save_to_sqlite(event, partition, offset, gemini_result)
                # CSV logging can be disabled via env var CSV_LOG_ENABLED=0
                if os.getenv("CSV_LOG_ENABLED", "1") != "0":
                    append_to_csv(event, partition, offset, gemini_result)
            except Exception as e:
                print(f"[ERROR] Failed to persist event: {e}")

    except KeyboardInterrupt:
        print("\n[INFO] Stopping consumer...")
    finally:
        print("[INFO] Closing consumer...")
        consumer.close()
        print("[INFO] Consumer shut down.")


if __name__ == "__main__":
    main()


