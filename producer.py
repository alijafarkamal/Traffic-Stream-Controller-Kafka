#!/usr/bin/env python3
"""
producer.py

Simulated traffic event producer.

- Generates fake traffic events (vehicle_id, speed, location, timestamp)
- Sends events as JSON messages to Kafka topic 'traffic-events'
- Can be run independently from consumer

Requirements:
- Local Kafka running on localhost:9092
- Topic: traffic-events
"""

import json
import os
import random
import string
import time
from datetime import datetime, timezone
from typing import Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "traffic-events"


def generate_vehicle_id(num_chars: int = 6) -> str:
    """Generate a random vehicle ID like 'CAR-ABC123'."""
    letters = "".join(random.choices(string.ascii_uppercase, k=3))
    digits = "".join(random.choices(string.digits, k=3))
    return f"CAR-{letters}{digits}"


def generate_location() -> Dict[str, float]:
    """
    Generate a pseudo-random location.

    For simplicity, use fixed rough bounds (e.g., for a single city).
    """
    # Example: somewhere near a city center (lat, lon)
    base_lat, base_lon = 37.7749, -122.4194  # San Francisco-like
    lat = base_lat + random.uniform(-0.05, 0.05)
    lon = base_lon + random.uniform(-0.05, 0.05)
    return {"lat": round(lat, 6), "lon": round(lon, 6)}


def generate_event() -> Dict:
    """Generate a single traffic event payload."""
    return {
        "vehicle_id": generate_vehicle_id(),
        "speed_kmh": round(random.uniform(0, 140), 2),
        "location": generate_location(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer instance with JSON serialization."""
    max_retries = 5
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
                retries=2,
                linger_ms=5,
                request_timeout_ms=10000,  # 10 second timeout
                api_version=(0, 10, 1),  # Specify API version to avoid version check delays
                metadata_max_age_ms=30000,  # Refresh metadata every 30s
            )
            # Test connection by trying to get cluster metadata (this happens automatically on first send)
            # We'll test it by checking if we can get the bootstrap servers
            # The actual connection test will happen on first send
            return producer
        except (NoBrokersAvailable, KafkaError) as exc:
            if attempt < max_retries - 1:
                print(f"[WARNING] Kafka connection attempt {attempt + 1}/{max_retries} failed. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"[ERROR] Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
                print(f"[ERROR] {str(exc)[:200]}")
                print("[INFO] Make sure Kafka is running: docker ps | grep kafka")
                print("[INFO] Or start Kafka: docker-compose up -d")
                print("[INFO] Kafka may need a few seconds to be fully ready after starting")
                raise
        except Exception as exc:
            print(f"[ERROR] Unexpected error connecting to Kafka: {exc}")
            raise


def main():
    """
    Main loop:
    - Every second, generate events for multiple vehicles
    - Send them to Kafka
    Behavior can be controlled with env vars:
      - EVENTS_PER_SECOND: number of events per second (default 10)
    """
    events_per_second = int(os.getenv("EVENTS_PER_SECOND", "10"))

    print(f"[INFO] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = create_producer()
        print(f"[INFO] Producer created. Testing connection with first message...")
        
        # Test connection by sending a dummy message
        test_event = generate_event()
        try:
            future = producer.send(TOPIC_NAME, key=test_event["vehicle_id"], value=test_event)
            record_metadata = future.get(timeout=10)
            print(f"[INFO] ✓ Connection successful! Producing to topic '{TOPIC_NAME}' at {events_per_second} events/sec")
        except KafkaError as e:
            print(f"[ERROR] Connection test failed: {str(e)[:200]}")
            print("[INFO] Kafka may still be starting up. Retrying in 3 seconds...")
            time.sleep(3)
            # Try one more time
            try:
                future = producer.send(TOPIC_NAME, key=test_event["vehicle_id"], value=test_event)
                record_metadata = future.get(timeout=10)
                print(f"[INFO] ✓ Connection successful on retry!")
            except KafkaError as e2:
                print(f"[FATAL] Connection test failed again: {str(e2)[:200]}")
                print("[HINT] Make sure Kafka is fully started: docker ps | grep kafka")
                print("[HINT] Wait a few seconds after 'docker-compose up -d' for Kafka to be ready")
                producer.close()
                return
                
    except (NoBrokersAvailable, KafkaError) as e:
        print(f"[FATAL] Failed to create producer: {str(e)[:200]}")
        print(f"[HINT] Start Kafka with: docker-compose up -d")
        print(f"[HINT] Wait 10-15 seconds for Kafka to be fully ready")
        return
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted during startup. Exiting.")
        return

    try:
        while True:
            # allow dynamic change of event rate via env var
            events_per_second = int(os.getenv("EVENTS_PER_SECOND", str(events_per_second)))
            start_time = time.time()
            events_sent = 0
            for _ in range(events_per_second):
                event = generate_event()
                key = event["vehicle_id"]
                try:
                    future = producer.send(TOPIC_NAME, key=key, value=event)
                    record_metadata = future.get(timeout=5)
                    events_sent += 1
                    # Print every event for visibility in GUI
                    print(
                        f"[PRODUCED] p={record_metadata.partition} "
                        f"o={record_metadata.offset} "
                        f"vehicle={event['vehicle_id']} speed={event['speed_kmh']:.1f}km/h"
                    )
                except KafkaError as e:
                    error_msg = str(e)[:150]
                    print(f"[ERROR] Send failed: {error_msg}")
                    # Don't spam errors - wait a bit before retrying
                    time.sleep(0.5)
            
            # Print summary every second
            if events_sent > 0:
                print(f"[STATUS] Sent {events_sent}/{events_per_second} events this second")
            elapsed = time.time() - start_time
            sleep_time = max(0.0, 1.0 - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping producer...")
    finally:
        print("[INFO] Flushing and closing producer...")
        producer.flush()
        producer.close()
        print("[INFO] Producer shut down.")


if __name__ == "__main__":
    main()


