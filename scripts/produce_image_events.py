"""Simulate Kafka image-validation events from the static Phase 1 manifest."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer

from src.streaming.events import make_image_event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="polygon-image-events")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--event-run-id", default="static-manifest", help="Execution key used to isolate and deduplicate this event batch")
    parser.add_argument("--interval-seconds", type=float, default=0.05)
    args = parser.parse_args()
    if args.count < 1 or args.interval_seconds < 0:
        parser.error("count must be positive and interval-seconds cannot be negative")
    with open(args.manifest, encoding="utf-8", newline="") as handle:
        records = list(csv.DictReader(handle))
    if not records:
        parser.error("image manifest contains no rows")
    if args.count > len(records):
        parser.error(f"count {args.count} exceeds manifest rows {len(records)}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        client_id="polygon-image-event-simulator",
        acks="all",
        retries=5,
        value_serializer=lambda event: json.dumps(event, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    try:
        for record in records[: args.count]:
            event = make_image_event(record["filename"], record["validation_status"], args.event_run_id)
            producer.send(args.topic, key=event["event_id"], value=event).get(timeout=15)
            print(json.dumps(event, sort_keys=True))
            if args.interval_seconds:
                time.sleep(args.interval_seconds)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    print(f"Published {args.count} simulated events to {args.topic!r} at {args.bootstrap_server}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
