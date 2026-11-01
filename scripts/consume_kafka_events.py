"""Run Spark Structured Streaming against the demo Kafka topic."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.streaming.spark_consumer import run_structured_stream


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="polygon-image-events")
    parser.add_argument("--event-run-id", default=None, help="Process only events from this execution key")
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER_URL"))
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET") or None)
    parser.add_argument("--s3-prefix", default=os.getenv("S3_PREFIX", "polygon-image-platform"))
    parser.add_argument("--aws-region", default=os.getenv("AWS_REGION"))
    parser.add_argument("--aws-profile", default=os.getenv("AWS_PROFILE") or None)
    args = parser.parse_args()
    result = run_structured_stream(
        args.run_dir,
        args.bootstrap_servers,
        args.topic,
        args.master,
        args.s3_bucket,
        args.s3_prefix,
        args.aws_region,
        args.aws_profile,
        args.event_run_id,
    )
    print(
        f"Streaming complete: {result['valid_events_processed']} valid events, "
        f"{result['quarantined_events']} quarantined"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
