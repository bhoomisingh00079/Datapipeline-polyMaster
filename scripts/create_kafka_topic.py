"""Create the demo Kafka topic without changing a running broker's configuration."""

from __future__ import annotations

import argparse

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="polygon-image-events")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("partitions and replication-factor must be positive")
    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_server, client_id="polygon-topic-admin")
    try:
        try:
            futures = admin.create_topics([
                NewTopic(args.topic, num_partitions=args.partitions, replication_factor=args.replication_factor)
            ], validate_only=False)
            for future in futures.values():
                future.get(timeout=30)
            print(f"Created topic {args.topic!r} at {args.bootstrap_server}")
        except TopicAlreadyExistsError:
            print(f"Topic {args.topic!r} already exists")
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
