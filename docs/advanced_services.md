# Optional local services

The CA1 pipeline is the baseline and does not need Docker, Java, Spark, Kafka, or
Airflow. These examples assume Docker Desktop is running, the ZIP is at
`data/raw/polygon_dataset.zip`, and the baseline CA1 run has completed. Compose
profiles start only the services named in the command.

## Spark batch and Delta Lake

Start Spark's master, worker, and app container:

```bash
docker compose --profile batch up -d --build
```

Run the distributed summaries against a completed CA1 run (replace the run ID):

```bash
docker compose --profile batch exec spark-app \
  python scripts/run_spark_batch.py --run-dir data/staging/<run-id>
```

Run the optional local Delta schema evolution and time-travel demonstration:

```bash
docker compose --profile batch exec spark-app \
  python scripts/write_delta_demo.py --run-dir data/staging/<run-id>
```

Spark results are written beneath that run's `spark_processed/` and `delta/`
directories. Each command refuses to overwrite an existing output directory;
use a fresh CA1 run ID for a rerun.

## Kafka and Structured Streaming

Start the Kafka broker and Spark services:

```bash
docker compose --profile stream up -d --build
```

Create the topic and publish up to 50 deterministic simulated events from the
manifest:

```bash
docker compose --profile stream exec spark-app \
  python scripts/create_kafka_topic.py --bootstrap-server kafka:29092
docker compose --profile stream exec spark-app \
  python scripts/produce_image_events.py \
  --manifest data/staging/<run-id>/image_manifest.csv \
  --bootstrap-server kafka:29092 --count 50
```

Consume the currently available events once, archive them to Parquet, quarantine
invalid events, and merge valid events by event ID into Delta:

```bash
docker compose --profile stream exec spark-app \
  python scripts/consume_kafka_events.py \
  --run-dir data/staging/<run-id> --bootstrap-servers kafka:29092
```

The stream uses Spark's `availableNow` trigger, so it terminates after the events
currently available have been processed. To add more events, use a fresh dataset
run directory for the next streaming execution because checkpoints are retained.

## Full end-to-end pipeline with Airflow, Kafka, Spark, and Delta

The `full` profile starts Airflow, Kafka, Spark master/worker, and the Spark
application container together. The Airflow DAG runs extraction, image inventory,
quality validation, the CA1 Pandas/Parquet transform, Spark batch summaries, event
topic creation, simulated image events, Spark Structured Streaming, a Delta merge,
and final output verification. It uses the same local project data directory
throughout. This is the recommended end-to-end execution path. Airflow credentials
below are local container secrets; keep `.env` untracked.

```bash
cp .env.example .env
```

Set `AIRFLOW_UID` to the result of `id -u`. Generate separate values for
`AIRFLOW_DB_PASSWORD` and `AIRFLOW_API_SECRET_KEY` (for example with
`python3 -c 'import secrets; print(secrets.token_hex(32))'`) and place them only in
`.env`. Set `SPARK_MASTER_URL=spark://spark-master:7077` for the distributed full
profile. Ensure the dataset file exists at `data/raw/polygon_dataset.zip`.

Build and start all end-to-end services:

```bash
docker compose --profile full up -d --build
```

Open `http://127.0.0.1:8080`, enable `polygon_image_end_to_end_pipeline`, and
trigger it manually. Its default parameters process 50 simulated image events;
set `event_count` to a value from 1 through the dataset image count to change the
demo size. Each DAG run uses a distinct event key and filters the shared Kafka
topic to its own events. After a failure, start a new DAG run so each attempt gets
fresh staging and checkpoint paths.

The DAG defaults to local storage. S3 is optional and only uploads if the DAG run
configuration explicitly sets `storage_backend` to `s3` and supplies `s3_bucket`;
AWS credentials must be available to the container through the standard
environment or credential chain. AWS upload is not needed for the full local run.
The `airflow` profile alone starts Airflow without the Kafka/Spark services and is
useful for DAG development; use `full` to execute this end-to-end DAG.

To stop services while preserving their named volumes:

```bash
docker compose --profile airflow down
docker compose --profile full down
docker compose --profile batch down
docker compose --profile stream down
```

## Runtime status

The complete Spark, Delta, Kafka, and Airflow orchestration is implemented in
code, but service execution is not yet verified. In this environment the Docker CLI is
installed while the Docker daemon socket is inaccessible. Compose configuration
can be checked without starting containers; image builds and end-to-end service
runs require Docker Desktop to be available.
