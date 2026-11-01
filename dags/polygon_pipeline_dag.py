"""Manual Airflow DAG for the full local batch workflow.

Airflow passes only run paths and small configuration values between tasks; the
dataset, manifests, and DataFrames remain on the shared local data volume.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task


PROJECT_ROOT = Path("/opt/airflow")


def _run(script: str, *args: str) -> None:
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / script), *args],
        cwd=PROJECT_ROOT,
        check=True,
    )


@dag(
    dag_id="polygon_image_end_to_end_pipeline",
    description="Run ingestion, CA1, Spark, simulated Kafka events, Structured Streaming, Delta, and verification.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    # Run directories and streaming checkpoints are intentionally immutable;
    # use a new DAG run ID after a failed run instead of retrying partial output.
    default_args={"retries": 0},
    params={
        "dataset_zip": os.getenv("DATASET_ZIP", "/opt/airflow/data/raw/polygon_dataset.zip"),
        "storage_backend": "local",
        "s3_bucket": "",
        "event_count": 50,
    },
    tags=["polygon", "batch", "ca1"],
)
def polygon_image_batch_pipeline():
    @task(execution_timeout=timedelta(minutes=5))
    def check_input_dataset() -> dict:
        context = get_current_context()
        params = context["params"]
        dataset_zip = Path(params["dataset_zip"])
        if not dataset_zip.is_file():
            raise FileNotFoundError(f"Dataset ZIP not found in Airflow container: {dataset_zip}")
        with zipfile.ZipFile(dataset_zip) as archive:
            names = [item.filename for item in archive.infolist() if not item.is_dir()]
            def is_apple_metadata(name: str) -> bool:
                parts = Path(name).parts
                return any(part.casefold() == "__macosx" or part.startswith("._") for part in parts)

            csv_count = sum(Path(name).name.casefold() == "targets.csv" and not is_apple_metadata(name) for name in names)
            image_count = sum(name.casefold().endswith(".png") and not is_apple_metadata(name) for name in names)
        if csv_count != 1 or image_count == 0:
            raise ValueError(f"Expected one targets.csv and at least one PNG; found CSV={csv_count}, PNG={image_count}")
        backend = str(params.get("storage_backend", "local")).lower()
        if backend not in {"local", "s3"}:
            raise ValueError("storage_backend must be local or s3")
        bucket = str(params.get("s3_bucket", "")).strip()
        if backend == "s3" and not bucket:
            raise ValueError("s3_bucket is required when storage_backend=s3")
        event_count = int(params.get("event_count", 50))
        image_count = sum(name.casefold().endswith(".png") and not is_apple_metadata(name) for name in names)
        if event_count < 1 or event_count > image_count:
            raise ValueError(f"event_count must be between 1 and the {image_count} PNG images in the dataset")
        raw_run_id = str(context["run_id"])
        run_id = re.sub(r"[^A-Za-z0-9_.-]", "-", raw_run_id)
        return {
            "dataset_zip": str(dataset_zip),
            "run_id": run_id,
            "output_dir": str(PROJECT_ROOT / "data" / "staging" / "airflow"),
            "run_dir": str(PROJECT_ROOT / "data" / "staging" / "airflow" / run_id),
            "storage_backend": backend,
            "s3_bucket": bucket,
            "event_count": event_count,
            "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "kafka_topic": "polygon-image-events",
            "source_file_count": len(names),
        }

    @task(execution_timeout=timedelta(minutes=30))
    def extract_and_inventory(config: dict) -> dict:
        _run(
            "ingest_dataset.py",
            "--dataset-zip", config["dataset_zip"],
            "--output-dir", config["output_dir"],
            "--run-id", config["run_id"],
        )
        return config

    @task(execution_timeout=timedelta(minutes=5))
    def wait_for_full_stack(config: dict) -> dict:
        endpoints = [("spark-master", 7077), ("kafka", 29092)]
        deadline = time.monotonic() + 240
        pending = set(endpoints)
        while pending and time.monotonic() < deadline:
            for host, port in tuple(pending):
                try:
                    with socket.create_connection((host, port), timeout=2):
                        pending.remove((host, port))
                except OSError:
                    pass
            if pending:
                time.sleep(3)
        if pending:
            raise TimeoutError(f"Full stack services did not become reachable: {sorted(pending)}")
        return config

    @task(execution_timeout=timedelta(minutes=30))
    def validate_metadata_and_images(config: dict) -> dict:
        _run("validate_dataset.py", "--run-dir", config["run_dir"])
        return config

    @task(execution_timeout=timedelta(minutes=30))
    def pandas_ca1_transform(config: dict) -> dict:
        _run("transform_ca1.py", "--run-dir", config["run_dir"])
        return config

    @task(execution_timeout=timedelta(hours=1))
    def spark_batch_processing(config: dict) -> dict:
        master = os.getenv("SPARK_MASTER_URL", "local[2]")
        _run("run_spark_batch.py", "--run-dir", config["run_dir"], "--master", master)
        return config

    @task(execution_timeout=timedelta(minutes=5))
    def create_kafka_topic(config: dict) -> dict:
        _run(
            "create_kafka_topic.py",
            "--bootstrap-server", config["kafka_bootstrap_servers"],
            "--topic", config["kafka_topic"],
        )
        return config

    @task(execution_timeout=timedelta(minutes=15))
    def publish_simulated_image_events(config: dict) -> dict:
        _run(
            "produce_image_events.py",
            "--manifest", str(Path(config["run_dir"]) / "image_manifest.csv"),
            "--bootstrap-server", config["kafka_bootstrap_servers"],
            "--topic", config["kafka_topic"],
            "--count", str(config["event_count"]),
            "--event-run-id", config["run_id"],
        )
        return config

    @task(execution_timeout=timedelta(hours=1))
    def structured_stream_to_delta(config: dict) -> dict:
        args = [
            "consume_kafka_events.py",
            "--run-dir", config["run_dir"],
            "--bootstrap-servers", config["kafka_bootstrap_servers"],
            "--topic", config["kafka_topic"],
            "--event-run-id", config["run_id"],
            "--master", os.getenv("SPARK_MASTER_URL", "local[2]"),
            "--s3-bucket", config["s3_bucket"] if config["storage_backend"] == "s3" else "",
        ]
        if config["storage_backend"] == "s3":
            args.extend(["--s3-bucket", config["s3_bucket"]])
            args.extend(["--s3-prefix", os.getenv("S3_PREFIX", "polygon-image-platform")])
            if os.getenv("AWS_REGION"):
                args.extend(["--aws-region", os.getenv("AWS_REGION", "")])
            if os.getenv("AWS_PROFILE"):
                args.extend(["--aws-profile", os.getenv("AWS_PROFILE", "")])
        _run(*args)
        return config

    @task(execution_timeout=timedelta(hours=1))
    def upload_to_s3_if_requested(config: dict) -> dict:
        if config["storage_backend"] == "s3":
            _run(
                "upload_to_s3.py",
                "--run-dir", config["run_dir"],
                "--bucket", config["s3_bucket"],
                "--prefix", os.getenv("S3_PREFIX", "polygon-image-platform"),
            )
        return config

    @task(execution_timeout=timedelta(minutes=5))
    def verify_outputs_and_write_summary(config: dict) -> None:
        run_dir = Path(config["run_dir"])
        ingestion = json.loads((run_dir / "ingestion_summary.json").read_text(encoding="utf-8"))
        quality = json.loads((run_dir / "quality_report.json").read_text(encoding="utf-8"))
        transformation = json.loads((run_dir / "transformation_summary.json").read_text(encoding="utf-8"))
        spark = json.loads((run_dir / "spark_processed" / "spark_summary.json").read_text(encoding="utf-8"))
        stream = json.loads((run_dir / "streaming" / "stream_summary.json").read_text(encoding="utf-8"))
        if quality["error_count"] or transformation["metadata_rows_after"] != ingestion["targets_csv_row_count"]:
            raise RuntimeError("Local output verification failed; inspect run artifacts")
        if not all(item["read_back_verified"] for item in transformation["outputs"].values()):
            raise RuntimeError("One or more CA1 Parquet outputs failed read-back verification")
        if stream["delta_rows"] != config["event_count"]:
            raise RuntimeError(
                f"Delta event row count mismatch: expected {config['event_count']}, found {stream['delta_rows']}"
            )
        if config["storage_backend"] == "s3":
            remote = json.loads((run_dir / "s3_upload_summary.json").read_text(encoding="utf-8"))
            if remote["objects_uploaded_and_verified"] <= 0:
                raise RuntimeError("S3 upload did not verify any objects")
        result = {
            "run_id": config["run_id"],
            "storage_backend": config["storage_backend"],
            "dataset_zip": Path(config["dataset_zip"]).name,
            "metadata_rows": ingestion["targets_csv_row_count"],
            "image_count": ingestion["image_count"],
            "quality_status": quality["quality_status"],
            "quality_errors": quality["error_count"],
            "quality_warnings": quality["warning_count"],
            "ca1_parquet_outputs": list(transformation["outputs"]),
            "spark_outputs": list(spark["outputs"]),
            "kafka_topic": stream["topic"],
            "simulated_events_published": config["event_count"],
            "stream_events_processed": stream["valid_events_processed"],
            "stream_events_quarantined": stream["quarantined_events"],
            "delta_rows": stream["delta_rows"],
            "delta_versions": stream["delta_versions"],
        }
        (run_dir / "pipeline_run_summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    config = check_input_dataset()
    ready = wait_for_full_stack(config)
    extracted = extract_and_inventory(ready)
    validated = validate_metadata_and_images(extracted)
    transformed = pandas_ca1_transform(validated)
    spark_processed = spark_batch_processing(transformed)
    stored = upload_to_s3_if_requested(spark_processed)
    topic = create_kafka_topic(stored)
    published = publish_simulated_image_events(topic)
    streamed = structured_stream_to_delta(published)
    verify_outputs_and_write_summary(streamed)


polygon_image_batch_pipeline()
