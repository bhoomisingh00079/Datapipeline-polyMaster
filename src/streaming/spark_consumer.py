"""Spark Structured Streaming consumer with local Parquet and Delta sinks."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


LOGGER = logging.getLogger("polygon_pipeline.stream")
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5"
DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.1"
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_run_id", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("schema_version", IntegerType(), True),
])


def _upload_batch(client: Any, bucket: str, prefix: str, run_id: str, batch_id: int, path: Path) -> list[dict]:
    verified = []
    for local_file in sorted(path.glob("*.parquet")):
        key = "/".join(part.strip("/") for part in (
            prefix,
            "streaming/events",
            f"run_id={run_id}",
            f"batch_id={batch_id}",
            local_file.name,
        ) if part.strip("/"))
        client.upload_file(str(local_file), bucket, key, ExtraArgs={"ContentType": "application/vnd.apache.parquet"})
        remote_size = int(client.head_object(Bucket=bucket, Key=key)["ContentLength"])
        local_size = local_file.stat().st_size
        if remote_size != local_size:
            raise RuntimeError(f"S3 verification failed for s3://{bucket}/{key}")
        verified.append({"key": key, "size_bytes": local_size})
    return verified


def run_structured_stream(
    run_dir: Path,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "polygon-image-events",
    master: str | None = None,
    bucket: str | None = None,
    prefix: str = "polygon-image-platform",
    region: str | None = None,
    profile: str | None = None,
    event_run_id: str | None = None,
) -> dict:
    """Consume currently available Kafka events once with checkpointed Spark streaming.

    Each batch is archived as local Parquet, merged idempotently into a local Delta
    table by `event_id`, and optionally uploaded to an existing S3 bucket when the
    caller explicitly supplies `bucket`.
    """
    run_dir = run_dir.expanduser().resolve()
    stream_root = run_dir / "streaming"
    if stream_root.exists():
        raise FileExistsError(f"Streaming output already exists; use a fresh run: {stream_root}")
    stream_root.mkdir(parents=True)
    checkpoint = stream_root / "checkpoints" / "kafka_to_delta"
    parquet_root = stream_root / "parquet_archive"
    quarantine_root = stream_root / "quarantine"
    delta_path = stream_root / "delta" / "image_events"
    ingestion_path = run_dir / "ingestion_summary.json"
    if not ingestion_path.is_file():
        raise FileNotFoundError(f"Phase 1 run summary not found: {ingestion_path}")
    run_id = json.loads(ingestion_path.read_text(encoding="utf-8"))["run_id"]

    package_list = f"{DELTA_PACKAGE},{KAFKA_CONNECTOR}"
    spark_builder = (
        SparkSession.builder
        .appName("polygon-image-kafka-structured-streaming")
        .config("spark.jars.packages", package_list)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
    )
    driver_host = os.getenv("SPARK_DRIVER_HOST")
    if driver_host:
        spark_builder = (
            spark_builder
            .config("spark.driver.host", driver_host)
            .config("spark.driver.bindAddress", "0.0.0.0")
        )
    spark = spark_builder.master(master or os.getenv("SPARK_MASTER_URL") or "local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    s3_client = None
    if bucket:
        session_args = {}
        if profile:
            session_args["profile_name"] = profile
        if region:
            session_args["region_name"] = region
        s3_client = boto3.Session(**session_args).client("s3")

    batch_summaries: list[dict] = []

    def process_batch(kafka_batch: DataFrame, batch_id: int) -> None:
        raw = kafka_batch.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        decoded = raw.withColumn("event", F.from_json("raw_json", EVENT_SCHEMA))
        if event_run_id:
            decoded = decoded.filter(F.col("event.event_run_id") == event_run_id)
        decoded = decoded.withColumn("parsed_event_time", F.to_timestamp("event.event_timestamp"))
        invalid = decoded.filter(
            F.col("event").isNull()
            | (F.length(F.trim(F.col("event.event_id"))) == 0)
            | (F.length(F.trim(F.col("event.filename"))) == 0)
            | (F.length(F.trim(F.col("event.event_type"))) == 0)
            | (F.length(F.trim(F.col("event.validation_status"))) == 0)
            | F.col("parsed_event_time").isNull()
        )
        invalid_count = invalid.count()
        if invalid_count:
            invalid_path = quarantine_root / f"batch_id={batch_id}"
            invalid.select("raw_json", "kafka_topic", "kafka_partition", "kafka_offset").write.mode("overwrite").parquet(str(invalid_path))

        valid = (
            decoded.filter(
                F.col("event").isNotNull()
                & (F.length(F.trim(F.col("event.event_id"))) > 0)
                & (F.length(F.trim(F.col("event.filename"))) > 0)
                & (F.length(F.trim(F.col("event.event_type"))) > 0)
                & (F.length(F.trim(F.col("event.validation_status"))) > 0)
                & F.col("parsed_event_time").isNotNull()
            )
            .select("event.*", "parsed_event_time", "kafka_topic", "kafka_partition", "kafka_offset")
            .withColumnRenamed("parsed_event_time", "event_time")
            .dropDuplicates(["event_id"])
        ).cache()
        valid_count = valid.count()
        if valid_count:
            parquet_path = parquet_root / f"batch_id={batch_id}"
            valid.write.mode("overwrite").parquet(str(parquet_path), compression="snappy")
            if not DeltaTable.isDeltaTable(spark, str(delta_path)):
                valid.write.format("delta").mode("errorifexists").save(str(delta_path))
            else:
                (
                    DeltaTable.forPath(spark, str(delta_path)).alias("target")
                    .merge(valid.alias("updates"), "target.event_id = updates.event_id")
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            uploaded = _upload_batch(s3_client, bucket, prefix, run_id, batch_id, parquet_path) if s3_client else []
            batch_summaries.append({
                "batch_id": int(batch_id),
                "valid_events": int(valid_count),
                "quarantined_events": int(invalid_count),
                "s3_objects_verified": uploaded,
            })
            LOGGER.info("stream_batch=%d valid=%d quarantined=%d", batch_id, valid_count, invalid_count)
        elif invalid_count:
            batch_summaries.append({"batch_id": int(batch_id), "valid_events": 0, "quarantined_events": int(invalid_count), "s3_objects_verified": []})
        valid.unpersist()

    try:
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .load()
        )
        query = (
            source.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        delta_versions = []
        delta_rows = 0
        if DeltaTable.isDeltaTable(spark, str(delta_path)):
            delta_table = DeltaTable.forPath(spark, str(delta_path))
            delta_rows = spark.read.format("delta").load(str(delta_path)).count()
            delta_versions = [int(row["version"]) for row in delta_table.history().orderBy("version").collect()]
        summary = {
            "run_id": run_id,
            "source_type": "simulated_events_from_static_dataset_manifest",
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "event_run_id_filter": event_run_id,
            "trigger": "availableNow",
            "checkpoint": str(checkpoint),
            "batches": batch_summaries,
            "valid_events_processed": sum(item["valid_events"] for item in batch_summaries),
            "quarantined_events": sum(item["quarantined_events"] for item in batch_summaries),
            "delta_table_path": str(delta_path) if delta_rows else None,
            "delta_rows": delta_rows,
            "delta_versions": delta_versions,
            "s3_upload_enabled": bool(bucket),
        }
        summary_path = stream_root / "stream_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return summary
    finally:
        spark.stop()
