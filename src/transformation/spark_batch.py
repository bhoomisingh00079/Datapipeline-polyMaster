"""Distributed batch analytics over CA1 Parquet using PySpark."""

from __future__ import annotations

import json
import logging
import os
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)


LOGGER = logging.getLogger("polygon_pipeline.spark")

METADATA_SCHEMA = StructType([
    StructField("filename", StringType(), nullable=False),
    StructField("sides", LongType(), nullable=False),
    StructField("bg_color", StringType(), nullable=False),
    StructField("fg_color", StringType(), nullable=False),
    StructField("bound_circle_x", LongType(), nullable=False),
    StructField("bound_circle_y", LongType(), nullable=False),
    StructField("bound_circle_r", LongType(), nullable=False),
    StructField("rotation", LongType(), nullable=False),
    StructField("image_width", LongType(), nullable=False),
    StructField("image_height", LongType(), nullable=False),
    StructField("color_mode", StringType(), nullable=False),
    StructField("image_format", StringType(), nullable=False),
    StructField("file_size_bytes", LongType(), nullable=False),
    StructField("validation_status", StringType(), nullable=False),
    StructField("rotation_bucket", StringType(), nullable=False),
])


def run_spark_batch(run_dir: Path, master: str | None = None) -> dict:
    """Enforce the CA1 schema, derive distributed summaries, and write Parquet."""
    run_dir = run_dir.expanduser().resolve()
    source = run_dir / "processed" / "image_metadata.parquet"
    if not source.is_file():
        raise FileNotFoundError(f"CA1 metadata Parquet not found: {source}")
    output_dir = run_dir / "spark_processed"
    if output_dir.exists():
        raise FileExistsError(f"Spark output already exists; choose a new run: {output_dir}")
    output_dir.mkdir(parents=True)

    spark_builder = (
        SparkSession.builder
        .appName("polygon-image-batch")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    master = master or os.getenv("SPARK_MASTER_URL") or "local[2]"
    driver_host = os.getenv("SPARK_DRIVER_HOST")
    if driver_host:
        spark_builder = (
            spark_builder
            .config("spark.driver.host", driver_host)
            .config("spark.driver.bindAddress", "0.0.0.0")
        )
    spark = spark_builder.master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        data = spark.read.schema(METADATA_SCHEMA).parquet(str(source))
        source_count = data.count()
        if source_count == 0:
            raise ValueError("Spark input contains no records")
        null_predicates = [F.col(field.name).isNull() for field in METADATA_SCHEMA.fields]
        required_nulls = data.filter(reduce(lambda left, right: left | right, null_predicates)).count()
        invalid_rows = data.filter(
            (F.col("sides") < 3)
            | (F.col("bound_circle_r") <= 0)
            | (F.col("rotation") < 0)
            | (F.col("rotation") > 359)
            | (F.col("validation_status") != "valid")
        ).count()
        if required_nulls or invalid_rows:
            raise ValueError(f"Spark schema/business checks failed: nulls={required_nulls}, invalid_rows={invalid_rows}")

        enriched = (
            data
            .withColumn(
                "polygon_type",
                F.when(F.col("sides") == 3, "triangle")
                .when(F.col("sides") == 4, "quadrilateral")
                .when(F.col("sides") == 5, "pentagon")
                .when(F.col("sides") == 6, "hexagon")
                .otherwise("other"),
            )
            .withColumn("color_pair", F.concat_ws("->", "bg_color", "fg_color"))
            .withColumn(
                "rotation_quadrant",
                F.when(F.col("rotation") < 90, "0-89")
                .when(F.col("rotation") < 180, "90-179")
                .when(F.col("rotation") < 270, "180-269")
                .otherwise("270-359"),
            )
        )

        outputs = {
            "polygon_distribution": enriched.groupBy("sides", "polygon_type").count(),
            "color_pairs": enriched.groupBy("bg_color", "fg_color", "color_pair").count(),
            "rotation_summary": enriched.groupBy("rotation_quadrant").count(),
            "image_property_summary": enriched.groupBy(
                "image_width", "image_height", "color_mode", "image_format", "validation_status"
            ).count(),
        }
        output_details = {}
        for name, frame in outputs.items():
            path = output_dir / name
            frame.write.mode("errorifexists").parquet(str(path), compression="snappy")
            read_back = spark.read.parquet(str(path))
            count = read_back.count()
            if count != frame.count():
                raise RuntimeError(f"Spark Parquet read-back count mismatch for {name}")
            output_details[f"{name}/"] = {
                "path": str(path),
                "rows": count,
                "columns": read_back.columns,
                "schema": read_back.schema.jsonValue(),
                "read_back_verified": True,
            }
            LOGGER.info("spark_output=%s rows=%d", path, count)

        summary = {
            "run_id": run_dir.name,
            "spark_version": spark.version,
            "master": master,
            "input_path": str(source),
            "input_rows": source_count,
            "schema_enforced": True,
            "null_rows": required_nulls,
            "invalid_rows": invalid_rows,
            "output_format": "Parquet (Snappy)",
            "outputs": output_details,
        }
        summary_path = output_dir / "spark_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return summary
    finally:
        spark.stop()
