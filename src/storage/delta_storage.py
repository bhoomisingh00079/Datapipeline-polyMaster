"""Optional local Delta Lake curated-table and transaction demonstration."""

from __future__ import annotations

import json
from importlib.metadata import version
import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def write_delta_demo(run_dir: Path, master: str | None = None) -> dict:
    """Write a curated Delta table, evolve one field via MERGE, and read history."""
    run_dir = run_dir.expanduser().resolve()
    source_path = run_dir / "processed" / "image_metadata.parquet"
    if not source_path.is_file():
        raise FileNotFoundError(f"CA1 metadata Parquet not found: {source_path}")
    delta_path = run_dir / "delta" / "polygon_metadata"
    if delta_path.exists():
        raise FileExistsError(f"Delta output already exists: {delta_path}")
    delta_path.parent.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName("polygon-image-delta-demo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
    )
    builder = builder.master(master or os.getenv("SPARK_MASTER_URL") or "local[2]")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        metadata = spark.read.parquet(str(source_path))
        source_rows = metadata.count()
        if source_rows == 0:
            raise ValueError("Delta input has no rows")
        metadata.write.format("delta").mode("errorifexists").save(str(delta_path))

        first_record = metadata.select("filename").orderBy("filename").limit(1).collect()
        if not first_record:
            raise ValueError("Could not choose a record for the controlled Delta MERGE demonstration")
        target_filename = first_record[0]["filename"]
        updates = (
            metadata.filter(F.col("filename") == target_filename)
            .select("*")
            .withColumn("quality_review_note", F.lit("Delta MERGE schema-evolution demonstration"))
        )
        (
            DeltaTable.forPath(spark, str(delta_path)).alias("target")
            .merge(updates.alias("updates"), "target.filename = updates.filename")
            .whenMatchedUpdateAll()
            .execute()
        )

        current = spark.read.format("delta").load(str(delta_path))
        current_rows = current.count()
        review_count = current.filter(F.col("quality_review_note").isNotNull()).count()
        if current_rows != source_rows or review_count != 1:
            raise RuntimeError(
                f"Delta verification failed: rows={current_rows}/{source_rows}, review_rows={review_count}"
            )
        history = DeltaTable.forPath(spark, str(delta_path)).history()
        history_rows = history.orderBy("version").collect()
        version_zero = spark.read.format("delta").option("versionAsOf", 0).load(str(delta_path))
        version_zero_rows = version_zero.count()
        if version_zero_rows != source_rows or "quality_review_note" in version_zero.columns:
            raise RuntimeError("Delta time-travel read-back did not match the initial table version")

        summary = {
            "delta_version": version("delta-spark"),
            "spark_version": spark.version,
            "table_path": str(delta_path),
            "initial_rows": source_rows,
            "current_rows": current_rows,
            "schema_evolved_column": "quality_review_note",
            "merge_updated_rows": review_count,
            "merge_target_filename": target_filename,
            "history_versions": [int(row["version"]) for row in history_rows],
            "time_travel_version": 0,
            "time_travel_rows": version_zero_rows,
            "current_schema": current.schema.jsonValue(),
        }
        summary_path = run_dir / "delta" / "delta_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return summary
    finally:
        spark.stop()
