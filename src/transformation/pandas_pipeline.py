"""Clean, enrich, analyze, and write the standalone CA1 dataset with Pandas."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from src.validation.metadata_quality import INTEGER_COLUMNS, normalize_filename


LOGGER = logging.getLogger("polygon_pipeline.transform")


def _write_parquet(frame: pd.DataFrame, path: Path) -> dict[str, str]:
    frame.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    read_back = pd.read_parquet(path, engine="pyarrow")
    if len(read_back) != len(frame) or list(read_back.columns) != list(frame.columns):
        raise RuntimeError(f"Parquet read-back check failed for {path}")
    expected_schema = {column: str(dtype) for column, dtype in frame.dtypes.items()}
    actual_schema = {column: str(dtype) for column, dtype in read_back.dtypes.items()}
    if actual_schema != expected_schema:
        raise RuntimeError(f"Parquet schema read-back check failed for {path}: {actual_schema}")
    return actual_schema


def transform_ca1(run_dir: Path) -> dict:
    """Transform a validated Phase 1/2 run; source files are read-only."""
    run_dir = run_dir.expanduser().resolve()
    ingestion_path = run_dir / "ingestion_summary.json"
    quality_path = run_dir / "quality_report.json"
    if not ingestion_path.is_file() or not quality_path.is_file():
        raise FileNotFoundError("Run Phase 1 ingestion and Phase 2 validation before transformation")

    ingestion = json.loads(ingestion_path.read_text(encoding="utf-8"))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    if quality["error_count"]:
        raise ValueError(
            f"CA1 transformation stopped: quality report contains {quality['error_count']} errors; "
            f"see {quality_path}"
        )

    extracted_root = Path(ingestion["extracted_root"])
    metadata_path = extracted_root / ingestion["targets_csv_path"]
    manifest_path = Path(ingestion["image_manifest_path"])
    raw = pd.read_csv(metadata_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if len(raw) != quality["metadata_row_count"]:
        raise RuntimeError("Metadata row count changed after Phase 2 validation")
    if len(raw) != quality["image_manifest_row_count"]:
        raise RuntimeError("Metadata and image counts differ; refusing to drop or fabricate records")

    transformed = raw.copy()
    cleaning: list[dict] = []
    for column, check in quality["index_column_checks"].items():
        if check["redundant_export_index"]:
            transformed = transformed.drop(columns=[column])
            cleaning.append({
                "operation": "drop_redundant_export_index",
                "column": column,
                "reason": "Verified exact sequential values 0..n-1 in Phase 2.",
                "columns_removed": 1,
                "source_rows_unchanged": len(transformed),
            })

    if "filename" not in transformed.columns:
        raise ValueError("Required filename field is missing")
    before_filename = transformed["filename"].copy()
    transformed["filename"] = transformed["filename"].astype(str).str.strip()
    filename_normalized = int((before_filename != transformed["filename"]).sum())
    cleaning.append({"operation": "trim_filename_whitespace", "rows_affected": filename_normalized})

    for column in ("bg_color", "fg_color"):
        if column not in transformed.columns:
            raise ValueError(f"Required color field {column!r} is missing")
        before = transformed[column].copy()
        transformed[column] = transformed[column].astype(str).str.strip().str.lower()
        cleaning.append({
            "operation": "normalize_color_text",
            "column": column,
            "rows_affected": int((before != transformed[column]).sum()),
        })

    for column in INTEGER_COLUMNS:
        if column not in transformed.columns:
            raise ValueError(f"Required numeric field {column!r} is missing")
        transformed[column] = pd.to_numeric(transformed[column], errors="raise").astype("int64")
        cleaning.append({"operation": "cast_to_int64", "column": column, "rows_affected": len(transformed)})

    rotation_360_count = int(transformed["rotation"].eq(360).sum())
    if rotation_360_count:
        transformed["rotation"] = transformed["rotation"] % 360
        cleaning.append({
            "operation": "normalize_rotation_degrees",
            "rule": "Map 360 degrees to the equivalent 0 degrees using modulo 360.",
            "rows_affected": rotation_360_count,
        })

    manifest = pd.read_csv(manifest_path, dtype=str, keep_default_na=False)
    if len(manifest) != quality["image_manifest_row_count"]:
        raise RuntimeError("Image manifest row count changed after Phase 2 validation")
    manifest["_normalized_filename"] = manifest["filename"].map(normalize_filename)
    if manifest["_normalized_filename"].duplicated().any():
        raise ValueError("Image manifest contains ambiguous duplicate filenames")
    lookup = manifest.set_index("_normalized_filename")
    transformed["_normalized_filename"] = transformed["filename"].map(normalize_filename)
    missing_image = ~transformed["_normalized_filename"].isin(lookup.index)
    if missing_image.any():
        raise ValueError(f"{int(missing_image.sum())} metadata rows do not match an image")

    image_fields = [
        "image_width",
        "image_height",
        "color_mode",
        "image_format",
        "file_size_bytes",
        "validation_status",
    ]
    absent = [field for field in image_fields if field not in lookup.columns]
    if absent:
        raise ValueError(f"Image manifest is missing enrichment fields: {absent}")
    enrichment = lookup[image_fields].copy()
    enrichment["image_width"] = pd.to_numeric(enrichment["image_width"], errors="raise").astype("int64")
    enrichment["image_height"] = pd.to_numeric(enrichment["image_height"], errors="raise").astype("int64")
    enrichment["file_size_bytes"] = pd.to_numeric(enrichment["file_size_bytes"], errors="raise").astype("int64")
    transformed = transformed.join(enrichment, on="_normalized_filename", validate="many_to_one")
    transformed = transformed.drop(columns=["_normalized_filename"])
    if not transformed["validation_status"].eq("valid").all():
        raise ValueError("One or more images failed Pillow validation")
    cleaning.append({"operation": "enrich_image_properties", "rows_affected": len(transformed), "columns": image_fields})

    # The pipeline never imputes or silently drops rows. Phase 2 blocks errors;
    # warnings (edge-crossing circles) remain visible in its report and outputs.
    if len(transformed) != len(raw):
        raise RuntimeError("Transformation changed the row count")

    transformed["rotation_bucket"] = pd.cut(
        transformed["rotation"],
        bins=[-1, 89, 179, 269, 359],
        labels=["0-89", "90-179", "180-269", "270-359"],
    ).astype("string")

    processed_dir = run_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=False)
    outputs: dict[str, pd.DataFrame] = {
        "image_metadata.parquet": transformed,
        "polygon_distribution.parquet": (
            transformed.groupby("sides", as_index=False, observed=True).size().rename(columns={"size": "image_count"})
        ),
        "color_distribution.parquet": pd.concat(
            [
                transformed.groupby("bg_color", as_index=False, observed=True).size().rename(columns={"bg_color": "color", "size": "image_count"}).assign(color_role="background"),
                transformed.groupby("fg_color", as_index=False, observed=True).size().rename(columns={"fg_color": "color", "size": "image_count"}).assign(color_role="foreground"),
            ],
            ignore_index=True,
        )[["color_role", "color", "image_count"]],
        "color_pairs.parquet": (
            transformed.groupby(["bg_color", "fg_color"], as_index=False, observed=True).size().rename(columns={"size": "image_count"})
        ),
        "rotation_summary.parquet": (
            transformed.groupby("rotation_bucket", as_index=False, observed=True).size().rename(columns={"size": "image_count"})
        ),
        "image_quality_summary.parquet": (
            transformed.groupby(["image_width", "image_height", "color_mode", "image_format", "validation_status"], as_index=False, observed=True)
            .size().rename(columns={"size": "image_count"})
        ),
    }

    output_details = {}
    for name, frame in outputs.items():
        path = processed_dir / name
        read_back_schema = _write_parquet(frame, path)
        output_details[name] = {
            "path": str(path),
            "rows": int(len(frame)),
            "columns": list(frame.columns),
            "read_back_schema": read_back_schema,
            "read_back_verified": True,
        }
        LOGGER.info("parquet_output=%s rows=%d", path, len(frame))

    summary = {
        "run_id": ingestion["run_id"],
        "quality_status": quality["quality_status"],
        "metadata_rows_before": int(len(raw)),
        "metadata_rows_after": int(len(transformed)),
        "rows_dropped": 0,
        "imputation_performed": False,
        "cleaning_operations": cleaning,
        "dtypes": {column: str(dtype) for column, dtype in transformed.dtypes.items()},
        "output_format": "Parquet (Snappy)",
        "outputs": output_details,
    }
    summary_path = run_dir / "transformation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("transformation_summary=%s rows=%d", summary_path, len(transformed))
    return summary
