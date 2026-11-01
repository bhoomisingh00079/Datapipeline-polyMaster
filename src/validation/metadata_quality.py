"""Validate metadata and reconcile it with the extracted image manifest."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from pathlib import Path, PurePosixPath

import pandas as pd


REQUIRED_COLUMNS = (
    "filename",
    "sides",
    "bg_color",
    "fg_color",
    "bound_circle_x",
    "bound_circle_y",
    "bound_circle_r",
    "rotation",
)
NUMERIC_COLUMNS = (
    "sides",
    "bound_circle_x",
    "bound_circle_y",
    "bound_circle_r",
    "rotation",
)
INTEGER_COLUMNS = NUMERIC_COLUMNS


def normalize_filename(value: object) -> str:
    """Normalize path-like filename values for reconciliation, without editing source."""
    text = str(value or "").strip().replace("\\", "/")
    return PurePosixPath(text).name.casefold()


def _is_index_column(column: str) -> bool:
    return column == "Unnamed: 0" or column == "" or column.startswith("Unnamed:")


def validate_metadata_and_manifest(csv_path: Path, manifest_path: Path) -> tuple[list[dict], dict]:
    """Return issue rows and a summary; never drop or impute source records."""
    metadata = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    manifest = pd.read_csv(manifest_path, dtype=str, keep_default_na=False)
    issues: list[dict] = []

    def issue(code: str, severity: str, details: str, row_index: int | str = "", filename: str = "", field: str = "", value: object = "") -> None:
        issues.append({
            "record_index": row_index,
            "filename": filename,
            "issue_code": code,
            "severity": severity,
            "field": field,
            "value": str(value),
            "details": details,
        })

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in metadata.columns]
    for column in missing_columns:
        issue("missing_required_column", "error", f"Required metadata column {column!r} is absent.", field=column)

    missing_by_column = {
        column: int(metadata[column].map(lambda value: not str(value).strip()).sum())
        for column in metadata.columns
    }
    for column, count in missing_by_column.items():
        if count:
            issue("missing_values", "error", f"{count} rows are blank in column {column!r}.", field=column, value=count)

    index_columns = [column for column in metadata.columns if _is_index_column(str(column))]
    index_checks: dict[str, dict] = {}
    for column in index_columns:
        parsed = pd.to_numeric(metadata[column], errors="coerce")
        is_sequential = (
            len(parsed) == len(metadata)
            and parsed.notna().all()
            and parsed.is_unique
            and parsed.reset_index(drop=True).equals(pd.Series(range(len(metadata)), dtype="int64"))
        )
        index_checks[column] = {
            "present": True,
            "sequential_zero_based": bool(is_sequential),
            "redundant_export_index": bool(is_sequential),
        }
        if not is_sequential:
            issue("non_redundant_export_index", "warning", f"Index-like column {column!r} is not exactly 0..n-1; retain it until reviewed.", field=column)

    if "filename" in metadata.columns:
        filenames = metadata["filename"].astype(str)
        normalized = filenames.map(normalize_filename)
        empty_names = normalized.eq("")
        duplicate_names = normalized.ne("") & normalized.duplicated(keep=False)
        for idx in metadata.index[duplicate_names]:
            issue("duplicate_metadata_filename", "error", "Filename is duplicated after trimming and case-folding.", int(idx), filenames.iloc[idx], "filename", filenames.iloc[idx])
        for idx in metadata.index[empty_names]:
            issue("missing_filename", "error", "Filename is empty after normalization.", int(idx), "", "filename", filenames.iloc[idx])
    else:
        filenames = pd.Series([""] * len(metadata), index=metadata.index)
        normalized = filenames

    if "filename" in manifest.columns:
        manifest_names = manifest["filename"].map(normalize_filename)
    elif "relative_path" in manifest.columns:
        manifest_names = manifest["relative_path"].map(normalize_filename)
    else:
        manifest_names = pd.Series([""] * len(manifest))
        issue("manifest_missing_filename", "error", "Image manifest has neither filename nor relative_path.")

    manifest_duplicate = manifest_names.ne("") & manifest_names.duplicated(keep=False)
    for idx in manifest.index[manifest_duplicate]:
        issue("duplicate_image_filename", "error", "More than one image has this normalized filename.", filename=str(manifest.iloc[idx].get("filename", "")), field="filename", value=manifest_names.iloc[idx])

    image_lookup: dict[str, dict] = {}
    for idx, row in manifest.iterrows():
        name = manifest_names.iloc[idx] if idx < len(manifest_names) else ""
        if name and name not in image_lookup:
            image_lookup[name] = row.to_dict()
        if str(row.get("validation_status", "")).casefold() != "valid":
            issue("invalid_image", "error", str(row.get("validation_error", "Image did not pass Pillow validation.")), filename=str(row.get("filename", "")), field="validation_status", value=row.get("validation_status", ""))

    metadata_names = set(normalized) - {""}
    image_names = set(manifest_names) - {""}
    exact_match_count = 0
    normalized_match_count = 0
    unmatched_metadata_count = 0
    for idx, name in enumerate(normalized):
        if not name:
            continue
        if name not in image_lookup:
            unmatched_metadata_count += 1
            issue("metadata_without_image", "error", "No image manifest entry matches this filename.", idx, filenames.iloc[idx], "filename", filenames.iloc[idx])
        else:
            source_name = str(image_lookup[name].get("filename", ""))
            if filenames.iloc[idx] == source_name:
                exact_match_count += 1
            else:
                normalized_match_count += 1
                issue("normalized_filename_match", "warning", f"Matched after trimming/case-folding; manifest filename is {source_name!r}.", idx, filenames.iloc[idx], "filename", filenames.iloc[idx])

    unmatched_images = sorted(image_names - metadata_names)
    for name in unmatched_images:
        record = image_lookup[name]
        issue("image_without_metadata", "error", "Image has no metadata row.", filename=str(record.get("filename", "")), field="filename", value=name)

    for column in NUMERIC_COLUMNS:
        if column not in metadata.columns:
            continue
        source_values = metadata[column].astype(str).str.strip()
        parsed = pd.to_numeric(source_values, errors="coerce")
        invalid_numeric = source_values.ne("") & parsed.isna()
        for idx in metadata.index[invalid_numeric]:
            issue("invalid_numeric_value", "error", f"Value in {column!r} cannot be parsed as a number.", int(idx), str(filenames.iloc[idx]), column, source_values.iloc[idx])
        non_integer = parsed.notna() & parsed.map(lambda value: not math.isclose(float(value), round(float(value)), abs_tol=1e-9))
        for idx in metadata.index[non_integer]:
            issue("non_integer_numeric_value", "error", f"Expected an integer-valued {column!r} value.", int(idx), str(filenames.iloc[idx]), column, source_values.iloc[idx])

        valid = parsed.dropna()
        if column == "sides":
            out_of_range = valid < 3
            reason = "Polygon side count must be at least 3."
        elif column == "bound_circle_r":
            out_of_range = valid <= 0
            reason = "Bounding-circle radius must be greater than 0."
        elif column == "rotation":
            out_of_range = (valid < 0) | (valid > 360)
            reason = "Rotation must be within the observed degree domain 0..360 inclusive."
        else:
            out_of_range = valid < 0
            reason = f"{column} must not be negative."
        for idx in valid.index[out_of_range]:
            issue("numeric_domain_violation", "error", reason, int(idx), str(filenames.iloc[idx]), column, source_values.iloc[idx])

    # Coordinate and circle checks can use actual dimensions from the manifest.
    for idx, row in metadata.iterrows():
        name = normalized.iloc[idx] if idx < len(normalized) else ""
        image = image_lookup.get(name)
        if not image:
            continue
        try:
            width, height = int(image["image_width"]), int(image["image_height"])
            x = float(row["bound_circle_x"])
            y = float(row["bound_circle_y"])
            radius = float(row["bound_circle_r"])
        except (KeyError, TypeError, ValueError):
            continue
        filename = str(row.get("filename", ""))
        if not (0 <= x < width):
            issue("coordinate_outside_image", "error", f"X center must be in [0, {width - 1}].", int(idx), filename, "bound_circle_x", row.get("bound_circle_x", ""))
        if not (0 <= y < height):
            issue("coordinate_outside_image", "error", f"Y center must be in [0, {height - 1}].", int(idx), filename, "bound_circle_y", row.get("bound_circle_y", ""))
        if radius > 0 and (x - radius < 0 or x + radius > width - 1 or y - radius < 0 or y + radius > height - 1):
            issue("circle_extent_crosses_image_edge", "warning", "Bounding circle extends beyond the image canvas; review whether clipping is permitted.", int(idx), filename, "bound_circle_r", row.get("bound_circle_r", ""))
        try:
            rotation = float(row["rotation"])
        except (KeyError, TypeError, ValueError):
            rotation = None
        if rotation == 360:
            issue("rotation_at_360_boundary", "warning", "360 degrees is equivalent to 0 degrees; retain source value until transformation policy is approved.", int(idx), filename, "rotation", row.get("rotation", ""))

    issue_counts = Counter(item["issue_code"] for item in issues)
    severity_counts = Counter(item["severity"] for item in issues)
    row_match = len(metadata) - unmatched_metadata_count
    summary = {
        "metadata_csv": str(csv_path),
        "image_manifest": str(manifest_path),
        "metadata_row_count": int(len(metadata)),
        "image_manifest_row_count": int(len(manifest)),
        "required_columns": list(REQUIRED_COLUMNS),
        "metadata_columns": [str(column) for column in metadata.columns],
        "missing_required_columns": missing_columns,
        "missing_values_by_column": missing_by_column,
        "index_column_checks": index_checks,
        "filename_reconciliation": {
            "exact_matches": exact_match_count,
            "normalized_matches": normalized_match_count,
            "metadata_without_image": unmatched_metadata_count,
            "images_without_metadata": len(unmatched_images),
        },
        "quality_issues_by_code": dict(sorted(issue_counts.items())),
        "quality_issues_by_severity": dict(sorted(severity_counts.items())),
        "error_count": severity_counts["error"],
        "warning_count": severity_counts["warning"],
        "quality_status": "failed" if severity_counts["error"] else ("passed_with_warnings" if severity_counts["warning"] else "passed"),
        "source_data_modified": False,
        "imputation_performed": False,
    }
    return issues, summary
