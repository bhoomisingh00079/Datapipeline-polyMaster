"""Opt-in Boto3 upload and read-after-write verification for an existing S3 bucket."""

from __future__ import annotations

import json
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3


LOGGER = logging.getLogger("polygon_pipeline.s3")


def _s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part.strip("/"))


def upload_run(
    run_dir: Path,
    bucket: str,
    prefix: str = "polygon-image-platform",
    region: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """Upload raw assets/reports/Parquet to an existing bucket and verify sizes.

    This function never creates or changes buckets, ACLs, policies, or credentials.
    Callers must explicitly select the S3 backend. Bucket-level public access and
    encryption defaults should be set by the account owner before uploading.
    """
    if not bucket or bucket.strip() != bucket:
        raise ValueError("An existing S3 bucket name must be supplied without whitespace")
    run_dir = run_dir.expanduser().resolve()
    ingestion_path = run_dir / "ingestion_summary.json"
    quality_path = run_dir / "quality_report.json"
    transform_path = run_dir / "transformation_summary.json"
    for path in (ingestion_path, quality_path, transform_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required local CA1 artifact is missing: {path}")

    ingestion = json.loads(ingestion_path.read_text(encoding="utf-8"))
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    transformation = json.loads(transform_path.read_text(encoding="utf-8"))
    date_part = datetime.fromisoformat(ingestion["generated_at_utc"]).date().isoformat()
    run_id = str(ingestion["run_id"])
    prefix = prefix.strip("/")
    raw_base = _s3_key(prefix, "raw", f"ingestion_date={date_part}", f"run_id={run_id}")
    validated_base = _s3_key(prefix, "validated", f"run_id={run_id}")
    processed_base = _s3_key(prefix, "processed", f"ingestion_date={date_part}", f"run_id={run_id}")

    extracted_root = Path(ingestion["extracted_root"])
    targets_csv = extracted_root / ingestion["targets_csv_path"]
    image_root = extracted_root
    images = sorted(
        (path for path in image_root.rglob("*") if path.is_file() and path.suffix.casefold() == ".png"),
        key=lambda path: path.as_posix().casefold(),
    )
    expected_images = int(ingestion["image_count"])
    if len(images) != expected_images:
        raise RuntimeError(f"Expected {expected_images} extracted PNGs, found {len(images)}")

    ingestion_for_s3 = {
        key: value
        for key, value in ingestion.items()
        if key not in {"source_zip", "extracted_root", "image_manifest_path"}
    }
    ingestion_for_s3["source_zip_filename"] = Path(ingestion["source_zip"]).name
    quality_for_s3 = dict(quality)
    quality_for_s3["metadata_csv"] = "raw/targets.csv"
    quality_for_s3["image_manifest"] = "validated/image_manifest.csv"
    quality_for_s3.pop("validation_issues_path", None)
    transform_for_s3 = dict(transformation)
    transform_for_s3["outputs"] = {
        name: {**details, "path": f"processed/{name}"}
        for name, details in transformation["outputs"].items()
    }

    def json_bytes(value: dict[str, Any]) -> bytes:
        return (json.dumps(value, indent=2) + "\n").encode("utf-8")

    upload_items: list[tuple[Path | None, str, bytes | None, str | None]] = [
        (targets_csv, _s3_key(raw_base, "targets.csv"), None, None),
        (run_dir / "image_manifest.csv", _s3_key(validated_base, "image_manifest.csv"), None, None),
        (None, _s3_key(validated_base, "quality_report.json"), json_bytes(quality_for_s3), "application/json"),
        (run_dir / "validation_issues.csv", _s3_key(validated_base, "validation_issues.csv"), None, None),
        (None, _s3_key(validated_base, "ingestion_summary.json"), json_bytes(ingestion_for_s3), "application/json"),
        (None, _s3_key(validated_base, "transformation_summary.json"), json_bytes(transform_for_s3), "application/json"),
    ]
    upload_items.extend(
        (path, _s3_key(raw_base, "images", path.name), None, None) for path in images
    )
    for name in transformation["outputs"]:
        path = run_dir / "processed" / name
        if not path.is_file():
            raise FileNotFoundError(f"Parquet output is missing: {path}")
        upload_items.append((path, _s3_key(processed_base, name), None, None))

    session_args: dict[str, str] = {}
    if profile:
        session_args["profile_name"] = profile
    if region:
        session_args["region_name"] = region
    client = boto3.Session(**session_args).client("s3")
    verified: list[dict[str, Any]] = []
    total_bytes = 0
    for local_path, key, body, explicit_content_type in upload_items:
        content_type = explicit_content_type
        if local_path is not None:
            if not local_path.is_file():
                raise FileNotFoundError(f"Upload source is missing: {local_path}")
            if content_type is None:
                content_type, _ = mimetypes.guess_type(local_path.name)
            local_size = local_path.stat().st_size
        else:
            local_size = len(body or b"")
        try:
            if local_path is not None:
                extra_args = {"ContentType": content_type} if content_type else None
                client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
            else:
                client.put_object(Bucket=bucket, Key=key, Body=body or b"", ContentType=content_type or "application/octet-stream")
            head = client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise RuntimeError(f"S3 upload/verification failed for s3://{bucket}/{key}: {exc}") from exc
        remote_size = int(head["ContentLength"])
        if remote_size != local_size:
            raise RuntimeError(
                f"S3 size mismatch for s3://{bucket}/{key}: local={local_size}, remote={remote_size}"
            )
        total_bytes += local_size
        verified.append({"key": key, "size_bytes": local_size, "verified": True})
        if len(verified) % 500 == 0:
            LOGGER.info("uploaded_and_verified_objects=%d", len(verified))

    result = {
        "bucket": bucket,
        "prefix": prefix,
        "run_id": run_id,
        "ingestion_date": date_part,
        "objects_uploaded_and_verified": len(verified),
        "total_bytes": total_bytes,
        "verification": "head_object content-length matched each local file",
        "objects": verified,
    }
    summary_path = run_dir / "s3_upload_summary.json"
    summary_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("s3_bucket=%s prefix=%s objects=%d bytes=%d", bucket, prefix, len(verified), total_bytes)
    return result
