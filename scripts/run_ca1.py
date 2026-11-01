"""Run the independent local CA1 pipeline: ingest, validate, transform, Parquet."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset-zip",
        type=Path,
        default=Path(os.getenv("DATASET_ZIP", "data/raw/polygon_dataset.zip")),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/staging"))
    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    )
    parser.add_argument(
        "--storage-backend",
        choices=("local", "s3"),
        default=os.getenv("STORAGE_BACKEND", "local").lower(),
        help="local is the default; s3 uploads raw images and all outputs to an existing bucket",
    )
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", ""))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", "polygon-image-platform"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", ""))
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE", ""))
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.run_id):
        parser.error("run-id may contain only letters, numbers, dot, underscore, and hyphen")
    archive = args.dataset_zip.expanduser().resolve()
    if not archive.is_file():
        parser.error(f"dataset ZIP does not exist: {archive}")
    if args.storage_backend == "s3" and not args.bucket:
        parser.error("--bucket or S3_BUCKET is required when --storage-backend=s3")
    output_dir = args.output_dir.expanduser().resolve()

    stages = [
        [sys.executable, "scripts/ingest_dataset.py", "--dataset-zip", str(archive), "--output-dir", str(output_dir), "--run-id", args.run_id],
        [sys.executable, "scripts/validate_dataset.py", "--run-dir", str(output_dir / args.run_id)],
        [sys.executable, "scripts/transform_ca1.py", "--run-dir", str(output_dir / args.run_id)],
    ]
    if args.storage_backend == "s3":
        upload = [
            sys.executable,
            "scripts/upload_to_s3.py",
            "--run-dir",
            str(output_dir / args.run_id),
            "--bucket",
            args.bucket,
            "--prefix",
            args.prefix,
        ]
        if args.region:
            upload.extend(("--region", args.region))
        if args.profile:
            upload.extend(("--profile", args.profile))
        stages.append(upload)
    for stage in stages:
        print("Running:", " ".join(stage[1:]), flush=True)
        subprocess.run(stage, cwd=ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
