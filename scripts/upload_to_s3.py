"""Explicitly upload one completed CA1 run to an existing private S3 bucket."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.storage.s3_storage import upload_run


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), required=not bool(os.getenv("S3_BUCKET")))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", "polygon-image-platform"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION"))
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE") or None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = upload_run(args.run_dir, args.bucket, args.prefix, args.region, args.profile)
    print(f"Uploaded and verified {result['objects_uploaded_and_verified']} S3 objects ({result['total_bytes']} bytes).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
