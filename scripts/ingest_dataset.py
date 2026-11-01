"""Extract the supplied ZIP and write a first-pass image inventory."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make direct invocation (`python scripts/ingest_dataset.py`) work from the repo.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.archive import extract_dataset
from src.ingestion.image_inventory import MANIFEST_FIELDS, inventory_images


LOGGER = logging.getLogger("polygon_pipeline.ingestion")


def count_csv_rows(csv_path: Path) -> tuple[int, list[str]]:
    """Count records and capture headers without imposing Phase 2 schema rules."""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        return sum(1 for _ in reader), columns


def write_manifest(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset-zip",
        type=Path,
        default=Path(os.getenv("DATASET_ZIP", "data/raw/polygon_dataset.zip")),
        help="Input dataset ZIP (default: DATASET_ZIP or data/raw/polygon_dataset.zip)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/staging"),
        help="Parent directory for this run's extraction and reports",
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        help="Unique run identifier used to avoid overwriting previous output",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.run_id):
        raise SystemExit("run-id may contain only letters, numbers, dot, underscore, and hyphen")

    archive_path = args.dataset_zip.expanduser().resolve()
    if not archive_path.is_file():
        raise SystemExit(f"Dataset ZIP does not exist: {archive_path}")
    run_dir = args.output_dir.expanduser().resolve() / args.run_id
    extracted_dir = run_dir / "extracted"
    run_dir.mkdir(parents=True, exist_ok=False)
    LOGGER.info("dataset_zip=%s", archive_path)
    LOGGER.info("run_dir=%s", run_dir)

    extraction = extract_dataset(archive_path, extracted_dir)
    LOGGER.info(
        "extracted_files=%d targets_csv=%s png_files=%d ignored_macos_metadata=%d ignored_other=%d",
        extraction.extracted_files,
        extraction.targets_csv.relative_to(extracted_dir).as_posix(),
        len(extraction.image_paths),
        len(extraction.ignored_metadata),
        len(extraction.ignored_other),
    )

    metadata_rows, columns = count_csv_rows(extraction.targets_csv)
    records, image_summary = inventory_images(extraction.image_paths, extracted_dir)
    manifest_path = run_dir / "image_manifest.csv"
    write_manifest(manifest_path, records)
    summary = {
        "run_id": args.run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_zip": str(archive_path),
        "source_zip_size_bytes": archive_path.stat().st_size,
        "extracted_root": str(extracted_dir),
        "targets_csv_path": extraction.targets_csv.relative_to(extracted_dir).as_posix(),
        "targets_csv_row_count": metadata_rows,
        "targets_csv_columns": columns,
        "extracted_dataset_file_count": extraction.extracted_files,
        "ignored_macos_metadata_count": len(extraction.ignored_metadata),
        "ignored_macos_metadata_examples": list(extraction.ignored_metadata[:10]),
        "ignored_other_file_count": len(extraction.ignored_other),
        "ignored_other_examples": list(extraction.ignored_other[:10]),
        **image_summary,
        "image_manifest_path": str(manifest_path),
    }
    summary_path = run_dir / "ingestion_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    LOGGER.info(
        "metadata_rows=%d images=%d valid_images=%d invalid_images=%d dimensions=%s modes=%s",
        metadata_rows,
        image_summary["image_count"],
        image_summary["valid_image_count"],
        image_summary["invalid_image_count"],
        image_summary["dimensions"],
        image_summary["color_modes"],
    )
    LOGGER.info("manifest=%s summary=%s", manifest_path, summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
