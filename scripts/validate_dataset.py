"""Generate metadata/image quality diagnostics for a Phase 1 ingestion run."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.validation.metadata_quality import validate_metadata_and_manifest


LOGGER = logging.getLogger("polygon_pipeline.validation")
ISSUE_FIELDS = ("record_index", "filename", "issue_code", "severity", "field", "value", "details")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True, help="Phase 1 run directory under data/staging")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    run_dir = args.run_dir.expanduser().resolve()
    summary_path = run_dir / "ingestion_summary.json"
    if not summary_path.is_file():
        raise SystemExit(f"Missing Phase 1 ingestion summary: {summary_path}")
    ingestion = json.loads(summary_path.read_text(encoding="utf-8"))
    extracted_root = Path(ingestion["extracted_root"])
    csv_path = extracted_root / ingestion["targets_csv_path"]
    manifest_path = Path(ingestion["image_manifest_path"])
    for path in (csv_path, manifest_path):
        if not path.is_file():
            raise SystemExit(f"Required Phase 1 artifact not found: {path}")

    issues, report = validate_metadata_and_manifest(csv_path, manifest_path)
    issues_path = run_dir / "validation_issues.csv"
    with issues_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ISSUE_FIELDS)
        writer.writeheader()
        writer.writerows(issues)
    report_path = run_dir / "quality_report.json"
    report["validation_issues_path"] = str(issues_path)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    LOGGER.info(
        "status=%s metadata_rows=%d images=%d exact_matches=%d normalized_matches=%d errors=%d warnings=%d",
        report["quality_status"],
        report["metadata_row_count"],
        report["image_manifest_row_count"],
        report["filename_reconciliation"]["exact_matches"],
        report["filename_reconciliation"]["normalized_matches"],
        report["error_count"],
        report["warning_count"],
    )
    LOGGER.info("quality_report=%s issues=%s", report_path, issues_path)
    return 0 if report["quality_status"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
