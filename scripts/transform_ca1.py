"""Run the CA1 Pandas transformation on one Phase 1/2 staging run."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transformation.pandas_pipeline import transform_ca1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    summary = transform_ca1(args.run_dir)
    print(f"CA1 transformation complete: {summary['metadata_rows_after']} rows; {len(summary['outputs'])} Parquet outputs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
