"""Run the optional PySpark batch processor on a completed CA1 run."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transformation.spark_batch import run_spark_batch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--master", default=None, help="Spark master URL; defaults to SPARK_MASTER_URL or local[2]")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = run_spark_batch(args.run_dir, args.master)
    print(f"Spark batch complete: {result['input_rows']} input rows; {len(result['outputs'])} Parquet summaries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
