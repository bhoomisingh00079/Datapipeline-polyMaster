"""Run the optional local Delta Lake curated-table demonstration."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.storage.delta_storage import write_delta_demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--master", default=None)
    args = parser.parse_args()
    result = write_delta_demo(args.run_dir, args.master)
    print(
        f"Delta demo complete: {result['current_rows']} rows, "
        f"versions={result['history_versions']}, time-travel rows={result['time_travel_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
