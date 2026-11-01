"""Pillow-based inventory of extracted PNG images."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from PIL import Image


MANIFEST_FIELDS = (
    "filename",
    "relative_path",
    "image_width",
    "image_height",
    "color_mode",
    "image_format",
    "file_size_bytes",
    "validation_status",
    "validation_error",
)


def inventory_images(image_paths: tuple[Path, ...], extracted_root: Path) -> tuple[list[dict], dict]:
    """Inspect every image; retain invalid entries with a reason in the manifest."""
    records: list[dict] = []
    dimensions: Counter[str] = Counter()
    modes: Counter[str] = Counter()
    formats: Counter[str] = Counter()
    valid_count = 0

    for path in image_paths:
        record = {
            "filename": path.name,
            "relative_path": path.relative_to(extracted_root).as_posix(),
            "image_width": "",
            "image_height": "",
            "color_mode": "",
            "image_format": "",
            "file_size_bytes": path.stat().st_size,
            "validation_status": "invalid",
            "validation_error": "",
        }
        try:
            with Image.open(path) as image:
                image.verify()
            with Image.open(path) as image:
                record["image_width"] = image.width
                record["image_height"] = image.height
                record["color_mode"] = image.mode
                record["image_format"] = image.format or ""
            record["validation_status"] = "valid"
            valid_count += 1
            dimensions[f"{record['image_width']}x{record['image_height']}"] += 1
            modes[record["color_mode"]] += 1
            formats[record["image_format"]] += 1
        except Exception as exc:  # Preserve the failed image and its diagnostic.
            record["validation_error"] = f"{type(exc).__name__}: {exc}"
        records.append(record)

    summary = {
        "image_count": len(records),
        "valid_image_count": valid_count,
        "invalid_image_count": len(records) - valid_count,
        "dimensions": dict(sorted(dimensions.items())),
        "color_modes": dict(sorted(modes.items())),
        "formats": dict(sorted(formats.items())),
    }
    return records, summary
