"""Safe extraction and dynamic dataset discovery for ZIP archives."""

from __future__ import annotations

import shutil
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


class DatasetArchiveError(ValueError):
    """Raised when an archive is ambiguous, unsafe, or incomplete."""


@dataclass(frozen=True)
class ExtractionResult:
    extracted_root: Path
    targets_csv: Path
    image_paths: tuple[Path, ...]
    extracted_files: int
    ignored_metadata: tuple[str, ...]
    ignored_other: tuple[str, ...]


def _validated_parts(member_name: str) -> tuple[str, ...]:
    """Return safe POSIX path parts, rejecting traversal and platform paths."""
    if not member_name or "\x00" in member_name or "\\" in member_name:
        raise DatasetArchiveError(f"Unsafe ZIP member path: {member_name!r}")
    path = PurePosixPath(member_name)
    if path.is_absolute() or any(part in ("..", "") for part in path.parts):
        raise DatasetArchiveError(f"Unsafe ZIP member path: {member_name!r}")
    if not path.parts:
        raise DatasetArchiveError(f"Empty ZIP member path: {member_name!r}")
    if path.parts and ":" in path.parts[0]:
        raise DatasetArchiveError(f"Drive-qualified ZIP member path: {member_name!r}")
    return tuple(part for part in path.parts if part not in (".", ""))


def _is_metadata(parts: tuple[str, ...]) -> bool:
    return any(
        part == "__MACOSX"
        or part == ".DS_Store"
        or part.startswith("._")
        or part == ".Spotlight-V100"
        for part in parts
    )


def extract_dataset(archive_path: Path, destination: Path) -> ExtractionResult:
    """Extract dataset CSV/PNG files without trusting paths stored in the ZIP.

    Only `targets.csv` and PNG payloads are extracted. macOS metadata and other
    archive entries are counted and reported instead of copied into the dataset.
    `destination` must be a fresh directory so an extraction never overwrites data.
    """
    archive_path = archive_path.expanduser().resolve()
    destination = destination.expanduser().resolve()
    if not archive_path.is_file():
        raise FileNotFoundError(f"Dataset ZIP does not exist: {archive_path}")
    if destination.exists() and any(destination.iterdir()):
        raise FileExistsError(f"Extraction destination is not empty: {destination}")
    destination.mkdir(parents=True, exist_ok=True)

    try:
        archive = zipfile.ZipFile(archive_path)
    except (OSError, zipfile.BadZipFile) as exc:
        raise DatasetArchiveError(f"Could not read ZIP archive {archive_path}: {exc}") from exc

    with archive:
        selected: list[tuple[zipfile.ZipInfo, tuple[str, ...]]] = []
        csv_members: list[tuple[zipfile.ZipInfo, tuple[str, ...]]] = []
        png_members: list[tuple[zipfile.ZipInfo, tuple[str, ...]]] = []
        ignored_metadata: list[str] = []
        ignored_other: list[str] = []
        seen_paths: set[str] = set()

        for info in archive.infolist():
            parts = _validated_parts(info.filename)
            if info.is_dir():
                continue
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise DatasetArchiveError(f"Symbolic links are not allowed in the ZIP: {info.filename}")
            if _is_metadata(parts):
                ignored_metadata.append(info.filename)
                continue

            name = parts[-1].lower()
            is_targets = name == "targets.csv"
            is_png = name.endswith(".png")
            if not (is_targets or is_png):
                ignored_other.append(info.filename)
                continue

            key = "/".join(parts).casefold()
            if key in seen_paths:
                raise DatasetArchiveError(f"Duplicate dataset path in ZIP: {info.filename}")
            seen_paths.add(key)
            member = (info, parts)
            selected.append(member)
            (csv_members if is_targets else png_members).append(member)

        if len(csv_members) != 1:
            raise DatasetArchiveError(
                f"Expected exactly one targets.csv; found {len(csv_members)}"
            )
        if not png_members:
            raise DatasetArchiveError("No PNG images were found in the ZIP archive")

        root_resolved = destination.resolve()
        extracted: dict[str, Path] = {}
        for info, parts in selected:
            output_path = destination.joinpath(*parts)
            resolved = output_path.resolve()
            if resolved != root_resolved and root_resolved not in resolved.parents:
                raise DatasetArchiveError(f"ZIP member escapes extraction directory: {info.filename}")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.exists():
                raise FileExistsError(f"Refusing to overwrite extracted file: {output_path}")
            with archive.open(info) as source, output_path.open("xb") as target:
                shutil.copyfileobj(source, target)
            extracted["/".join(parts)] = output_path

    csv_key = "/".join(csv_members[0][1])
    image_paths = tuple(
        extracted["/".join(parts)]
        for _, parts in sorted(png_members, key=lambda item: "/".join(item[1]).casefold())
    )
    return ExtractionResult(
        extracted_root=destination,
        targets_csv=extracted[csv_key],
        image_paths=image_paths,
        extracted_files=len(extracted),
        ignored_metadata=tuple(ignored_metadata),
        ignored_other=tuple(ignored_other),
    )
