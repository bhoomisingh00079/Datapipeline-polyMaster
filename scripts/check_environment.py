"""Report local prerequisites without installing packages or contacting AWS."""

from __future__ import annotations

import importlib.metadata
import platform
import shutil
import subprocess
import sys


def command_output(command: list[str]) -> str:
    if not shutil.which(command[0]):
        return "not installed"
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=8)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"unavailable ({exc})"
    if result.returncode:
        error_lines = result.stderr.strip().splitlines()
        return error_lines[0] if error_lines else f"exit {result.returncode}"
    output = (result.stdout + result.stderr).strip().splitlines()
    return output[0] if output else f"exit {result.returncode}"


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "not installed in this Python"


def main() -> None:
    print(f"OS: {platform.platform()}")
    print(f"Architecture: {platform.machine()}")
    print(f"Python: {sys.version.split()[0]} ({sys.executable})")
    for name in ("pandas", "numpy", "Pillow", "pyarrow", "boto3", "pyspark", "pytest"):
        print(f"{name}: {package_version(name)}")
    print(f"Java: {command_output(['java', '-version'])}")
    print(f"Docker CLI: {command_output(['docker', '--version'])}")
    print(f"Docker Compose: {command_output(['docker', 'compose', 'version'])}")
    print(f"Docker daemon: {command_output(['docker', 'info', '--format', '{{.OSType}}/{{.Architecture}}'])}")
    print("AWS: not contacted; this check never reads credentials or creates resources")


if __name__ == "__main__":
    main()
