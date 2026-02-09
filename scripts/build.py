#!/usr/bin/env python3
"""
Build script for creating standalone parallel-cli binaries.

Usage:
    python scripts/build.py          # Build using current environment
    uv run scripts/build.py          # Build using uv (recommended)

This creates a standalone executable in dist/parallel-cli/
The output is a folder (onedir mode) for fast startup times.
"""

import hashlib
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def has_uv() -> bool:
    """Check if uv is available."""
    try:
        subprocess.run(["uv", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def ensure_dependencies():
    """Ensure build dependencies are installed."""
    project_root = Path(__file__).parent.parent

    if has_uv():
        print("Using uv to install dependencies...")
        subprocess.run(
            ["uv", "pip", "install", "-e", ".[cli]", "pyinstaller>=6.0.0"],
            cwd=project_root,
            check=True,
        )
    else:
        print("Using pip to install dependencies...")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", ".[cli]", "pyinstaller>=6.0.0"],
            cwd=project_root,
            check=True,
        )


def get_platform_string() -> str:
    """Get platform identifier string."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Normalize architecture names
    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("arm64", "aarch64"):
        arch = "arm64"
    else:
        arch = machine

    # Normalize OS names
    if system == "darwin":
        os_name = "darwin"
    elif system == "linux":
        os_name = "linux"
    elif system == "windows":
        os_name = "windows"
    else:
        os_name = system

    return f"{os_name}-{arch}"


def get_version() -> str:
    """Get version from pyproject.toml."""
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    content = pyproject.read_text()

    for line in content.splitlines():
        if line.startswith("version"):
            # Parse version = "x.y.z"
            return line.split("=")[1].strip().strip('"')

    return "0.0.0"


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_folder_size(folder: Path) -> int:
    """Get total size of a folder in bytes."""
    total = 0
    for f in folder.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def build(skip_deps: bool = False):
    """Build the standalone executable."""
    project_root = Path(__file__).parent.parent
    dist_dir = project_root / "dist"
    build_dir = project_root / "build"

    print("=" * 50)
    print("Building parallel-cli standalone executable")
    print("=" * 50)

    # Get build info
    version = get_version()
    platform_str = get_platform_string()
    print(f"Version: {version}")
    print(f"Platform: {platform_str}")

    # Ensure dependencies
    if not skip_deps:
        print("\nInstalling dependencies...")
        ensure_dependencies()

    # Clean previous builds
    print("\nCleaning previous builds...")
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    if build_dir.exists():
        shutil.rmtree(build_dir)

    # Run PyInstaller
    print("\nRunning PyInstaller...")
    spec_file = project_root / "parallel-web-tools.spec"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            str(spec_file),
            "--clean",
            "--noconfirm",
        ],
        cwd=project_root,
    )

    if result.returncode != 0:
        print("Build failed!")
        sys.exit(1)

    # Check output - onedir mode creates a folder
    app_dir = dist_dir / "parallel-cli"
    exe_name = "parallel-cli.exe" if platform.system() == "Windows" else "parallel-cli"
    exe_path = app_dir / exe_name

    if not exe_path.exists():
        print(f"Error: Expected output not found at {exe_path}")
        sys.exit(1)

    # Calculate folder size
    folder_size = get_folder_size(app_dir) / (1024 * 1024)  # MB

    # Create zip archive for distribution
    archive_name = f"parallel-cli-{platform_str}"
    print(f"\nCreating archive: {archive_name}.zip")
    archive_path = shutil.make_archive(
        str(dist_dir / archive_name),
        "zip",
        root_dir=dist_dir,
        base_dir="parallel-cli",
    )
    archive_path = Path(archive_path)

    # Calculate checksum of the archive
    checksum = calculate_checksum(archive_path)
    archive_size = archive_path.stat().st_size / (1024 * 1024)  # MB

    print("\n" + "=" * 50)
    print("Build successful!")
    print("=" * 50)
    print(f"Output folder: {app_dir}")
    print(f"Folder size: {folder_size:.1f} MB")
    print(f"Archive: {archive_path}")
    print(f"Archive size: {archive_size:.1f} MB")
    print(f"SHA256: {checksum}")

    # Create manifest
    manifest = {
        "version": version,
        "format": "zip",
        "platforms": {
            platform_str: {
                "archive": archive_path.name,
                "checksum": checksum,
                "size": archive_path.stat().st_size,
            }
        },
    }

    manifest_path = dist_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Manifest: {manifest_path}")

    # Create checksum file for the archive
    checksum_file = archive_path.with_suffix(".zip.sha256")
    checksum_file.write_text(checksum)
    print(f"Checksum file: {checksum_file}")

    # Create platform-specific directory structure for releases
    release_dir = dist_dir / version / platform_str
    release_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(archive_path, release_dir / archive_path.name)
    shutil.copy(checksum_file, release_dir / checksum_file.name)

    print(f"\nRelease files: {release_dir}")
    print("\nTo test the build:")
    print(f"  {exe_path} --version")


if __name__ == "__main__":
    skip_deps = "--skip-deps" in sys.argv
    build(skip_deps=skip_deps)
