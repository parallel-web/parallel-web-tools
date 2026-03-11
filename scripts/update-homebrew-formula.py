#!/usr/bin/env python3
"""
Generate a Homebrew cask for parallel-cli from GitHub release assets.

Usage:
    # Auto-detect latest release
    python scripts/update-homebrew-formula.py

    # Specific version
    python scripts/update-homebrew-formula.py --version 0.1.2

    # Write to a specific output path
    python scripts/update-homebrew-formula.py --output /path/to/Casks/parallel-cli.rb

    # Use local checksum files (for CI, after downloading release assets)
    python scripts/update-homebrew-formula.py --checksums-dir ./artifacts
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = "parallel-web/parallel-web-tools"
TEMPLATE = Path(__file__).parent.parent / "homebrew" / "parallel-cli.rb"

PLATFORMS = {
    "darwin-arm64": "PLACEHOLDER_DARWIN_ARM64",
    "darwin-x64": "PLACEHOLDER_DARWIN_X64",
}


def get_latest_version() -> str:
    """Get the latest release version from GitHub."""
    result = subprocess.run(
        ["gh", "release", "view", "--repo", REPO, "--json", "tagName"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    return data["tagName"].lstrip("v")


def get_checksums_from_release(version: str) -> dict[str, str]:
    """Download checksum files from a GitHub release."""
    tag = f"v{version}"
    checksums = {}

    for platform in PLATFORMS:
        asset_name = f"parallel-cli-{platform}.zip.sha256"
        result = subprocess.run(
            [
                "gh",
                "release",
                "download",
                tag,
                "--repo",
                REPO,
                "--pattern",
                asset_name,
                "--output",
                "-",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            checksums[platform] = result.stdout.strip()
        else:
            print(f"Warning: Could not fetch checksum for {platform}", file=sys.stderr)

    return checksums


def get_checksums_from_dir(checksums_dir: Path) -> dict[str, str]:
    """Read checksum files from a local directory."""
    checksums = {}

    for platform in PLATFORMS:
        checksum_file = checksums_dir / f"parallel-cli-{platform}.zip.sha256"
        if checksum_file.exists():
            checksums[platform] = checksum_file.read_text().strip()
        else:
            print(f"Warning: Checksum file not found: {checksum_file}", file=sys.stderr)

    return checksums


def generate_cask(version: str, checksums: dict[str, str]) -> str:
    """Generate the Homebrew cask with real version and checksums."""
    content = TEMPLATE.read_text()

    # Replace version
    content = re.sub(
        r'version ".*?"',
        f'version "{version}"',
        content,
    )

    # Replace checksums
    for platform, placeholder in PLATFORMS.items():
        if platform in checksums:
            content = content.replace(placeholder, checksums[platform])
        else:
            print(
                f"Warning: No checksum for {platform}, leaving placeholder",
                file=sys.stderr,
            )

    return content


def main():
    parser = argparse.ArgumentParser(description="Generate Homebrew cask for parallel-cli")
    parser.add_argument(
        "--version",
        help="Release version (default: latest)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--checksums-dir",
        help="Directory containing .sha256 checksum files (for CI use)",
    )
    args = parser.parse_args()

    # Determine version
    version = args.version
    if not version:
        print("Fetching latest release version...", file=sys.stderr)
        version = get_latest_version()
    print(f"Version: {version}", file=sys.stderr)

    # Get checksums
    if args.checksums_dir:
        checksums = get_checksums_from_dir(Path(args.checksums_dir))
    else:
        print("Fetching checksums from GitHub release...", file=sys.stderr)
        checksums = get_checksums_from_release(version)

    print(f"Found checksums for: {', '.join(checksums.keys())}", file=sys.stderr)

    # Generate cask
    cask = generate_cask(version, checksums)

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(cask)
        print(f"Cask written to: {output_path}", file=sys.stderr)
    else:
        print(cask)


if __name__ == "__main__":
    main()
