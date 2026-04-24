# build-parallel-cli-binary

Build and verify a local standalone `parallel-cli` binary archive for the current platform.

## When to use

- User asks to build/distribute `parallel-cli` binary locally.
- User is confused why `uv build` only creates `.tar.gz` / `.whl` files.
- Maintainer needs release-style artifacts for manual testing.

## Key distinction

- `uv build` => Python package distributions only (sdist/wheel).
- `scripts/build.py` => standalone binary folder + platform zip + checksum.

## Prerequisites

- Run from repository root.
- Python + `uv` installed.

## Commands

```bash
# 1) Install dependencies needed for standalone CLI build
uv sync --extra cli
uv pip install "pyinstaller>=6.0.0"

# 2) Build standalone artifacts for current platform
uv run python scripts/build.py --skip-deps

# 3) Smoke-test binary
./dist/parallel-cli/parallel-cli --version
```

## Expected outputs

In `dist/`:

- `parallel-cli/` (onedir executable layout)
- `parallel-cli-<platform>.zip`
- `parallel-cli-<platform>.zip.sha256`
- `manifest.json`

## Distribution notes

- `install-cli.sh` downloads `parallel-cli-<platform>.zip` and verifies `*.sha256` from GitHub Releases.
- For install script compatibility, upload both the zip and checksum to a release tag like `vX.Y.Z`.
- Cross-platform binaries should be built on each target OS/arch (or via CI matrix in `.github/workflows/release.yml`).
