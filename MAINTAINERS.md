# Maintainers Guide

Instructions for releasing and maintaining the parallel-web-tools package.

## Release Process

### 1. Update Version

Update the version in these places:
- `pyproject.toml`: `version = "X.Y.Z"`
- `parallel_web_tools/__init__.py`: `__version__ = "X.Y.Z"`
- `tests/test_cli.py`: version assertion in `test_version`
- `parallel_web_tools/integrations/bigquery/cloud_function/requirements.txt`: `parallel-web-tools>=X.Y.Z`

### 2. Create a GitHub Release

1. Go to **Releases** → **Create new release**
2. Create a new tag matching the version (e.g., `v0.2.0`)
3. Add release notes describing changes
4. Click **Publish release**

This triggers two workflows:
- **release.yml**: Builds standalone binaries for all platforms
- **publish.yml**: Publishes to PyPI

### 3. Verify the Release

After the workflows complete:

```bash
# Test binary installation
curl -fsSL https://raw.githubusercontent.com/parallel-web/parallel-web-tools/main/install-cli.sh | bash
parallel-cli --version

# Test PyPI installation
pip install parallel-web-tools[all] --upgrade
parallel-cli --version
```

## Standalone Binaries

### Supported Platforms

| Platform | Runner | Binary Name |
|----------|--------|-------------|
| macOS Apple Silicon | `macos-15` | `parallel-cli-darwin-arm64` |
| macOS Intel | `macos-15-large` | `parallel-cli-darwin-x64` |
| Linux x64 | `ubuntu-latest` | `parallel-cli-linux-x64` |
| Windows x64 | `windows-latest` | `parallel-cli-windows-x64.exe` |

Note: Linux arm64 is not supported (no GitHub-hosted ARM64 runners available).

### Local Build

```bash
# Build for current platform
uv run scripts/build.py

# Test the binary
./dist/parallel-cli --version
```

### Binary Size

Expect ~100-150 MB per binary (includes Python runtime and all dependencies).

## PyPI Publishing

### First-Time Setup

Configure trusted publishing (no API tokens needed):

1. Go to https://pypi.org/manage/account/publishing/
2. Click **Add a new pending publisher**
3. Fill in:
   - PyPI project name: `parallel-web-tools`
   - Owner: `parallel-developers`
   - Repository: `parallel-web-tools`
   - Workflow name: `publish.yml`
   - Environment name: `pypi`

For Test PyPI (recommended for testing):
1. Go to https://test.pypi.org/manage/account/publishing/
2. Same steps, but use environment name: `test-pypi`

### Manual Publishing

To publish without creating a release:

1. Go to **Actions** → **Publish to PyPI**
2. Click **Run workflow**
3. Check "Publish to Test PyPI" for testing
4. Click **Run workflow**

### Package Extras

The package supports optional dependencies:

```bash
pip install parallel-web-tools           # Core library only
pip install parallel-web-tools[cli]      # + CLI tools (click, rich, questionary)
pip install parallel-web-tools[duckdb]   # + DuckDB connector
pip install parallel-web-tools[bigquery] # + BigQuery connector
pip install parallel-web-tools[all]      # Everything
```

## Managing Releases

### Delete a Test Release

```bash
# Delete release and tag
gh release delete v0.0.1-test --cleanup-tag

# Or separately:
gh release delete v0.0.1-test
git push origin --delete v0.0.1-test
```

### Pre-releases

For testing without affecting "latest":
1. Create release as usual
2. Check **Set as a pre-release**

Pre-releases won't be installed by the install script (which uses GitHub's "latest" release API).

### Yanking a PyPI Release

If a release has issues:

```bash
# Yank (hide from default install, but still accessible)
pip install twine
twine yank parallel-web-tools==0.0.1

# Or delete entirely (not recommended)
# Must be done via PyPI web interface within 24 hours
```

## CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `ci.yml` | Push to main, PRs | Run tests and type checking |
| `release.yml` | Release created | Build binaries for all platforms |
| `publish.yml` | Release published | Publish to PyPI |

### Manually Trigger Workflows

```bash
# Trigger release build manually
gh workflow run release.yml -f tag=v0.0.1

# Trigger PyPI publish manually
gh workflow run publish.yml -f test_pypi=true
```

## Troubleshooting

### Binary build fails

1. Check the Actions log for the specific platform
2. Common issues:
   - Missing hidden imports in `parallel-web-tools.spec`
   - Platform-specific dependency issues

### PyPI publish fails

1. Verify trusted publishing is configured correctly
2. Check the environment name matches (`pypi` or `test-pypi`)
3. Ensure version number hasn't been used before

### Install script fails

1. Check if binaries exist on the release
2. Verify checksum files are present (`.sha256`)
3. Test the download URL manually:
   ```bash
   curl -fsSL https://github.com/parallel-web/parallel-web-tools/releases/download/v0.0.1/parallel-cli-darwin-arm64
   ```
