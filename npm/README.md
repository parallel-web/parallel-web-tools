# parallel-cli

npm wrapper for [parallel-cli](https://github.com/parallel-web/parallel-web-tools) — the CLI for the Parallel API.

## Install

```bash
npm install -g parallel-cli
```

## Usage

```bash
parallel-cli --help
parallel-cli search "your query"
parallel-cli enrich input.csv --recipe company-info
parallel-cli monitor watch https://example.com
```

## How it works

This package downloads the pre-built `parallel-cli` binary for your platform during `npm install`. The binary is fetched from [GitHub Releases](https://github.com/parallel-web/parallel-web-tools/releases).

### Supported platforms

- macOS (Apple Silicon / arm64)
- macOS (Intel / x64)
- Linux (x64)
- Linux (arm64)
- Windows (x64)

## Environment variables

| Variable | Description |
|---|---|
| `PARALLEL_CLI_SKIP_DOWNLOAD` | Set to `1` to skip the binary download during install |
| `GITHUB_TOKEN` | GitHub token to avoid rate limits when downloading |
| `PARALLEL_CLI_MIRROR` | Custom base URL for downloading binaries |

## License

MIT
