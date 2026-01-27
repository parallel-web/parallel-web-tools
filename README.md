# Parallel-Web-Tools

CLI and data enrichment utilities for the [Parallel API](https://docs.parallel.ai).

> **Note:** This package provides the `parallel-cli` command-line tool and data enrichment utilities in the `parallel-web-tools` package.
> It depends on [`parallel-web`](https://github.com/parallel-web/parallel-sdk-python), the official
> Parallel Python SDK, but does not contain it. Install `parallel-web` separately if you need
> direct SDK access.

## Features

- **CLI for Humans & AI Agents** - Works interactively or fully via command-line arguments
- **Web Search** - AI-powered search with domain filtering and date ranges
- **Content Extraction** - Extract clean markdown from any URL
- **Data Enrichment** - Enrich CSV, DuckDB, and BigQuery data with AI
- **AI-Assisted Planning** - Use natural language to define what data you want
- **Multiple Integrations** - Polars, DuckDB, Snowflake, BigQuery, Spark

## Installation

### Standalone CLI (Recommended)

Install the standalone `parallel-cli` binary for search, extract, enrichment, and deep research (no Python required):

```bash
curl -fsSL https://parallel.ai/install.sh | bash
```

This automatically detects your platform (macOS/Linux, x64/arm64) and installs to `~/.local/bin`.

> **Note:** The standalone binary supports `search`, `extract`, and `enrich run` with CLI arguments and CSV files. For YAML config files, interactive planner, DuckDB/BigQuery sources, or deployment commands, use pip install.

### Python Package

For programmatic usage or additional features:

```bash
# Minimal CLI (search, extract, enrich with CLI args)
pip install parallel-web-tools

# + YAML config files and interactive planner
pip install parallel-web-tools[cli]

# + Data integrations
pip install parallel-web-tools[duckdb]       # DuckDB (includes cli, polars)
pip install parallel-web-tools[bigquery]     # BigQuery (includes cli)
pip install parallel-web-tools[spark]        # Apache Spark

# Full install with all features
pip install parallel-web-tools[all]
```

## CLI Overview

```
parallel-cli
├── auth                    # Check authentication status
├── login                   # OAuth login (or use PARALLEL_API_KEY env var)
├── logout                  # Remove stored credentials
├── search                  # Web search
├── extract                 # Extract content from URLs
└── enrich                  # Data enrichment commands
    ├── run                 # Run enrichment
    ├── plan                # Create YAML config
    ├── suggest             # AI suggests output columns
    └── deploy              # Deploy to cloud systems (requires pip install)
```

## Quick Start

### 1. Authenticate

```bash
# Interactive OAuth login
parallel-cli login

# Or set environment variable
export PARALLEL_API_KEY=your_api_key
```

### 2. Search the Web

```bash
# Natural language search
parallel-cli search "What is Anthropic's latest AI model?" --json

# Keyword search with filters
parallel-cli search -q "bitcoin price" --after-date 2024-01-01 --json

# Search specific domains
parallel-cli search "SEC filings for Apple" --include-domains sec.gov --json
```

### 3. Extract Content from URLs

```bash
# Extract content as markdown
parallel-cli extract https://example.com --json

# Extract with a specific focus
parallel-cli extract https://company.com --objective "Find pricing info" --json

# Get full page content
parallel-cli extract https://example.com --full-content --json
```

### 4. Enrich Data

```bash
# Let AI suggest what columns to add
parallel-cli enrich suggest "Find the CEO and annual revenue" --json

# Create a config file (interactive)
parallel-cli enrich plan -o config.yaml

# Create a config file (non-interactive, for AI agents)
parallel-cli enrich plan -o config.yaml \
    --source-type csv \
    --source companies.csv \
    --target enriched.csv \
    --source-columns '[{"name": "company", "description": "Company name"}]' \
    --intent "Find the CEO and annual revenue"

# Run enrichment from config
parallel-cli enrich run config.yaml

# Run enrichment directly (no config file needed)
parallel-cli enrich run \
    --source-type csv \
    --source companies.csv \
    --target enriched.csv \
    --source-columns '[{"name": "company", "description": "Company name"}]' \
    --intent "Find the CEO and annual revenue"
```

### 5. Deploy to Cloud Systems

```bash
# Deploy to BigQuery for SQL-native enrichment
parallel-cli enrich deploy --system bigquery --project my-gcp-project
```

## Non-Interactive Mode (for AI Agents & Scripts)

All commands support `--json` output and can be fully controlled via CLI arguments:

```bash
# Search with JSON output
parallel-cli search "query" --json

# Extract with JSON output
parallel-cli extract https://url.com --json

# Suggest columns with JSON output
parallel-cli enrich suggest "Find CEO" --json

# Plan without prompts (provide all args)
parallel-cli enrich plan -o config.yaml \
    --source-type csv \
    --source input.csv \
    --target output.csv \
    --source-columns '[{"name": "company", "description": "Company name"}]' \
    --enriched-columns '[{"name": "ceo", "description": "CEO name"}]'

# Or use --intent to let AI determine the columns
parallel-cli enrich plan -o config.yaml \
    --source-type csv \
    --source input.csv \
    --target output.csv \
    --source-columns '[{"name": "company", "description": "Company name"}]' \
    --intent "Find CEO, revenue, and headquarters"
```

## Integrations

| Integration | Type | Install | Documentation |
|-------------|------|---------|---------------|
| **Polars** | Python DataFrame | `pip install parallel-web-tools[polars]` | [Setup Guide](docs/polars-setup.md) |
| **DuckDB** | SQL + Python | `pip install parallel-web-tools[duckdb]` | [Setup Guide](docs/duckdb-setup.md) |
| **Snowflake** | SQL UDF | `pip install parallel-web-tools[snowflake]` | [Setup Guide](docs/snowflake-setup.md) |
| **BigQuery** | Cloud Function | `pip install parallel-web-tools[bigquery]` | [Setup Guide](docs/bigquery-setup.md) |
| **Spark** | SQL UDF | `pip install parallel-web-tools[spark]` | [Demo Notebook](notebooks/spark_enrichment_demo.ipynb) |

### Quick Integration Examples

**Polars:**
```python
import polars as pl
from parallel_web_tools.integrations.polars import parallel_enrich

df = pl.DataFrame({"company": ["Google", "Microsoft"]})
result = parallel_enrich(
    df,
    input_columns={"company_name": "company"},
    output_columns=["CEO name", "Founding year"],
)
print(result.result)
```

**DuckDB:**
```python
import duckdb
from parallel_web_tools.integrations.duckdb import enrich_table

conn = duckdb.connect()
conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")
result = enrich_table(
    conn,
    source_table="companies",
    input_columns={"company_name": "name"},
    output_columns=["CEO name", "Founding year"],
)
print(result.result.fetchdf())
```

## Programmatic Usage

```python
from parallel_web_tools import run_enrichment, run_enrichment_from_dict

# From YAML file
run_enrichment("config.yaml")

# From dictionary
run_enrichment_from_dict({
    "source": "data.csv",
    "target": "enriched.csv",
    "source_type": "csv",
    "source_columns": [{"name": "company", "description": "Company name"}],
    "enriched_columns": [{"name": "ceo", "description": "CEO name"}]
})
```

## YAML Configuration Format

```yaml
source: input.csv
target: output.csv
source_type: csv  # csv, duckdb, or bigquery
processor: core-fast  # lite, base, core, pro, ultra (add -fast for speed)

source_columns:
  - name: company_name
    description: The name of the company

enriched_columns:
  - name: ceo
    description: The CEO of the company
    type: str  # str, int, float, bool
  - name: revenue
    description: Annual revenue in USD
    type: float
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PARALLEL_API_KEY` | API key for authentication (alternative to `parallel-cli login`) |
| `DUCKDB_FILE` | Default DuckDB file path |
| `BIGQUERY_PROJECT` | Default BigQuery project ID |

## Related Packages

- [`parallel-web`](https://github.com/parallel-web/parallel-sdk-python) - Official Parallel Python SDK (this package depends on it)

## Development

```bash
git clone https://github.com/parallel-web/parallel-web-tools.git
cd parallel-web-tools
uv sync --all-extras
uv run pytest tests/ -v
```

## License

MIT
