# DuckDB Setup Guide

This guide covers how to use the Parallel DuckDB integration for data enrichment.

## Architecture

Two approaches are available:

### 1. Batch Processing (Recommended)

```
DuckDB Table
       │
       ▼
enrich_table(conn, source_table, input_columns, output_columns)
       │
       ▼
Parallel Task Group API (parallel processing)
       │
       ▼
DuckDB Relation with new columns
```

### 2. SQL UDF (Row-by-Row)

```
DuckDB SQL Query
       │
       ▼
parallel_enrich(input_json, output_columns)
       │
       ▼
Parallel Task API (one call per row)
       │
       ▼
JSON string result
```

**Performance Note**: Batch processing is significantly faster for multiple rows (4-5x or more) because it processes all rows in parallel.

## Prerequisites

1. **Python 3.12+**
2. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)

## Installation

```bash
pip install parallel-web-tools[duckdb]
```

Or with all dependencies:

```bash
pip install parallel-web-tools[all]
```

## Quick Start - Batch Processing

```python
import duckdb
from parallel_web_tools.integrations.duckdb import enrich_table

# Create a connection and sample data
conn = duckdb.connect()
conn.execute("""
    CREATE TABLE companies AS SELECT * FROM (VALUES
        ('Google', 'google.com'),
        ('Microsoft', 'microsoft.com'),
        ('Apple', 'apple.com')
    ) AS t(name, website)
""")

# Enrich the table
result = enrich_table(
    conn,
    source_table="companies",
    input_columns={
        "company_name": "name",
        "website": "website",
    },
    output_columns=[
        "CEO name",
        "Founding year",
        "Headquarters city",
    ],
)

# Access results
print(result.result.fetchdf())
print(f"Success: {result.success_count}, Errors: {result.error_count}")
```

Output:
```
       name          website          ceo_name  founding_year  headquarters_city
0    Google       google.com     Sundar Pichai           1998      Mountain View
1  Microsoft  microsoft.com     Satya Nadella           1975            Redmond
2     Apple       apple.com          Tim Cook           1976          Cupertino
Success: 3, Errors: 0
```

## Quick Start - SQL UDF

```python
import duckdb
import json
from parallel_web_tools.integrations.duckdb import register_parallel_functions

conn = duckdb.connect()
conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

# Register the UDF
register_parallel_functions(conn, processor="lite-fast")

# Use in SQL
results = conn.execute("""
    SELECT
        name,
        parallel_enrich(
            json_object('company_name', name),
            json_array('CEO name', 'Founding year')
        ) as enriched
    FROM companies
""").fetchall()

# Parse the JSON result
for name, enriched_json in results:
    data = json.loads(enriched_json)
    print(f"{name}: CEO = {data.get('ceo_name')}, Founded = {data.get('founding_year')}")
```

## Authentication

Set your API key via environment variable:

```bash
export PARALLEL_API_KEY="your-api-key"
```

Or pass it directly:

```python
# Batch processing
result = enrich_table(conn, ..., api_key="your-api-key")

# SQL UDF
register_parallel_functions(conn, api_key="your-api-key")
```

## API Reference

### `enrich_table()`

```python
def enrich_table(
    conn: duckdb.DuckDBPyConnection,
    source_table: str,
    input_columns: dict[str, str],
    output_columns: list[str],
    result_table: str | None = None,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    include_basis: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
) -> EnrichmentResult
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `conn` | `DuckDBPyConnection` | required | DuckDB connection |
| `source_table` | `str` | required | Table name or SQL query |
| `input_columns` | `dict[str, str]` | required | Mapping of input descriptions to column names |
| `output_columns` | `list[str]` | required | List of output column descriptions |
| `result_table` | `str \| None` | `None` | Optional permanent table to create |
| `api_key` | `str \| None` | `None` | API key (uses env var if not provided) |
| `processor` | `str` | `"lite-fast"` | Parallel processor to use |
| `timeout` | `int` | `600` | Timeout in seconds |
| `include_basis` | `bool` | `False` | Include citations in results |
| `progress_callback` | `Callable` | `None` | Callback for progress updates |

**Returns:** `EnrichmentResult`

### `EnrichmentResult`

```python
@dataclass
class EnrichmentResult:
    relation: duckdb.DuckDBPyRelation  # Enriched data as DuckDB relation
    success_count: int                  # Number of successful rows
    error_count: int                    # Number of failed rows
    errors: list[dict[str, Any]]        # Error details
    elapsed_time: float                 # Processing time in seconds
```

### `register_parallel_functions()`

```python
def register_parallel_functions(
    conn: duckdb.DuckDBPyConnection,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `conn` | `DuckDBPyConnection` | required | DuckDB connection |
| `api_key` | `str \| None` | `None` | API key (uses env var if not provided) |
| `processor` | `str` | `"lite-fast"` | Parallel processor to use |
| `timeout` | `int` | `300` | Timeout in seconds per row |

## Usage Examples

### Batch Processing with Progress

```python
def on_progress(completed: int, total: int):
    print(f"Progress: {completed}/{total} ({100*completed/total:.0f}%)")

result = enrich_table(
    conn,
    source_table="companies",
    input_columns={"company_name": "name"},
    output_columns=["CEO name"],
    progress_callback=on_progress,
)
```

### Creating a Permanent Result Table

```python
result = enrich_table(
    conn,
    source_table="companies",
    input_columns={"company_name": "name"},
    output_columns=["CEO name", "Founding year"],
    result_table="enriched_companies",  # Creates permanent table
)

# Query the permanent table later
conn.execute("SELECT * FROM enriched_companies").fetchall()
```

### Using a SQL Query as Source

```python
result = enrich_table(
    conn,
    source_table="""
        SELECT name, website
        FROM companies
        WHERE active = true
        LIMIT 100
    """,
    input_columns={"company_name": "name", "website": "website"},
    output_columns=["CEO name"],
)
```

### Including Citations (Basis)

```python
result = enrich_table(
    conn,
    source_table="companies",
    input_columns={"company_name": "name"},
    output_columns=["CEO name"],
    include_basis=True,
)

# Access citations
df = result.result.fetchdf()
for _, row in df.iterrows():
    print(f"CEO: {row['ceo_name']}")
    print(f"Sources: {row['_basis']}")
```

### SQL UDF with Multiple Inputs

```sql
SELECT
    name,
    website,
    parallel_enrich(
        json_object(
            'company_name', name,
            'website', website,
            'industry', industry
        ),
        json_array(
            'CEO name',
            'Number of employees',
            'Annual revenue (USD)'
        )
    ) as enriched
FROM companies
```

### Error Handling

```python
result = enrich_table(conn, ...)

if result.error_count > 0:
    print(f"Failed rows: {result.error_count}")
    for error in result.errors:
        print(f"  Row {error['row']}: {error['error']}")

# Errors appear as NULL in the result
df = result.result.fetchdf()
successful = df[df['ceo_name'].notna()]
```

## Processor Options

| Processor | Speed | Cost | Best For |
|-----------|-------|------|----------|
| `lite`, `lite-fast` | Fastest | ~$0.005/row | Basic metadata, high volume |
| `base`, `base-fast` | Fast | ~$0.01/row | Standard enrichments |
| `core`, `core-fast` | Medium | ~$0.025/row | Cross-referenced data |
| `pro`, `pro-fast` | Slow | ~$0.10/row | Deep research |

## Column Name Mapping

Output columns are automatically converted to valid SQL identifiers:

| Description | Column Name |
|-------------|-------------|
| `"CEO name"` | `ceo_name` |
| `"Founding year (YYYY)"` | `founding_year` |
| `"Annual revenue [USD]"` | `annual_revenue` |
| `"2024 Revenue"` | `col_2024_revenue` |

## Best Practices

### 1. Use Batch Processing for Multiple Rows

```python
# Good - processes all rows in parallel
result = enrich_table(conn, "companies", ...)

# Slower - one API call per row
register_parallel_functions(conn)
conn.execute("SELECT *, parallel_enrich(...) FROM companies")
```

### 2. Be Specific in Descriptions

```python
# Good - specific descriptions
output_columns = [
    "CEO name (current CEO or equivalent leader)",
    "Founding year (YYYY format)",
    "Annual revenue (USD, most recent fiscal year)",
]

# Less specific - may get inconsistent results
output_columns = ["CEO", "Year", "Revenue"]
```

### 3. Use Appropriate Processors

- **High volume, basic data**: Use `lite-fast`
- **Standard company info**: Use `base-fast`
- **Research-quality data**: Use `pro-fast`

### 4. Handle Errors Gracefully

```python
result = enrich_table(conn, ...)

# Check for errors before using results
if result.error_count > 0:
    logger.warning(f"{result.error_count} rows failed enrichment")

# Errors don't stop processing - partial results are returned
```

## Troubleshooting

### "Column not found"

Ensure the column names in `input_columns` values match your table:

```python
# Wrong - column name doesn't exist
input_columns={"company_name": "Company"}  # Capital C

# Correct
input_columns={"company_name": "company"}  # Lowercase
```

### Timeout Errors

Increase the timeout for large batches or complex processors:

```python
result = enrich_table(
    conn,
    ...,
    timeout=1200,  # 20 minutes
)
```

### Authentication Errors

Check your API key:

```bash
# Verify env var is set
echo $PARALLEL_API_KEY

# Or pass directly
result = enrich_table(..., api_key="your-key")
```

### Slow Performance with SQL UDF

The SQL UDF processes one row at a time. For better performance with multiple rows, use batch processing:

```python
# Instead of this (slow)
conn.execute("SELECT *, parallel_enrich(...) FROM companies")

# Use this (fast)
result = enrich_table(conn, "companies", ...)
```

## Next Steps

- See the [demo notebook](../notebooks/duckdb_enrichment_demo.ipynb) for more examples
- Check [Parallel Documentation](https://docs.parallel.ai) for API details
- View [parallel-web-tools on GitHub](https://github.com/parallel-web/parallel-web-tools)
