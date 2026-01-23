# Polars Setup Guide

This guide covers how to use the Parallel Polars integration for DataFrame-native data enrichment.

## Architecture

```
Polars DataFrame
       │
       ▼
parallel_enrich(df, input_columns, output_columns)
       │
       ▼
Parallel Task Group API (batch processing)
       │
       ▼
Polars DataFrame with new columns
```

The integration processes all rows in a single batch for efficiency, then adds the enriched columns back to your DataFrame.

## Prerequisites

1. **Python 3.12+**
2. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)

## Installation

```bash
pip install parallel-web-tools[polars]
```

Or with all dependencies:

```bash
pip install parallel-web-tools[all]
```

## Quick Start

```python
import polars as pl
from parallel_web_tools.integrations.polars import parallel_enrich

# Create a DataFrame
df = pl.DataFrame({
    "company": ["Google", "Microsoft", "Apple"],
    "website": ["google.com", "microsoft.com", "apple.com"],
})

# Enrich with company information
result = parallel_enrich(
    df,
    input_columns={
        "company_name": "company",
        "website": "website",
    },
    output_columns=[
        "CEO name",
        "Founding year",
        "Headquarters city",
    ],
)

# Access the enriched DataFrame
print(result.result)
print(f"Success: {result.success_count}, Errors: {result.error_count}")
```

Output:
```
shape: (3, 6)
┌───────────┬───────────────┬─────────────────┬──────────────┬──────────────────┐
│ company   ┆ website       ┆ ceo_name        ┆ founding_year┆ headquarters_city│
│ ---       ┆ ---           ┆ ---             ┆ ---          ┆ ---              │
│ str       ┆ str           ┆ str             ┆ str          ┆ str              │
╞═══════════╪═══════════════╪═════════════════╪══════════════╪══════════════════╡
│ Google    ┆ google.com    ┆ Sundar Pichai   ┆ 1998         ┆ Mountain View    │
│ Microsoft ┆ microsoft.com ┆ Satya Nadella   ┆ 1975         ┆ Redmond          │
│ Apple     ┆ apple.com     ┆ Tim Cook        ┆ 1976         ┆ Cupertino        │
└───────────┴───────────────┴─────────────────┴──────────────┴──────────────────┘
Success: 3, Errors: 0
```

## Authentication

Set your API key via environment variable:

```bash
export PARALLEL_API_KEY="your-api-key"
```

Or pass it directly:

```python
result = parallel_enrich(
    df,
    input_columns={"company_name": "company"},
    output_columns=["CEO name"],
    api_key="your-api-key",
)
```

## API Reference

### `parallel_enrich()`

```python
def parallel_enrich(
    df: pl.DataFrame,
    input_columns: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    include_basis: bool = False,
) -> EnrichmentResult
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `pl.DataFrame` | required | DataFrame to enrich |
| `input_columns` | `dict[str, str]` | required | Mapping of input descriptions to column names |
| `output_columns` | `list[str]` | required | List of output column descriptions |
| `api_key` | `str \| None` | `None` | API key (uses env var if not provided) |
| `processor` | `str` | `"lite-fast"` | Parallel processor to use |
| `timeout` | `int` | `600` | Timeout in seconds |
| `include_basis` | `bool` | `False` | Include citations in results |

**Returns:** `EnrichmentResult`

### `EnrichmentResult`

```python
@dataclass
class EnrichmentResult:
    dataframe: pl.DataFrame      # Enriched DataFrame
    success_count: int           # Number of successful rows
    error_count: int             # Number of failed rows
    errors: list[dict[str, Any]] # Error details
    elapsed_time: float          # Processing time in seconds
```

### `parallel_enrich_lazy()`

Same as `parallel_enrich()` but accepts a `pl.LazyFrame`. Collects the LazyFrame before processing.

## Usage Examples

### Basic Company Enrichment

```python
import polars as pl
from parallel_web_tools.integrations.polars import parallel_enrich

df = pl.DataFrame({
    "name": ["Tesla", "SpaceX", "Neuralink"],
})

result = parallel_enrich(
    df,
    input_columns={"company_name": "name"},
    output_columns=[
        "CEO name",
        "Industry",
        "Year founded",
        "Headquarters",
    ],
)

print(result.result)
```

### Multiple Input Columns

```python
df = pl.DataFrame({
    "company": ["Acme Corp"],
    "domain": ["acme.com"],
    "location": ["San Francisco, CA"],
})

result = parallel_enrich(
    df,
    input_columns={
        "company_name": "company",
        "website": "domain",
        "headquarters": "location",
    },
    output_columns=[
        "Number of employees",
        "Annual revenue (USD)",
        "Main products",
    ],
)
```

### Using Different Processors

```python
# Fast, basic metadata
result = parallel_enrich(df, ..., processor="lite-fast")

# Standard enrichments
result = parallel_enrich(df, ..., processor="base-fast")

# Deep research
result = parallel_enrich(df, ..., processor="pro-fast")
```

### Including Citations

```python
result = parallel_enrich(
    df,
    input_columns={"company_name": "company"},
    output_columns=["CEO name"],
    include_basis=True,
)

# Access citations
for row in result.result.iter_rows(named=True):
    print(f"CEO: {row['ceo_name']}")
    print(f"Sources: {row['_basis']}")
```

### Error Handling

```python
result = parallel_enrich(df, ...)

if result.error_count > 0:
    print(f"Failed rows: {result.error_count}")
    for error in result.errors:
        print(f"  Row {error['row']}: {error['error']}")

# Filter successful rows only
successful_df = result.result.filter(
    pl.col("ceo_name").is_not_null()
)
```

### With LazyFrames

```python
# Read from CSV lazily
lf = pl.scan_csv("companies.csv")

# Filter and select
lf = lf.filter(pl.col("active") == True).select(["name", "website"])

# Enrich (will collect the LazyFrame)
from parallel_web_tools.integrations.polars import parallel_enrich_lazy

result = parallel_enrich_lazy(
    lf,
    input_columns={"company_name": "name", "website": "website"},
    output_columns=["CEO name"],
)
```

### Large Dataset Processing

For large datasets, consider processing in batches:

```python
def enrich_in_batches(df: pl.DataFrame, batch_size: int = 100):
    """Process large DataFrames in batches."""
    results = []

    for i in range(0, len(df), batch_size):
        batch = df.slice(i, batch_size)
        result = parallel_enrich(
            batch,
            input_columns={"company_name": "company"},
            output_columns=["CEO name"],
        )
        results.append(result.result)

    return pl.concat(results)
```

## Processor Options

| Processor | Speed | Cost | Best For |
|-----------|-------|------|----------|
| `lite`, `lite-fast` | Fastest | ~$0.005/row | Basic metadata, high volume |
| `base`, `base-fast` | Fast | ~$0.01/row | Standard enrichments |
| `core`, `core-fast` | Medium | ~$0.025/row | Cross-referenced data |
| `pro`, `pro-fast` | Slow | ~$0.10/row | Deep research |

## Column Name Mapping

Output columns are automatically converted to valid Python identifiers:

| Description | Column Name |
|-------------|-------------|
| `"CEO name"` | `ceo_name` |
| `"Founding year (YYYY)"` | `founding_year` |
| `"Annual revenue [USD]"` | `annual_revenue` |
| `"2024 Revenue"` | `col_2024_revenue` |

## Best Practices

### 1. Be Specific in Descriptions

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

### 2. Use Appropriate Processors

- **High volume, basic data**: Use `lite-fast`
- **Standard company info**: Use `base-fast`
- **Research-quality data**: Use `pro-fast`

### 3. Handle Errors Gracefully

```python
result = parallel_enrich(df, ...)

# Check for errors before using results
if result.error_count > 0:
    logger.warning(f"{result.error_count} rows failed enrichment")

# Errors don't stop processing - partial results are returned
```

### 4. Consider Batch Sizes

The integration processes all rows in a single batch. For very large datasets (1000+ rows), consider:
- Processing in smaller batches
- Using `lite-fast` processor for faster results
- Increasing timeout for large batches

## Troubleshooting

### "Column not found in DataFrame"

Ensure the column names in `input_columns` values match your DataFrame:

```python
# Wrong - column name doesn't exist
input_columns={"company_name": "Company"}  # Capital C

# Correct
input_columns={"company_name": "company"}  # Lowercase
```

### Timeout Errors

Increase the timeout for large batches:

```python
result = parallel_enrich(
    df,
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
result = parallel_enrich(..., api_key="your-key")
```

## Next Steps

- See the [demo notebook](../notebooks/polars_enrichment_demo.ipynb) for more examples
- Check [Parallel Documentation](https://docs.parallel.ai) for API details
- View [parallel-web-tools on GitHub](https://github.com/parallel-web/parallel-web-tools)
