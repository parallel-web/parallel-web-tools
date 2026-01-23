# Snowflake Setup Guide

This guide covers how to deploy and use the Parallel Snowflake integration for data enrichment.

## Architecture

```
Snowflake SQL Query
       │
       ▼
parallel_enrich(input_data, output_columns)
       │
       ▼
Python UDF with External Access Integration
       │
       ▼
Parallel Task API (via HTTPS)
       │
       ▼
VARIANT result with enriched data
```

The integration uses Snowflake's External Access Integration feature to allow UDFs to make secure HTTPS calls to the Parallel API.

## Prerequisites

1. **Snowflake Account** with ACCOUNTADMIN privileges (or equivalent)
2. **Python 3.12+** (for Python deployment)
3. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)

## Installation

```bash
pip install parallel-web-tools[snowflake]
```

## Quick Start - Python Deployment

The easiest way to deploy is using the Python helper:

```python
from parallel_web_tools.integrations.snowflake import deploy_parallel_functions

deploy_parallel_functions(
    account="your-account.us-east-1",
    user="your-user",
    password="your-password",
    parallel_api_key="your-parallel-api-key",
)
```

This creates:
- Database: `PARALLEL_INTEGRATION`
- Schema: `ENRICHMENT`
- Network rule for `api.parallel.ai`
- Secret with your API key
- External access integration
- `parallel_enrich()` UDF
- Roles: `PARALLEL_DEVELOPER` and `PARALLEL_USER`

## Quick Start - Manual SQL Deployment

If you prefer to run SQL manually:

### Step 1: Get SQL Templates

```python
from parallel_web_tools.integrations.snowflake import get_setup_sql, get_udf_sql

# Get setup SQL with your API key
setup_sql = get_setup_sql(api_key="your-parallel-api-key")
print(setup_sql)

# Get UDF creation SQL
udf_sql = get_udf_sql()
print(udf_sql)
```

### Step 2: Run in Snowflake

Execute the SQL scripts in order:

1. Run `01_setup.sql` to create network infrastructure
2. Run `02_create_udf.sql` to create the UDF

## SQL Usage

### Basic Enrichment

```sql
SELECT parallel_enrich(
    OBJECT_CONSTRUCT('company_name', 'Google'),
    ARRAY_CONSTRUCT('CEO name', 'Founding year')
) AS enriched_data;
```

Result:
```json
{
  "ceo_name": "Sundar Pichai",
  "founding_year": "1998",
  "basis": [...]
}
```

### Multiple Input Fields

```sql
SELECT parallel_enrich(
    OBJECT_CONSTRUCT(
        'company_name', 'Apple',
        'website', 'apple.com',
        'industry', 'Technology'
    ),
    ARRAY_CONSTRUCT(
        'CEO name',
        'Founding year',
        'Headquarters city',
        'Number of employees'
    )
) AS enriched_data;
```

### Custom Processor

```sql
SELECT parallel_enrich(
    OBJECT_CONSTRUCT('company_name', 'Microsoft'),
    ARRAY_CONSTRUCT('CEO name', 'Recent news headline'),
    'base-fast'  -- processor option
) AS enriched_data;
```

### Enrich Table Rows

```sql
SELECT
    company_name,
    parallel_enrich(
        OBJECT_CONSTRUCT('company_name', company_name, 'website', website),
        ARRAY_CONSTRUCT('CEO name', 'Industry', 'Founding year')
    ) AS enriched_data
FROM companies
LIMIT 10;
```

### Parse Enriched Results

```sql
WITH enriched AS (
    SELECT
        company_name,
        parallel_enrich(
            OBJECT_CONSTRUCT('company_name', company_name),
            ARRAY_CONSTRUCT('CEO name', 'Founding year')
        ) AS data
    FROM companies
)
SELECT
    company_name,
    data:ceo_name::STRING AS ceo,
    data:founding_year::STRING AS founded,
    data:basis AS sources
FROM enriched;
```

### Save Results to Table

```sql
CREATE TABLE enriched_companies AS
SELECT
    c.*,
    e.enriched_data:ceo_name::STRING AS ceo_name,
    e.enriched_data:founding_year::STRING AS founding_year
FROM companies c
CROSS JOIN LATERAL (
    SELECT parallel_enrich(
        OBJECT_CONSTRUCT('company_name', c.company_name),
        ARRAY_CONSTRUCT('CEO name', 'Founding year')
    ) AS enriched_data
) e;
```

## API Reference

### `deploy_parallel_functions()`

```python
def deploy_parallel_functions(
    account: str,
    user: str,
    password: str | None = None,
    warehouse: str = "COMPUTE_WH",
    database: str = "PARALLEL_INTEGRATION",
    schema: str = "ENRICHMENT",
    role: str = "ACCOUNTADMIN",
    parallel_api_key: str | None = None,
    authenticator: str | None = None,
) -> None
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `account` | `str` | required | Snowflake account identifier |
| `user` | `str` | required | Snowflake username |
| `password` | `str \| None` | `None` | Password (or use authenticator) |
| `warehouse` | `str` | `"COMPUTE_WH"` | Warehouse to use |
| `database` | `str` | `"PARALLEL_INTEGRATION"` | Database to create |
| `schema` | `str` | `"ENRICHMENT"` | Schema to create |
| `role` | `str` | `"ACCOUNTADMIN"` | Role for deployment |
| `parallel_api_key` | `str \| None` | `None` | API key (uses env var if not provided) |
| `authenticator` | `str \| None` | `None` | Auth method (e.g., "externalbrowser") |

### `cleanup_parallel_functions()`

```python
def cleanup_parallel_functions(
    account: str,
    user: str,
    password: str | None = None,
    warehouse: str = "COMPUTE_WH",
    role: str = "ACCOUNTADMIN",
    authenticator: str | None = None,
) -> None
```

Removes all Parallel integration objects from Snowflake.

### SQL Function: `parallel_enrich()`

```sql
-- Version 1: Default processor (lite-fast)
parallel_enrich(input_data OBJECT, output_columns ARRAY) RETURNS VARIANT

-- Version 2: Custom processor
parallel_enrich(input_data OBJECT, output_columns ARRAY, processor VARCHAR) RETURNS VARIANT
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `input_data` | `OBJECT` | JSON object with input data |
| `output_columns` | `ARRAY` | Array of output column descriptions |
| `processor` | `VARCHAR` | Processor to use (optional) |

**Returns:** `VARIANT` containing enriched data or error

## Processor Options

| Processor | Speed | Cost | Best For |
|-----------|-------|------|----------|
| `lite`, `lite-fast` | Fastest | ~$0.005/row | Basic metadata, high volume |
| `base`, `base-fast` | Fast | ~$0.01/row | Standard enrichments |
| `core`, `core-fast` | Medium | ~$0.025/row | Cross-referenced data |
| `pro`, `pro-fast` | Slow | ~$0.10/row | Deep research |

## Column Name Mapping

Output columns are automatically converted to valid JSON property names:

| Description | Property Name |
|-------------|---------------|
| `"CEO name"` | `ceo_name` |
| `"Founding year (YYYY)"` | `founding_year` |
| `"Annual revenue [USD]"` | `annual_revenue` |
| `"2024 Revenue"` | `col_2024_revenue` |

## Security

### How it Works

1. **Network Rule**: Only allows egress to `api.parallel.ai:443`
2. **Secret**: API key stored encrypted (not visible in SQL)
3. **External Access Integration**: Combines rule and secret
4. **UDF**: Uses integration to make secure API calls

### Roles

Two roles are created:

- **PARALLEL_DEVELOPER**: Can create and modify UDFs
- **PARALLEL_USER**: Can execute UDFs only

Grant PARALLEL_USER to users who need to run enrichments:

```sql
GRANT ROLE PARALLEL_USER TO USER analyst_user;
```

## Error Handling

Errors are returned as JSON in the result:

```sql
SELECT parallel_enrich(
    OBJECT_CONSTRUCT('company_name', 'NonexistentCompanyXYZ'),
    ARRAY_CONSTRUCT('CEO name')
):error::STRING AS error_message;
```

Common errors:
- `"No API key provided"` - Secret not configured
- `"Timeout waiting for enrichment"` - API took too long
- `"API request failed: ..."` - Network or API error

## Cleanup

### Using Python

```python
from parallel_web_tools.integrations.snowflake import cleanup_parallel_functions

cleanup_parallel_functions(
    account="your-account",
    user="your-user",
    password="your-password",
)
```

### Using SQL

```python
from parallel_web_tools.integrations.snowflake import get_cleanup_sql
print(get_cleanup_sql())
```

Then execute the SQL in Snowflake.

## Troubleshooting

### "External access integration not found"

Ensure the integration was created:

```sql
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'parallel_api%';
```

If not found, re-run `01_setup.sql`.

### "Network rule violation"

The network rule may not be allowing traffic. Verify:

```sql
SHOW NETWORK RULES LIKE 'parallel_api%';
```

### "Secret not found"

The API key secret may be missing:

```sql
SHOW SECRETS LIKE 'parallel_api%';
```

Re-run setup with correct API key.

### Timeout Errors

For complex enrichments, the 5-minute timeout may not be enough. Consider:
- Using `lite-fast` processor for faster results
- Processing fewer rows per query
- Breaking large enrichments into batches

### Permission Errors

Ensure you have the required role:

```sql
USE ROLE ACCOUNTADMIN;  -- Or PARALLEL_DEVELOPER
```

## Cost Considerations

Each row enrichment makes one API call. Costs depend on:

1. **Number of rows**: Each row = one API call
2. **Processor used**: `pro` is 20x more expensive than `lite`
3. **Output columns**: More columns may require more processing

Estimate costs:
- `lite-fast`: ~$0.005/row
- `base-fast`: ~$0.01/row
- `pro-fast`: ~$0.10/row

## Best Practices

### 1. Use Specific Descriptions

```sql
-- Good - specific
ARRAY_CONSTRUCT(
    'CEO name (current CEO or equivalent leader)',
    'Founding year (YYYY format)'
)

-- Less specific - may get inconsistent results
ARRAY_CONSTRUCT('CEO', 'Year')
```

### 2. Start with lite-fast

Use `lite-fast` for testing, then switch to `base-fast` or higher for production.

### 3. Process in Batches

For large tables, process in batches to manage costs and avoid timeouts:

```sql
SELECT parallel_enrich(...)
FROM companies
WHERE id BETWEEN 1 AND 100;
```

### 4. Cache Results

Store enriched results in a table to avoid re-processing:

```sql
CREATE TABLE enriched_cache AS
SELECT company_name, parallel_enrich(...) AS data
FROM companies;
```

## Next Steps

- See the [demo notebook](../notebooks/snowflake_enrichment_demo.ipynb) for more examples
- Check [Parallel Documentation](https://docs.parallel.ai) for API details
- View [parallel-web-tools on GitHub](https://github.com/parallel-web/parallel-web-tools)
