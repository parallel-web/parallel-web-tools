# Snowflake Setup Guide

This guide covers how to deploy and use the Parallel Snowflake integration for data enrichment.

## Architecture

```
Snowflake SQL Query
       │
       ▼
TABLE(parallel_enrich(...) OVER (PARTITION BY 1))
       │
       ▼
Python UDTF with end_partition() batching
       │
       ▼
Single Parallel Task Group API call (all rows batched)
       │
       ▼
(input VARIANT, enriched VARIANT) results
```

The integration uses a **User Defined Table Function (UDTF)** with `end_partition()` to batch all rows in a partition into a single API call. This is much faster than row-by-row processing.

## Prerequisites

1. **Snowflake Account** - Paid account required (trial accounts don't support External Access)
2. **ACCOUNTADMIN Role** - Required for creating integrations (see [Manual Deployment](#manual-sql-deployment-for-admins) if you don't have this)
3. **MFA Setup** - If your account requires MFA, you'll need an authenticator app configured
4. **Python 3.12+** with `parallel-web-tools[snowflake]` installed
5. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)

## How It Works

The Snowflake integration uses a **UDTF (User Defined Table Function)** with batching:

```
parallel_enrich() UDTF
    │
    ├── process(): Collects each row
    ├── end_partition(): Batches all rows together
    │
    └── Calls: enrich_batch() with ALL rows at once
                    │
                    └── Single Task Group with N runs
```

**Performance:**
- `PARTITION BY 1` → all rows in one batch → 1 API call
- Without partition → each row separate → N API calls (slow)

## Finding Your Account Identifier

Your Snowflake account identifier is in your Snowsight URL:

```
https://app.snowflake.com/ORGNAME/ACCOUNTNAME/worksheets
                         ↑       ↑
                         └───┬───┘
                     Account: ORGNAME-ACCOUNTNAME
```

**Examples:**
- URL: `https://app.snowflake.com/myorg/myaccount/` → Account: `myorg-myaccount`
- URL: `https://app.snowflake.com/us-east-1/xy12345/` → Account: `xy12345.us-east-1`

You can also run this SQL in Snowsight:
```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
```

## Installation

```bash
pip install parallel-web-tools[snowflake]
```

Or with uv:
```bash
uv add "parallel-web-tools[snowflake]"
```

> **Note:** The standalone `parallel-cli` binary does not include deployment commands. You must install via pip/uv with the `[snowflake]` extra to use `parallel-cli enrich deploy --system snowflake`.

## Quick Start - CLI Deployment

The easiest way to deploy is using the CLI:

### Basic (Password Auth)

```bash
parallel-cli enrich deploy --system snowflake \
    --account ORGNAME-ACCOUNTNAME \
    --user your-username \
    --password "your-password" \
    --warehouse COMPUTE_WH
```

### With MFA

If your account requires MFA, you need to:

1. **Set up an authenticator app** (Google Authenticator, Duo, etc.) in Snowsight:
   - Go to your profile → Security → Multi-factor Authentication
   - Enroll your authenticator app

2. **Run with the passcode flag:**
```bash
parallel-cli enrich deploy --system snowflake \
    --account ORGNAME-ACCOUNTNAME \
    --user your-username \
    --password "your-password" \
    --authenticator username_password_mfa \
    --passcode 123456 \
    --warehouse COMPUTE_WH
```

Replace `123456` with the current 6-digit code from your authenticator app. Run the command immediately after getting a fresh code (they expire quickly).

### Environment Variables

To avoid passing sensitive values on the command line:

```bash
# Use single quotes if password contains special characters like !
export SNOWFLAKE_PASSWORD='your!password'
export PARALLEL_API_KEY='your-api-key'

parallel-cli enrich deploy --system snowflake \
    --account ORGNAME-ACCOUNTNAME \
    --user your-username \
    --password "$SNOWFLAKE_PASSWORD" \
    --authenticator username_password_mfa \
    --passcode 123456 \
    --warehouse COMPUTE_WH
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--account` | required | Snowflake account identifier |
| `--user` | required | Snowflake username |
| `--password` | - | Snowflake password |
| `--warehouse` | `COMPUTE_WH` | Warehouse to use |
| `--role` | `ACCOUNTADMIN` | Role for deployment |
| `--authenticator` | `externalbrowser` | Auth method |
| `--passcode` | - | MFA code from authenticator app |
| `--api-key` | env var | Parallel API key |

## Quick Start - Python Deployment

```python
from parallel_web_tools.integrations.snowflake import deploy_parallel_functions

deploy_parallel_functions(
    account="orgname-accountname",
    user="your-user",
    password="your-password",
    parallel_api_key="your-parallel-api-key",
    # For MFA:
    authenticator="username_password_mfa",
    passcode="123456",
)
```

This creates:
- Database: `PARALLEL_INTEGRATION`
- Schema: `ENRICHMENT`
- Network rule for `api.parallel.ai`
- Secret with your API key
- External access integration
- `parallel_enrich()` UDTF (batched table function)
- Roles: `PARALLEL_DEVELOPER` and `PARALLEL_USER`

## Manual SQL Deployment (For Admins)

If you don't have ACCOUNTADMIN access, ask your Snowflake admin to run the setup SQL.

### Generate SQL for Admin

```bash
# Generate the SQL scripts
python -c "
from parallel_web_tools.integrations.snowflake import get_setup_sql, get_udf_sql

print('=== SETUP SQL (run first) ===')
print(get_setup_sql('YOUR_PARALLEL_API_KEY'))
print()
print('=== UDF SQL (run second) ===')
print(get_udf_sql())
"
```

Replace `YOUR_PARALLEL_API_KEY` with your actual API key, then send the output to your admin.

### What Admin Needs to Run

1. **Run as ACCOUNTADMIN** - Required for External Access Integration
2. **Execute setup SQL** - Creates network rule, secret, integration
3. **Execute UDF SQL** - Creates the parallel_enrich() function

After admin completes setup, users with `PARALLEL_USER` role can use the function.

## SQL Usage

The function is a table function (UDTF) that requires `PARTITION BY` for batching.

### Basic Enrichment

```sql
WITH companies AS (
    SELECT * FROM (VALUES
        ('Google', 'google.com'),
        ('Anthropic', 'anthropic.com'),
        ('Apple', 'apple.com')
    ) AS t(company_name, website)
)
SELECT
    e.input:company_name::STRING AS company_name,
    e.input:website::STRING AS website,
    e.enriched:ceo_name::STRING AS ceo_name,
    e.enriched:founding_year::STRING AS founding_year
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
         ARRAY_CONSTRUCT('CEO name', 'Founding year')
     ) OVER (PARTITION BY 1)) e;
```

**Sample output:**

| company_name | website | ceo_name | founding_year |
|--------------|---------|----------|---------------|
| Google | google.com | Sundar Pichai | 1998 |
| Anthropic | anthropic.com | Dario Amodei | 2021 |
| Apple | apple.com | Tim Cook | 1976 |

**Key points:**
- `TO_JSON(OBJECT_CONSTRUCT(...))` creates the input
- `PARTITION BY 1` batches all rows into single API call
- Returns `input` (original data) and `enriched` (results)

The raw `enriched` VARIANT column contains:
```json
{
  "ceo_name": "Sundar Pichai",
  "founding_year": "1998",
  "basis": [{"field": "ceo_name", "citations": [...], "confidence": "high"}]
}
```

### Custom Processor

```sql
SELECT
    e.input:company_name::STRING AS company_name,
    e.enriched:ceo_name::STRING AS ceo_name
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name)),
         ARRAY_CONSTRUCT('CEO name'),
         'base-fast'  -- processor option
     ) OVER (PARTITION BY 1)) e;
```

### Save Results to Table

```sql
CREATE TABLE enriched_companies AS
SELECT
    e.input:company_name::STRING AS company_name,
    e.input:website::STRING AS website,
    e.enriched:ceo_name::STRING AS ceo_name,
    e.enriched:founding_year::STRING AS founding_year
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
         ARRAY_CONSTRUCT('CEO name', 'Founding year')
     ) OVER (PARTITION BY 1)) e;
```

## Troubleshooting

### "External access is not supported for trial accounts"

Snowflake trial accounts cannot make external HTTP calls. You need to upgrade to a paid Snowflake account (Standard edition or above).

### "Failed to connect... SAML Identity Provider"

SSO/SAML isn't configured for your account. Use password authentication instead:

```bash
parallel-cli enrich deploy --system snowflake \
    --account your-account \
    --user your-user \
    --password "your-password" \
    ...
```

### "Multi-factor authentication is required"

Your account requires MFA. Set up an authenticator app in Snowsight, then use:

```bash
parallel-cli enrich deploy --system snowflake \
    --authenticator username_password_mfa \
    --passcode 123456 \
    ...
```

### "Role 'ACCOUNTADMIN' is not granted to this user"

You don't have ACCOUNTADMIN. Try a different role you have access to:

```bash
parallel-cli enrich deploy --system snowflake \
    --role SYSADMIN \
    ...
```

Or check your roles:
```sql
SHOW GRANTS TO USER your_username;
```

### "Insufficient privileges to operate on account"

ACCOUNTADMIN is required for creating External Access Integrations. Either:
1. Get ACCOUNTADMIN access from your Snowflake admin
2. Have an admin run the SQL manually (see [Manual SQL Deployment](#manual-sql-deployment-for-admins))

### "Integration does not exist or not authorized"

The External Access Integration wasn't created. This usually means:
- You don't have ACCOUNTADMIN privileges
- The setup SQL didn't complete successfully

Re-run with ACCOUNTADMIN role or have an admin run the setup.

### "Package 'parallel-web-tools' not found" or PyPI errors

The UDF uses `parallel-web-tools` from PyPI. Ensure:
1. The `SNOWFLAKE.PYPI_REPOSITORY_USER` role is granted (setup SQL does this automatically)
2. Your Snowflake account has PyPI repository access enabled

```sql
-- Verify PyPI access
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE your_role;
```

### Authentication Pop-ups

If you see repeated authentication prompts, install keyring support:

```bash
pip install "snowflake-connector-python[secure-local-storage]"
```

### Timeout Errors

For very large batches, the 30-minute timeout may not be enough. Consider:
- Using `lite-fast` processor for faster results
- Using `PARTITION BY` to split into smaller batches
- Processing fewer rows per partition

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
    passcode: str | None = None,
    force: bool = False,
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
| `authenticator` | `str \| None` | `None` | Auth method (e.g., "username_password_mfa") |
| `passcode` | `str \| None` | `None` | MFA code from authenticator app |
| `force` | `bool` | `False` | Skip confirmation for existing resources |

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
TABLE(parallel_enrich(input_json VARCHAR, output_columns ARRAY))
    RETURNS TABLE (input VARIANT, enriched VARIANT)

-- Version 2: Custom processor
TABLE(parallel_enrich(input_json VARCHAR, output_columns ARRAY, processor VARCHAR))
    RETURNS TABLE (input VARIANT, enriched VARIANT)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `input_json` | `VARCHAR` | JSON string via `TO_JSON(OBJECT_CONSTRUCT(...))` |
| `output_columns` | `ARRAY` | Array of output column descriptions |
| `processor` | `VARCHAR` | Processor to use (optional, default: `lite-fast`) |

**Returns:** Table with `input` (original data) and `enriched` (results) VARIANT columns

**Usage:** Must use with `OVER (PARTITION BY ...)` for batching

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

## Cost Considerations

Costs depend on:

1. **Number of rows**: Each row = one enrichment run (but batched efficiently)
2. **Processor used**: `pro` is 20x more expensive than `lite`
3. **Output columns**: More columns may require more processing

Estimate costs:
- `lite-fast`: ~$0.005/row
- `base-fast`: ~$0.01/row
- `pro-fast`: ~$0.10/row

**Note:** With `PARTITION BY 1`, all rows are batched into a single API call, reducing latency significantly.

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

### 3. Use PARTITION BY for Batching

The `PARTITION BY` clause controls how rows are batched into API calls:

```sql
-- All rows in one batch (single API call)
TABLE(parallel_enrich(...) OVER (PARTITION BY 1))

-- One batch per region (one API call per region)
TABLE(parallel_enrich(...) OVER (PARTITION BY region))

-- One batch per date (process daily data separately)
TABLE(parallel_enrich(...) OVER (PARTITION BY DATE_TRUNC('day', created_at)))
```

**When to use each approach:**

| Pattern | Use Case |
|---------|----------|
| `PARTITION BY 1` | Small datasets (<1000 rows), fastest for few rows |
| `PARTITION BY column` | Large datasets, natural groupings, incremental processing |
| `PARTITION BY CEIL(ROW_NUMBER() OVER () / 100)` | Fixed batch sizes |

**Example: Partition by existing column**

```sql
-- Process each region as a separate batch
SELECT
    e.input:company_name::STRING AS company_name,
    e.input:region::STRING AS region,
    e.enriched:ceo_name::STRING AS ceo_name
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'region', t.region)),
         ARRAY_CONSTRUCT('CEO name')
     ) OVER (PARTITION BY t.region)) e;
```

**Example: Fixed batch sizes**

```sql
-- Process in batches of 100 rows
WITH numbered AS (
    SELECT *, CEIL(ROW_NUMBER() OVER (ORDER BY company_name) / 100.0) AS batch_id
    FROM companies
)
SELECT
    e.input:company_name::STRING AS company_name,
    e.enriched:ceo_name::STRING AS ceo_name
FROM numbered t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name)),
         ARRAY_CONSTRUCT('CEO name')
     ) OVER (PARTITION BY t.batch_id)) e;
```

**Example: Incremental processing by date**

```sql
-- Only process today's new records
SELECT
    e.input:company_name::STRING AS company_name,
    e.enriched:ceo_name::STRING AS ceo_name
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name)),
         ARRAY_CONSTRUCT('CEO name')
     ) OVER (PARTITION BY DATE_TRUNC('day', t.created_at))) e
WHERE t.created_at >= CURRENT_DATE;
```

### 4. Cache Results

Store enriched results in a table to avoid re-processing:

```sql
CREATE TABLE enriched_cache AS
SELECT e.input, e.enriched
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name)),
         ARRAY_CONSTRUCT('CEO name', 'Founding year')
     ) OVER (PARTITION BY 1)) e;
```

## Cleanup

### Using CLI

```bash
# Not yet implemented - use Python or SQL
```

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

## Next Steps

- See the [demo notebook](../notebooks/snowflake_enrichment_demo.ipynb) for more examples
- Check [Parallel Documentation](https://docs.parallel.ai) for API details
- View [parallel-web-tools on GitHub](https://github.com/parallel-web/parallel-web-tools)
