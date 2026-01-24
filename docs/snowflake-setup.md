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

1. **Snowflake Account** - Paid account required (trial accounts don't support External Access)
2. **ACCOUNTADMIN Role** - Required for creating integrations (see [Manual Deployment](#manual-sql-deployment-for-admins) if you don't have this)
3. **MFA Setup** - If your account requires MFA, you'll need an authenticator app configured
4. **Python 3.12+** with `parallel-web-tools[snowflake]` installed
5. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)

## How It Works

The Snowflake integration uses the `parallel-web-tools` package from PyPI, sharing the same core enrichment logic as BigQuery and Spark integrations:

```
Snowflake UDF
    │
    ├── Uses: ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
    ├── Package: parallel-web-tools (from PyPI)
    │
    └── Calls: enrich_batch() from parallel_web_tools.core
                    │
                    └── Parallel Task Group API
```

This ensures consistent behavior across all platforms.

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
- `parallel_enrich()` UDF
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

First, set your context or use fully qualified names:

```sql
-- Option 1: Set context
USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- Option 2: Use fully qualified names
SELECT PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(...);
```

### Basic Enrichment

```sql
-- Returns JSON with enriched fields and basis/citations
SELECT PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
    OBJECT_CONSTRUCT('company_name', 'Google', 'website', 'google.com'),
    ARRAY_CONSTRUCT('CEO name', 'Founding year', 'Brief description')
) AS enriched_data;
```

Result:
```json
{
  "ceo_name": "Sundar Pichai",
  "founding_year": "1998",
  "brief_description": "Google is a multinational technology company...",
  "basis": [...]
}
```

### Parsing JSON Results

```sql
-- Extract fields from the JSON result
SELECT
    data:ceo_name::STRING AS ceo_name,
    data:founding_year::STRING AS founding_year,
    data:brief_description::STRING AS description,
    data:basis AS basis
FROM (
    SELECT PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
        OBJECT_CONSTRUCT('company_name', 'Google', 'website', 'google.com'),
        ARRAY_CONSTRUCT('CEO name', 'Founding year', 'Brief description')
    ) AS data
);
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

For complex enrichments, the 5-minute timeout may not be enough. Consider:
- Using `lite-fast` processor for faster results
- Processing fewer rows per query
- Breaking large enrichments into batches

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
