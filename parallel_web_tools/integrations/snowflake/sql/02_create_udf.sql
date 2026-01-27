-- =============================================================================
-- Parallel Enrichment UDFs for Snowflake
-- =============================================================================
-- This script creates the parallel_enrich() UDF for data enrichment.
--
-- Prerequisites:
-- - Run 01_setup.sql first to create network rule, secret, and integration
-- - PARALLEL_DEVELOPER role or ACCOUNTADMIN
-- - SNOWFLAKE.PYPI_REPOSITORY_USER role granted (for PyPI package access)
--
-- Usage:
-- After running this script, you can use parallel_enrich() in SQL queries:
--
--   SELECT PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
--       OBJECT_CONSTRUCT('company_name', 'Google'),
--       ARRAY_CONSTRUCT('CEO name', 'Founding year')
--   ) AS enriched_data;
-- =============================================================================

USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Internal UDF (with API key parameter)
-- =============================================================================
-- This is the internal implementation using parallel-web-tools from PyPI.
-- It shares the same core enrichment logic as BigQuery and Spark integrations.

CREATE OR REPLACE FUNCTION parallel_enrich_internal(
    input_data OBJECT,
    output_columns ARRAY,
    processor VARCHAR,
    api_key_override VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('parallel-web-tools')
HANDLER = 'enrich'
EXTERNAL_ACCESS_INTEGRATIONS = (parallel_api_access_integration)
SECRETS = ('api_key' = parallel_api_key)
AS $$
import _snowflake
from parallel_web_tools.core import enrich_batch


def enrich(input_data: dict, output_columns: list, processor: str, api_key_override: str) -> dict:
    # Get API key from secret or override
    if api_key_override:
        api_key = api_key_override
    else:
        api_key = _snowflake.get_generic_secret_string("api_key")

    if not api_key:
        return {"error": "No API key provided"}

    try:
        # Use shared core enrichment logic (same as BigQuery/Spark)
        results = enrich_batch(
            inputs=[input_data],
            output_columns=list(output_columns),
            api_key=api_key,
            processor=processor,
            timeout=300,
            include_basis=True,
            source="snowflake",
        )

        if results and len(results) > 0:
            return results[0]
        return {"error": "No results received"}

    except Exception as e:
        return {"error": f"Enrichment failed: {str(e)}"}
$$;

-- =============================================================================
-- Public UDF Wrappers
-- =============================================================================
-- These are the user-facing functions that call the internal implementation.

-- Wrapper 1: Default processor (lite-fast)
CREATE OR REPLACE FUNCTION parallel_enrich(
    input_data OBJECT,
    output_columns ARRAY
)
RETURNS VARIANT
LANGUAGE SQL
AS $$
    parallel_enrich_internal(input_data, output_columns, 'lite-fast', '')
$$;

-- Wrapper 2: Custom processor
CREATE OR REPLACE FUNCTION parallel_enrich(
    input_data OBJECT,
    output_columns ARRAY,
    processor VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS $$
    parallel_enrich_internal(input_data, output_columns, processor, '')
$$;

-- =============================================================================
-- Grant permissions
-- =============================================================================

GRANT USAGE ON FUNCTION parallel_enrich(OBJECT, ARRAY) TO ROLE PARALLEL_USER;
GRANT USAGE ON FUNCTION parallel_enrich(OBJECT, ARRAY, VARCHAR) TO ROLE PARALLEL_USER;

-- =============================================================================
-- Verification
-- =============================================================================

SELECT 'parallel_enrich() UDF created successfully' AS status;
