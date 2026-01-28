-- =============================================================================
-- Parallel Enrichment UDTF for Snowflake
-- =============================================================================
-- Batched table function that processes all rows in a partition via single API call.
--
-- Prerequisites:
-- - Run 01_setup.sql first to create network rule, secret, and integration
-- - PARALLEL_DEVELOPER role or ACCOUNTADMIN
--
-- Usage:
--   SELECT
--       e.input:company_name::STRING AS company_name,
--       e.input:website::STRING AS website,
--       e.enriched:ceo_name::STRING AS ceo_name,
--       e.enriched:founding_year::STRING AS founding_year
--   FROM my_table t,
--        TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
--            TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
--            ARRAY_CONSTRUCT('CEO name', 'Founding year')
--        ) OVER (PARTITION BY 1)) e;
-- =============================================================================

USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Batched UDTF (all rows in partition processed in single API call)
-- =============================================================================

CREATE OR REPLACE FUNCTION parallel_enrich(
    input_json VARCHAR,
    output_columns ARRAY,
    processor VARCHAR DEFAULT 'lite-fast'
)
RETURNS TABLE (input VARIANT, enriched VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('parallel-web-tools')
HANDLER = 'EnrichHandler'
EXTERNAL_ACCESS_INTEGRATIONS = (parallel_api_access_integration)
SECRETS = ('api_key' = parallel_api_key)
AS $$
import json
import _snowflake
from parallel_web_tools.core import enrich_batch


class EnrichHandler:
    def __init__(self):
        self.api_key = _snowflake.get_generic_secret_string("api_key")
        self.rows = []
        self.output_columns = []
        self.processor = "lite-fast"

    def process(self, input_json, output_columns, processor):
        self.output_columns = list(output_columns) if output_columns else []
        self.processor = processor if processor else "lite-fast"
        try:
            self.rows.append(json.loads(input_json) if input_json else {})
        except:
            self.rows.append({})

    def end_partition(self):
        if not self.rows:
            return
        if not self.api_key:
            for row in self.rows:
                yield (row, {"error": "No API key provided"})
            return
        try:
            results = enrich_batch(
                inputs=self.rows,
                output_columns=self.output_columns,
                api_key=self.api_key,
                processor=self.processor,
                timeout=1800,
                poll_interval=2,
                include_basis=True,
                source="snowflake",
            )
            for row, r in zip(self.rows, results):
                yield (row, r)
        except Exception as e:
            for row in self.rows:
                yield (row, {"error": str(e)})
$$;

-- =============================================================================
-- Grant permissions
-- =============================================================================

GRANT USAGE ON FUNCTION parallel_enrich(VARCHAR, ARRAY, VARCHAR) TO ROLE PARALLEL_USER;

-- =============================================================================
-- Verification
-- =============================================================================

SELECT 'parallel_enrich() UDTF created (batched via end_partition)' AS status;
