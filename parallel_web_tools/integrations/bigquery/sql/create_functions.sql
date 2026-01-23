-- BigQuery Remote Functions for Parallel API
--
-- This SQL creates the remote functions after deploying the Cloud Function.
--
-- Replace placeholders:
--   {project_id}     - Your GCP project ID
--   {dataset_id}     - Dataset for functions (default: parallel_functions)
--   {location}       - Region (e.g., us-central1)
--   {connection_id}  - BigQuery connection name (default: parallel-connection)
--   {function_url}   - Deployed Cloud Function URL

-- Main enrichment function
CREATE OR REPLACE FUNCTION `{project_id}.{dataset_id}.parallel_enrich`(
    input_data STRING,
    output_columns STRING
)
RETURNS STRING
REMOTE WITH CONNECTION `{project_id}.{location}.{connection_id}`
OPTIONS (
    endpoint = '{function_url}',
    user_defined_context = [("processor", "lite-fast")]
);

-- Convenience function for company enrichment
CREATE OR REPLACE FUNCTION `{project_id}.{dataset_id}.parallel_enrich_company`(
    company_name STRING,
    company_website STRING,
    fields STRING
)
RETURNS STRING
AS (
    `{project_id}.{dataset_id}.parallel_enrich`(
        JSON_OBJECT('company_name', company_name, 'website', company_website),
        fields
    )
);

-- Example queries:
--
-- SELECT parallel_enrich(
--     JSON_OBJECT('company_name', 'Google', 'website', 'google.com'),
--     JSON_ARRAY('CEO name', 'Founding year', 'Brief description')
-- );
--
-- SELECT parallel_enrich_company(
--     'Apple',
--     'apple.com',
--     JSON_ARRAY('CEO name', 'Market cap', 'Industry')
-- );
