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
-- Accepts JSON input and returns JSON output
CREATE OR REPLACE FUNCTION `{project_id}.{dataset_id}.parallel_enrich`(
    input_data JSON,
    output_columns JSON
)
RETURNS JSON
REMOTE WITH CONNECTION `{project_id}.{location}.{connection_id}`
OPTIONS (
    endpoint = '{function_url}',
    user_defined_context = [("processor", "lite-fast")]
);
