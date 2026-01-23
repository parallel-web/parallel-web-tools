-- =============================================================================
-- Parallel Enrichment UDFs for Snowflake
-- =============================================================================
-- This script creates the parallel_enrich() UDF for data enrichment.
--
-- Prerequisites:
-- - Run 01_setup.sql first to create network rule, secret, and integration
-- - PARALLEL_DEVELOPER role or ACCOUNTADMIN
--
-- Usage:
-- After running this script, you can use parallel_enrich() in SQL queries:
--
--   SELECT parallel_enrich(
--       OBJECT_CONSTRUCT('company_name', 'Google'),
--       ARRAY_CONSTRUCT('CEO name', 'Founding year')
--   ) AS enriched_data;
-- =============================================================================

USE DATABASE PARALLEL_INTEGRATION;
USE SCHEMA ENRICHMENT;

-- =============================================================================
-- Internal UDF (with API key parameter)
-- =============================================================================
-- This is the internal implementation. Users should call the public wrapper.

CREATE OR REPLACE FUNCTION parallel_enrich_internal(
    input_data OBJECT,
    output_columns ARRAY,
    processor VARCHAR,
    api_key_override VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
HANDLER = 'enrich'
EXTERNAL_ACCESS_INTEGRATIONS = (parallel_api_access_integration)
SECRETS = ('api_key' = parallel_api_key)
AS $$
import _snowflake
import json
import time
import requests

def enrich(input_data: dict, output_columns: list, processor: str, api_key_override: str) -> dict:
    """
    Enrich data using the Parallel API.

    Args:
        input_data: Dictionary of input data (e.g., {"company_name": "Google"})
        output_columns: List of output column descriptions (e.g., ["CEO name", "Founding year"])
        processor: Parallel processor to use (e.g., "lite-fast", "base-fast")
        api_key_override: Optional API key override (empty string to use secret)

    Returns:
        Dictionary with enriched data or error
    """
    # Get API key from secret or override
    if api_key_override:
        api_key = api_key_override
    else:
        api_key = _snowflake.get_generic_secret_string('api_key')

    if not api_key:
        return {"error": "No API key provided"}

    # Build output schema from column descriptions
    output_properties = {}
    for col in output_columns:
        # Extract base name (before parentheses, brackets, etc.)
        base_name = col.split("(")[0].split("[")[0].strip()

        # Convert to valid property name
        prop_name = base_name.lower().replace(" ", "_").replace("-", "_")
        prop_name = "".join(c for c in prop_name if c.isalnum() or c == "_")

        # Add prefix if starts with number
        if prop_name and prop_name[0].isdigit():
            prop_name = "col_" + prop_name

        if not prop_name:
            prop_name = "column"

        output_properties[prop_name] = {
            "type": "string",
            "description": col
        }

    output_schema = {
        "type": "json",
        "json_schema": {
            "type": "object",
            "properties": output_properties,
            "required": list(output_properties.keys())
        }
    }

    # API configuration
    base_url = "https://api.parallel.ai"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "User-Agent": "Parallel-Snowflake-Integration/1.0"
    }

    try:
        # Step 1: Create task group
        create_response = requests.post(
            f"{base_url}/v1beta/tasks/groups",
            headers=headers,
            json={},
            timeout=30
        )
        create_response.raise_for_status()

        task_group = create_response.json()
        taskgroup_id = task_group.get("task_group_id")

        if not taskgroup_id:
            return {"error": "Failed to create task group", "response": str(task_group)}

        # Step 2: Add run with input data
        run_input = {
            "default_task_spec": {
                "output_schema": output_schema
            },
            "inputs": [
                {"input": input_data, "processor": processor}
            ]
        }

        add_response = requests.post(
            f"{base_url}/v1beta/tasks/groups/{taskgroup_id}/runs",
            headers=headers,
            json=run_input,
            timeout=30
        )
        add_response.raise_for_status()

        # Step 3: Poll for completion (5 minute timeout)
        max_wait = 300
        start_time = time.time()

        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{base_url}/v1beta/tasks/groups/{taskgroup_id}",
                headers=headers,
                timeout=30
            )
            status_response.raise_for_status()

            status_data = status_response.json()
            counts = status_data.get("status", {}).get("task_run_status_counts", {})
            completed = counts.get("completed", 0)
            failed = counts.get("failed", 0)
            total = status_data.get("status", {}).get("num_task_runs", 1)

            if completed + failed >= total:
                break

            time.sleep(2)
        else:
            return {"error": "Timeout waiting for enrichment to complete"}

        # Step 4: Get results via streaming endpoint
        results_response = requests.get(
            f"{base_url}/v1beta/tasks/groups/{taskgroup_id}/runs",
            headers=headers,
            params={"include_input": "true", "include_output": "true"},
            timeout=120,
            stream=True
        )
        results_response.raise_for_status()

        # Parse Server-Sent Events
        for line in results_response.iter_lines():
            if line and line.startswith(b'data: '):
                try:
                    event_data = json.loads(line.decode('utf-8')[6:])
                    if event_data.get("type") == "task_run.state":
                        output = event_data.get("output", {})
                        content = output.get("content")

                        # Parse content (may be string or dict)
                        if isinstance(content, str):
                            try:
                                result = json.loads(content)
                            except json.JSONDecodeError:
                                result = {"result": content}
                        elif isinstance(content, dict):
                            result = content
                        else:
                            result = {"result": str(content)}

                        # Add basis (citations) if available
                        basis = output.get("basis", [])
                        if basis:
                            result["basis"] = basis

                        return result
                except json.JSONDecodeError:
                    continue

        return {"error": "No results received"}

    except requests.exceptions.Timeout:
        return {"error": "Request timeout"}
    except requests.exceptions.RequestException as e:
        return {"error": f"API request failed: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
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

-- Test the function (requires valid API key in secret)
-- SELECT parallel_enrich(
--     OBJECT_CONSTRUCT('company_name', 'Google'),
--     ARRAY_CONSTRUCT('CEO name', 'Founding year')
-- ) AS enriched_data;

SELECT 'UDF created successfully! Test with: SELECT parallel_enrich(OBJECT_CONSTRUCT(''company_name'', ''Google''), ARRAY_CONSTRUCT(''CEO name''))' AS status;
