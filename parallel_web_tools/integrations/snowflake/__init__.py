"""
Parallel Snowflake Integration

SQL UDF-based data enrichment for Snowflake using the Parallel API.

Installation:
    pip install parallel-web-tools[snowflake]

Deployment:
    from parallel_web_tools.integrations.snowflake import deploy_parallel_functions

    deploy_parallel_functions(
        account="your-account",
        user="your-user",
        password="your-password",
        parallel_api_key="your-api-key",
    )

SQL Usage:
    SELECT PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
        OBJECT_CONSTRUCT('company_name', 'Google'),
        ARRAY_CONSTRUCT('CEO name', 'Founding year')
    ) AS enriched_data;

SQL Templates:
    Use get_setup_sql(), get_udf_sql(), and get_cleanup_sql() to get
    the SQL templates for manual execution.
"""

from parallel_web_tools.integrations.snowflake.deploy import (
    cleanup_parallel_functions,
    deploy_parallel_functions,
    get_cleanup_sql,
    get_setup_sql,
    get_sql_template,
    get_udf_sql,
)

__all__ = [
    "deploy_parallel_functions",
    "cleanup_parallel_functions",
    "get_sql_template",
    "get_setup_sql",
    "get_udf_sql",
    "get_cleanup_sql",
]
