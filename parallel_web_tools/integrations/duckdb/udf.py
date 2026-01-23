"""
DuckDB UDF Registration

Provides SQL user-defined functions for row-by-row enrichment in DuckDB.

Example:
    import duckdb
    from parallel_web_tools.integrations.duckdb import register_parallel_functions

    conn = duckdb.connect()
    register_parallel_functions(conn, api_key="your-key")

    conn.execute('''
        SELECT
            name,
            parallel_enrich(
                json_object('company_name', name),
                json_array('CEO name', 'Founding year')
            ) as enriched
        FROM companies
    ''').fetchall()
"""

from __future__ import annotations

import json

import duckdb

from parallel_web_tools.core import enrich_single


def _parallel_enrich_scalar(
    input_json: str,
    output_columns_json: str,
    api_key: str | None,
    processor: str,
    timeout: int,
) -> str:
    """
    Internal function that performs enrichment for a single row.

    This function is called by the registered DuckDB UDF.

    Args:
        input_json: JSON string containing input data.
        output_columns_json: JSON array of output column descriptions.
        api_key: Parallel API key.
        processor: Parallel processor to use.
        timeout: Timeout in seconds.

    Returns:
        JSON string with enriched data or error.
    """
    try:
        # Parse inputs
        input_data = json.loads(input_json)
        output_columns = json.loads(output_columns_json)

        if not isinstance(output_columns, list):
            return json.dumps({"error": "output_columns must be a JSON array"})

        # Call the shared enrichment function
        result = enrich_single(
            input_data=input_data,
            output_columns=output_columns,
            api_key=api_key,
            processor=processor,
            timeout=timeout,
            include_basis=True,
        )

        return json.dumps(result)

    except json.JSONDecodeError as e:
        return json.dumps({"error": f"JSON parse error: {e}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def register_parallel_functions(
    conn: duckdb.DuckDBPyConnection,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> None:
    """
    Register Parallel enrichment functions in a DuckDB connection.

    After calling this function, you can use `parallel_enrich()` in SQL queries
    to enrich data row by row.

    Args:
        conn: DuckDB connection.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is "lite-fast".
            Options: lite, lite-fast, base, base-fast, core, core-fast, pro, pro-fast
        timeout: Timeout in seconds for each enrichment. Default is 300 (5 min).

    Example:
        >>> import duckdb
        >>> from parallel_web_tools.integrations.duckdb import register_parallel_functions
        >>>
        >>> conn = duckdb.connect()
        >>> register_parallel_functions(conn, processor="base-fast")
        >>>
        >>> # Use in SQL
        >>> conn.execute('''
        ...     SELECT
        ...         name,
        ...         parallel_enrich(
        ...             json_object('company_name', name, 'website', website),
        ...             json_array('CEO name', 'Founding year')
        ...         ) as enriched
        ...     FROM companies
        ... ''').fetchall()

    Note:
        The UDF processes one row at a time, making API calls sequentially.
        For bulk enrichment, use `enrich_table()` which uses batch processing
        for much better performance.

    SQL Usage:
        parallel_enrich(input_json VARCHAR, output_columns VARCHAR) -> VARCHAR

        - input_json: JSON object with input data, e.g., json_object('company_name', 'Google')
        - output_columns: JSON array of output descriptions, e.g., json_array('CEO name')
        - Returns: JSON string with enriched data or {"error": "..."} on failure
    """

    # Create wrapper function with type hints for DuckDB type inference
    def enrich_wrapper(input_json: str, output_columns_json: str) -> str:
        return _parallel_enrich_scalar(
            input_json=input_json,
            output_columns_json=output_columns_json,
            api_key=api_key,
            processor=processor,
            timeout=timeout,
        )

    # Register the function
    conn.create_function(
        "parallel_enrich",
        enrich_wrapper,
        side_effects=True,  # API calls are not pure
    )


def unregister_parallel_functions(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Unregister Parallel enrichment functions from a DuckDB connection.

    Args:
        conn: DuckDB connection.
    """
    try:
        conn.remove_function("parallel_enrich")
    except Exception:
        pass  # Function may not exist
