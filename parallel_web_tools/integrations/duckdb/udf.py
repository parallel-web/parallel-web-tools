"""
DuckDB UDF Registration

Provides SQL user-defined functions for data enrichment in DuckDB.

This implementation uses vectorized UDFs with asyncio.gather to process
all rows concurrently, rather than sequentially.

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

import asyncio
import json
from typing import Any

import duckdb
import pyarrow as pa
from _duckdb._func import PythonUDFType

from parallel_web_tools.core import build_output_schema
from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.user_agent import get_default_headers


async def _enrich_all_async(
    items: list[dict[str, Any]],
    output_columns: list[str],
    api_key: str,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> list[str]:
    """
    Enrich all items concurrently using asyncio.gather.

    Args:
        items: List of input dictionaries to enrich.
        output_columns: List of descriptions for columns to enrich.
        api_key: Parallel API key.
        processor: Parallel processor to use.
        timeout: Timeout in seconds for each API call.

    Returns:
        List of JSON strings containing enrichment results (same order as inputs).
    """
    from parallel import AsyncParallel
    from parallel.types import JsonSchemaParam, TaskSpecParam

    client = AsyncParallel(api_key=api_key, default_headers=get_default_headers("duckdb"))
    output_schema = build_output_schema(output_columns)
    task_spec = TaskSpecParam(output_schema=JsonSchemaParam(type="json", json_schema=output_schema))

    async def process_one(item: dict[str, Any]) -> str:
        try:
            task_run = await client.task_run.create(
                input=dict(item),
                task_spec=task_spec,
                processor=processor,
            )
            result = await client.task_run.result(task_run.run_id, api_timeout=timeout)
            content = result.output.content
            if isinstance(content, dict):
                return json.dumps(content)
            return json.dumps({"result": str(content)})
        except Exception as e:
            return json.dumps({"error": str(e)})

    return await asyncio.gather(*[process_one(item) for item in items])


def _enrich_batch_sync(
    input_jsons: list[str],
    output_columns_json: str,
    api_key: str,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> list[str]:
    """
    Enrich a batch of inputs synchronously by running async code.

    Args:
        input_jsons: List of JSON strings containing input data.
        output_columns_json: JSON array of output column descriptions.
        api_key: Parallel API key.
        processor: Parallel processor to use.
        timeout: Timeout in seconds for each API call.

    Returns:
        List of JSON strings with enriched data or errors.
    """
    # Parse output columns once (same for all rows)
    try:
        output_columns = json.loads(output_columns_json)
        if not isinstance(output_columns, list):
            error = json.dumps({"error": "output_columns must be a JSON array"})
            return [error] * len(input_jsons)
    except json.JSONDecodeError as e:
        error = json.dumps({"error": f"Invalid output_columns JSON: {e}"})
        return [error] * len(input_jsons)

    # Parse all input JSONs, tracking errors
    items: list[dict[str, Any]] = []
    parse_errors: dict[int, str] = {}

    for i, input_json in enumerate(input_jsons):
        try:
            item = json.loads(input_json)
            items.append(item)
        except json.JSONDecodeError as e:
            parse_errors[i] = json.dumps({"error": f"Invalid input JSON: {e}"})
            items.append({})  # Placeholder

    # Filter out items with parse errors for processing
    valid_indices = [i for i in range(len(items)) if i not in parse_errors]
    valid_items = [items[i] for i in valid_indices]

    if not valid_items:
        # All inputs had parse errors
        return [parse_errors.get(i, json.dumps({"error": "Unknown error"})) for i in range(len(input_jsons))]

    # Run async enrichment - handle both standalone and nested event loop cases (e.g., Jupyter)
    try:
        # Check if there's already a running event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        # We're inside an existing event loop (e.g., Jupyter notebook)
        # Use nest_asyncio to allow nested event loops
        import nest_asyncio

        nest_asyncio.apply()
        results = asyncio.run(_enrich_all_async(valid_items, output_columns, api_key, processor, timeout))
    else:
        # No existing event loop, use standard asyncio.run
        results = asyncio.run(_enrich_all_async(valid_items, output_columns, api_key, processor, timeout))

    # Map results back to original positions
    output: list[str] = [""] * len(input_jsons)
    result_idx = 0
    for i in range(len(input_jsons)):
        if i in parse_errors:
            output[i] = parse_errors[i]
        else:
            output[i] = results[result_idx]
            result_idx += 1

    return output


def register_parallel_functions(
    conn: duckdb.DuckDBPyConnection,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> None:
    """
    Register Parallel enrichment functions in a DuckDB connection.

    After calling this function, you can use `parallel_enrich()` in SQL queries
    to enrich data. The UDF uses vectorized processing with asyncio.gather to
    process all rows concurrently.

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

    SQL Usage:
        parallel_enrich(input_json VARCHAR, output_columns VARCHAR) -> VARCHAR

        - input_json: JSON object with input data, e.g., json_object('company_name', 'Google')
        - output_columns: JSON array of output descriptions, e.g., json_array('CEO name')
        - Returns: JSON string with enriched data or {"error": "..."} on failure
    """
    # Resolve and capture the API key at registration time
    key = resolve_api_key(api_key)

    def enrich_vectorized(input_col: pa.Array, output_col: pa.Array) -> pa.Array:
        """
        Vectorized UDF that processes all rows concurrently.

        Args:
            input_col: PyArrow array of input JSON strings.
            output_col: PyArrow array of output column JSON strings.

        Returns:
            PyArrow array of result JSON strings.
        """
        # Convert PyArrow arrays to Python lists
        input_jsons = input_col.to_pylist()
        output_columns_jsons = output_col.to_pylist()

        # All rows should have the same output_columns, use the first one
        output_columns_json = output_columns_jsons[0] if output_columns_jsons else "[]"

        # Process all inputs concurrently
        results = _enrich_batch_sync(
            input_jsons=input_jsons,
            output_columns_json=output_columns_json,
            api_key=key,
            processor=processor,
            timeout=timeout,
        )

        return pa.array(results, type=pa.string())

    # Register the vectorized function
    conn.create_function(
        "parallel_enrich",
        enrich_vectorized,
        ["VARCHAR", "VARCHAR"],
        "VARCHAR",
        type=PythonUDFType.ARROW,
        side_effects=True,
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
