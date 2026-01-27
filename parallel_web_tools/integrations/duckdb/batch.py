"""
DuckDB Batch Processing

Provides efficient batch enrichment for DuckDB tables using the Parallel Task Group API.

Example:
    import duckdb
    from parallel_web_tools.integrations.duckdb import enrich_table

    conn = duckdb.connect()
    conn.execute("CREATE TABLE companies (name VARCHAR, website VARCHAR)")
    conn.execute("INSERT INTO companies VALUES ('Google', 'google.com')")

    result = enrich_table(
        conn,
        source_table="companies",
        input_columns={"company_name": "name", "website": "website"},
        output_columns=["CEO name", "Founding year"],
    )

    result.result.fetchdf()
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

import duckdb

from parallel_web_tools.core import EnrichmentResult, build_output_schema, enrich_batch

if TYPE_CHECKING:
    DuckDBEnrichmentResult = EnrichmentResult[duckdb.DuckDBPyRelation]


def enrich_table(
    conn: duckdb.DuckDBPyConnection,
    source_table: str,
    input_columns: dict[str, str],
    output_columns: list[str],
    result_table: str | None = None,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    include_basis: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
) -> EnrichmentResult:
    """
    Enrich a DuckDB table using the Parallel API.

    This function reads data from a source table, enriches it using the
    Parallel API, and returns a DuckDB relation with the results.

    Args:
        conn: DuckDB connection.
        source_table: Name of the source table or SQL query to read from.
        input_columns: Mapping from Parallel input descriptions to table column names.
            Example: {"company_name": "name", "website": "url"}
            - Keys are the descriptions/names passed to Parallel API
            - Values are the column names in the source table
        output_columns: List of descriptions for columns to generate.
            Example: ["CEO name", "Founding year", "Headquarters"]
        result_table: Optional name for a result table to create.
            If provided, creates a permanent table; otherwise returns a relation.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is "lite-fast".
            Options: lite, lite-fast, base, base-fast, core, core-fast, pro, pro-fast
        timeout: Timeout in seconds for the enrichment. Default is 600 (10 min).
        include_basis: Whether to include basis/citations in results. Default is False.
        progress_callback: Optional callback function(completed, total) for progress updates.

    Returns:
        EnrichmentResult containing:
        - result: DuckDB relation with enriched data
        - success_count: Number of successful enrichments
        - error_count: Number of failed enrichments
        - errors: List of error details
        - elapsed_time: Processing time in seconds

    Example:
        >>> import duckdb
        >>> from parallel_web_tools.integrations.duckdb import enrich_table
        >>>
        >>> conn = duckdb.connect()
        >>> conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")
        >>>
        >>> result = enrich_table(
        ...     conn,
        ...     source_table="companies",
        ...     input_columns={"company_name": "name"},
        ...     output_columns=["CEO name", "Founding year"],
        ... )
        >>>
        >>> print(result.result.fetchdf())
    """
    start_time = time.time()

    # Read source data
    source_cols = list(input_columns.values())
    select_cols = ", ".join(f'"{col}"' for col in source_cols)

    # Handle both table names and queries
    if source_table.strip().upper().startswith("SELECT"):
        query = source_table
    else:
        query = f"SELECT {select_cols} FROM {source_table}"

    rows = conn.execute(query).fetchall()
    col_names = [desc[0] for desc in conn.description]

    if not rows:
        # Return empty result
        schema = build_output_schema(output_columns)
        prop_names = list(schema["properties"].keys())
        empty_cols = ", ".join(f"NULL::VARCHAR AS {name}" for name in prop_names)
        if include_basis:
            empty_cols += ", NULL::VARCHAR AS _basis"
        empty_query = f"SELECT {select_cols}, {empty_cols} FROM {source_table} WHERE 1=0"
        rel = conn.sql(empty_query)

        return EnrichmentResult(
            result=rel,
            success_count=0,
            error_count=0,
            errors=[],
            elapsed_time=time.time() - start_time,
        )

    # Convert rows to input dicts
    inputs = []
    col_index_map = {name: i for i, name in enumerate(col_names)}

    for row in rows:
        input_data = {}
        for desc, col_name in input_columns.items():
            idx = col_index_map.get(col_name)
            if idx is not None:
                value = row[idx]
                if value is not None:
                    input_data[desc] = str(value)
        inputs.append(input_data)

    # Call the shared enrichment function
    results = enrich_batch(
        inputs=inputs,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
        source="duckdb",
    )

    # Build output schema to get property names
    schema = build_output_schema(output_columns)
    prop_names = list(schema["properties"].keys())

    # Process results
    errors = []
    success_count = 0
    error_count = 0

    enriched_rows = []
    for i, (original_row, result) in enumerate(zip(rows, results, strict=True)):
        row_data = list(original_row)

        if "error" in result:
            error_count += 1
            errors.append({"row": i, "error": result["error"]})
            for _ in prop_names:
                row_data.append(None)
            if include_basis:
                row_data.append(None)
        else:
            success_count += 1
            for name in prop_names:
                row_data.append(result.get(name))
            if include_basis:
                row_data.append(result.get("basis"))

        enriched_rows.append(tuple(row_data))

        if progress_callback:
            progress_callback(i + 1, len(rows))

    # Create result relation
    all_col_names = list(col_names) + prop_names
    if include_basis:
        all_col_names.append("_basis")

    # Create a temporary table with results
    temp_table = f"_parallel_enriched_{int(time.time() * 1000)}"

    # Build column definitions
    col_defs = ", ".join(f'"{name}" VARCHAR' for name in all_col_names)
    conn.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs})")

    # Insert data
    if enriched_rows:
        placeholders = ", ".join(["?"] * len(all_col_names))
        insert_sql = f"INSERT INTO {temp_table} VALUES ({placeholders})"
        conn.executemany(insert_sql, enriched_rows)

    # Create result relation or table
    if result_table:
        conn.execute(f"CREATE TABLE {result_table} AS SELECT * FROM {temp_table}")
        rel = conn.sql(f"SELECT * FROM {result_table}")
    else:
        rel = conn.sql(f"SELECT * FROM {temp_table}")

    elapsed = time.time() - start_time

    return EnrichmentResult(
        result=rel,
        success_count=success_count,
        error_count=error_count,
        errors=errors,
        elapsed_time=elapsed,
    )
