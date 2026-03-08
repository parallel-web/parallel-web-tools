"""
DuckDB FindAll Integration — Table-generating function.

Turns a natural language description into a DuckDB table using the
Parallel FindAll API. Only matched candidates are returned, with
enrichment data unpacked into real columns.

Example:
    import duckdb
    from parallel_web_tools.integrations.duckdb import findall_table

    conn = duckdb.connect()
    result = findall_table(
        conn,
        "find all countries that won the FIFA World Cup "
        "and tell me how many times they won and their capital city",
    )
    result.result.show()
"""

from __future__ import annotations

import json
import time
from typing import Any

import duckdb

from parallel_web_tools.core.findall import run_findall
from parallel_web_tools.core.result import EnrichmentResult
from parallel_web_tools.core.sql_utils import quote_identifier


def _unpack_output(candidate: dict[str, Any]) -> dict[str, Any]:
    """Unpack the output field into flat enrichment/match values.

    The API returns output as::

        {"field_name": {"type": "enrichment"|"match_condition", "value": ...}, ...}

    This extracts the ``value`` for each field and merges it into the
    candidate dict, removing the raw ``output`` key.
    """
    output = candidate.get("output")
    if not output or not isinstance(output, dict):
        return candidate

    result = {k: v for k, v in candidate.items() if k != "output"}
    for field_name, field_data in output.items():
        if isinstance(field_data, dict) and "value" in field_data:
            result[field_name] = field_data["value"]
        else:
            result[field_name] = field_data
    return result


def _flatten_candidates(candidates: list[dict[str, Any]]) -> tuple[list[str], list[tuple]]:
    """Flatten candidate dicts into column names and row tuples.

    Filters to matched candidates only, unpacks the output field,
    and removes internal fields (candidate_id, match_status, basis).

    Returns:
        (column_names, rows) where rows is a list of tuples.
    """
    if not candidates:
        return [], []

    # Filter to matched only and unpack output
    matched = [_unpack_output(c) for c in candidates if c.get("match_status") == "matched"]

    if not matched:
        return [], []

    # Drop internal fields that aren't useful for the table
    skip_keys = {"candidate_id", "match_status", "basis"}

    # Collect all keys in order (preserving first-seen order)
    seen: dict[str, None] = {}
    for c in matched:
        for k in c:
            if k not in seen and k not in skip_keys:
                seen[k] = None
    col_names = list(seen)

    rows = []
    for c in matched:
        row = []
        for col in col_names:
            val = c.get(col)
            # Serialize nested dicts/lists to strings for DuckDB VARCHAR columns
            if isinstance(val, (dict, list)):
                val = json.dumps(val)
            elif val is not None and not isinstance(val, str):
                val = str(val)
            row.append(val)
        rows.append(tuple(row))

    return col_names, rows


def findall_table(
    conn: duckdb.DuckDBPyConnection,
    objective: str,
    *,
    generator: str = "preview",
    match_limit: int = 10,
    enrich: bool = True,
    result_table: str | None = None,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 15,
    on_status: Any | None = None,
) -> EnrichmentResult:
    """Run FindAll and materialize matched results as a DuckDB table.

    This is the DuckDB equivalent of ``run_findall()`` — it takes a natural
    language objective, discovers matching entities from the web, and returns
    them as a queryable DuckDB relation. Only matched candidates are included.

    If the objective includes requests for additional data (e.g., "and tell me
    their CEO name"), those are automatically extracted as enrichment columns.

    Args:
        conn: DuckDB connection.
        objective: Natural language description of what to find
            (e.g. "find all fortune 500 companies and their CEO").
        generator: Generator tier. Default "preview" (~10 candidates, fast).
            Options: preview, base, core, pro.
        match_limit: Max matched candidates (5-1000). Default 10.
        enrich: Whether to apply suggested enrichments. Default True.
        result_table: If provided, persist results to a named table.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not set.
        timeout: Max wait in seconds (default 3600).
        poll_interval: Seconds between status checks (default 15).
        on_status: Optional callback(status, findall_id, metrics).

    Returns:
        EnrichmentResult with .result as a DuckDB relation.

    Example:
        >>> conn = duckdb.connect()
        >>> result = findall_table(conn, "fortune 500 companies", match_limit=5)
        >>> result.result.show()
        >>> print(f"Found {result.success_count} matches")
    """
    start_time = time.time()

    # Run FindAll end-to-end (with enrichments if suggested by ingest)
    findall_result = run_findall(
        objective=objective,
        generator=generator,
        match_limit=match_limit,
        api_key=api_key,
        timeout=timeout,
        poll_interval=poll_interval,
        on_status=on_status,
        source="duckdb",
        enrich=enrich,
    )

    candidates = findall_result.get("candidates", [])
    col_names, rows = _flatten_candidates(candidates)

    if not col_names:
        # No results — return an empty relation
        rel = conn.sql("SELECT 1 WHERE 1=0")
        return EnrichmentResult(
            result=rel,
            success_count=0,
            error_count=0,
            errors=[],
            elapsed_time=time.time() - start_time,
        )

    # Create temp table with results
    temp_table = f"_parallel_findall_{int(time.time() * 1000)}"
    col_defs = ", ".join(f'"{name}" VARCHAR' for name in col_names)
    temp_quoted = quote_identifier(temp_table)

    conn.execute(f"CREATE TEMP TABLE {temp_quoted} ({col_defs})")

    if rows:
        placeholders = ", ".join(["?"] * len(col_names))
        conn.executemany(
            f"INSERT INTO {temp_quoted} VALUES ({placeholders})",
            rows,
        )

    # Optionally persist to a named table
    if result_table:
        result_quoted = quote_identifier(result_table)
        conn.execute(f"CREATE OR REPLACE TABLE {result_quoted} AS SELECT * FROM {temp_quoted}")
        rel = conn.sql(f"SELECT * FROM {result_quoted}")
    else:
        rel = conn.sql(f"SELECT * FROM {temp_quoted}")

    return EnrichmentResult(
        result=rel,
        success_count=len(rows),
        error_count=0,
        errors=[],
        elapsed_time=time.time() - start_time,
    )
