"""
Parallel DuckDB Integration

DataFrame-native and SQL-based data enrichment for DuckDB using the Parallel API.

Two approaches are available:

1. **Batch Processing (Recommended)**: Use `enrich_table()` for efficient
   parallel enrichment of entire tables.

2. **SQL UDF**: Use `register_parallel_functions()` for row-by-row enrichment
   in SQL queries. Simpler but slower.

Installation:
    pip install parallel-web-tools[duckdb]

Example (Batch):
    import duckdb
    from parallel_web_tools.integrations.duckdb import enrich_table

    conn = duckdb.connect()
    conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

    result = enrich_table(
        conn,
        source_table="companies",
        input_columns={"company_name": "name"},
        output_columns=["CEO name", "Founding year"],
    )

    print(result.result.fetchdf())

Example (SQL UDF):
    import duckdb
    from parallel_web_tools.integrations.duckdb import register_parallel_functions

    conn = duckdb.connect()
    register_parallel_functions(conn)

    conn.execute('''
        SELECT
            name,
            parallel_enrich(
                json_object('company_name', name),
                json_array('CEO name')
            ) as enriched
        FROM companies
    ''').fetchall()
"""

from parallel_web_tools.core.result import EnrichmentResult
from parallel_web_tools.integrations.duckdb.batch import enrich_table
from parallel_web_tools.integrations.duckdb.udf import (
    register_parallel_functions,
    unregister_parallel_functions,
)

__all__ = [
    "enrich_table",
    "EnrichmentResult",
    "register_parallel_functions",
    "unregister_parallel_functions",
]
