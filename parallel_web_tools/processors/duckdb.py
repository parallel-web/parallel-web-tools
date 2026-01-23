"""DuckDB processor for data enrichment."""

import os

import duckdb
import polars as pl

from parallel_web_tools.core import InputSchema, parse_input_and_output_models, run_tasks


def process_duckdb(schema: InputSchema) -> None:
    """Process DuckDB table and enrich data."""
    InputModel, OutputModel = parse_input_and_output_models(schema)
    duckdb_file = os.getenv("DUCKDB_FILE")
    if duckdb_file is None:
        raise OSError("Missing DUCKDB_FILE in .env.local")

    with duckdb.connect(duckdb_file) as con:
        data = con.sql(f"SELECT * from {schema.source}").pl().to_dicts()

        output_rows = run_tasks(data, InputModel, OutputModel, schema.processor)

        # Write output_rows to the target table
        df = pl.DataFrame(output_rows)  # noqa: F841
        con.sql(f"CREATE OR REPLACE TABLE {schema.target} AS SELECT * FROM df")
