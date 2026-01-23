"""Data processors for different source types."""

from parallel_web_tools.processors.csv import process_csv


def process_duckdb(schema):
    """Process DuckDB table and enrich data. Lazy import to avoid requiring duckdb."""
    from parallel_web_tools.processors.duckdb import process_duckdb as _process_duckdb

    return _process_duckdb(schema)


def process_bigquery(schema):
    """Process BigQuery table and enrich data. Lazy import to avoid requiring sqlalchemy."""
    from parallel_web_tools.processors.bigquery import process_bigquery as _process_bigquery

    return _process_bigquery(schema)


__all__ = ["process_csv", "process_duckdb", "process_bigquery"]
