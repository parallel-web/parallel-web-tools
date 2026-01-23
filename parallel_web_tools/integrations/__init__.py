"""Database and data platform integrations for parallel-web-tools."""

from parallel_web_tools.integrations import (
    bigquery,
    duckdb,
    polars,
    snowflake,
    spark,
)

__all__ = [
    "bigquery",
    "duckdb",
    "polars",
    "snowflake",
    "spark",
]
