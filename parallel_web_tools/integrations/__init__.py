"""Database and data platform integrations for parallel-web-tools.

Submodules are lazily imported to avoid loading heavy dependencies (like pyspark)
when they're not needed. Import the specific integration you need:

    from parallel_web_tools.integrations import bigquery
    from parallel_web_tools.integrations import spark  # Only loads pyspark here
"""

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parallel_web_tools.integrations import (
        bigquery as bigquery,
    )
    from parallel_web_tools.integrations import (
        duckdb as duckdb,
    )
    from parallel_web_tools.integrations import (
        polars as polars,
    )
    from parallel_web_tools.integrations import (
        snowflake as snowflake,
    )
    from parallel_web_tools.integrations import (
        spark as spark,
    )

__all__ = [
    "bigquery",
    "duckdb",
    "polars",
    "snowflake",
    "spark",
]

_SUBMODULES = {
    "bigquery": "parallel_web_tools.integrations.bigquery",
    "duckdb": "parallel_web_tools.integrations.duckdb",
    "polars": "parallel_web_tools.integrations.polars",
    "snowflake": "parallel_web_tools.integrations.snowflake",
    "spark": "parallel_web_tools.integrations.spark",
}


def __getattr__(name: str):
    """Lazily import submodules on first access."""
    if name in _SUBMODULES:
        module = importlib.import_module(_SUBMODULES[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
