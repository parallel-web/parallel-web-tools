"""User-Agent generation for Parallel API requests.

This module provides utilities to generate User-Agent headers that identify
the source of API requests (standalone CLI vs database integrations).
"""

from __future__ import annotations

import platform
import sys
from enum import Enum
from importlib.metadata import version
from typing import Literal


def _get_version() -> str:
    """Get package version without causing circular imports."""
    try:
        return version("parallel-web-tools")
    except Exception:
        return "unknown"


# Type for valid source identifiers (named differently to avoid conflict with schema.SourceType)
ClientSource = Literal["cli", "duckdb", "bigquery", "snowflake", "spark", "polars", "python"]


class Source(str, Enum):
    """Source identifiers for User-Agent."""

    CLI = "cli"
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    SPARK = "spark"
    POLARS = "polars"
    PYTHON = "python"  # Direct Python library usage


def get_user_agent(source: ClientSource = "python") -> str:
    """Generate a User-Agent string for Parallel API requests.

    The User-Agent format is:
        parallel-web-tools/{version} ({source}) Python/{py_version} {system}/{release}

    Examples:
        parallel-web-tools/0.0.8 (cli) Python/3.11.5 Darwin/23.0.0
        parallel-web-tools/0.0.8 (duckdb) Python/3.11.5 Linux/5.15.0

    Args:
        source: The source of the API request. One of:
            - "cli": Standalone parallel-cli
            - "duckdb": DuckDB integration
            - "bigquery": BigQuery integration
            - "snowflake": Snowflake integration
            - "spark": Apache Spark integration
            - "python": Direct Python library usage (default)

    Returns:
        A User-Agent string identifying the client.
    """
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    system = platform.system()
    release = platform.release()

    return f"parallel-web-tools/{_get_version()} ({source}) Python/{py_version} {system}/{release}"


def get_default_headers(source: ClientSource = "python") -> dict[str, str]:
    """Get default HTTP headers including User-Agent for Parallel API requests.

    Args:
        source: The source of the API request (see get_user_agent for options).

    Returns:
        A dictionary of headers suitable for passing to HTTP clients.
    """
    return {"User-Agent": get_user_agent(source)}


# Thread-local storage for source context
_source_context: ClientSource = "python"


def set_source_context(source: ClientSource) -> None:
    """Set the source context for the current thread.

    This is used to automatically determine the source when creating
    API clients without explicitly passing the source.

    Args:
        source: The source identifier to set.
    """
    global _source_context
    _source_context = source


def get_source_context() -> ClientSource:
    """Get the current source context.

    Returns:
        The source identifier for the current context.
    """
    return _source_context
