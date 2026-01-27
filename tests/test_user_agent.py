"""Tests for the user agent module."""

import platform
import re
import sys

from parallel_web_tools.core.user_agent import (
    Source,
    get_default_headers,
    get_source_context,
    get_user_agent,
    set_source_context,
)


class TestGetUserAgent:
    """Tests for get_user_agent function."""

    def test_default_source_is_python(self):
        """Default source should be 'python'."""
        ua = get_user_agent()
        assert "(python)" in ua

    def test_cli_source(self):
        """CLI source should be included in user agent."""
        ua = get_user_agent("cli")
        assert "(cli)" in ua

    def test_duckdb_source(self):
        """DuckDB source should be included in user agent."""
        ua = get_user_agent("duckdb")
        assert "(duckdb)" in ua

    def test_bigquery_source(self):
        """BigQuery source should be included in user agent."""
        ua = get_user_agent("bigquery")
        assert "(bigquery)" in ua

    def test_snowflake_source(self):
        """Snowflake source should be included in user agent."""
        ua = get_user_agent("snowflake")
        assert "(snowflake)" in ua

    def test_spark_source(self):
        """Spark source should be included in user agent."""
        ua = get_user_agent("spark")
        assert "(spark)" in ua

    def test_polars_source(self):
        """Polars source should be included in user agent."""
        ua = get_user_agent("polars")
        assert "(polars)" in ua

    def test_includes_version(self):
        """User agent should include package version."""
        ua = get_user_agent()
        assert ua.startswith("parallel-tools/")
        # Version format: X.X.X
        assert re.search(r"parallel-tools/\d+\.\d+\.\d+", ua)

    def test_includes_python_version(self):
        """User agent should include Python version."""
        ua = get_user_agent()
        py_version = f"Python/{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        assert py_version in ua

    def test_includes_platform(self):
        """User agent should include platform info."""
        ua = get_user_agent()
        system = platform.system()
        assert system in ua


class TestGetDefaultHeaders:
    """Tests for get_default_headers function."""

    def test_returns_dict_with_user_agent(self):
        """Should return dict with User-Agent header."""
        headers = get_default_headers()
        assert "User-Agent" in headers
        assert headers["User-Agent"].startswith("parallel-tools/")

    def test_source_is_passed_through(self):
        """Source should be passed through to user agent."""
        headers = get_default_headers("cli")
        assert "(cli)" in headers["User-Agent"]


class TestSourceContext:
    """Tests for source context management."""

    def test_default_context_is_python(self):
        """Default source context should be 'python'."""
        # Reset to ensure clean state
        set_source_context("python")
        assert get_source_context() == "python"

    def test_set_and_get_context(self):
        """Should be able to set and get source context."""
        set_source_context("cli")
        assert get_source_context() == "cli"

        set_source_context("duckdb")
        assert get_source_context() == "duckdb"

        # Reset for other tests
        set_source_context("python")


class TestSourceEnum:
    """Tests for Source enum."""

    def test_all_sources_defined(self):
        """All expected sources should be defined."""
        assert Source.CLI.value == "cli"
        assert Source.DUCKDB.value == "duckdb"
        assert Source.BIGQUERY.value == "bigquery"
        assert Source.SNOWFLAKE.value == "snowflake"
        assert Source.SPARK.value == "spark"
        assert Source.POLARS.value == "polars"
        assert Source.PYTHON.value == "python"
