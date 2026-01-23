"""Tests for the DuckDB integration module."""

import json
from unittest import mock

import duckdb
import pytest

from parallel_web_tools.integrations.duckdb import (
    EnrichmentResult,
    enrich_table,
    register_parallel_functions,
    unregister_parallel_functions,
)


@pytest.fixture
def conn():
    """Create a fresh DuckDB connection for each test."""
    connection = duckdb.connect()
    yield connection
    connection.close()


class TestEnrichmentResult:
    """Tests for EnrichmentResult dataclass."""

    def test_default_values(self, conn):
        """Should have correct default values."""
        rel = conn.sql("SELECT 1 as col")
        result = EnrichmentResult(
            result=rel,
            success_count=1,
            error_count=0,
        )

        assert result.errors == []
        assert result.elapsed_time == 0.0

    def test_all_fields(self, conn):
        """Should store all fields correctly."""
        rel = conn.sql("SELECT 1 as col, 2 as col2")
        errors = [{"row": 0, "error": "test error"}]

        result = EnrichmentResult(
            result=rel,
            success_count=1,
            error_count=1,
            errors=errors,
            elapsed_time=1.5,
        )

        assert result.success_count == 1
        assert result.error_count == 1
        assert result.errors == errors
        assert result.elapsed_time == 1.5


class TestEnrichTable:
    """Tests for enrich_table function."""

    def test_empty_table(self, conn):
        """Should handle empty table."""
        conn.execute("CREATE TABLE empty_companies (name VARCHAR)")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = []

            result = enrich_table(
                conn,
                source_table="empty_companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        assert result.success_count == 0
        assert result.error_count == 0

    def test_successful_enrichment(self, conn):
        """Should enrich table successfully."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google', 'google.com'),
                ('Microsoft', 'microsoft.com')
            ) AS t(name, website)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai", "founding_year": "1998"},
                {"ceo_name": "Satya Nadella", "founding_year": "1975"},
            ]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name", "website": "website"},
                output_columns=["CEO name", "Founding year"],
                api_key="test-key",
            )

        assert result.success_count == 2
        assert result.error_count == 0

        df = result.result.fetchdf()
        assert "ceo_name" in df.columns
        assert "founding_year" in df.columns
        assert df["ceo_name"].tolist() == ["Sundar Pichai", "Satya Nadella"]

    def test_preserves_original_columns(self, conn):
        """Should preserve original table columns."""
        conn.execute("""
            CREATE TABLE companies AS SELECT 'Google' as name, 'Tech' as industry
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        df = result.result.fetchdf()
        assert "name" in df.columns
        assert df["name"].iloc[0] == "Google"

    def test_error_handling(self, conn):
        """Should handle errors in individual rows."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google'),
                ('InvalidCompany')
            ) AS t(name)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"error": "Company not found"},
            ]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        assert result.success_count == 1
        assert result.error_count == 1
        assert len(result.errors) == 1
        assert result.errors[0]["row"] == 1

        df = result.result.fetchdf()
        assert df["ceo_name"].iloc[0] == "Sundar Pichai"
        assert df["ceo_name"].iloc[1] is None

    def test_include_basis(self, conn):
        """Should include basis when include_basis=True."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {
                    "ceo_name": "Sundar Pichai",
                    "basis": [{"field": "ceo_name", "reasoning": "test"}],
                }
            ]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                include_basis=True,
            )

        df = result.result.fetchdf()
        assert "_basis" in df.columns

    def test_no_basis_when_disabled(self, conn):
        """Should not include basis when include_basis=False."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                include_basis=False,
            )

        df = result.result.fetchdf()
        assert "_basis" not in df.columns

    def test_passes_api_key(self, conn):
        """Should pass api_key to enrich_batch."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                api_key="my-secret-key",
            )

        assert mock_batch.call_args.kwargs["api_key"] == "my-secret-key"

    def test_passes_processor(self, conn):
        """Should pass processor to enrich_batch."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                processor="pro-fast",
            )

        assert mock_batch.call_args.kwargs["processor"] == "pro-fast"

    def test_passes_timeout(self, conn):
        """Should pass timeout to enrich_batch."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                timeout=1200,
            )

        assert mock_batch.call_args.kwargs["timeout"] == 1200

    def test_default_parameters(self, conn):
        """Should use default parameters when not specified."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        call_kwargs = mock_batch.call_args.kwargs
        assert call_kwargs["processor"] == "lite-fast"
        assert call_kwargs["timeout"] == 600
        assert call_kwargs["include_basis"] is False

    def test_sql_query_as_source(self, conn):
        """Should handle SQL query as source_table."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google', true),
                ('Inactive', false)
            ) AS t(name, active)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            result = enrich_table(
                conn,
                source_table="SELECT name FROM companies WHERE active = true",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        # Should only process one row (Google)
        assert result.success_count == 1

    def test_handles_null_values(self, conn):
        """Should handle NULL values in input columns."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google'),
                (NULL)
            ) AS t(name)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"ceo_name": "Unknown"},
            ]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        # Check that NULL values were filtered from inputs
        inputs = mock_batch.call_args.kwargs["inputs"]
        assert inputs[0] == {"company_name": "Google"}
        assert inputs[1] == {}  # NULL value should result in empty dict

    def test_progress_callback(self, conn):
        """Should call progress callback."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google'),
                ('Microsoft')
            ) AS t(name)
        """)

        progress_calls = []

        def on_progress(completed, total):
            progress_calls.append((completed, total))

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"ceo_name": "Satya Nadella"},
            ]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                progress_callback=on_progress,
            )

        assert len(progress_calls) == 2
        assert progress_calls[0] == (1, 2)
        assert progress_calls[1] == (2, 2)

    def test_creates_result_table(self, conn):
        """Should create permanent result table when specified."""
        conn.execute("CREATE TABLE companies AS SELECT 'Google' as name")

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
                result_table="enriched_companies",
            )

        # Should be able to query the result table
        df = conn.execute("SELECT * FROM enriched_companies").fetchdf()
        assert df["ceo_name"].iloc[0] == "Sundar Pichai"


class TestRegisterParallelFunctions:
    """Tests for register_parallel_functions function."""

    def test_registers_function(self, conn):
        """Should register parallel_enrich function."""
        with mock.patch("parallel_web_tools.integrations.duckdb.udf.enrich_single") as mock_single:
            mock_single.return_value = {"ceo_name": "Test"}

            register_parallel_functions(conn, api_key="test-key")

            # Function should be callable
            result = conn.execute("""
                SELECT parallel_enrich(
                    '{"company_name": "Google"}',
                    '["CEO name"]'
                )
            """).fetchone()[0]

            data = json.loads(result)
            assert data["ceo_name"] == "Test"

    def test_passes_parameters(self, conn):
        """Should pass parameters to enrich_single."""
        with mock.patch("parallel_web_tools.integrations.duckdb.udf.enrich_single") as mock_single:
            mock_single.return_value = {"ceo_name": "Test"}

            register_parallel_functions(
                conn,
                api_key="my-key",
                processor="pro-fast",
                timeout=500,
            )

            conn.execute("""
                SELECT parallel_enrich('{"company": "Google"}', '["CEO name"]')
            """).fetchone()

        call_kwargs = mock_single.call_args.kwargs
        assert call_kwargs["api_key"] == "my-key"
        assert call_kwargs["processor"] == "pro-fast"
        assert call_kwargs["timeout"] == 500

    def test_handles_json_error(self, conn):
        """Should return error for invalid JSON input."""
        register_parallel_functions(conn)

        result = conn.execute("""
            SELECT parallel_enrich('invalid json', '["CEO name"]')
        """).fetchone()[0]

        data = json.loads(result)
        assert "error" in data
        assert "JSON" in data["error"]

    def test_handles_enrichment_error(self, conn):
        """Should return error when enrichment fails."""
        with mock.patch("parallel_web_tools.integrations.duckdb.udf.enrich_single") as mock_single:
            mock_single.side_effect = Exception("API error")

            register_parallel_functions(conn)

            result = conn.execute("""
                SELECT parallel_enrich('{"company": "Google"}', '["CEO name"]')
            """).fetchone()[0]

            data = json.loads(result)
            assert "error" in data
            assert "API error" in data["error"]


class TestUnregisterParallelFunctions:
    """Tests for unregister_parallel_functions function."""

    def test_unregisters_function(self, conn):
        """Should unregister the function."""
        with mock.patch("parallel_web_tools.integrations.duckdb.udf.enrich_single"):
            register_parallel_functions(conn)
            unregister_parallel_functions(conn)

        # Function should no longer exist
        with pytest.raises(duckdb.CatalogException):
            conn.execute("SELECT parallel_enrich('{}', '[]')").fetchone()

    def test_handles_nonexistent_function(self, conn):
        """Should not raise when function doesn't exist."""
        # Should not raise
        unregister_parallel_functions(conn)


class TestIntegration:
    """Integration tests for the DuckDB module."""

    def test_full_workflow(self, conn):
        """Test a complete enrichment workflow."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Tesla', 'Automotive'),
                ('SpaceX', 'Aerospace')
            ) AS t(name, industry)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Elon Musk", "founding_year": "2003"},
                {"ceo_name": "Elon Musk", "founding_year": "2002"},
            ]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name", "sector": "industry"},
                output_columns=["CEO name", "Founding year"],
            )

        assert result.success_count == 2
        assert result.error_count == 0

        df = result.result.fetchdf()

        # Check original columns preserved
        assert df["name"].tolist() == ["Tesla", "SpaceX"]
        assert df["industry"].tolist() == ["Automotive", "Aerospace"]

        # Check new columns added
        assert df["ceo_name"].tolist() == ["Elon Musk", "Elon Musk"]
        assert df["founding_year"].tolist() == ["2003", "2002"]

    def test_mixed_success_and_errors(self, conn):
        """Test handling mix of successful and failed enrichments."""
        conn.execute("""
            CREATE TABLE companies AS SELECT * FROM (VALUES
                ('Google'),
                ('FakeCompany123'),
                ('Microsoft')
            ) AS t(name)
        """)

        with mock.patch("parallel_web_tools.integrations.duckdb.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"error": "Company not found"},
                {"ceo_name": "Satya Nadella"},
            ]

            result = enrich_table(
                conn,
                source_table="companies",
                input_columns={"company_name": "name"},
                output_columns=["CEO name"],
            )

        assert result.success_count == 2
        assert result.error_count == 1
        assert len(result.errors) == 1
        assert result.errors[0]["row"] == 1

        df = result.result.fetchdf()
        ceo_names = df["ceo_name"].tolist()
        assert ceo_names[0] == "Sundar Pichai"
        assert ceo_names[1] is None
        assert ceo_names[2] == "Satya Nadella"
