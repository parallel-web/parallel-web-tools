"""Tests for the Polars integration module."""

from unittest import mock

import polars as pl
import pytest

from parallel_web_tools.integrations.polars import (
    EnrichmentResult,
    parallel_enrich,
    parallel_enrich_lazy,
)


class TestEnrichmentResult:
    """Tests for EnrichmentResult dataclass."""

    def test_default_values(self):
        """Should have correct default values."""
        df = pl.DataFrame({"col": [1]})
        result = EnrichmentResult(
            result=df,
            success_count=1,
            error_count=0,
        )

        assert result.errors == []
        assert result.elapsed_time == 0.0

    def test_all_fields(self):
        """Should store all fields correctly."""
        df = pl.DataFrame({"col": [1, 2]})
        errors = [{"row": 0, "error": "test error"}]

        result = EnrichmentResult(
            result=df,
            success_count=1,
            error_count=1,
            errors=errors,
            elapsed_time=1.5,
        )

        assert result.result.equals(df)
        assert result.success_count == 1
        assert result.error_count == 1
        assert result.errors == errors
        assert result.elapsed_time == 1.5


class TestParallelEnrich:
    """Tests for parallel_enrich function."""

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pl.DataFrame({"company": []})

        result = parallel_enrich(
            df,
            input_columns={"company_name": "company"},
            output_columns=["CEO name"],
        )

        assert result.result.is_empty()
        assert result.success_count == 0
        assert result.error_count == 0
        assert result.errors == []

    def test_missing_column_raises_error(self):
        """Should raise ValueError for missing columns."""
        df = pl.DataFrame({"company": ["Google"]})

        with pytest.raises(ValueError) as exc_info:
            parallel_enrich(
                df,
                input_columns={"company_name": "missing_column"},
                output_columns=["CEO name"],
            )

        assert "Columns not found" in str(exc_info.value)
        assert "missing_column" in str(exc_info.value)

    def test_successful_enrichment(self):
        """Should enrich DataFrame successfully."""
        df = pl.DataFrame(
            {
                "company": ["Google", "Microsoft"],
                "website": ["google.com", "microsoft.com"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai", "founding_year": "1998"},
                {"ceo_name": "Satya Nadella", "founding_year": "1975"},
            ]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company", "website": "website"},
                output_columns=["CEO name", "Founding year"],
                api_key="test-key",
            )

        assert result.success_count == 2
        assert result.error_count == 0
        assert "ceo_name" in result.result.columns
        assert "founding_year" in result.result.columns
        assert result.result["ceo_name"].to_list() == ["Sundar Pichai", "Satya Nadella"]
        assert result.result["founding_year"].to_list() == ["1998", "1975"]

    def test_preserves_original_columns(self):
        """Should preserve original DataFrame columns."""
        df = pl.DataFrame(
            {
                "company": ["Google"],
                "industry": ["Technology"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        assert "company" in result.result.columns
        assert "industry" in result.result.columns
        assert result.result["company"].to_list() == ["Google"]
        assert result.result["industry"].to_list() == ["Technology"]

    def test_error_handling(self):
        """Should handle errors in individual rows."""
        df = pl.DataFrame(
            {
                "company": ["Google", "InvalidCompany"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"error": "Company not found"},
            ]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        assert result.success_count == 1
        assert result.error_count == 1
        assert len(result.errors) == 1
        assert result.errors[0]["row"] == 1
        assert "Company not found" in result.errors[0]["error"]
        # Successful row should have value
        assert result.result["ceo_name"][0] == "Sundar Pichai"
        # Failed row should have None
        assert result.result["ceo_name"][1] is None

    def test_include_basis(self):
        """Should include basis when include_basis=True."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {
                    "ceo_name": "Sundar Pichai",
                    "basis": [{"field": "ceo_name", "reasoning": "test"}],
                }
            ]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                include_basis=True,
            )

        assert "_basis" in result.result.columns
        basis_value = result.result["_basis"].to_list()[0]
        assert basis_value == [{"field": "ceo_name", "reasoning": "test"}]

    def test_no_basis_when_disabled(self):
        """Should not include basis when include_basis=False."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                include_basis=False,
            )

        assert "_basis" not in result.result.columns

    def test_passes_api_key(self):
        """Should pass api_key to enrich_batch."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                api_key="my-secret-key",
            )

        assert mock_batch.call_args.kwargs["api_key"] == "my-secret-key"

    def test_passes_processor(self):
        """Should pass processor to enrich_batch."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                processor="pro-fast",
            )

        assert mock_batch.call_args.kwargs["processor"] == "pro-fast"

    def test_passes_timeout(self):
        """Should pass timeout to enrich_batch."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                timeout=1200,
            )

        assert mock_batch.call_args.kwargs["timeout"] == 1200

    def test_default_parameters(self):
        """Should use default parameters when not specified."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        call_kwargs = mock_batch.call_args.kwargs
        assert call_kwargs["processor"] == "lite-fast"
        assert call_kwargs["timeout"] == 600
        assert call_kwargs["include_basis"] is False

    def test_handles_none_values(self):
        """Should handle None values in input columns."""
        df = pl.DataFrame(
            {
                "company": ["Google", None],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"ceo_name": "Unknown"},
            ]

            parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        # Check that None values were filtered from inputs
        inputs = mock_batch.call_args.kwargs["inputs"]
        assert inputs[0] == {"company_name": "Google"}
        assert inputs[1] == {}  # None value should result in empty dict entry

    def test_converts_non_string_values(self):
        """Should convert non-string values to strings."""
        df = pl.DataFrame(
            {
                "company_id": [123, 456],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"company_name": "Company A"},
                {"company_name": "Company B"},
            ]

            parallel_enrich(
                df,
                input_columns={"company_id": "company_id"},
                output_columns=["Company name"],
            )

        inputs = mock_batch.call_args.kwargs["inputs"]
        assert inputs[0] == {"company_id": "123"}
        assert inputs[1] == {"company_id": "456"}

    def test_multiple_input_columns(self):
        """Should handle multiple input columns."""
        df = pl.DataFrame(
            {
                "name": ["Google"],
                "website": ["google.com"],
                "location": ["Mountain View"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Sundar Pichai"}]

            parallel_enrich(
                df,
                input_columns={
                    "company_name": "name",
                    "website": "website",
                    "headquarters": "location",
                },
                output_columns=["CEO name"],
            )

        inputs = mock_batch.call_args.kwargs["inputs"]
        assert inputs[0] == {
            "company_name": "Google",
            "website": "google.com",
            "headquarters": "Mountain View",
        }

    def test_multiple_output_columns(self):
        """Should handle multiple output columns."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            with mock.patch("parallel_web_tools.integrations.polars.enrich.build_output_schema") as mock_schema:
                mock_schema.return_value = {
                    "properties": {
                        "ceo_name": {},
                        "founding_year": {},
                        "headquarters": {},
                    }
                }
                mock_batch.return_value = [
                    {
                        "ceo_name": "Sundar Pichai",
                        "founding_year": "1998",
                        "headquarters": "Mountain View",
                    }
                ]

                result = parallel_enrich(
                    df,
                    input_columns={"company_name": "company"},
                    output_columns=["CEO name", "Founding year", "Headquarters"],
                )

        assert "ceo_name" in result.result.columns
        assert "founding_year" in result.result.columns
        assert "headquarters" in result.result.columns

    def test_elapsed_time_recorded(self):
        """Should record elapsed time."""
        df = pl.DataFrame({"company": ["Google"]})

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        assert result.elapsed_time >= 0


class TestParallelEnrichLazy:
    """Tests for parallel_enrich_lazy function."""

    def test_collects_lazyframe(self):
        """Should collect LazyFrame before processing."""
        lf = pl.DataFrame({"company": ["Google", "Microsoft"]}).lazy()

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"ceo_name": "Satya Nadella"},
            ]

            result = parallel_enrich_lazy(
                lf,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        assert isinstance(result.result, pl.DataFrame)
        assert not isinstance(result.result, pl.LazyFrame)
        assert result.success_count == 2

    def test_passes_all_parameters(self):
        """Should pass all parameters to parallel_enrich."""
        lf = pl.DataFrame({"company": ["Google"]}).lazy()

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test"}]

            parallel_enrich_lazy(
                lf,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
                api_key="test-key",
                processor="pro-fast",
                timeout=1200,
                include_basis=True,
            )

        call_kwargs = mock_batch.call_args.kwargs
        assert call_kwargs["api_key"] == "test-key"
        assert call_kwargs["processor"] == "pro-fast"
        assert call_kwargs["timeout"] == 1200
        assert call_kwargs["include_basis"] is True

    def test_with_lazy_operations(self):
        """Should work with chained lazy operations."""
        df = pl.DataFrame(
            {
                "company": ["Google", "Microsoft", "Apple"],
                "active": [True, True, False],
            }
        )
        lf = df.lazy().filter(pl.col("active")).select("company")

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"ceo_name": "Satya Nadella"},
            ]

            result = parallel_enrich_lazy(
                lf,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        # Should only process 2 rows (Apple filtered out)
        assert len(result.result) == 2
        assert result.success_count == 2


class TestIntegration:
    """Integration tests for the Polars module."""

    def test_full_workflow(self):
        """Test a complete enrichment workflow."""
        df = pl.DataFrame(
            {
                "name": ["Tesla", "SpaceX"],
                "industry": ["Automotive", "Aerospace"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {
                    "ceo_name": "Elon Musk",
                    "founding_year": "2003",
                },
                {
                    "ceo_name": "Elon Musk",
                    "founding_year": "2002",
                },
            ]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "name", "sector": "industry"},
                output_columns=["CEO name", "Founding year"],
            )

        # Check result structure
        assert result.success_count == 2
        assert result.error_count == 0

        # Check original columns preserved
        assert result.result["name"].to_list() == ["Tesla", "SpaceX"]
        assert result.result["industry"].to_list() == ["Automotive", "Aerospace"]

        # Check new columns added
        assert result.result["ceo_name"].to_list() == ["Elon Musk", "Elon Musk"]
        assert result.result["founding_year"].to_list() == ["2003", "2002"]

    def test_mixed_success_and_errors(self):
        """Test handling mix of successful and failed enrichments."""
        df = pl.DataFrame(
            {
                "company": ["Google", "FakeCompany123", "Microsoft"],
            }
        )

        with mock.patch("parallel_web_tools.integrations.polars.enrich.enrich_batch") as mock_batch:
            mock_batch.return_value = [
                {"ceo_name": "Sundar Pichai"},
                {"error": "Company not found"},
                {"ceo_name": "Satya Nadella"},
            ]

            result = parallel_enrich(
                df,
                input_columns={"company_name": "company"},
                output_columns=["CEO name"],
            )

        assert result.success_count == 2
        assert result.error_count == 1
        assert len(result.errors) == 1
        assert result.errors[0]["row"] == 1

        # Check values
        ceo_names = result.result["ceo_name"].to_list()
        assert ceo_names[0] == "Sundar Pichai"
        assert ceo_names[1] is None
        assert ceo_names[2] == "Satya Nadella"
