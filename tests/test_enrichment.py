"""Tests for the core.batch module."""

import os
from types import SimpleNamespace
from unittest import mock

import pytest

from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.batch import (
    build_output_schema,
    enrich_batch,
    enrich_single,
    extract_basis,
    run_tasks,
)


class TestResolveApiKey:
    """Tests for resolve_api_key function."""

    def test_explicit_api_key(self):
        """Should return explicit api_key when provided."""
        result = resolve_api_key(api_key="test-key-123")
        assert result == "test-key-123"

    def test_env_var_fallback(self):
        """Should use PARALLEL_API_KEY env var when no explicit key."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "env-key-456"}):
            result = resolve_api_key()
            assert result == "env-key-456"

    def test_oauth_fallback(self):
        """Should use stored OAuth credentials when no env var."""
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)

            with mock.patch("parallel_web_tools.core.auth._load_stored_token") as mock_load:
                mock_load.return_value = "oauth-key-789"
                result = resolve_api_key()
                assert result == "oauth-key-789"

    def test_no_key_raises_error(self):
        """Should raise ValueError when no API key found."""
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)

            with mock.patch("parallel_web_tools.core.auth._load_stored_token") as mock_load:
                mock_load.return_value = None

                with pytest.raises(ValueError) as exc_info:
                    resolve_api_key()

                assert "Parallel API key required" in str(exc_info.value)

    def test_explicit_key_takes_priority(self):
        """Explicit key should override env var."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "env-key"}):
            result = resolve_api_key(api_key="explicit-key")
            assert result == "explicit-key"


class TestBuildOutputSchema:
    """Tests for build_output_schema function."""

    def test_simple_column_name(self):
        """Simple column names should be converted to snake_case."""
        schema = build_output_schema(["CEO name"])

        assert schema["type"] == "object"
        assert "ceo_name" in schema["properties"]
        assert schema["properties"]["ceo_name"]["type"] == "string"
        assert schema["properties"]["ceo_name"]["description"] == "CEO name"
        assert "ceo_name" in schema["required"]

    def test_column_with_parentheses(self):
        """Parenthetical descriptions should be stripped from property name."""
        schema = build_output_schema(["Founding year (YYYY format)"])

        assert "founding_year" in schema["properties"]
        assert schema["properties"]["founding_year"]["description"] == "Founding year (YYYY format)"

    def test_column_with_brackets(self):
        """Bracketed text should be stripped from property name."""
        schema = build_output_schema(["Revenue [USD millions]"])

        assert "revenue" in schema["properties"]
        assert schema["properties"]["revenue"]["description"] == "Revenue [USD millions]"

    def test_column_with_braces(self):
        """Braced text should be stripped from property name."""
        schema = build_output_schema(["Stock ticker {NYSE/NASDAQ}"])

        assert "stock_ticker" in schema["properties"]

    def test_column_starting_with_number(self):
        """Columns starting with numbers should get 'col_' prefix."""
        schema = build_output_schema(["2024 revenue"])

        assert "col_2024_revenue" in schema["properties"]

    def test_special_characters_removed(self):
        """Special characters should be removed from property names."""
        schema = build_output_schema(["CEO's name & title!"])

        assert "ceos_name__title" in schema["properties"]

    def test_empty_column_fallback(self):
        """Empty column names should fallback to 'column'."""
        schema = build_output_schema(["(just a description)"])

        assert "column" in schema["properties"]

    def test_multiple_columns(self):
        """Multiple columns should all be included."""
        schema = build_output_schema(
            [
                "CEO name",
                "Founding year",
                "Headquarters",
            ]
        )

        assert len(schema["properties"]) == 3
        assert "ceo_name" in schema["properties"]
        assert "founding_year" in schema["properties"]
        assert "headquarters" in schema["properties"]
        assert len(schema["required"]) == 3

    def test_hyphenated_names(self):
        """Hyphens should be converted to underscores."""
        schema = build_output_schema(["year-over-year growth"])

        assert "year_over_year_growth" in schema["properties"]

    def test_empty_list(self):
        """Empty column list should return valid but empty schema."""
        schema = build_output_schema([])

        assert schema["type"] == "object"
        assert schema["properties"] == {}
        assert schema["required"] == []


class TestExtractBasis:
    """Tests for extract_basis function."""

    def test_no_basis_attribute(self):
        """Should return empty list when no basis attribute."""
        output = SimpleNamespace(content={"result": "test"})

        result = extract_basis(output)

        assert result == []

    def test_empty_basis(self):
        """Should return empty list when basis is empty."""
        output = SimpleNamespace(basis=[])

        result = extract_basis(output)

        assert result == []

    def test_none_basis(self):
        """Should return empty list when basis is None."""
        output = SimpleNamespace(basis=None)

        result = extract_basis(output)

        assert result == []

    def test_field_level_basis_with_citations(self):
        """Should extract field-level basis with citations."""
        citation = SimpleNamespace(
            url="https://example.com",
            excerpts=["excerpt 1", "excerpt 2"],
        )
        field_basis = SimpleNamespace(
            field="ceo_name",
            citations=[citation],
            reasoning="Found on company website",
            confidence="high",
        )
        output = SimpleNamespace(basis=[field_basis])

        result = extract_basis(output)

        assert len(result) == 1
        assert result[0]["field"] == "ceo_name"
        assert result[0]["citations"][0]["url"] == "https://example.com"
        assert result[0]["citations"][0]["excerpts"] == ["excerpt 1", "excerpt 2"]
        assert result[0]["reasoning"] == "Found on company website"
        assert result[0]["confidence"] == "high"

    def test_field_level_basis_without_optional_fields(self):
        """Should handle basis without optional fields."""
        field_basis = SimpleNamespace(
            field="ceo_name",
            citations=None,
            reasoning=None,
            confidence=None,
        )
        output = SimpleNamespace(basis=[field_basis])

        result = extract_basis(output)

        assert len(result) == 1
        assert result[0]["field"] == "ceo_name"
        assert "citations" not in result[0]
        assert "reasoning" not in result[0]
        assert "confidence" not in result[0]

    def test_simple_basis_format(self):
        """Should handle simpler basis format with url/title/excerpts."""
        simple_basis = SimpleNamespace(
            url="https://example.com/article",
            title="Article Title",
            excerpts=["relevant excerpt"],
        )
        output = SimpleNamespace(basis=[simple_basis])

        result = extract_basis(output)

        assert len(result) == 1
        assert result[0]["url"] == "https://example.com/article"
        assert result[0]["title"] == "Article Title"
        assert result[0]["excerpts"] == ["relevant excerpt"]

    def test_citation_without_excerpts(self):
        """Should handle citations without excerpts."""
        citation = SimpleNamespace(url="https://example.com")
        field_basis = SimpleNamespace(
            field="ceo_name",
            citations=[citation],
        )
        output = SimpleNamespace(basis=[field_basis])

        result = extract_basis(output)

        assert result[0]["citations"][0]["url"] == "https://example.com"
        assert result[0]["citations"][0]["excerpts"] == []

    def test_multiple_basis_entries(self):
        """Should handle multiple basis entries."""
        field_basis_1 = SimpleNamespace(field="ceo_name", reasoning="reason 1")
        field_basis_2 = SimpleNamespace(field="founding_year", reasoning="reason 2")
        output = SimpleNamespace(basis=[field_basis_1, field_basis_2])

        result = extract_basis(output)

        assert len(result) == 2
        assert result[0]["field"] == "ceo_name"
        assert result[1]["field"] == "founding_year"

    def test_empty_field_basis_skipped(self):
        """Should skip basis entries with no extractable data."""
        empty_basis = SimpleNamespace()  # No attributes
        valid_basis = SimpleNamespace(field="ceo_name")
        output = SimpleNamespace(basis=[empty_basis, valid_basis])

        result = extract_basis(output)

        assert len(result) == 1
        assert result[0]["field"] == "ceo_name"


class TestEnrichBatch:
    """Tests for enrich_batch function."""

    def test_empty_inputs(self):
        """Should return empty list for empty inputs."""
        result = enrich_batch([], ["CEO name"])

        assert result == []

    def test_successful_enrichment(self):
        """Should return enriched results for valid inputs."""
        # Create mock objects
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1", "run_2"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 2, "failed": 0}
        mock_status.status.num_task_runs = 2
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Mock the stream of events
        event_1 = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content={"ceo_name": "Sundar Pichai"},
                basis=[],
            ),
        )
        event_2 = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_2", error=None),
            output=SimpleNamespace(
                content={"ceo_name": "Satya Nadella"},
                basis=[],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event_1, event_2]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):  # Skip sleeps
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[
                            {"company_name": "Google"},
                            {"company_name": "Microsoft"},
                        ],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert len(result) == 2
        assert result[0]["ceo_name"] == "Sundar Pichai"
        assert result[1]["ceo_name"] == "Satya Nadella"

    def test_content_as_string_json(self):
        """Should parse JSON string content."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Content as JSON string
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content='{"ceo_name": "Tim Cook"}',
                basis=[],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Apple"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert result[0]["ceo_name"] == "Tim Cook"

    def test_content_as_invalid_json_string(self):
        """Should handle invalid JSON string content."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Content as non-JSON string
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content="plain text response",
                basis=[],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Apple"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert result[0]["result"] == "plain text response"

    def test_content_as_other_type(self):
        """Should handle non-dict/non-string content."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Content as number
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content=12345,
                basis=[],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Apple"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert result[0]["result"] == "12345"

    def test_include_basis_true(self):
        """Should include basis when include_basis=True."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        field_basis = SimpleNamespace(field="ceo_name", reasoning="test")
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content={"ceo_name": "Test CEO"},
                basis=[field_basis],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                        include_basis=True,
                    )

        assert "basis" in result[0]
        assert result[0]["basis"][0]["field"] == "ceo_name"

    def test_include_basis_false(self):
        """Should not include basis when include_basis=False."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        field_basis = SimpleNamespace(field="ceo_name", reasoning="test")
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(
                content={"ceo_name": "Test CEO"},
                basis=[field_basis],
            ),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                        include_basis=False,
                    )

        assert "basis" not in result[0]

    def test_run_error_handling(self):
        """Should handle run errors."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 0, "failed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error="API error occurred"),
            output=None,
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert "error" in result[0]
        assert "API error" in result[0]["error"]

    def test_missing_result(self):
        """Should handle missing results for some run_ids."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1", "run_2"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 2
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Only return event for run_1, not run_2
        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(content={"ceo_name": "Test"}, basis=[]),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "A"}, {"company_name": "B"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert result[0]["ceo_name"] == "Test"
        assert result[1]["error"] == "No result"

    def test_no_run_ids_returned(self):
        """Should handle case when no run_ids are returned."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = []
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert len(result) == 1
        assert "error" in result[0]
        assert "Failed to add runs" in result[0]["error"]

    def test_exception_handling(self):
        """Should return errors for all inputs on exception."""
        with mock.patch("parallel_web_tools.core.batch.resolve_api_key") as mock_resolve:
            mock_resolve.side_effect = Exception("Connection failed")

            result = enrich_batch(
                inputs=[{"company_name": "A"}, {"company_name": "B"}],
                output_columns=["CEO name"],
                api_key="test-key",
            )

        assert len(result) == 2
        assert "error" in result[0]
        assert "error" in result[1]
        assert "Connection failed" in result[0]["error"]

    def test_processor_passed_correctly(self):
        """Should pass processor to run inputs."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        event = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(content={}, basis=[]),
        )
        mock_client.beta.task_group.get_runs.return_value = [event]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                        processor="pro-fast",
                    )

        # Check that processor was passed correctly
        call_args = mock_client.beta.task_group.add_runs.call_args
        assert call_args.kwargs["inputs"][0]["processor"] == "pro-fast"

    def test_ignores_non_task_run_events(self):
        """Should ignore events that are not task_run.state."""
        mock_client = mock.MagicMock()
        mock_task_group = mock.MagicMock()
        mock_task_group.task_group_id = "tgrp_123"
        mock_client.beta.task_group.create.return_value = mock_task_group

        mock_add_response = mock.MagicMock()
        mock_add_response.run_ids = ["run_1"]
        mock_client.beta.task_group.add_runs.return_value = mock_add_response

        mock_status = mock.MagicMock()
        mock_status.status.task_run_status_counts = {"completed": 1}
        mock_status.status.num_task_runs = 1
        mock_status.status.is_active = False
        mock_client.beta.task_group.retrieve.return_value = mock_status

        # Include various event types
        event_other = SimpleNamespace(type="other.event")
        event_valid = SimpleNamespace(
            type="task_run.state",
            run=SimpleNamespace(run_id="run_1", error=None),
            output=SimpleNamespace(content={"ceo_name": "Test"}, basis=[]),
        )
        mock_client.beta.task_group.get_runs.return_value = [event_other, event_valid]

        with mock.patch("parallel.Parallel") as mock_parallel:
            with mock.patch("parallel_web_tools.core.batch.resolve_api_key", return_value="test-key"):
                with mock.patch("parallel_web_tools.core.batch.time.sleep"):
                    mock_parallel.return_value = mock_client

                    result = enrich_batch(
                        inputs=[{"company_name": "Test"}],
                        output_columns=["CEO name"],
                        api_key="test-key",
                    )

        assert result[0]["ceo_name"] == "Test"


class TestEnrichSingle:
    """Tests for enrich_single function."""

    def test_delegates_to_enrich_batch(self):
        """Should delegate to enrich_batch with single-item list."""
        with mock.patch("parallel_web_tools.core.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"ceo_name": "Test CEO"}]

            result = enrich_single(
                input_data={"company_name": "Test"},
                output_columns=["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
                include_basis=True,
            )

            mock_batch.assert_called_once_with(
                [{"company_name": "Test"}],
                ["CEO name"],
                api_key="test-key",
                processor="lite-fast",
                timeout=300,
                include_basis=True,
            )
            assert result == {"ceo_name": "Test CEO"}

    def test_empty_result_handling(self):
        """Should return error dict when batch returns empty."""
        with mock.patch("parallel_web_tools.core.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = []

            result = enrich_single(
                input_data={"company_name": "Test"},
                output_columns=["CEO name"],
            )

            assert result == {"error": "No result"}

    def test_default_parameters(self):
        """Should use default parameters."""
        with mock.patch("parallel_web_tools.core.batch.enrich_batch") as mock_batch:
            mock_batch.return_value = [{"result": "ok"}]

            enrich_single(
                input_data={"company_name": "Test"},
                output_columns=["CEO name"],
            )

            call_kwargs = mock_batch.call_args.kwargs
            assert call_kwargs.get("api_key") is None
            assert call_kwargs.get("processor") == "lite-fast"
            assert call_kwargs.get("timeout") == 300
            assert call_kwargs.get("include_basis") is True


class TestRunTasks:
    """Tests for run_tasks function with Parallel SDK."""

    def test_run_tasks_basic(self):
        """Should process batch tasks using the Parallel SDK and return results."""
        from pydantic import BaseModel

        class InputModel(BaseModel):
            company: str

        class OutputModel(BaseModel):
            ceo: str

        # Mock the Parallel SDK client
        mock_client = mock.MagicMock()

        # Mock task group create
        mock_client.beta.task_group.create.return_value = mock.MagicMock(task_group_id="tgrp_123")

        # Mock add_runs
        mock_client.beta.task_group.add_runs.return_value = mock.MagicMock(run_ids=["run_1", "run_2"])

        # Mock retrieve (status check) - return completed immediately
        mock_client.beta.task_group.retrieve.return_value = mock.MagicMock(
            status=mock.MagicMock(is_active=False, task_run_status_counts={"completed": 2})
        )

        # Mock get_runs (streaming results)
        mock_event1 = mock.MagicMock(
            type="task_run.state",
            input=mock.MagicMock(input={"company": "Anthropic"}),
            output=mock.MagicMock(content={"ceo": "Dario Amodei"}),
        )
        mock_event2 = mock.MagicMock(
            type="task_run.state",
            input=mock.MagicMock(input={"company": "OpenAI"}),
            output=mock.MagicMock(content={"ceo": "Sam Altman"}),
        )
        mock_client.beta.task_group.get_runs.return_value = [mock_event1, mock_event2]

        with mock.patch("parallel_web_tools.core.auth.resolve_api_key", return_value="test-key"):
            with mock.patch("parallel.Parallel", return_value=mock_client):
                with mock.patch("time.sleep"):  # Speed up test
                    input_data = [
                        {"company": "Anthropic"},
                        {"company": "OpenAI"},
                    ]

                    results = run_tasks(input_data, InputModel, OutputModel, "lite-fast")

        assert len(results) == 2
        assert results[0]["company"] == "Anthropic"
        assert results[0]["ceo"] == "Dario Amodei"
        assert results[1]["company"] == "OpenAI"
        assert results[1]["ceo"] == "Sam Altman"
        # Check batch_id and timestamp are added
        assert "batch_id" in results[0]
        assert "insertion_timestamp" in results[0]
