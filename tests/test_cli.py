"""Tests for the CLI commands."""

import json
import os
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import (
    build_config_from_args,
    main,
    parse_columns,
    suggest_from_intent,
)


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


class TestParseColumns:
    """Tests for parse_columns helper function."""

    def test_parse_valid_columns(self):
        """Should parse valid JSON columns."""
        json_str = '[{"name": "company", "description": "Company name"}]'
        result = parse_columns(json_str)
        assert result == [{"name": "company", "description": "Company name"}]

    def test_parse_multiple_columns(self):
        """Should parse multiple columns."""
        json_str = '[{"name": "a", "description": "A"}, {"name": "b", "description": "B"}]'
        result = parse_columns(json_str)
        assert len(result) == 2
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "b"

    def test_parse_none(self):
        """Should return None for None input."""
        assert parse_columns(None) is None

    def test_parse_invalid_json(self):
        """Should raise BadParameter for invalid JSON."""
        from click import BadParameter

        with pytest.raises(BadParameter):
            parse_columns("not valid json")

    def test_parse_not_array(self):
        """Should raise BadParameter for non-array JSON."""
        from click import BadParameter

        with pytest.raises(BadParameter):
            parse_columns('{"name": "test"}')

    def test_parse_missing_name(self):
        """Should raise BadParameter for missing name field."""
        from click import BadParameter

        with pytest.raises(BadParameter):
            parse_columns('[{"description": "test"}]')

    def test_parse_missing_description(self):
        """Should raise BadParameter for missing description field."""
        from click import BadParameter

        with pytest.raises(BadParameter):
            parse_columns('[{"name": "test"}]')


class TestBuildConfigFromArgs:
    """Tests for build_config_from_args helper function."""

    def test_build_config(self):
        """Should build config dict from args."""
        config = build_config_from_args(
            source_type="csv",
            source="input.csv",
            target="output.csv",
            source_columns=[{"name": "a", "description": "A"}],
            enriched_columns=[{"name": "b", "description": "B"}],
            processor="core-fast",
        )

        assert config["source_type"] == "csv"
        assert config["source"] == "input.csv"
        assert config["target"] == "output.csv"
        assert config["processor"] == "core-fast"
        assert len(config["source_columns"]) == 1
        assert len(config["enriched_columns"]) == 1


class TestMainCLI:
    """Tests for the main CLI group."""

    def test_help(self, runner):
        """Should show help message."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Parallel CLI" in result.output
        assert "auth" in result.output
        assert "login" in result.output
        assert "search" in result.output
        assert "extract" in result.output
        assert "enrich" in result.output

    def test_version(self, runner):
        """Should show version."""
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.0.2" in result.output


class TestAuthCommand:
    """Tests for the auth command."""

    def test_auth_with_env_var(self, runner):
        """Should show authenticated via environment."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "test-key"}):
            result = runner.invoke(main, ["auth"])
            assert result.exit_code == 0
            assert "PARALLEL_API_KEY" in result.output or "environment" in result.output

    def test_auth_not_authenticated(self, runner, tmp_path):
        """Should show not authenticated when no credentials."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                result = runner.invoke(main, ["auth"])
                assert result.exit_code == 0
                assert "Not authenticated" in result.output or "not" in result.output.lower()


class TestLogoutCommand:
    """Tests for the logout command."""

    def test_logout_no_credentials(self, runner, tmp_path):
        """Should handle logout when no credentials exist."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            result = runner.invoke(main, ["logout"])
            assert result.exit_code == 0
            assert "No stored credentials" in result.output or "no" in result.output.lower()


class TestSearchCommand:
    """Tests for the search command."""

    def test_search_help(self, runner):
        """Should show search help."""
        result = runner.invoke(main, ["search", "--help"])
        assert result.exit_code == 0
        assert "Search the web" in result.output
        assert "--json" in result.output

    def test_search_no_args(self, runner):
        """Should error without objective or query."""
        result = runner.invoke(main, ["search"])
        assert result.exit_code != 0
        assert "objective" in result.output.lower() or "query" in result.output.lower()

    @pytest.mark.skip(reason="Requires mocking Parallel SDK which is imported lazily")
    def test_search_with_json_output(self, runner):
        """Should output JSON format."""
        # This test would require API integration
        pass


class TestExtractCommand:
    """Tests for the extract command."""

    def test_extract_help(self, runner):
        """Should show extract help."""
        result = runner.invoke(main, ["extract", "--help"])
        assert result.exit_code == 0
        assert "Extract content" in result.output
        assert "--json" in result.output

    @pytest.mark.skip(reason="Requires mocking Parallel SDK which is imported lazily")
    def test_extract_with_json_output(self, runner):
        """Should output JSON format."""
        # This test would require API integration
        pass


class TestEnrichGroup:
    """Tests for the enrich command group."""

    def test_enrich_help(self, runner):
        """Should show enrich subcommands."""
        result = runner.invoke(main, ["enrich", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "plan" in result.output
        assert "suggest" in result.output
        assert "deploy" in result.output


class TestEnrichRunCommand:
    """Tests for the enrich run command."""

    def test_enrich_run_help(self, runner):
        """Should show enrich run help."""
        result = runner.invoke(main, ["enrich", "run", "--help"])
        assert result.exit_code == 0
        assert "--source-type" in result.output
        assert "--source" in result.output
        assert "--target" in result.output
        assert "--intent" in result.output

    def test_enrich_run_no_args(self, runner):
        """Should error without config or CLI args."""
        result = runner.invoke(main, ["enrich", "run"])
        assert result.exit_code != 0
        assert "config" in result.output.lower() or "arguments" in result.output.lower()

    def test_enrich_run_missing_required(self, runner):
        """Should error with partial CLI args."""
        result = runner.invoke(main, ["enrich", "run", "--source-type", "csv"])
        assert result.exit_code != 0
        assert "Missing" in result.output or "required" in result.output.lower()

    def test_enrich_run_both_enriched_and_intent(self, runner):
        """Should error when both --enriched-columns and --intent provided."""
        result = runner.invoke(
            main,
            [
                "enrich",
                "run",
                "--source-type",
                "csv",
                "--source",
                "input.csv",
                "--target",
                "output.csv",
                "--source-columns",
                '[{"name": "a", "description": "A"}]',
                "--enriched-columns",
                '[{"name": "b", "description": "B"}]',
                "--intent",
                "Find something",
            ],
        )
        assert result.exit_code != 0
        assert "either" in result.output.lower() or "not both" in result.output.lower()


class TestEnrichPlanCommand:
    """Tests for the enrich plan command."""

    def test_enrich_plan_help(self, runner):
        """Should show enrich plan help."""
        result = runner.invoke(main, ["enrich", "plan", "--help"])
        assert result.exit_code == 0
        assert "--output" in result.output or "-o" in result.output
        assert "--intent" in result.output

    def test_enrich_plan_non_interactive(self, runner, tmp_path):
        """Should create config file in non-interactive mode."""
        output_file = tmp_path / "config.yaml"

        result = runner.invoke(
            main,
            [
                "enrich",
                "plan",
                "-o",
                str(output_file),
                "--source-type",
                "csv",
                "--source",
                "input.csv",
                "--target",
                "output.csv",
                "--source-columns",
                '[{"name": "company", "description": "Company name"}]',
                "--enriched-columns",
                '[{"name": "ceo", "description": "CEO name", "type": "str"}]',
            ],
        )

        assert result.exit_code == 0
        assert output_file.exists()

        # Verify YAML content
        import yaml

        with open(output_file) as f:
            config = yaml.safe_load(f)

        assert config["source_type"] == "csv"
        assert config["source"] == "input.csv"
        assert config["target"] == "output.csv"


class TestEnrichSuggestCommand:
    """Tests for the enrich suggest command."""

    def test_enrich_suggest_help(self, runner):
        """Should show enrich suggest help."""
        result = runner.invoke(main, ["enrich", "suggest", "--help"])
        assert result.exit_code == 0
        assert "--json" in result.output

    def test_enrich_suggest_with_json_output(self, runner):
        """Should output JSON format."""
        mock_response = {
            "output_schema": {
                "properties": {
                    "ceo": {"type": "string", "description": "CEO name"},
                    "revenue": {"type": "number", "description": "Annual revenue"},
                }
            },
            "title": "Company info",
            "warnings": [],
        }

        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch("httpx.Client") as mock_client_class:
                mock_client = mock.MagicMock()
                mock_response_obj = mock.MagicMock()
                mock_response_obj.json.return_value = mock_response
                mock_response_obj.raise_for_status = mock.MagicMock()
                mock_client.post.return_value = mock_response_obj
                mock_client.__enter__ = mock.MagicMock(return_value=mock_client)
                mock_client.__exit__ = mock.MagicMock(return_value=False)
                mock_client_class.return_value = mock_client

                result = runner.invoke(main, ["enrich", "suggest", "Find CEO and revenue", "--json"])

                assert result.exit_code == 0
                output = json.loads(result.output)
                assert "enriched_columns" in output
                assert "processor" in output


class TestEnrichDeployCommand:
    """Tests for the enrich deploy command."""

    def test_enrich_deploy_help(self, runner):
        """Should show enrich deploy help."""
        result = runner.invoke(main, ["enrich", "deploy", "--help"])
        assert result.exit_code == 0
        assert "--system" in result.output
        assert "--project" in result.output
        assert "bigquery" in result.output

    def test_enrich_deploy_bigquery_no_project(self, runner):
        """Should error without --project for BigQuery."""
        result = runner.invoke(main, ["enrich", "deploy", "--system", "bigquery"])
        assert result.exit_code != 0
        assert "project" in result.output.lower()


class TestSuggestFromIntent:
    """Tests for suggest_from_intent helper function."""

    def test_suggest_from_intent_basic(self):
        """Should call Parallel API and parse response."""
        mock_response = {
            "output_schema": {
                "properties": {
                    "ceo": {"type": "string", "description": "CEO name"},
                }
            },
            "title": "Find CEO",
            "warnings": [],
        }

        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch("httpx.Client") as mock_client_class:
                mock_client = mock.MagicMock()
                mock_response_obj = mock.MagicMock()
                mock_response_obj.json.return_value = mock_response
                mock_response_obj.raise_for_status = mock.MagicMock()
                mock_client.post.return_value = mock_response_obj
                mock_client.__enter__ = mock.MagicMock(return_value=mock_client)
                mock_client.__exit__ = mock.MagicMock(return_value=False)
                mock_client_class.return_value = mock_client

                result = suggest_from_intent("Find the CEO")

                assert "enriched_columns" in result
                assert len(result["enriched_columns"]) == 1
                assert result["enriched_columns"][0]["name"] == "ceo"

    def test_suggest_from_intent_with_source_columns(self):
        """Should include source columns context in intent."""
        mock_response = {
            "output_schema": {"properties": {"ceo": {"type": "string", "description": "CEO"}}},
            "title": "",
            "warnings": [],
        }

        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch("parallel_web_tools.cli.commands.httpx.Client") as mock_client_class:
                mock_client = mock.MagicMock()
                mock_response_obj = mock.MagicMock()
                mock_response_obj.json.return_value = mock_response
                mock_response_obj.raise_for_status = mock.MagicMock()
                mock_client.post.return_value = mock_response_obj
                mock_client.__enter__ = mock.MagicMock(return_value=mock_client)
                mock_client.__exit__ = mock.MagicMock(return_value=False)
                mock_client_class.return_value = mock_client

                source_cols = [{"name": "company", "description": "Company name"}]
                result = suggest_from_intent("Find CEO", source_cols)

                # Verify that the function returned valid results
                assert "enriched_columns" in result

                # Verify the call was made
                assert mock_client.post.called
