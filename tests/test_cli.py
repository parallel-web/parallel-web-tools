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
    parse_comma_separated,
    parse_inline_data,
    suggest_from_intent,
)


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


class TestParseCommaSeparated:
    """Tests for parse_comma_separated helper function."""

    def test_single_value(self):
        """Should handle single value."""
        result = parse_comma_separated(("example.com",))
        assert result == ["example.com"]

    def test_comma_separated(self):
        """Should split comma-separated values."""
        result = parse_comma_separated(("google.com,github.com",))
        assert result == ["google.com", "github.com"]

    def test_repeated_flags(self):
        """Should handle repeated flags."""
        result = parse_comma_separated(("google.com", "github.com"))
        assert result == ["google.com", "github.com"]

    def test_mixed_usage(self):
        """Should handle mix of comma-separated and repeated."""
        result = parse_comma_separated(("google.com,github.com", "twitter.com"))
        assert result == ["google.com", "github.com", "twitter.com"]

    def test_whitespace_handling(self):
        """Should trim whitespace around values."""
        result = parse_comma_separated(("google.com , github.com",))
        assert result == ["google.com", "github.com"]

    def test_empty_tuple(self):
        """Should return empty list for empty tuple."""
        result = parse_comma_separated(())
        assert result == []

    def test_skips_empty_strings(self):
        """Should skip empty strings from trailing commas."""
        result = parse_comma_separated(("google.com,",))
        assert result == ["google.com"]


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
        assert result is not None
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


class TestParseInlineData:
    """Tests for parse_inline_data helper function."""

    def test_parse_valid_data(self):
        """Should parse valid JSON array and create temp CSV."""
        data = '[{"company": "Google", "industry": "Tech"}, {"company": "Apple", "industry": "Tech"}]'
        csv_path, source_columns = parse_inline_data(data)

        try:
            # Verify temp file was created
            assert os.path.exists(csv_path)
            assert csv_path.endswith(".csv")

            # Verify source columns were inferred
            assert len(source_columns) == 2
            col_names = [c["name"] for c in source_columns]
            assert "company" in col_names
            assert "industry" in col_names

            # Verify CSV content
            import csv as csv_module

            with open(csv_path) as f:
                reader = csv_module.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                assert rows[0]["company"] == "Google"
                assert rows[1]["company"] == "Apple"
        finally:
            os.unlink(csv_path)

    def test_parse_single_item(self):
        """Should work with a single item array."""
        data = '[{"name": "Test"}]'
        csv_path, source_columns = parse_inline_data(data)

        try:
            assert os.path.exists(csv_path)
            assert len(source_columns) == 1
            assert source_columns[0]["name"] == "name"
        finally:
            os.unlink(csv_path)

    def test_parse_invalid_json(self):
        """Should raise BadParameter for invalid JSON."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="Invalid JSON"):
            parse_inline_data("not valid json")

    def test_parse_not_array(self):
        """Should raise BadParameter for non-array JSON."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="must be a JSON array"):
            parse_inline_data('{"name": "test"}')

    def test_parse_empty_array(self):
        """Should raise BadParameter for empty array."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="cannot be empty"):
            parse_inline_data("[]")

    def test_parse_not_objects(self):
        """Should raise BadParameter for array of non-objects."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="array of objects"):
            parse_inline_data('["a", "b", "c"]')


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
        assert "0.0.9" in result.output


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

    def test_search_help_shows_comma_separated(self, runner):
        """Should mention comma-separated in domain options help."""
        result = runner.invoke(main, ["search", "--help"])
        assert result.exit_code == 0
        assert "--include-domains" in result.output
        assert "--exclude-domains" in result.output
        assert "comma-separated" in result.output

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


class TestFetchCommand:
    """Tests for the fetch command (alias for extract)."""

    def test_fetch_help(self, runner):
        """Should show fetch help (same as extract)."""
        result = runner.invoke(main, ["fetch", "--help"])
        assert result.exit_code == 0
        assert "Extract content" in result.output
        assert "--json" in result.output

    def test_fetch_in_main_help(self, runner):
        """Should show fetch as a command in main help."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "fetch" in result.output


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

    def test_enrich_run_help_shows_data_option(self, runner):
        """Should show --data option in help."""
        result = runner.invoke(main, ["enrich", "run", "--help"])
        assert result.exit_code == 0
        assert "--data" in result.output
        assert "Inline JSON data" in result.output

    def test_enrich_run_data_and_source_error(self, runner):
        """Should error when both --data and --source provided."""
        result = runner.invoke(
            main,
            [
                "enrich",
                "run",
                "--data",
                '[{"company": "Google"}]',
                "--source",
                "input.csv",
                "--target",
                "output.csv",
                "--intent",
                "Find CEO",
            ],
        )
        assert result.exit_code != 0
        assert "data" in result.output.lower() and "source" in result.output.lower()

    def test_enrich_run_data_with_non_csv_error(self, runner):
        """Should error when --data used with non-csv source type."""
        result = runner.invoke(
            main,
            [
                "enrich",
                "run",
                "--data",
                '[{"company": "Google"}]',
                "--source-type",
                "duckdb",
                "--target",
                "output.csv",
                "--intent",
                "Find CEO",
            ],
        )
        assert result.exit_code != 0
        assert "csv" in result.output.lower()

    def test_enrich_run_data_invalid_json(self, runner):
        """Should error with invalid JSON data."""
        result = runner.invoke(
            main,
            [
                "enrich",
                "run",
                "--data",
                "not valid json",
                "--target",
                "output.csv",
                "--intent",
                "Find CEO",
            ],
        )
        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "json" in result.output.lower()


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


class TestCLIExtrasAndStandaloneMode:
    """Tests for CLI extras detection and standalone mode behavior.

    The standalone CLI (PyInstaller binary) has limited features:
    - No YAML config file support (requires pyyaml)
    - No interactive planner (requires questionary)
    - Only CSV source type (no DuckDB/BigQuery)

    These tests verify the graceful degradation when extras aren't available.
    """

    def test_cli_extras_available_when_installed(self):
        """CLI extras should be available when pyyaml and questionary are installed."""
        from parallel_web_tools.cli import commands

        # In test environment, extras are installed
        assert commands._CLI_EXTRAS_AVAILABLE is True

    def test_enrich_plan_registered_when_extras_available(self, runner):
        """enrich plan command should be available when CLI extras are installed."""
        result = runner.invoke(main, ["enrich", "--help"])
        assert result.exit_code == 0
        assert "plan" in result.output

    def test_enrich_run_yaml_config_works_when_extras_available(self, runner, tmp_path):
        """YAML config should work when CLI extras are installed."""
        import yaml

        config_file = tmp_path / "config.yaml"
        config = {
            "source_type": "csv",
            "source": "input.csv",
            "target": "output.csv",
            "source_columns": [{"name": "company", "description": "Company name"}],
            "enriched_columns": [{"name": "ceo", "description": "CEO name"}],
        }
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Should not show "YAML config files require the CLI extras" error
        result = runner.invoke(main, ["enrich", "run", str(config_file)])
        assert "YAML config files require the CLI extras" not in result.output

    def test_enrich_run_yaml_error_when_extras_missing(self, runner, tmp_path):
        """Should show helpful error when trying YAML config without extras."""
        from parallel_web_tools.cli import commands

        config_file = tmp_path / "config.yaml"
        config_file.write_text("source_type: csv")

        # Patch the flag to simulate missing extras
        with mock.patch.object(commands, "_CLI_EXTRAS_AVAILABLE", False):
            result = runner.invoke(main, ["enrich", "run", str(config_file)])
            assert "YAML config files require the CLI extras" in result.output
            assert "pip install parallel-web-tools" in result.output

    def test_source_types_include_duckdb_bigquery_when_not_standalone(self, runner):
        """Non-standalone CLI should support duckdb and bigquery source types."""
        from parallel_web_tools.cli import commands

        # When not in standalone mode, all source types are available
        assert commands._STANDALONE_MODE is False
        assert "csv" in commands.AVAILABLE_SOURCE_TYPES
        assert "duckdb" in commands.AVAILABLE_SOURCE_TYPES
        assert "bigquery" in commands.AVAILABLE_SOURCE_TYPES


class TestUpdateCommand:
    """Tests for the update command."""

    def test_update_shows_pip_message_when_not_standalone(self, runner):
        """Update command should show pip instructions when not in standalone mode."""
        result = runner.invoke(main, ["update"])
        assert result.exit_code == 0
        assert "only available for standalone CLI" in result.output
        assert "pip install --upgrade" in result.output

    def test_update_check_shows_pip_message_when_not_standalone(self, runner):
        """Update --check should also show pip instructions when not in standalone mode."""
        result = runner.invoke(main, ["update", "--check"])
        assert result.exit_code == 0
        assert "only available for standalone CLI" in result.output

    def test_update_command_exists_in_help(self, runner):
        """Update command should appear in CLI help."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "update" in result.output


class TestConfigCommand:
    """Tests for the config command."""

    def test_config_shows_standalone_message_when_not_standalone(self, runner):
        """Config command should show standalone-only message when not in standalone mode."""
        result = runner.invoke(main, ["config"])
        assert result.exit_code == 0
        assert "only available for standalone CLI" in result.output

    def test_config_command_exists_in_help(self, runner):
        """Config command should appear in CLI help."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "config" in result.output
