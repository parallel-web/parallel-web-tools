"""Tests for the CLI commands."""

import json
import os
import sys
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import (
    EXIT_API_ERROR,
    EXIT_AUTH_ERROR,
    EXIT_BAD_INPUT,
    EXIT_TIMEOUT,
    _content_to_markdown,
    _handle_error,
    build_config_from_args,
    main,
    parse_columns,
    parse_comma_separated,
    parse_inline_data,
    suggest_from_intent,
    validate_enrich_args,
    write_json_output,
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
        assert "0.0.14" in result.output


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


class TestSearchCommandHelp:
    """Tests for the search command help and validation."""

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


class TestExtractCommandHelp:
    """Tests for the extract command help."""

    def test_extract_help(self, runner):
        """Should show extract help."""
        result = runner.invoke(main, ["extract", "--help"])
        assert result.exit_code == 0
        assert "Extract content" in result.output
        assert "--json" in result.output


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
        assert "json" in commands.AVAILABLE_SOURCE_TYPES
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

    def test_config_show_all_in_standalone(self, runner):
        """Config command should show all settings in standalone mode."""
        from parallel_web_tools.cli import commands

        with mock.patch.object(commands, "_STANDALONE_MODE", True):
            with mock.patch("parallel_web_tools.cli.updater.is_auto_update_check_enabled", return_value=True):
                result = runner.invoke(main, ["config"])
                assert result.exit_code == 0
                assert "auto-update-check" in result.output
                assert "on" in result.output

    def test_config_get_specific_key_in_standalone(self, runner):
        """Config command should show a specific key value in standalone mode."""
        from parallel_web_tools.cli import commands

        with mock.patch.object(commands, "_STANDALONE_MODE", True):
            with mock.patch("parallel_web_tools.cli.updater.is_auto_update_check_enabled", return_value=False):
                result = runner.invoke(main, ["config", "auto-update-check"])
                assert result.exit_code == 0
                assert "off" in result.output

    def test_config_set_key_in_standalone(self, runner):
        """Config command should set a key value in standalone mode."""
        from parallel_web_tools.cli import commands

        with mock.patch.object(commands, "_STANDALONE_MODE", True):
            with mock.patch("parallel_web_tools.cli.updater.set_auto_update_check") as mock_set:
                with mock.patch("parallel_web_tools.cli.updater.is_auto_update_check_enabled", return_value=True):
                    result = runner.invoke(main, ["config", "auto-update-check", "on"])
                    assert result.exit_code == 0
                    assert "Set" in result.output
                    mock_set.assert_called_once_with(True)

    def test_config_invalid_key_in_standalone(self, runner):
        """Config command should reject invalid keys in standalone mode."""
        from parallel_web_tools.cli import commands

        with mock.patch.object(commands, "_STANDALONE_MODE", True):
            result = runner.invoke(main, ["config", "invalid-key"])
            assert result.exit_code != 0
            assert "Unknown config key" in result.output


class TestHandleError:
    """Tests for the _handle_error helper function."""

    def test_handle_error_console_output(self):
        """Should print rich error and exit with given code."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_error(ValueError("something broke"), exit_code=EXIT_BAD_INPUT, prefix="Validation")

        assert exc_info.value.code == EXIT_BAD_INPUT

    def test_handle_error_json_output(self, capsys):
        """Should output JSON error and exit."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_error(RuntimeError("api down"), output_json=True, exit_code=EXIT_API_ERROR)

        assert exc_info.value.code == EXIT_API_ERROR
        output = json.loads(capsys.readouterr().out)
        assert output["error"]["message"] == "api down"
        assert output["error"]["type"] == "RuntimeError"

    def test_handle_error_default_exit_code(self):
        """Should default to EXIT_API_ERROR."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_error(Exception("test"))

        assert exc_info.value.code == EXIT_API_ERROR


class TestWriteJsonOutput:
    """Tests for write_json_output helper function."""

    def test_write_to_file(self, tmp_path):
        """Should write JSON to file."""
        output_file = tmp_path / "output.json"
        data = {"key": "value", "count": 42}

        write_json_output(data, str(output_file), output_json=False)

        assert output_file.exists()
        loaded = json.loads(output_file.read_text())
        assert loaded == data

    def test_write_to_stdout(self, capsys):
        """Should print JSON to stdout when output_json is True."""
        data = {"results": [1, 2, 3]}

        write_json_output(data, None, output_json=True)

        output = json.loads(capsys.readouterr().out)
        assert output == data

    def test_write_to_both(self, tmp_path, capsys):
        """Should write to file AND stdout when both specified."""
        output_file = tmp_path / "output.json"
        data = {"result": "ok"}

        write_json_output(data, str(output_file), output_json=True)

        # File should be written
        assert output_file.exists()
        file_data = json.loads(output_file.read_text())
        assert file_data == data

        # stdout should contain the JSON data
        captured = capsys.readouterr().out
        assert '"result"' in captured
        assert '"ok"' in captured

    def test_write_neither(self, tmp_path, capsys):
        """Should do nothing when no output_file and output_json is False."""
        data = {"result": "ok"}

        write_json_output(data, None, output_json=False)

        assert capsys.readouterr().out == ""


class TestContentToMarkdown:
    """Tests for _content_to_markdown function."""

    def test_none_returns_empty(self):
        """Should return empty string for None."""
        assert _content_to_markdown(None) == ""

    def test_string_returned_as_is(self):
        """Should return strings unchanged."""
        assert _content_to_markdown("hello world") == "hello world"

    def test_dict_with_text_key(self):
        """Should extract text from {text: '...'} structure."""
        assert _content_to_markdown({"text": "the content"}) == "the content"

    def test_dict_with_multiple_keys(self):
        """Should convert dict keys to markdown headings."""
        result = _content_to_markdown({"summary": "A summary.", "conclusion": "Done."})
        assert "# Summary" in result
        assert "A summary." in result
        assert "# Conclusion" in result
        assert "Done." in result

    def test_dict_with_list_values(self):
        """Should convert list values to bullet points."""
        result = _content_to_markdown({"findings": ["item 1", "item 2"]})
        assert "# Findings" in result
        assert "- item 1" in result
        assert "- item 2" in result

    def test_dict_with_nested_dict(self):
        """Should recursively convert nested dicts."""
        result = _content_to_markdown({"section": {"sub_topic": "content here"}})
        assert "# Section" in result
        assert "## Sub Topic" in result
        assert "content here" in result

    def test_list_of_strings(self):
        """Should convert list of strings to bullet points."""
        result = _content_to_markdown(["a", "b", "c"])
        assert "- a" in result
        assert "- b" in result
        assert "- c" in result

    def test_list_of_dicts(self):
        """Should recursively process list of dicts."""
        result = _content_to_markdown([{"name": "Alice"}, {"name": "Bob"}])
        assert "Name" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_non_string_non_dict_non_list(self):
        """Should convert other types to string."""
        assert _content_to_markdown(42) == "42"
        assert _content_to_markdown(True) == "True"

    def test_heading_level_capped_at_6(self):
        """Should not exceed 6 levels of headings."""
        # Deeply nested
        result = _content_to_markdown({"a": {"b": {"c": {"d": {"e": {"f": {"g": "deep"}}}}}}})
        # The deepest heading should still be ######
        assert "#######" not in result

    def test_key_underscores_converted_to_spaces_and_titled(self):
        """Should convert underscored keys to title case."""
        result = _content_to_markdown({"key_findings_summary": "text"})
        assert "# Key Findings Summary" in result

    def test_dict_with_non_string_value(self):
        """Should convert non-string/non-dict/non-list values to strings."""
        result = _content_to_markdown({"count": 42})
        assert "42" in result

    def test_dict_list_with_nested_dicts(self):
        """Should handle list of dicts inside a dict."""
        content = {
            "sources": [
                {"url": "https://example.com", "title": "Example"},
            ]
        }
        result = _content_to_markdown(content)
        assert "# Sources" in result
        assert "example.com" in result


class TestValidateEnrichArgs:
    """Tests for validate_enrich_args function."""

    def test_valid_args_with_enriched_columns(self):
        """Should not raise with all required args and enriched_columns."""
        validate_enrich_args("csv", "input.csv", "output.csv", "[]", "[]", None)

    def test_valid_args_with_intent(self):
        """Should not raise with all required args and intent."""
        validate_enrich_args("csv", "input.csv", "output.csv", "[]", None, "Find CEO")

    def test_both_enriched_and_intent_raises(self):
        """Should raise when both enriched_columns and intent are provided."""
        import click

        with pytest.raises(click.UsageError, match="not both"):
            validate_enrich_args("csv", "input.csv", "output.csv", "[]", "[]", "intent")

    def test_missing_source_type(self):
        """Should raise when source_type is missing."""
        import click

        with pytest.raises(click.UsageError, match="--source-type"):
            validate_enrich_args(None, "input.csv", "output.csv", "[]", "[]", None)

    def test_no_output_spec_raises(self):
        """Should raise when neither enriched_columns nor intent provided."""
        import click

        with pytest.raises(click.UsageError, match="--enriched-columns OR --intent"):
            validate_enrich_args("csv", "input.csv", "output.csv", "[]", None, None)

    def test_all_none_does_not_raise(self):
        """Should not raise when all args are None (no partial args)."""
        validate_enrich_args(None, None, None, None, None, None)


class TestSearchCommandMocked:
    """Tests for the search command with mocked Parallel SDK."""

    def test_search_successful_json_output(self, runner):
        """Should output JSON for successful search."""
        mock_search_result = mock.MagicMock()
        mock_search_result.search_id = "search_123"
        mock_search_result.results = [
            mock.MagicMock(
                url="https://example.com",
                title="Example",
                publish_date="2024-01-01",
                excerpts=["An excerpt"],
            )
        ]
        mock_search_result.warnings = []

        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch.dict("sys.modules"):
                mock_parallel_mod = mock.MagicMock()
                mock_client = mock.MagicMock()
                mock_client.beta.search.return_value = mock_search_result
                mock_parallel_mod.Parallel.return_value = mock_client
                sys.modules["parallel"] = mock_parallel_mod

                result = runner.invoke(main, ["search", "test query", "--json"])

                del sys.modules["parallel"]

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["search_id"] == "search_123"
        assert output["status"] == "ok"
        assert len(output["results"]) == 1
        assert output["results"][0]["url"] == "https://example.com"

    def test_search_api_error_json_mode(self, runner):
        """Should output JSON error when API fails in --json mode."""
        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch.dict("sys.modules"):
                mock_parallel_mod = mock.MagicMock()
                mock_client = mock.MagicMock()
                mock_client.beta.search.side_effect = RuntimeError("API unavailable")
                mock_parallel_mod.Parallel.return_value = mock_client
                sys.modules["parallel"] = mock_parallel_mod

                result = runner.invoke(main, ["search", "test query", "--json"])

                del sys.modules["parallel"]

        assert result.exit_code == EXIT_API_ERROR
        output = json.loads(result.output)
        assert output["error"]["message"] == "API unavailable"
        assert output["error"]["type"] == "RuntimeError"

    def test_search_api_error_console_mode(self, runner):
        """Should output formatted error when API fails in console mode."""
        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch.dict("sys.modules"):
                mock_parallel_mod = mock.MagicMock()
                mock_client = mock.MagicMock()
                mock_client.beta.search.side_effect = RuntimeError("API unavailable")
                mock_parallel_mod.Parallel.return_value = mock_client
                sys.modules["parallel"] = mock_parallel_mod

                result = runner.invoke(main, ["search", "test query"])

                del sys.modules["parallel"]

        assert result.exit_code == EXIT_API_ERROR
        assert "API unavailable" in result.output


class TestExtractCommandMocked:
    """Tests for the extract command with mocked Parallel SDK."""

    def test_extract_api_error_json_mode(self, runner):
        """Should output JSON error when extract API fails in --json mode."""
        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch.dict("sys.modules"):
                mock_parallel_mod = mock.MagicMock()
                mock_client = mock.MagicMock()
                mock_client.beta.extract.side_effect = ConnectionError("Network error")
                mock_parallel_mod.Parallel.return_value = mock_client
                sys.modules["parallel"] = mock_parallel_mod

                result = runner.invoke(main, ["extract", "https://example.com", "--json"])

                del sys.modules["parallel"]

        assert result.exit_code == EXIT_API_ERROR
        output = json.loads(result.output)
        assert output["error"]["type"] == "ConnectionError"
        assert "Network error" in output["error"]["message"]

    def test_extract_successful_json_output(self, runner):
        """Should output structured JSON for successful extraction."""
        mock_extract_result = mock.MagicMock()
        mock_extract_result.extract_id = "ext_123"
        mock_page = mock.MagicMock()
        mock_page.url = "https://example.com"
        mock_page.title = "Example Page"
        mock_page.excerpts = ["Some excerpt"]
        mock_page.full_content = None
        mock_extract_result.results = [mock_page]
        mock_extract_result.errors = []

        with mock.patch("parallel_web_tools.cli.commands.get_api_key", return_value="test-key"):
            with mock.patch.dict("sys.modules"):
                mock_parallel_mod = mock.MagicMock()
                mock_client = mock.MagicMock()
                mock_client.beta.extract.return_value = mock_extract_result
                mock_parallel_mod.Parallel.return_value = mock_client
                sys.modules["parallel"] = mock_parallel_mod

                result = runner.invoke(main, ["extract", "https://example.com", "--json"])

                del sys.modules["parallel"]

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["extract_id"] == "ext_123"
        assert output["status"] == "ok"
        assert len(output["results"]) == 1
        assert output["results"][0]["url"] == "https://example.com"


class TestEnrichDeploySnowflake:
    """Tests for the enrich deploy command Snowflake path."""

    def test_deploy_snowflake_missing_account(self, runner):
        """Should error without --account for Snowflake."""
        result = runner.invoke(main, ["enrich", "deploy", "--system", "snowflake", "--user", "testuser"])
        assert result.exit_code != 0
        assert "account" in result.output.lower()

    def test_deploy_snowflake_missing_user(self, runner):
        """Should error without --user for Snowflake."""
        result = runner.invoke(
            main,
            ["enrich", "deploy", "--system", "snowflake", "--account", "abc123.us-east-1"],
        )
        assert result.exit_code != 0
        assert "user" in result.output.lower()


class TestOutputResearchResultJsonPath:
    """Tests for _output_research_result JSON output path."""

    def test_json_output_to_stdout(self, runner):
        """Should output JSON to stdout via research run --json."""
        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_json",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_json",
                "status": "completed",
                "output": {"content": {"text": "findings"}, "basis": []},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Q?", "--poll-interval", "1", "--json"],
            )

        assert result.exit_code == 0
        # Extract JSON from mixed output (console + JSON)
        lines = result.output.strip().split("\n")
        json_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith("{"):
                json_start = i
                break

        assert json_start is not None
        json_text = "\n".join(lines[json_start:])
        output = json.loads(json_text)
        assert output["run_id"] == "trun_json"
        assert output["status"] == "completed"

    def test_json_output_with_file_replaces_content(self, runner, tmp_path):
        """JSON output should reference content_file when output file is set."""
        output_base = tmp_path / "report"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_both",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_both",
                "status": "completed",
                "output": {"content": "text content", "basis": []},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Q?", "-o", str(output_base), "--poll-interval", "1", "--json"],
            )

        assert result.exit_code == 0

        # JSON file should have content_file reference instead of content
        json_file = tmp_path / "report.json"
        data = json.loads(json_file.read_text())
        assert "content" not in data["output"]
        assert data["output"]["content_file"] == "report.md"


class TestExitCodes:
    """Tests for distinct CLI exit codes."""

    def test_exit_code_values(self):
        """Exit codes should have distinct expected values."""
        assert EXIT_BAD_INPUT == 2
        assert EXIT_AUTH_ERROR == 3
        assert EXIT_API_ERROR == 4
        assert EXIT_TIMEOUT == 5

    def test_research_timeout_exit_code(self, runner):
        """Research run should exit with EXIT_TIMEOUT on timeout."""
        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.side_effect = TimeoutError("timed out after 10s")

            result = runner.invoke(
                main,
                ["research", "run", "Q?", "--poll-interval", "1", "--timeout", "10"],
            )

        assert result.exit_code == EXIT_TIMEOUT

    def test_research_timeout_json_output(self, runner):
        """Research timeout should output JSON error in --json mode."""
        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.side_effect = TimeoutError("timed out")

            result = runner.invoke(
                main,
                ["research", "run", "Q?", "--poll-interval", "1", "--json"],
            )

        assert result.exit_code == EXIT_TIMEOUT
        # Parse JSON from output
        lines = result.output.strip().split("\n")
        json_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith("{"):
                json_start = i
                break
        assert json_start is not None
        json_text = "\n".join(lines[json_start:])
        output = json.loads(json_text)
        assert output["error"]["type"] == "TimeoutError"

    def test_login_failure_exit_code(self, runner):
        """Login failure should exit with EXIT_AUTH_ERROR."""
        with mock.patch("parallel_web_tools.cli.commands.get_api_key") as mock_key:
            mock_key.side_effect = Exception("auth failed")

            result = runner.invoke(main, ["login"])

        assert result.exit_code == EXIT_AUTH_ERROR


class TestEnrichNoWait:
    """Tests for enrich run --no-wait."""

    def test_enrich_run_no_wait_prints_taskgroup_id(self, runner):
        """Should print taskgroup_id and hints when --no-wait is used."""
        with mock.patch("parallel_web_tools.cli.commands.run_enrichment_from_dict") as mock_run:
            mock_run.return_value = {
                "taskgroup_id": "tgrp_nowait_123",
                "url": "https://platform.parallel.ai/view/task-run-group/tgrp_nowait_123",
                "num_runs": 5,
            }

            result = runner.invoke(
                main,
                [
                    "enrich",
                    "run",
                    "--no-wait",
                    "--source-type",
                    "csv",
                    "--source",
                    "input.csv",
                    "--target",
                    "output.csv",
                    "--source-columns",
                    '[{"name": "company", "description": "Company name"}]',
                    "--enriched-columns",
                    '[{"name": "ceo", "description": "CEO name"}]',
                ],
            )

        assert result.exit_code == 0
        assert "tgrp_nowait_123" in result.output
        assert "enrich status" in result.output
        assert "enrich poll" in result.output
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs["no_wait"] is True

    def test_enrich_run_help_shows_no_wait(self, runner):
        """Should show --no-wait in help."""
        result = runner.invoke(main, ["enrich", "run", "--help"])
        assert result.exit_code == 0
        assert "--no-wait" in result.output


class TestEnrichStatusCommand:
    """Tests for the enrich status command."""

    def test_enrich_status_help(self, runner):
        """Should show enrich status help."""
        result = runner.invoke(main, ["enrich", "status", "--help"])
        assert result.exit_code == 0
        assert "TASKGROUP_ID" in result.output
        assert "--json" in result.output

    def test_enrich_status_shows_formatted_output(self, runner):
        """Should show formatted status info."""
        with mock.patch("parallel_web_tools.cli.commands.get_task_group_status") as mock_status:
            mock_status.return_value = {
                "taskgroup_id": "tgrp_status_123",
                "status_counts": {"completed": 3, "failed": 1},
                "is_active": False,
                "num_runs": 4,
                "url": "https://platform.parallel.ai/view/task-run-group/tgrp_status_123",
            }

            result = runner.invoke(main, ["enrich", "status", "tgrp_status_123"])

        assert result.exit_code == 0
        assert "tgrp_status_123" in result.output
        assert "3 completed" in result.output
        assert "1 failed" in result.output
        assert "4 total" in result.output

    def test_enrich_status_json_output(self, runner):
        """Should output JSON when --json flag is set."""
        with mock.patch("parallel_web_tools.cli.commands.get_task_group_status") as mock_status:
            mock_status.return_value = {
                "taskgroup_id": "tgrp_json",
                "status_counts": {"completed": 2},
                "is_active": False,
                "num_runs": 2,
                "url": "https://platform.parallel.ai/view/task-run-group/tgrp_json",
            }

            result = runner.invoke(main, ["enrich", "status", "tgrp_json", "--json"])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["taskgroup_id"] == "tgrp_json"
        assert output["num_runs"] == 2

    def test_enrich_status_running_group(self, runner):
        """Should show running status for active groups."""
        with mock.patch("parallel_web_tools.cli.commands.get_task_group_status") as mock_status:
            mock_status.return_value = {
                "taskgroup_id": "tgrp_running",
                "status_counts": {"completed": 1, "running": 4},
                "is_active": True,
                "num_runs": 5,
                "url": "https://platform.parallel.ai/view/task-run-group/tgrp_running",
            }

            result = runner.invoke(main, ["enrich", "status", "tgrp_running"])

        assert result.exit_code == 0
        assert "running" in result.output


class TestEnrichPollCommand:
    """Tests for the enrich poll command."""

    def test_enrich_poll_help(self, runner):
        """Should show enrich poll help."""
        result = runner.invoke(main, ["enrich", "poll", "--help"])
        assert result.exit_code == 0
        assert "TASKGROUP_ID" in result.output
        assert "--timeout" in result.output
        assert "--poll-interval" in result.output
        assert "--json" in result.output
        assert "--output" in result.output

    def test_enrich_poll_waits_and_outputs_summary(self, runner):
        """Should wait for completion and show summary."""
        with mock.patch("parallel_web_tools.cli.commands.poll_task_group") as mock_poll:
            mock_poll.return_value = [
                {"input": {"company": "A"}, "output": {"ceo": "CEO A"}},
                {"input": {"company": "B"}, "output": {"ceo": "CEO B"}},
            ]

            result = runner.invoke(main, ["enrich", "poll", "tgrp_poll_123"])

        assert result.exit_code == 0
        assert "complete" in result.output.lower()
        assert "2 completed" in result.output

    def test_enrich_poll_json_output(self, runner):
        """Should output full results as JSON with --json flag."""
        with mock.patch("parallel_web_tools.cli.commands.poll_task_group") as mock_poll:
            mock_poll.return_value = [
                {"input": {"company": "A"}, "output": {"ceo": "CEO A"}},
            ]

            result = runner.invoke(main, ["enrich", "poll", "tgrp_json", "--json"])

        assert result.exit_code == 0
        # Extract JSON from mixed output
        lines = result.output.strip().split("\n")
        json_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith("["):
                json_start = i
                break
        assert json_start is not None
        json_text = "\n".join(lines[json_start:])
        output = json.loads(json_text)
        assert len(output) == 1
        assert output[0]["output"]["ceo"] == "CEO A"

    def test_enrich_poll_saves_to_file(self, runner, tmp_path):
        """Should save results to file with --output."""
        output_file = tmp_path / "results.json"

        with mock.patch("parallel_web_tools.cli.commands.poll_task_group") as mock_poll:
            mock_poll.return_value = [
                {"input": {"company": "A"}, "output": {"ceo": "CEO A"}},
            ]

            result = runner.invoke(main, ["enrich", "poll", "tgrp_file", "--output", str(output_file)])

        assert result.exit_code == 0
        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert len(data) == 1
        assert data[0]["output"]["ceo"] == "CEO A"

    def test_enrich_poll_timeout(self, runner):
        """Should exit with timeout code on TimeoutError."""
        with mock.patch("parallel_web_tools.cli.commands.poll_task_group") as mock_poll:
            mock_poll.side_effect = TimeoutError("timed out after 10s")

            result = runner.invoke(main, ["enrich", "poll", "tgrp_timeout", "--timeout", "10"])

        assert result.exit_code == EXIT_TIMEOUT
        assert "Timeout" in result.output or "timed out" in result.output

    def test_enrich_poll_timeout_json_output(self, runner):
        """Should output JSON error on timeout with --json."""
        with mock.patch("parallel_web_tools.cli.commands.poll_task_group") as mock_poll:
            mock_poll.side_effect = TimeoutError("timed out")

            result = runner.invoke(main, ["enrich", "poll", "tgrp_timeout", "--json"])

        assert result.exit_code == EXIT_TIMEOUT
        lines = result.output.strip().split("\n")
        json_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith("{"):
                json_start = i
                break
        assert json_start is not None
        json_text = "\n".join(lines[json_start:])
        output = json.loads(json_text)
        assert output["error"]["type"] == "TimeoutError"


class TestEnrichRunJsonSourceType:
    """Tests for enrich run with --source-type json."""

    def test_enrich_run_accepts_json_source_type(self, runner):
        """CLI should accept --source-type json."""
        result = runner.invoke(main, ["enrich", "run", "--help"])
        assert result.exit_code == 0
        assert "json" in result.output

    def test_enrich_run_json_source_type_valid(self, runner):
        """Should accept json as a valid source type option."""
        with mock.patch("parallel_web_tools.cli.commands.run_enrichment_from_dict") as mock_run:
            mock_run.return_value = None

            result = runner.invoke(
                main,
                [
                    "enrich",
                    "run",
                    "--source-type",
                    "json",
                    "--source",
                    "input.json",
                    "--target",
                    "output.json",
                    "--source-columns",
                    '[{"name": "company", "description": "Company name"}]',
                    "--enriched-columns",
                    '[{"name": "ceo", "description": "CEO name"}]',
                ],
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        config = mock_run.call_args[0][0]
        assert config["source_type"] == "json"

    def test_enrich_plan_accepts_json_source_type(self, runner, tmp_path):
        """enrich plan should accept json as a valid source type."""
        output_file = tmp_path / "config.yaml"

        result = runner.invoke(
            main,
            [
                "enrich",
                "plan",
                "-o",
                str(output_file),
                "--source-type",
                "json",
                "--source",
                "input.json",
                "--target",
                "output.json",
                "--source-columns",
                '[{"name": "company", "description": "Company name"}]',
                "--enriched-columns",
                '[{"name": "ceo", "description": "CEO name"}]',
            ],
        )

        assert result.exit_code == 0
        assert output_file.exists()

        import yaml

        with open(output_file) as f:
            config = yaml.safe_load(f)

        assert config["source_type"] == "json"
