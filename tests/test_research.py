"""Tests for the deep research functionality."""

import json
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import _extract_executive_summary, main
from parallel_web_tools.core.research import (
    RESEARCH_PROCESSORS,
    _build_task_spec,
    _serialize_output,
    create_research_task,
    get_research_result,
    get_research_status,
    poll_research,
    run_research,
)


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_parallel_client():
    """Create a mock Parallel client."""
    mock_client = mock.MagicMock()
    with mock.patch("parallel_web_tools.core.research.create_client", return_value=mock_client):
        yield mock_client


# =============================================================================
# Core Research Function Tests
# =============================================================================


class TestCreateResearchTask:
    """Tests for create_research_task function."""

    def test_create_task_basic(self, mock_parallel_client):
        """Should create a task and return metadata."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_task.status = "pending"
        mock_parallel_client.task_run.create.return_value = mock_task

        result = create_research_task("What is AI?", processor="pro-fast")

        assert result["run_id"] == "trun_123"
        assert result["processor"] == "pro-fast"
        assert "result_url" in result
        mock_parallel_client.task_run.create.assert_called_once()

    def test_create_task_truncates_query(self, mock_parallel_client):
        """Should truncate query to 15000 chars."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        long_query = "x" * 20000
        create_research_task(long_query)

        call_args = mock_parallel_client.task_run.create.call_args
        assert len(call_args.kwargs["input"]) == 15000

    def test_create_task_auto_schema_no_task_spec(self, mock_parallel_client):
        """Should not pass task_spec for auto schema (default)."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        create_research_task("What is AI?", output_schema="auto")

        call_args = mock_parallel_client.task_run.create.call_args
        assert "task_spec" not in call_args.kwargs

    def test_create_task_text_schema(self, mock_parallel_client):
        """Should pass task_spec with text schema when output_schema='text'."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        create_research_task("What is AI?", output_schema="text")

        call_args = mock_parallel_client.task_run.create.call_args
        assert "task_spec" in call_args.kwargs
        task_spec = call_args.kwargs["task_spec"]
        assert task_spec["output_schema"]["type"] == "text"


class TestBuildTaskSpec:
    """Tests for _build_task_spec helper."""

    def test_auto_returns_none(self):
        """Should return None for auto schema."""
        assert _build_task_spec("auto") is None

    def test_text_returns_task_spec(self):
        """Should return TaskSpecParam with TextSchemaParam for text schema."""
        result = _build_task_spec("text")
        assert result is not None
        assert result["output_schema"]["type"] == "text"


class TestGetResearchStatus:
    """Tests for get_research_status function."""

    def test_get_status(self, mock_parallel_client):
        """Should retrieve task status."""
        mock_status = mock.MagicMock()
        mock_status.status = "running"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        result = get_research_status("trun_123")

        assert result["run_id"] == "trun_123"
        assert result["status"] == "running"
        mock_parallel_client.task_run.retrieve.assert_called_once_with(run_id="trun_123")


class TestGetResearchResult:
    """Tests for get_research_result function."""

    def test_get_result_basic(self, mock_parallel_client):
        """Should retrieve completed task result."""
        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Research findings"}, "basis": []}

        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        result = get_research_result("trun_123")

        assert result["run_id"] == "trun_123"
        assert result["status"] == "completed"
        assert "output" in result
        assert result["output"]["content"]["text"] == "Research findings"


class TestRunResearch:
    """Tests for run_research function."""

    def test_run_research_success(self, mock_parallel_client):
        """Should create task and poll until completion."""
        # Mock task creation
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        # Mock status polling - first running, then completed
        mock_status_running = mock.MagicMock()
        mock_status_running.status = "running"

        mock_status_completed = mock.MagicMock()
        mock_status_completed.status = "completed"

        mock_parallel_client.task_run.retrieve.side_effect = [
            mock_status_running,
            mock_status_completed,
        ]

        # Mock result retrieval
        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Research complete"}}

        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = run_research("What is AI?", poll_interval=1, timeout=10)

        assert result["status"] == "completed"
        assert "output" in result

    def test_run_research_timeout(self, mock_parallel_client):
        """Should raise TimeoutError when task doesn't complete."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "running"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with mock.patch("parallel_web_tools.core.polling.time.time") as mock_time:
                # Simulate timeout by returning increasing time values
                mock_time.side_effect = [0, 0, 5, 10, 15]

                with pytest.raises(TimeoutError):
                    run_research("What is AI?", timeout=10, poll_interval=1)

    def test_run_research_failed(self, mock_parallel_client):
        """Should raise RuntimeError when task fails."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "failed"
        mock_status.error = "Processing error"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="failed"):
                run_research("What is AI?", poll_interval=1)

    def test_run_research_text_schema(self, mock_parallel_client):
        """Should pass task_spec with text schema to SDK."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_text"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Markdown report"}}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = run_research("What is AI?", poll_interval=1, timeout=10, output_schema="text")

        assert result["status"] == "completed"
        call_args = mock_parallel_client.task_run.create.call_args
        assert "task_spec" in call_args.kwargs
        assert call_args.kwargs["task_spec"]["output_schema"]["type"] == "text"

    def test_run_research_auto_schema_no_task_spec(self, mock_parallel_client):
        """Should not pass task_spec for auto schema."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_auto"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "JSON result"}}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            run_research("What is AI?", poll_interval=1, timeout=10, output_schema="auto")

        call_args = mock_parallel_client.task_run.create.call_args
        assert "task_spec" not in call_args.kwargs

    def test_run_research_on_status_callback(self, mock_parallel_client):
        """Should call on_status callback during polling."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Done"}}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        statuses = []

        def on_status(status, run_id):
            statuses.append((status, run_id))

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            run_research("What is AI?", on_status=on_status, poll_interval=1)

        assert ("created", "trun_123") in statuses
        assert ("completed", "trun_123") in statuses


class TestPollResearch:
    """Tests for poll_research function."""

    def test_poll_existing_task(self, mock_parallel_client):
        """Should poll existing task until completion."""
        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Results"}}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = poll_research("trun_123", poll_interval=1)

        assert result["status"] == "completed"
        assert result["run_id"] == "trun_123"
        assert "output" in result


class TestResearchProcessors:
    """Tests for RESEARCH_PROCESSORS constant."""

    def test_processors_defined(self):
        """Should have expected processors."""
        # Fast variants
        assert "lite-fast" in RESEARCH_PROCESSORS
        assert "pro-fast" in RESEARCH_PROCESSORS
        assert "ultra8x-fast" in RESEARCH_PROCESSORS
        # Standard variants
        assert "lite" in RESEARCH_PROCESSORS
        assert "ultra" in RESEARCH_PROCESSORS
        assert "ultra8x" in RESEARCH_PROCESSORS

    def test_processors_have_descriptions(self):
        """All processors should have descriptions."""
        for _proc, desc in RESEARCH_PROCESSORS.items():
            assert isinstance(desc, str)
            assert len(desc) > 0


# =============================================================================
# CLI Research Command Tests
# =============================================================================


class TestResearchGroup:
    """Tests for the research command group."""

    def test_research_help(self, runner):
        """Should show research subcommands."""
        result = runner.invoke(main, ["research", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "status" in result.output
        assert "poll" in result.output
        assert "processors" in result.output

    def test_research_in_main_help(self, runner):
        """Research should appear in main CLI help."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "research" in result.output


class TestResearchRunCommand:
    """Tests for the research run command."""

    def test_research_run_help(self, runner):
        """Should show research run help."""
        result = runner.invoke(main, ["research", "run", "--help"])
        assert result.exit_code == 0
        assert "--processor" in result.output
        assert "--timeout" in result.output
        assert "--no-wait" in result.output
        assert "--text" in result.output
        assert "--output" in result.output

    def test_research_run_no_query(self, runner):
        """Should error without query or input file."""
        result = runner.invoke(main, ["research", "run"])
        assert result.exit_code != 0
        assert "query" in result.output.lower() or "input" in result.output.lower()

    def test_research_run_with_input_file(self, runner, tmp_path):
        """Should read query from file."""
        query_file = tmp_path / "query.txt"
        query_file.write_text("What is quantum computing?")

        with mock.patch("parallel_web_tools.cli.commands.create_research_task") as mock_create:
            mock_create.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "pending",
            }

            result = runner.invoke(main, ["research", "run", "--input-file", str(query_file), "--no-wait"])

            assert result.exit_code == 0
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert "quantum computing" in call_args[0][0]

    def test_research_run_no_wait(self, runner):
        """Should return immediately with --no-wait."""
        with mock.patch("parallel_web_tools.cli.commands.create_research_task") as mock_create:
            mock_create.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "pending",
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--no-wait"])

            assert result.exit_code == 0
            assert "trun_123" in result.output
            mock_create.assert_called_once()

    def test_research_run_with_wait(self, runner, tmp_path, monkeypatch):
        """Should poll and return results without --no-wait."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "completed",
                "output": {"content": {"text": "AI research findings"}},
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--poll-interval", "1", "--timeout", "10"])

            assert result.exit_code == 0
            assert "Research Complete" in result.output
            mock_run.assert_called_once()

    def test_research_run_text_flag(self, runner, tmp_path, monkeypatch):
        """Should pass output_schema='text' when --text is used."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_text",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_text",
                "status": "completed",
                "output": {
                    "content": "# Markdown Report\n\nThis is a markdown report with enough text to be meaningful.\n\n## Section\n\nBody."
                },
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--text", "--poll-interval", "1"])

            assert result.exit_code == 0
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args.kwargs
            assert call_kwargs["output_schema"] == "text"

    def test_research_run_default_auto_schema(self, runner, tmp_path, monkeypatch):
        """Should pass output_schema='auto' by default (no --text)."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_auto",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_auto",
                "status": "completed",
                "output": {"content": {"text": "Structured JSON result"}},
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--poll-interval", "1"])

            assert result.exit_code == 0
            call_kwargs = mock_run.call_args.kwargs
            assert call_kwargs["output_schema"] == "auto"

    def test_research_run_text_no_wait(self, runner):
        """Should pass output_schema when using --text with --no-wait."""
        with mock.patch("parallel_web_tools.cli.commands.create_research_task") as mock_create:
            mock_create.return_value = {
                "run_id": "trun_text_nw",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_text_nw",
                "status": "pending",
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--text", "--no-wait"])

            assert result.exit_code == 0
            mock_create.assert_called_once()
            call_kwargs = mock_create.call_args.kwargs
            assert call_kwargs["output_schema"] == "text"

    def test_research_run_text_in_help(self, runner):
        """Should show --text flag in help."""
        result = runner.invoke(main, ["research", "run", "--help"])
        assert result.exit_code == 0
        assert "--text" in result.output

    def test_research_run_dry_run_shows_schema(self, runner):
        """Should show output_schema in dry run output."""
        result = runner.invoke(main, ["research", "run", "What is AI?", "--dry-run", "--text"])
        assert result.exit_code == 0
        assert "text" in result.output

        result = runner.invoke(main, ["research", "run", "What is AI?", "--dry-run"])
        assert result.exit_code == 0
        assert "auto" in result.output


class TestResearchStatusCommand:
    """Tests for the research status command."""

    def test_research_status_help(self, runner):
        """Should show status help."""
        result = runner.invoke(main, ["research", "status", "--help"])
        assert result.exit_code == 0
        assert "RUN_ID" in result.output

    def test_research_status(self, runner):
        """Should show task status."""
        with mock.patch("parallel_web_tools.cli.commands.get_research_status") as mock_status:
            mock_status.return_value = {
                "run_id": "trun_123",
                "status": "running",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
            }

            result = runner.invoke(main, ["research", "status", "trun_123"])

            assert result.exit_code == 0
            assert "trun_123" in result.output
            assert "running" in result.output.lower()

    def test_research_status_json(self, runner):
        """Should output JSON with --json flag."""
        with mock.patch("parallel_web_tools.cli.commands.get_research_status") as mock_status:
            mock_status.return_value = {
                "run_id": "trun_123",
                "status": "completed",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
            }

            result = runner.invoke(main, ["research", "status", "trun_123", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["status"] == "completed"


class TestResearchPollCommand:
    """Tests for the research poll command."""

    def test_research_poll_help(self, runner):
        """Should show poll help."""
        result = runner.invoke(main, ["research", "poll", "--help"])
        assert result.exit_code == 0
        assert "RUN_ID" in result.output
        assert "--timeout" in result.output

    def test_research_poll(self, runner):
        """Should poll and return results."""
        with mock.patch("parallel_web_tools.cli.commands.poll_research") as mock_poll:
            mock_poll.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "completed",
                "output": {"content": {"text": "Research results here"}},
            }

            result = runner.invoke(main, ["research", "poll", "trun_123", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "Research Complete" in result.output


class TestResearchProcessorsCommand:
    """Tests for the research processors command."""

    def test_research_processors(self, runner):
        """Should list all processors."""
        result = runner.invoke(main, ["research", "processors"])
        assert result.exit_code == 0
        assert "pro-fast" in result.output
        assert "ultra" in result.output
        assert "ultra8x" in result.output


class TestResearchOutputFile:
    """Tests for saving research results to files."""

    def test_default_saves_json_only(self, runner, tmp_path):
        """Default (auto schema) should save only .json."""
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "completed",
                "output": {"content": {"market_size": "10B"}, "basis": []},
            }

            result = runner.invoke(
                main,
                ["research", "run", "What is AI?", "-o", str(tmp_path / "report"), "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert json_file.exists()
            assert not md_file.exists()

            data = json.loads(json_file.read_text())
            assert data["run_id"] == "trun_123"
            assert data["output"]["content"]["market_size"] == "10B"

    def test_text_saves_json_and_md(self, runner, tmp_path):
        """--text should save both .json (with content_file ref) and .md."""
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_text",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_text",
                "status": "completed",
                "output": {"content": "# Report\n\nFindings here.", "basis": [{"field": "content"}]},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Question?", "--text", "-o", str(tmp_path / "report"), "--poll-interval", "1"],
            )

            assert result.exit_code == 0

            # Both files exist
            assert json_file.exists()
            assert md_file.exists()

            # .md has the content
            assert md_file.read_text() == "# Report\n\nFindings here."

            # .json references .md and doesn't duplicate content
            data = json.loads(json_file.read_text())
            assert data["output"]["content_file"] == "report.md"
            assert "content" not in data["output"]
            assert data["output"]["basis"] == [{"field": "content"}]

    def test_output_strips_extension_from_path(self, runner, tmp_path):
        """-o with extension should still produce correct files."""
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_ext",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_ext",
                "status": "completed",
                "output": {"content": "Content here"},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Question?", "--text", "-o", str(md_file), "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert json_file.exists()
            assert md_file.exists()

    def test_auto_generate_filename_from_run_id(self, runner, tmp_path, monkeypatch):
        """Should auto-generate filename from run_id when no -o given."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_abc",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_abc",
                "status": "completed",
                "output": {"content": {"text": "Result"}},
            }

            # Default (auto schema, no -o)
            result = runner.invoke(main, ["research", "run", "Question?", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert (tmp_path / "trun_abc.json").exists()
            assert not (tmp_path / "trun_abc.md").exists()

    def test_auto_generate_filename_text(self, runner, tmp_path, monkeypatch):
        """Should auto-generate both files from run_id for --text."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_xyz",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_xyz",
                "status": "completed",
                "output": {"content": "Markdown content here"},
            }

            result = runner.invoke(main, ["research", "run", "Question?", "--text", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert (tmp_path / "trun_xyz.json").exists()
            assert (tmp_path / "trun_xyz.md").exists()


class TestSerializeOutput:
    """Tests for _serialize_output function."""

    def test_none_returns_empty_dict(self):
        """Should return empty dict for None."""
        assert _serialize_output(None) == {}

    def test_dict_returns_as_is(self):
        """Should return dict directly."""
        data = {"key": "value"}
        assert _serialize_output(data) == data

    def test_model_dump(self):
        """Should use model_dump() for Pydantic-like objects."""
        obj = mock.MagicMock()
        obj.model_dump.return_value = {"field": "value"}
        assert _serialize_output(obj) == {"field": "value"}

    def test_to_dict(self):
        """Should use to_dict() when model_dump not available."""
        obj = mock.MagicMock(spec=[])
        obj.to_dict = mock.MagicMock(return_value={"key": "val"})
        assert _serialize_output(obj) == {"key": "val"}

    def test_dunder_dict_fallback(self):
        """Should use __dict__ when no serialization methods available."""

        class Simple:
            def __init__(self):
                self.x = 1
                self.y = 2

        obj = Simple()
        result = _serialize_output(obj)
        assert result["x"] == 1
        assert result["y"] == 2

    def test_raw_fallback(self):
        """Should wrap in {raw: str(obj)} for other types."""
        assert _serialize_output(42) == {"raw": "42"}
        assert _serialize_output("text") == {"raw": "text"}


class TestRunResearchCancelled:
    """Tests for research cancellation handling."""

    def test_run_research_cancelled(self, mock_parallel_client):
        """Should raise RuntimeError when task is cancelled."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_cancel"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "cancelled"
        mock_status.error = None
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="cancelled"):
                run_research("What is AI?", poll_interval=1)


class TestPollResearchStatuses:
    """Tests for poll_research with various statuses."""

    def test_poll_calls_on_status(self, mock_parallel_client):
        """Should call on_status callback with 'polling' first."""
        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": "result"}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        statuses = []

        def on_status(status, run_id):
            statuses.append(status)

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            poll_research("trun_poll", on_status=on_status, poll_interval=1)

        assert statuses[0] == "polling"
        assert "completed" in statuses


class TestExtractExecutiveSummary:
    """Tests for _extract_executive_summary function."""

    def test_markdown_string_with_summary_before_heading(self):
        """Should extract text before the first ## heading."""
        content = "# Report Title\n\nThis is the executive summary.\n\n## Section 1\n\nDetails here."
        result = _extract_executive_summary(content)
        assert result == "This is the executive summary."

    def test_markdown_string_no_title(self):
        """Should extract text before the first ## heading when no title."""
        content = "This is the summary paragraph.\n\nMore summary text.\n\n## Details\n\nSection content."
        result = _extract_executive_summary(content)
        assert result == "This is the summary paragraph.\n\nMore summary text."

    def test_markdown_string_no_headings(self):
        """Should return all content when no ## headings exist."""
        content = "# Title\n\nThis is a short report with no subsections but enough text to be a summary."
        result = _extract_executive_summary(content)
        assert result is not None
        assert "short report" in result

    def test_markdown_string_empty(self):
        """Should return None for empty string."""
        assert _extract_executive_summary("") is None

    def test_markdown_string_too_short(self):
        """Should return None when summary is too short."""
        content = "# Title\n\nShort.\n\n## Section\n\nDetails."
        assert _extract_executive_summary(content) is None

    def test_dict_with_text_key(self):
        """Should extract summary from dict with text key."""
        content = {
            "text": "# Title\n\nThe executive summary here is long enough to be meaningful.\n\n## Section\n\nBody."
        }
        result = _extract_executive_summary(content)
        assert result is not None
        assert "executive summary" in result

    def test_dict_with_summary_key(self):
        """Should extract summary from dict with summary key."""
        content = {"summary": "This is the summary.", "key_findings": ["a", "b"]}
        result = _extract_executive_summary(content)
        assert result == "This is the summary."

    def test_dict_with_executive_summary_key(self):
        """Should extract from executive_summary key."""
        content = {"executive_summary": "Executive overview here.", "details": "..."}
        result = _extract_executive_summary(content)
        assert result == "Executive overview here."

    def test_none_content(self):
        """Should return None for None content."""
        assert _extract_executive_summary(None) is None

    def test_non_string_non_dict(self):
        """Should return None for unsupported types."""
        assert _extract_executive_summary(42) is None
        assert _extract_executive_summary([1, 2, 3]) is None


class TestResearchOutputExecutiveSummary:
    """Tests that the executive summary is always printed to console."""

    def test_research_run_prints_executive_summary(self, runner, tmp_path, monkeypatch):
        """Should print executive summary when research completes."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "completed",
                "output": {
                    "content": "# Deep Research Report\n\nThis is the executive summary of the research findings.\n\n## Section 1\n\nDetailed analysis here."
                },
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--text", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "Research Complete" in result.output
            assert "Executive Summary" in result.output
            assert "executive summary of the research" in result.output

    def test_research_poll_prints_executive_summary(self, runner):
        """Should print executive summary when polling completes."""
        with mock.patch("parallel_web_tools.cli.commands.poll_research") as mock_poll:
            mock_poll.return_value = {
                "run_id": "trun_456",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_456",
                "status": "completed",
                "output": {
                    "content": "# Report\n\nHere is a substantial executive summary with enough content.\n\n## Analysis\n\nBody text."
                },
            }

            result = runner.invoke(main, ["research", "poll", "trun_456", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "Executive Summary" in result.output
            assert "substantial executive summary" in result.output

    def test_no_summary_when_content_missing(self, runner, tmp_path, monkeypatch):
        """Should not crash when content is missing."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_789",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_789",
                "status": "completed",
                "output": {"other_field": "value"},
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "Research Complete" in result.output
            assert "Executive Summary" not in result.output

    def test_summary_shown_with_auto_schema(self, runner, tmp_path, monkeypatch):
        """Should print summary for auto schema (structured content)."""
        monkeypatch.chdir(tmp_path)

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_json",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_json",
                "status": "completed",
                "output": {
                    "content": {"summary": "This is a structured summary for testing the executive summary display."}
                },
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "Executive Summary" in result.output
