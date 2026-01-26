"""Tests for the deep research functionality."""

import json
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.research import (
    RESEARCH_PROCESSORS,
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
    with mock.patch("parallel.Parallel") as mock_cls:
        mock_client = mock.MagicMock()
        mock_cls.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_api_key():
    """Mock the API key resolution."""
    with mock.patch("parallel_web_tools.core.research.resolve_api_key", return_value="test-key"):
        yield


# =============================================================================
# Core Research Function Tests
# =============================================================================


class TestCreateResearchTask:
    """Tests for create_research_task function."""

    def test_create_task_basic(self, mock_parallel_client, mock_api_key):
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

    def test_create_task_truncates_query(self, mock_parallel_client, mock_api_key):
        """Should truncate query to 15000 chars."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        long_query = "x" * 20000
        create_research_task(long_query)

        call_args = mock_parallel_client.task_run.create.call_args
        assert len(call_args.kwargs["input"]) == 15000


class TestGetResearchStatus:
    """Tests for get_research_status function."""

    def test_get_status(self, mock_parallel_client, mock_api_key):
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

    def test_get_result_basic(self, mock_parallel_client, mock_api_key):
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

    def test_run_research_success(self, mock_parallel_client, mock_api_key):
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

        with mock.patch("parallel_web_tools.core.research.time.sleep"):
            result = run_research("What is AI?", poll_interval=1, timeout=10)

        assert result["status"] == "completed"
        assert "output" in result

    def test_run_research_timeout(self, mock_parallel_client, mock_api_key):
        """Should raise TimeoutError when task doesn't complete."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "running"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        with mock.patch("parallel_web_tools.core.research.time.sleep"):
            with mock.patch("parallel_web_tools.core.research.time.time") as mock_time:
                # Simulate timeout by returning increasing time values
                mock_time.side_effect = [0, 0, 5, 10, 15]

                with pytest.raises(TimeoutError):
                    run_research("What is AI?", timeout=10, poll_interval=1)

    def test_run_research_failed(self, mock_parallel_client, mock_api_key):
        """Should raise RuntimeError when task fails."""
        mock_task = mock.MagicMock()
        mock_task.run_id = "trun_123"
        mock_parallel_client.task_run.create.return_value = mock_task

        mock_status = mock.MagicMock()
        mock_status.status = "failed"
        mock_status.error = "Processing error"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        with mock.patch("parallel_web_tools.core.research.time.sleep"):
            with pytest.raises(RuntimeError, match="failed"):
                run_research("What is AI?", poll_interval=1)

    def test_run_research_on_status_callback(self, mock_parallel_client, mock_api_key):
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

        with mock.patch("parallel_web_tools.core.research.time.sleep"):
            run_research("What is AI?", on_status=on_status, poll_interval=1)

        assert ("created", "trun_123") in statuses
        assert ("completed", "trun_123") in statuses


class TestPollResearch:
    """Tests for poll_research function."""

    def test_poll_existing_task(self, mock_parallel_client, mock_api_key):
        """Should poll existing task until completion."""
        mock_status = mock.MagicMock()
        mock_status.status = "completed"
        mock_parallel_client.task_run.retrieve.return_value = mock_status

        mock_output = mock.MagicMock()
        mock_output.model_dump.return_value = {"content": {"text": "Results"}}
        mock_result = mock.MagicMock()
        mock_result.output = mock_output
        mock_parallel_client.task_run.result.return_value = mock_result

        with mock.patch("parallel_web_tools.core.research.time.sleep"):
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

    def test_research_run_json_output(self, runner):
        """Should output JSON with --json flag."""
        with mock.patch("parallel_web_tools.cli.commands.create_research_task") as mock_create:
            mock_create.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "pending",
            }

            result = runner.invoke(main, ["research", "run", "What is AI?", "--no-wait", "--json"])

            assert result.exit_code == 0
            # Find the JSON in the output
            lines = result.output.strip().split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("{"):
                    in_json = True
                if in_json:
                    json_lines.append(line)
                if in_json and line.strip().startswith("}"):
                    break
            output = json.loads("\n".join(json_lines))
            assert output["run_id"] == "trun_123"

    def test_research_run_with_wait(self, runner):
        """Should poll and return results without --no-wait."""
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

    def test_research_save_to_file_with_content(self, runner, tmp_path):
        """Should save content to separate markdown file."""
        output_base = tmp_path / "report"
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_123",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_123",
                "status": "completed",
                "output": {"content": {"text": "# Research findings\n\nThis is the report."}, "basis": []},
            }

            result = runner.invoke(
                main,
                ["research", "run", "What is AI?", "-o", str(output_base), "--poll-interval", "1"],
            )

            assert result.exit_code == 0

            # Check JSON file has output with content_file reference
            assert json_file.exists()
            data = json.loads(json_file.read_text())
            assert data["run_id"] == "trun_123"
            assert data["status"] == "completed"
            assert "output" in data
            assert "content" not in data["output"]
            assert data["output"]["content_file"] == "report.md"
            assert data["output"]["basis"] == []

            # Check markdown file has content
            assert md_file.exists()
            assert md_file.read_text() == "# Research findings\n\nThis is the report."

    def test_research_save_to_file_strips_extension(self, runner, tmp_path):
        """Should strip extension from output path and create both files."""
        output_with_ext = tmp_path / "report.json"
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
                ["research", "run", "Question?", "-o", str(output_with_ext), "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert json_file.exists()
            assert md_file.exists()

    def test_research_save_to_file_string_content(self, runner, tmp_path):
        """Should handle string content directly."""
        output_base = tmp_path / "report"
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_456",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_456",
                "status": "completed",
                "output": {"content": "Plain string content"},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Question?", "-o", str(output_base), "--poll-interval", "1"],
            )

            assert result.exit_code == 0

            # Check markdown file has content
            assert md_file.exists()
            assert md_file.read_text() == "Plain string content"

            # Check JSON references markdown file
            data = json.loads(json_file.read_text())
            assert data["output"]["content_file"] == "report.md"

    def test_research_save_to_file_no_content(self, runner, tmp_path):
        """Should handle output without content field."""
        output_base = tmp_path / "report"
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_789",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_789",
                "status": "completed",
                "output": {"other_field": "some value"},
            }

            result = runner.invoke(
                main,
                ["research", "run", "Question?", "-o", str(output_base), "--poll-interval", "1"],
            )

            assert result.exit_code == 0

            # No markdown file should be created
            assert not md_file.exists()

            # JSON should have original output
            data = json.loads(json_file.read_text())
            assert data["output"]["other_field"] == "some value"
            assert "content_file" not in data["output"]

    def test_research_save_to_file_structured_content(self, runner, tmp_path):
        """Should convert structured dict content to markdown."""
        output_base = tmp_path / "report"
        json_file = tmp_path / "report.json"
        md_file = tmp_path / "report.md"

        with mock.patch("parallel_web_tools.cli.commands.run_research") as mock_run:
            mock_run.return_value = {
                "run_id": "trun_structured",
                "result_url": "https://platform.parallel.ai/play/deep-research/trun_structured",
                "status": "completed",
                "output": {
                    "content": {
                        "summary": "This is the summary.",
                        "key_findings": ["Finding 1", "Finding 2"],
                        "detailed_analysis": {"section_one": "Details here."},
                    },
                    "basis": [],
                },
            }

            result = runner.invoke(
                main,
                ["research", "run", "Question?", "-o", str(output_base), "--poll-interval", "1"],
            )

            assert result.exit_code == 0

            # Markdown file should be created
            assert md_file.exists()
            md_content = md_file.read_text()

            # Check markdown has sections
            assert "# Summary" in md_content
            assert "This is the summary." in md_content
            assert "# Key Findings" in md_content
            assert "- Finding 1" in md_content
            assert "# Detailed Analysis" in md_content

            # JSON should reference markdown file
            data = json.loads(json_file.read_text())
            assert data["output"]["content_file"] == "report.md"
            assert "content" not in data["output"]
