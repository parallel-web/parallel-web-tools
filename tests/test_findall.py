"""Tests for the FindAll entity discovery functionality."""

import json
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.findall import (
    FINDALL_GENERATORS,
    FINDALL_TERMINAL_STATUSES,
    _extract_status_from_result,
    _extract_status_info,
    _serialize,
    cancel_findall_run,
    create_findall_run,
    get_findall_result,
    get_findall_status,
    ingest_findall,
    poll_findall,
    run_findall,
)


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_parallel_client():
    """Create a mock Parallel client with beta.findall namespace."""
    mock_client = mock.MagicMock()
    with mock.patch("parallel_web_tools.core.findall.create_client", return_value=mock_client):
        yield mock_client


def _make_status_obj(status="running", is_active=True, generated=0, matched=0):
    """Helper to build a mock status object with nested metrics."""
    metrics = mock.MagicMock()
    metrics.generated_candidates_count = generated
    metrics.matched_candidates_count = matched

    status_obj = mock.MagicMock()
    status_obj.status = status
    status_obj.is_active = is_active
    status_obj.metrics = metrics
    status_obj.termination_reason = None
    return status_obj


def _make_run(findall_id="findall_abc123", status="queued", is_active=True, generated=0, matched=0, generator="core"):
    """Helper to build a mock FindAll run object."""
    run = mock.MagicMock()
    run.findall_id = findall_id
    run.status = _make_status_obj(status, is_active, generated, matched)
    run.generator = generator
    run.created_at = "2026-01-01T00:00:00Z"
    run.modified_at = "2026-01-01T00:01:00Z"
    return run


def _make_result(findall_id="findall_abc123", generated=10, matched=3, candidates=None):
    """Helper to build a mock FindAll result object with nested .run.status."""
    if candidates is None:
        candidates = [
            {"name": "Acme Corp", "url": "https://acme.com", "description": "A company", "match_status": "matched"},
            {"name": "Beta Inc", "url": "https://beta.com", "description": "Another co", "match_status": "matched"},
            {"name": "Gamma LLC", "url": "https://gamma.com", "description": "Third one", "match_status": "matched"},
        ]

    result = mock.MagicMock()
    result.candidates = candidates

    # Result nests status under .run.status (not .status directly)
    run_obj = _make_run(findall_id, status="completed", is_active=False, generated=generated, matched=matched)
    result.run = run_obj

    # Ensure .status doesn't accidentally work at top level
    result.status = None

    return result


# =============================================================================
# Internal Helper Tests
# =============================================================================


class TestSerialize:
    """Tests for _serialize helper."""

    def test_none(self):
        assert _serialize(None) is None

    def test_primitives(self):
        assert _serialize("hello") == "hello"
        assert _serialize(42) == 42
        assert _serialize(3.14) == 3.14
        assert _serialize(True) is True

    def test_dict(self):
        assert _serialize({"a": 1}) == {"a": 1}

    def test_list(self):
        assert _serialize([1, "two", None]) == [1, "two", None]

    def test_nested_list(self):
        obj = mock.MagicMock()
        obj.model_dump.return_value = {"field": "value"}
        assert _serialize([obj]) == [{"field": "value"}]

    def test_model_dump(self):
        obj = mock.MagicMock()
        obj.model_dump.return_value = {"key": "val"}
        assert _serialize(obj) == {"key": "val"}

    def test_to_dict(self):
        obj = mock.MagicMock(spec=[])
        obj.to_dict = mock.MagicMock(return_value={"key": "val"})
        assert _serialize(obj) == {"key": "val"}

    def test_dunder_dict_fallback(self):
        class Simple:
            def __init__(self):
                self.x = 1
                self._private = "hidden"

        obj = Simple()
        result = _serialize(obj)
        assert result["x"] == 1
        assert "_private" not in result

    def test_str_fallback(self):
        # For objects with __dict__, _serialize uses the dict fallback
        assert _serialize(42) == 42
        # str goes through primitive check
        assert _serialize("text") == "text"


class TestExtractStatusInfo:
    """Tests for _extract_status_info helper."""

    def test_no_status_attribute(self):
        run = mock.MagicMock(spec=[])
        result = _extract_status_info(run)
        assert result == {"status": "unknown", "is_active": False, "metrics": {}}

    def test_status_none(self):
        run = mock.MagicMock()
        run.status = None
        result = _extract_status_info(run)
        assert result["status"] == "unknown"

    def test_full_status(self):
        run = _make_run(status="running", is_active=True, generated=5, matched=2)
        result = _extract_status_info(run)
        assert result["status"] == "running"
        assert result["is_active"] is True
        assert result["metrics"]["generated_candidates_count"] == 5
        assert result["metrics"]["matched_candidates_count"] == 2

    def test_no_metrics(self):
        run = mock.MagicMock()
        run.status.status = "queued"
        run.status.is_active = True
        run.status.metrics = None
        result = _extract_status_info(run)
        assert result["status"] == "queued"
        assert result["metrics"] == {}


class TestExtractStatusFromResult:
    """Tests for _extract_status_from_result helper (nested .run.status)."""

    def test_extracts_from_run_attribute(self):
        """The result endpoint nests status under .run - this is the key fix."""
        result_obj = _make_result(generated=11, matched=5)
        info = _extract_status_from_result(result_obj)
        assert info["status"] == "completed"
        assert info["metrics"]["generated_candidates_count"] == 11
        assert info["metrics"]["matched_candidates_count"] == 5

    def test_falls_back_to_direct_status(self):
        """When .run is absent, falls back to direct status extraction."""
        result_obj = mock.MagicMock()
        result_obj.run = None
        result_obj.status = _make_status_obj("completed", False, 7, 3)
        info = _extract_status_from_result(result_obj)
        assert info["status"] == "completed"
        assert info["metrics"]["generated_candidates_count"] == 7


# =============================================================================
# Core FindAll Function Tests
# =============================================================================


class TestIngestFindall:
    """Tests for ingest_findall function."""

    def test_ingest_basic(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.model_dump.return_value = {
            "entity_type": "companies",
            "match_conditions": [{"name": "industry", "description": "AI"}],
            "generator": "core",
        }
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        result = ingest_findall("Find AI companies")

        assert result == {
            "entity_type": "companies",
            "match_conditions": [{"name": "industry", "description": "AI"}],
            "generator": "core",
        }
        mock_parallel_client.beta.findall.ingest.assert_called_once_with(objective="Find AI companies")

    def test_ingest_passes_api_key(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.model_dump.return_value = {}
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        with mock.patch(
            "parallel_web_tools.core.findall.create_client", return_value=mock_parallel_client
        ) as mock_create:
            ingest_findall("query", api_key="test-key", source="cli")
            mock_create.assert_called_with("test-key", "cli")


class TestCreateFindallRun:
    """Tests for create_findall_run function."""

    def test_create_basic(self, mock_parallel_client):
        run = _make_run(status="queued", generator="core")
        mock_parallel_client.beta.findall.create.return_value = run

        result = create_findall_run(
            objective="Find companies",
            entity_type="companies",
            match_conditions=[{"name": "cond1", "description": "desc"}],
        )

        assert result["findall_id"] == "findall_abc123"
        assert result["status"] == "queued"
        assert result["generator"] == "core"
        assert result["created_at"] == "2026-01-01T00:00:00Z"
        mock_parallel_client.beta.findall.create.assert_called_once()

    def test_create_with_options(self, mock_parallel_client):
        run = _make_run(generator="pro")
        mock_parallel_client.beta.findall.create.return_value = run

        create_findall_run(
            objective="Find entities",
            entity_type="companies",
            match_conditions=[{"name": "c", "description": "d"}],
            generator="pro",
            match_limit=50,
            exclude_list=[{"name": "Excluded Co", "url": "https://excluded.com"}],
            metadata={"tag": "test"},
        )

        call_kwargs = mock_parallel_client.beta.findall.create.call_args.kwargs
        assert call_kwargs["generator"] == "pro"
        assert call_kwargs["match_limit"] == 50
        assert call_kwargs["exclude_list"] == [{"name": "Excluded Co", "url": "https://excluded.com"}]
        assert call_kwargs["metadata"] == {"tag": "test"}

    def test_create_omits_none_optionals(self, mock_parallel_client):
        run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = run

        create_findall_run(
            objective="q",
            entity_type="entities",
            match_conditions=[],
        )

        call_kwargs = mock_parallel_client.beta.findall.create.call_args.kwargs
        assert "exclude_list" not in call_kwargs
        assert "metadata" not in call_kwargs


class TestCancelFindallRun:
    """Tests for cancel_findall_run function."""

    def test_cancel(self, mock_parallel_client):
        result = cancel_findall_run("findall_xyz")

        assert result == {"findall_id": "findall_xyz", "status": "cancelled"}
        mock_parallel_client.beta.findall.cancel.assert_called_once_with(findall_id="findall_xyz")


class TestGetFindallStatus:
    """Tests for get_findall_status function."""

    def test_status_running(self, mock_parallel_client):
        run = _make_run(status="running", is_active=True, generated=5, matched=2, generator="base")
        mock_parallel_client.beta.findall.retrieve.return_value = run

        result = get_findall_status("findall_abc123")

        assert result["findall_id"] == "findall_abc123"
        assert result["status"] == "running"
        assert result["is_active"] is True
        assert result["metrics"]["generated_candidates_count"] == 5
        assert result["metrics"]["matched_candidates_count"] == 2
        assert result["generator"] == "base"
        assert result["created_at"] is not None
        assert result["modified_at"] is not None

    def test_status_completed(self, mock_parallel_client):
        run = _make_run(status="completed", is_active=False, generated=10, matched=5)
        mock_parallel_client.beta.findall.retrieve.return_value = run

        result = get_findall_status("findall_abc123")

        assert result["status"] == "completed"
        assert result["is_active"] is False


class TestGetFindallResult:
    """Tests for get_findall_result function."""

    def test_result_with_candidates(self, mock_parallel_client):
        result_obj = _make_result(generated=10, matched=3)
        mock_parallel_client.beta.findall.result.return_value = result_obj

        result = get_findall_result("findall_abc123")

        assert result["findall_id"] == "findall_abc123"
        assert result["status"] == "completed"
        assert result["metrics"]["generated_candidates_count"] == 10
        assert result["metrics"]["matched_candidates_count"] == 3
        assert len(result["candidates"]) == 3
        assert result["candidates"][0]["name"] == "Acme Corp"

    def test_result_empty_candidates(self, mock_parallel_client):
        result_obj = _make_result(candidates=[])
        mock_parallel_client.beta.findall.result.return_value = result_obj

        result = get_findall_result("findall_abc123")

        assert result["candidates"] == []

    def test_result_none_candidates(self, mock_parallel_client):
        result_obj = mock.MagicMock()
        result_obj.run = _make_run(status="completed", is_active=False)
        result_obj.status = None
        # spec out candidates so getattr returns the default
        del result_obj.candidates

        mock_parallel_client.beta.findall.result.return_value = result_obj

        result = get_findall_result("findall_abc123")

        assert result["candidates"] == []


class TestRunFindall:
    """Tests for run_findall function (full ingest + create + poll flow)."""

    def test_run_success(self, mock_parallel_client):
        # Mock ingest
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "companies"
        mock_schema.match_conditions = [{"name": "cond", "description": "d"}]
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        # Mock create
        mock_run = _make_run(status="queued")
        mock_parallel_client.beta.findall.create.return_value = mock_run

        # Mock polling: queued -> running -> completed
        mock_parallel_client.beta.findall.retrieve.side_effect = [
            _make_run(status="running", is_active=True, generated=5, matched=0),
            _make_run(status="completed", is_active=False, generated=10, matched=3),
        ]

        # Mock result
        mock_parallel_client.beta.findall.result.return_value = _make_result()

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = run_findall("Find AI companies", poll_interval=1, timeout=60)

        assert result["findall_id"] == "findall_abc123"
        assert result["status"] == "completed"
        assert len(result["candidates"]) == 3

    def test_run_timeout(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "companies"
        mock_schema.match_conditions = []
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run(status="queued")
        mock_parallel_client.beta.findall.create.return_value = mock_run

        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(status="running", is_active=True)

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with mock.patch("parallel_web_tools.core.polling.time.time") as mock_time:
                mock_time.side_effect = [0, 0, 5, 10, 15]
                with pytest.raises(TimeoutError, match="timed out"):
                    run_findall("query", timeout=10, poll_interval=1)

    def test_run_failed(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "entities"
        mock_schema.match_conditions = []
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = mock_run

        failed_run = _make_run(status="failed", is_active=False)
        failed_run.status.termination_reason = "internal_error"
        mock_parallel_client.beta.findall.retrieve.return_value = failed_run

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="failed"):
                run_findall("query", poll_interval=1)

    def test_run_cancelled(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "entities"
        mock_schema.match_conditions = []
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = mock_run

        cancelled_run = _make_run(status="cancelled", is_active=False)
        mock_parallel_client.beta.findall.retrieve.return_value = cancelled_run

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="cancelled"):
                run_findall("query", poll_interval=1)

    def test_run_on_status_callback(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "companies"
        mock_schema.match_conditions = [{"name": "c", "description": "d"}]
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = mock_run

        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(
            status="completed", is_active=False, generated=10, matched=5
        )
        mock_parallel_client.beta.findall.result.return_value = _make_result()

        statuses = []

        def on_status(status, findall_id, metrics):
            statuses.append((status, findall_id))

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            run_findall("query", on_status=on_status, poll_interval=1)

        # Should have ingested, created, then completed
        status_names = [s[0] for s in statuses]
        assert "ingested" in status_names
        assert "created" in status_names
        assert "completed" in status_names

    def test_run_failed_with_termination_reason(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "entities"
        mock_schema.match_conditions = []
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = mock_run

        failed_run = _make_run(status="failed", is_active=False)
        failed_run.status.termination_reason = "quota_exceeded"
        mock_parallel_client.beta.findall.retrieve.return_value = failed_run

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="quota_exceeded"):
                run_findall("query", poll_interval=1)

    def test_run_passes_exclude_list_and_metadata(self, mock_parallel_client):
        mock_schema = mock.MagicMock()
        mock_schema.entity_type = "companies"
        mock_schema.match_conditions = []
        mock_parallel_client.beta.findall.ingest.return_value = mock_schema

        mock_run = _make_run()
        mock_parallel_client.beta.findall.create.return_value = mock_run

        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(status="completed", is_active=False)
        mock_parallel_client.beta.findall.result.return_value = _make_result()

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            run_findall(
                "query",
                exclude_list=[{"name": "Foo", "url": "https://foo.com"}],
                metadata={"run_tag": "test"},
                poll_interval=1,
            )

        call_kwargs = mock_parallel_client.beta.findall.create.call_args.kwargs
        assert call_kwargs["exclude_list"] == [{"name": "Foo", "url": "https://foo.com"}]
        assert call_kwargs["metadata"] == {"run_tag": "test"}


class TestPollFindall:
    """Tests for poll_findall function."""

    def test_poll_completed(self, mock_parallel_client):
        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(
            status="completed", is_active=False, generated=10, matched=5
        )
        mock_parallel_client.beta.findall.result.return_value = _make_result()

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = poll_findall("findall_abc123", poll_interval=1)

        assert result["findall_id"] == "findall_abc123"
        assert result["status"] == "completed"
        assert len(result["candidates"]) == 3

    def test_poll_with_on_status(self, mock_parallel_client):
        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(status="completed", is_active=False)
        mock_parallel_client.beta.findall.result.return_value = _make_result()

        statuses = []

        def on_status(status, findall_id, metrics):
            statuses.append(status)

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            poll_findall("findall_abc123", on_status=on_status, poll_interval=1)

        assert statuses[0] == "polling"
        assert "completed" in statuses

    def test_poll_timeout(self, mock_parallel_client):
        mock_parallel_client.beta.findall.retrieve.return_value = _make_run(status="running", is_active=True)

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with mock.patch("parallel_web_tools.core.polling.time.time") as mock_time:
                mock_time.side_effect = [0, 0, 5, 10, 15]
                with pytest.raises(TimeoutError, match="timed out"):
                    poll_findall("findall_abc123", timeout=10, poll_interval=1)


# =============================================================================
# Constants Tests
# =============================================================================


class TestFindallGenerators:
    """Tests for FINDALL_GENERATORS constant."""

    def test_expected_generators(self):
        assert "preview" in FINDALL_GENERATORS
        assert "base" in FINDALL_GENERATORS
        assert "core" in FINDALL_GENERATORS
        assert "pro" in FINDALL_GENERATORS

    def test_generators_have_descriptions(self):
        for _gen, desc in FINDALL_GENERATORS.items():
            assert isinstance(desc, str)
            assert len(desc) > 0


class TestFindallTerminalStatuses:
    """Tests for FINDALL_TERMINAL_STATUSES constant."""

    def test_terminal_statuses(self):
        assert "completed" in FINDALL_TERMINAL_STATUSES
        assert "failed" in FINDALL_TERMINAL_STATUSES
        assert "cancelled" in FINDALL_TERMINAL_STATUSES
        assert "running" not in FINDALL_TERMINAL_STATUSES
        assert "queued" not in FINDALL_TERMINAL_STATUSES


# =============================================================================
# CLI FindAll Command Tests
# =============================================================================


class TestFindallGroup:
    """Tests for the findall command group."""

    def test_findall_help(self, runner):
        result = runner.invoke(main, ["findall", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "ingest" in result.output
        assert "status" in result.output
        assert "poll" in result.output
        assert "result" in result.output
        assert "cancel" in result.output

    def test_findall_in_main_help(self, runner):
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "findall" in result.output


class TestFindallRunCommand:
    """Tests for the findall run CLI command."""

    def test_run_help(self, runner):
        result = runner.invoke(main, ["findall", "run", "--help"])
        assert result.exit_code == 0
        assert "--generator" in result.output
        assert "--match-limit" in result.output
        assert "--timeout" in result.output
        assert "--poll-interval" in result.output
        assert "--no-wait" in result.output
        assert "--output" in result.output
        assert "--json" in result.output

    def test_run_no_objective(self, runner):
        result = runner.invoke(main, ["findall", "run"])
        assert result.exit_code != 0

    def test_run_no_wait(self, runner):
        with (
            mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest,
            mock.patch("parallel_web_tools.cli.commands.create_findall_run") as mock_create,
        ):
            mock_ingest.return_value = {
                "entity_type": "companies",
                "match_conditions": [{"name": "cond", "description": "desc"}],
            }
            mock_create.return_value = {
                "findall_id": "findall_test123",
                "status": "queued",
                "generator": "core",
            }

            result = runner.invoke(main, ["findall", "run", "Find AI companies", "--no-wait"])

            assert result.exit_code == 0
            assert "findall_test123" in result.output
            mock_ingest.assert_called_once()
            mock_create.assert_called_once()

    def test_run_no_wait_json(self, runner):
        with (
            mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest,
            mock.patch("parallel_web_tools.cli.commands.create_findall_run") as mock_create,
        ):
            mock_ingest.return_value = {
                "entity_type": "companies",
                "match_conditions": [],
            }
            mock_create.return_value = {
                "findall_id": "findall_json123",
                "status": "queued",
                "generator": "preview",
            }

            result = runner.invoke(main, ["findall", "run", "Find companies", "--no-wait", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["findall_id"] == "findall_json123"

    def test_run_with_wait(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.return_value = {
                "findall_id": "findall_wait123",
                "status": "completed",
                "metrics": {"generated_candidates_count": 10, "matched_candidates_count": 3},
                "candidates": [
                    {"name": "Co1", "url": "https://co1.com", "description": "First", "match_status": "matched"},
                ],
            }

            result = runner.invoke(
                main,
                ["findall", "run", "Find AI companies", "--poll-interval", "1", "--timeout", "10"],
            )

            assert result.exit_code == 0
            assert "FindAll Complete" in result.output
            assert "Co1" in result.output
            mock_run.assert_called_once()

    def test_run_with_wait_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.return_value = {
                "findall_id": "findall_json_wait",
                "status": "completed",
                "metrics": {"generated_candidates_count": 5, "matched_candidates_count": 2},
                "candidates": [],
            }

            result = runner.invoke(
                main,
                ["findall", "run", "Find stuff", "--json", "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["findall_id"] == "findall_json_wait"
            assert output["status"] == "completed"

    def test_run_no_matched_candidates(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.return_value = {
                "findall_id": "findall_empty",
                "status": "completed",
                "metrics": {"generated_candidates_count": 10, "matched_candidates_count": 0},
                "candidates": [
                    {"name": "Unmatched", "url": "https://x.com", "description": "nope", "match_status": "unmatched"},
                ],
            }

            result = runner.invoke(main, ["findall", "run", "Find stuff", "--poll-interval", "1"])

            assert result.exit_code == 0
            assert "No matched candidates found" in result.output

    def test_run_saves_to_output_file(self, runner, tmp_path):
        output_file = tmp_path / "results.json"

        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.return_value = {
                "findall_id": "findall_file",
                "status": "completed",
                "metrics": {"generated_candidates_count": 5, "matched_candidates_count": 2},
                "candidates": [
                    {"name": "Co1", "url": "https://co1.com", "description": "desc", "match_status": "matched"},
                ],
            }

            result = runner.invoke(
                main,
                ["findall", "run", "query", "-o", str(output_file), "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert output_file.exists()
            data = json.loads(output_file.read_text())
            assert data["findall_id"] == "findall_file"
            assert len(data["candidates"]) == 1


class TestFindallIngestCommand:
    """Tests for the findall ingest CLI command."""

    def test_ingest_help(self, runner):
        result = runner.invoke(main, ["findall", "ingest", "--help"])
        assert result.exit_code == 0
        assert "OBJECTIVE" in result.output

    def test_ingest_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest:
            mock_ingest.return_value = {
                "entity_type": "companies",
                "generator": "core",
                "match_conditions": [
                    {"name": "industry", "description": "AI / ML"},
                    {"name": "location", "description": "San Francisco"},
                ],
                "enrichments": [
                    {"name": "funding", "description": "Total funding raised"},
                ],
            }

            result = runner.invoke(main, ["findall", "ingest", "Find AI companies in SF"])

            assert result.exit_code == 0
            assert "companies" in result.output
            assert "industry" in result.output
            assert "funding" in result.output

    def test_ingest_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest:
            schema = {
                "entity_type": "companies",
                "generator": "core",
                "match_conditions": [{"name": "c", "description": "d"}],
            }
            mock_ingest.return_value = schema

            result = runner.invoke(main, ["findall", "ingest", "Find companies", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["entity_type"] == "companies"

    def test_ingest_no_enrichments(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest:
            mock_ingest.return_value = {
                "entity_type": "people",
                "generator": "base",
                "match_conditions": [{"name": "role", "description": "CEO"}],
                "enrichments": None,
            }

            result = runner.invoke(main, ["findall", "ingest", "Find CEOs"])

            assert result.exit_code == 0
            assert "people" in result.output
            # Should not crash on None enrichments


class TestFindallStatusCommand:
    """Tests for the findall status CLI command."""

    def test_status_help(self, runner):
        result = runner.invoke(main, ["findall", "status", "--help"])
        assert result.exit_code == 0
        assert "FINDALL_ID" in result.output

    def test_status_running(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_status") as mock_status:
            mock_status.return_value = {
                "findall_id": "findall_s123",
                "status": "running",
                "is_active": True,
                "generator": "core",
                "metrics": {"generated_candidates_count": 5, "matched_candidates_count": 2},
            }

            result = runner.invoke(main, ["findall", "status", "findall_s123"])

            assert result.exit_code == 0
            assert "findall_s123" in result.output
            assert "running" in result.output.lower()

    def test_status_completed(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_status") as mock_status:
            mock_status.return_value = {
                "findall_id": "findall_s456",
                "status": "completed",
                "is_active": False,
                "generator": "base",
                "metrics": {"generated_candidates_count": 20, "matched_candidates_count": 8},
            }

            result = runner.invoke(main, ["findall", "status", "findall_s456"])

            assert result.exit_code == 0
            assert "completed" in result.output.lower()
            # Should suggest getting results
            assert "result" in result.output.lower()

    def test_status_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_status") as mock_status:
            mock_status.return_value = {
                "findall_id": "findall_sjson",
                "status": "completed",
                "is_active": False,
                "generator": "core",
                "metrics": {},
            }

            result = runner.invoke(main, ["findall", "status", "findall_sjson", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["status"] == "completed"


class TestFindallPollCommand:
    """Tests for the findall poll CLI command."""

    def test_poll_help(self, runner):
        result = runner.invoke(main, ["findall", "poll", "--help"])
        assert result.exit_code == 0
        assert "FINDALL_ID" in result.output
        assert "--timeout" in result.output
        assert "--poll-interval" in result.output

    def test_poll_success(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.poll_findall") as mock_poll:
            mock_poll.return_value = {
                "findall_id": "findall_p123",
                "status": "completed",
                "metrics": {"generated_candidates_count": 15, "matched_candidates_count": 7},
                "candidates": [
                    {"name": "Match1", "url": "https://m1.com", "description": "First", "match_status": "matched"},
                ],
            }

            result = runner.invoke(
                main,
                ["findall", "poll", "findall_p123", "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert "FindAll Complete" in result.output
            assert "Match1" in result.output

    def test_poll_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.poll_findall") as mock_poll:
            mock_poll.return_value = {
                "findall_id": "findall_pjson",
                "status": "completed",
                "metrics": {},
                "candidates": [],
            }

            result = runner.invoke(
                main,
                ["findall", "poll", "findall_pjson", "--json", "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["findall_id"] == "findall_pjson"

    def test_poll_saves_output(self, runner, tmp_path):
        output_file = tmp_path / "poll-results.json"

        with mock.patch("parallel_web_tools.cli.commands.poll_findall") as mock_poll:
            mock_poll.return_value = {
                "findall_id": "findall_pfile",
                "status": "completed",
                "metrics": {"generated_candidates_count": 5, "matched_candidates_count": 2},
                "candidates": [
                    {"name": "C1", "url": "https://c1.com", "description": "d", "match_status": "matched"},
                ],
            }

            result = runner.invoke(
                main,
                ["findall", "poll", "findall_pfile", "-o", str(output_file), "--poll-interval", "1"],
            )

            assert result.exit_code == 0
            assert output_file.exists()
            data = json.loads(output_file.read_text())
            assert data["findall_id"] == "findall_pfile"


class TestFindallResultCommand:
    """Tests for the findall result CLI command."""

    def test_result_help(self, runner):
        result = runner.invoke(main, ["findall", "result", "--help"])
        assert result.exit_code == 0
        assert "FINDALL_ID" in result.output
        assert "--output" in result.output
        assert "--json" in result.output

    def test_result_with_matches(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_result") as mock_result:
            mock_result.return_value = {
                "findall_id": "findall_r123",
                "status": "completed",
                "metrics": {"generated_candidates_count": 10, "matched_candidates_count": 2},
                "candidates": [
                    {"name": "Alpha", "url": "https://alpha.com", "description": "Alpha co", "match_status": "matched"},
                    {"name": "Beta", "url": "https://beta.com", "description": "Beta co", "match_status": "matched"},
                ],
            }

            result = runner.invoke(main, ["findall", "result", "findall_r123"])

            assert result.exit_code == 0
            assert "Alpha" in result.output
            assert "Beta" in result.output
            assert "FindAll Complete" in result.output

    def test_result_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_result") as mock_result:
            mock_result.return_value = {
                "findall_id": "findall_rjson",
                "status": "completed",
                "metrics": {},
                "candidates": [{"name": "A", "match_status": "matched"}],
            }

            result = runner.invoke(main, ["findall", "result", "findall_rjson", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["findall_id"] == "findall_rjson"

    def test_result_saves_to_file(self, runner, tmp_path):
        output_file = tmp_path / "final-results"

        with mock.patch("parallel_web_tools.cli.commands.get_findall_result") as mock_result:
            mock_result.return_value = {
                "findall_id": "findall_rfile",
                "status": "completed",
                "metrics": {},
                "candidates": [],
            }

            result = runner.invoke(
                main,
                ["findall", "result", "findall_rfile", "-o", str(output_file)],
            )

            assert result.exit_code == 0
            json_path = tmp_path / "final-results.json"
            assert json_path.exists()


class TestFindallCancelCommand:
    """Tests for the findall cancel CLI command."""

    def test_cancel_help(self, runner):
        result = runner.invoke(main, ["findall", "cancel", "--help"])
        assert result.exit_code == 0
        assert "FINDALL_ID" in result.output

    def test_cancel_success(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.cancel_findall_run") as mock_cancel:
            mock_cancel.return_value = {"findall_id": "findall_c123", "status": "cancelled"}

            result = runner.invoke(main, ["findall", "cancel", "findall_c123"])

            assert result.exit_code == 0
            assert "findall_c123" in result.output

    def test_cancel_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.cancel_findall_run") as mock_cancel:
            mock_cancel.return_value = {"findall_id": "findall_cjson", "status": "cancelled"}

            result = runner.invoke(main, ["findall", "cancel", "findall_cjson", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["status"] == "cancelled"


class TestFindallErrorHandling:
    """Tests for error handling in CLI findall commands."""

    def test_run_timeout_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.side_effect = TimeoutError("FindAll run findall_t123 timed out after 60s")

            result = runner.invoke(main, ["findall", "run", "query", "--poll-interval", "1"])

            # EXIT_TIMEOUT = 2
            assert result.exit_code == 5
            assert "Timeout" in result.output or "timed out" in result.output

    def test_run_timeout_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.side_effect = TimeoutError("timed out")

            result = runner.invoke(main, ["findall", "run", "query", "--json", "--poll-interval", "1"])

            assert result.exit_code == 5
            output = json.loads(result.output)
            assert output["error"]["type"] == "TimeoutError"

    def test_run_runtime_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.run_findall") as mock_run:
            mock_run.side_effect = RuntimeError("FindAll run failed (quota_exceeded)")

            result = runner.invoke(main, ["findall", "run", "query", "--poll-interval", "1"])

            assert result.exit_code != 0

    def test_ingest_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.ingest_findall") as mock_ingest:
            mock_ingest.side_effect = Exception("API error")

            result = runner.invoke(main, ["findall", "ingest", "query"])

            assert result.exit_code != 0

    def test_status_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_findall_status") as mock_status:
            mock_status.side_effect = Exception("Not found")

            result = runner.invoke(main, ["findall", "status", "findall_bad"])

            assert result.exit_code != 0

    def test_poll_timeout_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.poll_findall") as mock_poll:
            mock_poll.side_effect = TimeoutError("timed out")

            result = runner.invoke(main, ["findall", "poll", "findall_t", "--poll-interval", "1"])

            assert result.exit_code == 5

    def test_cancel_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.cancel_findall_run") as mock_cancel:
            mock_cancel.side_effect = Exception("Already cancelled")

            result = runner.invoke(main, ["findall", "cancel", "findall_bad"])

            assert result.exit_code != 0
