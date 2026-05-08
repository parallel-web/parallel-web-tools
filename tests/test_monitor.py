"""Tests for the Monitor web tracking functionality."""

import json
from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.monitor import (
    MONITOR_EVENT_TYPES,
    MONITOR_FREQUENCY_PRESETS,
    MONITOR_PROCESSORS,
    MONITOR_TYPES,
    cancel_monitor,
    create_monitor,
    get_monitor,
    list_monitor_events,
    list_monitors,
    resolve_frequency,
    trigger_monitor,
    update_monitor,
)


@pytest.fixture
def runner():
    return CliRunner()


def _model(**fields):
    """Build a stand-in for an SDK pydantic model: model_dump returns the fields verbatim."""
    obj = SimpleNamespace(**fields)
    obj.model_dump = lambda mode="json": dict(fields)  # type: ignore[attr-defined]
    return obj


@pytest.fixture
def mock_client():
    """Patch create_client and yield a fully mocked Parallel client."""
    with mock.patch("parallel_web_tools.core.monitor.create_client") as factory:
        client = mock.MagicMock()
        factory.return_value = client
        yield client


# =============================================================================
# Constants
# =============================================================================


class TestConstants:
    def test_frequency_presets(self):
        assert MONITOR_FREQUENCY_PRESETS["hourly"] == "1h"
        assert MONITOR_FREQUENCY_PRESETS["daily"] == "1d"
        assert MONITOR_FREQUENCY_PRESETS["weekly"] == "1w"
        assert MONITOR_FREQUENCY_PRESETS["every_two_weeks"] == "2w"

    def test_event_types(self):
        assert "monitor.event.detected" in MONITOR_EVENT_TYPES
        assert "monitor.execution.completed" in MONITOR_EVENT_TYPES
        assert "monitor.execution.failed" in MONITOR_EVENT_TYPES

    def test_types_and_processors(self):
        assert "event_stream" in MONITOR_TYPES
        assert "snapshot" in MONITOR_TYPES
        assert "lite" in MONITOR_PROCESSORS
        assert "base" in MONITOR_PROCESSORS

    def test_resolve_frequency_alias(self):
        assert resolve_frequency("daily") == "1d"

    def test_resolve_frequency_passthrough(self):
        assert resolve_frequency("6h") == "6h"


# =============================================================================
# create_monitor
# =============================================================================


class TestCreateMonitor:
    def test_event_stream_basic(self, mock_client):
        mock_client.monitor.create.return_value = _model(monitor_id="mon_123", type="event_stream")

        result = create_monitor("track AI news", "daily")

        assert result["monitor_id"] == "mon_123"
        kwargs = mock_client.monitor.create.call_args.kwargs
        assert kwargs["frequency"] == "1d"  # alias resolved
        assert kwargs["type"] == "event_stream"
        assert kwargs["settings"] == {"query": "track AI news"}

    def test_event_stream_with_all_options(self, mock_client):
        mock_client.monitor.create.return_value = _model(monitor_id="mon_456")

        create_monitor(
            "track stuff",
            "1h",
            webhook="https://hook.example.com",
            metadata={"project": "test"},
            output_schema={"type": "object", "properties": {}},
            include_backfill=True,
            processor="base",
        )

        kwargs = mock_client.monitor.create.call_args.kwargs
        assert kwargs["frequency"] == "1h"
        assert kwargs["webhook"] == {
            "url": "https://hook.example.com",
            "event_types": ["monitor.event.detected"],
        }
        assert kwargs["metadata"] == {"project": "test"}
        assert kwargs["processor"] == "base"
        assert kwargs["settings"]["include_backfill"] is True
        assert kwargs["settings"]["output_schema"] == {
            "type": "json",
            "json_schema": {"type": "object", "properties": {}},
        }

    def test_snapshot_type(self, mock_client):
        mock_client.monitor.create.return_value = _model(monitor_id="mon_snap")

        create_monitor(
            None,
            "1d",
            type="snapshot",
            task_run_id="trun_xyz",
        )

        kwargs = mock_client.monitor.create.call_args.kwargs
        assert kwargs["type"] == "snapshot"
        assert kwargs["settings"] == {"task_run_id": "trun_xyz"}

    def test_event_stream_requires_query(self, mock_client):
        with pytest.raises(ValueError, match="query is required"):
            create_monitor(None, "1d")

    def test_snapshot_requires_task_run_id(self, mock_client):
        with pytest.raises(ValueError, match="task_run_id"):
            create_monitor(None, "1d", type="snapshot")

    def test_unsupported_type(self, mock_client):
        with pytest.raises(ValueError, match="Unsupported monitor type"):
            create_monitor("q", "1d", type="bogus")  # type: ignore[arg-type]


# =============================================================================
# list / get / update / cancel
# =============================================================================


class TestListMonitors:
    def test_basic(self, mock_client):
        mock_client.monitor.list.return_value = _model(
            monitors=[{"monitor_id": "mon_1"}],
            next_cursor=None,
        )

        result = list_monitors()

        assert isinstance(result["monitors"], list)
        assert result["monitors"][0]["monitor_id"] == "mon_1"

    def test_passes_filters(self, mock_client):
        mock_client.monitor.list.return_value = _model(monitors=[], next_cursor=None)

        list_monitors(cursor="cur_abc", limit=5, status=["active"], type=["event_stream"])

        kwargs = mock_client.monitor.list.call_args.kwargs
        assert kwargs == {
            "cursor": "cur_abc",
            "limit": 5,
            "status": ["active"],
            "type": ["event_stream"],
        }

    def test_omits_unset(self, mock_client):
        mock_client.monitor.list.return_value = _model(monitors=[], next_cursor=None)

        list_monitors()

        assert mock_client.monitor.list.call_args.kwargs == {}


class TestGetMonitor:
    def test_get(self, mock_client):
        mock_client.monitor.retrieve.return_value = _model(monitor_id="mon_abc")

        result = get_monitor("mon_abc")

        assert result["monitor_id"] == "mon_abc"
        mock_client.monitor.retrieve.assert_called_once_with("mon_abc")


class TestUpdateMonitor:
    def test_update_frequency(self, mock_client):
        mock_client.monitor.update.return_value = _model(monitor_id="mon_abc")

        update_monitor("mon_abc", frequency="hourly")

        args, kwargs = mock_client.monitor.update.call_args
        assert args == ("mon_abc",)
        assert kwargs == {"frequency": "1h"}

    def test_update_webhook(self, mock_client):
        mock_client.monitor.update.return_value = _model(monitor_id="mon_abc")

        update_monitor("mon_abc", webhook="https://hook.example.com")

        kwargs = mock_client.monitor.update.call_args.kwargs
        assert kwargs["webhook"] == {
            "url": "https://hook.example.com",
            "event_types": ["monitor.event.detected"],
        }

    def test_update_advanced_settings_sets_event_stream_type(self, mock_client):
        mock_client.monitor.update.return_value = _model(monitor_id="mon_abc")

        update_monitor("mon_abc", advanced_settings={"foo": "bar"})

        kwargs = mock_client.monitor.update.call_args.kwargs
        assert kwargs["settings"] == {"advanced_settings": {"foo": "bar"}}
        assert kwargs["type"] == "event_stream"

    def test_update_no_fields_raises(self, mock_client):
        with pytest.raises(ValueError, match="At least one field"):
            update_monitor("mon_abc")


class TestCancelMonitor:
    def test_cancel(self, mock_client):
        mock_client.monitor.cancel.return_value = _model(
            monitor_id="mon_abc",
            status="cancelled",
        )

        result = cancel_monitor("mon_abc")

        assert result["status"] == "cancelled"
        mock_client.monitor.cancel.assert_called_once_with("mon_abc")


# =============================================================================
# events / trigger
# =============================================================================


class TestListMonitorEvents:
    def test_basic(self, mock_client):
        mock_client.monitor.events.return_value = _model(
            events=[{"event_id": "ev_1"}],
            next_cursor=None,
        )

        result = list_monitor_events("mon_abc")

        assert result["events"][0]["event_id"] == "ev_1"
        mock_client.monitor.events.assert_called_once_with("mon_abc")

    def test_passes_filters(self, mock_client):
        mock_client.monitor.events.return_value = _model(events=[], next_cursor=None)

        list_monitor_events(
            "mon_abc",
            cursor="cur_x",
            event_group_id="egrp_y",
            include_completions=True,
            limit=50,
        )

        args, kwargs = mock_client.monitor.events.call_args
        assert args == ("mon_abc",)
        assert kwargs == {
            "cursor": "cur_x",
            "event_group_id": "egrp_y",
            "include_completions": True,
            "limit": 50,
        }


class TestTriggerMonitor:
    def test_trigger(self, mock_client):
        trigger_monitor("mon_abc")

        mock_client.monitor.trigger.assert_called_once_with("mon_abc")


# =============================================================================
# CLI
# =============================================================================


class TestMonitorGroup:
    def test_help_lists_subcommands(self, runner):
        result = runner.invoke(main, ["monitor", "--help"])
        assert result.exit_code == 0
        for sub in ("create", "list", "get", "update", "cancel", "events", "trigger"):
            assert sub in result.output


class TestMonitorCreateCommand:
    def test_help(self, runner):
        result = runner.invoke(main, ["monitor", "create", "--help"])
        assert result.exit_code == 0
        assert "--frequency" in result.output
        assert "--type" in result.output
        assert "--webhook" in result.output

    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as patched:
            patched.return_value = {
                "monitor_id": "mon_test",
                "type": "event_stream",
                "frequency": "1d",
            }
            result = runner.invoke(main, ["monitor", "create", "track AI news"])

        assert result.exit_code == 0
        assert "mon_test" in result.output
        kwargs = patched.call_args.kwargs
        assert kwargs["query"] == "track AI news"
        assert kwargs["frequency"] == "1d"
        assert kwargs["type"] == "event_stream"

    def test_with_frequency_alias(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as patched:
            patched.return_value = {"monitor_id": "mon_hr", "frequency": "1h", "type": "event_stream"}
            result = runner.invoke(main, ["monitor", "create", "track stuff", "--frequency", "hourly"])

        assert result.exit_code == 0
        assert patched.call_args.kwargs["frequency"] == "hourly"

    def test_snapshot_requires_task_run_id(self, runner):
        result = runner.invoke(main, ["monitor", "create", "--type", "snapshot"])
        assert result.exit_code != 0

    def test_event_stream_requires_query(self, runner):
        result = runner.invoke(main, ["monitor", "create"])
        assert result.exit_code != 0

    def test_json_output(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as patched:
            patched.return_value = {"monitor_id": "mon_json"}
            result = runner.invoke(main, ["monitor", "create", "test", "--json"])

        assert result.exit_code == 0
        assert json.loads(result.output)["monitor_id"] == "mon_json"

    def test_invalid_metadata_json(self, runner):
        result = runner.invoke(main, ["monitor", "create", "test", "--metadata", "not-json"])
        assert result.exit_code != 0


class TestMonitorListCommand:
    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as patched:
            patched.return_value = {
                "monitors": [
                    {
                        "monitor_id": "mon_1",
                        "type": "event_stream",
                        "frequency": "1d",
                        "status": "active",
                        "settings": {"query": "test"},
                    }
                ],
                "next_cursor": None,
            }
            result = runner.invoke(main, ["monitor", "list"])

        assert result.exit_code == 0
        assert "mon_1" in result.output

    def test_empty(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as patched:
            patched.return_value = {"monitors": [], "next_cursor": None}
            result = runner.invoke(main, ["monitor", "list"])

        assert result.exit_code == 0
        assert "No monitors" in result.output

    def test_status_filter(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as patched:
            patched.return_value = {"monitors": [], "next_cursor": None}
            runner.invoke(main, ["monitor", "list", "--status", "active", "--status", "cancelled"])

        assert patched.call_args.kwargs["status"] == ["active", "cancelled"]


class TestMonitorGetCommand:
    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor") as patched:
            patched.return_value = {
                "monitor_id": "mon_abc",
                "type": "event_stream",
                "frequency": "1d",
                "status": "active",
                "processor": "lite",
                "settings": {"query": "track news"},
            }
            result = runner.invoke(main, ["monitor", "get", "mon_abc"])

        assert result.exit_code == 0
        assert "mon_abc" in result.output
        assert "track news" in result.output


class TestMonitorUpdateCommand:
    def test_no_fields_fails(self, runner):
        result = runner.invoke(main, ["monitor", "update", "mon_abc"])
        assert result.exit_code != 0

    def test_frequency(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.update_monitor") as patched:
            patched.return_value = {"monitor_id": "mon_abc"}
            result = runner.invoke(main, ["monitor", "update", "mon_abc", "--frequency", "1h"])

        assert result.exit_code == 0
        assert patched.call_args.kwargs["frequency"] == "1h"


class TestMonitorCancelCommand:
    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.cancel_monitor") as patched:
            patched.return_value = {"monitor_id": "mon_abc", "status": "cancelled"}
            result = runner.invoke(main, ["monitor", "cancel", "mon_abc"])

        assert result.exit_code == 0
        assert "mon_abc" in result.output


class TestMonitorEventsCommand:
    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as patched:
            patched.return_value = {
                "events": [
                    {
                        "event_type": "event_stream",
                        "event_group_id": "egrp_abc",
                        "event_id": "ev_1",
                        "event_date": "2026-05-01",
                        "output": {"content": "Price changed"},
                    }
                ],
                "next_cursor": None,
            }
            result = runner.invoke(main, ["monitor", "events", "mon_abc"])

        assert result.exit_code == 0
        assert "egrp_abc" in result.output

    def test_event_group_id_filter(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as patched:
            patched.return_value = {"events": [], "next_cursor": None}
            runner.invoke(main, ["monitor", "events", "mon_abc", "--event-group-id", "egrp_xyz"])

        assert patched.call_args.kwargs["event_group_id"] == "egrp_xyz"


class TestMonitorTriggerCommand:
    def test_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.trigger_monitor") as patched:
            patched.return_value = None
            result = runner.invoke(main, ["monitor", "trigger", "mon_abc"])

        assert result.exit_code == 0
        assert "mon_abc" in result.output

    def test_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.trigger_monitor") as patched:
            patched.return_value = None
            result = runner.invoke(main, ["monitor", "trigger", "mon_abc", "--json"])

        assert result.exit_code == 0
        assert json.loads(result.output)["triggered"] is True


class TestMonitorErrorHandling:
    def test_create_api_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as patched:
            patched.side_effect = Exception("API error")
            result = runner.invoke(main, ["monitor", "create", "test"])

        assert result.exit_code != 0

    def test_get_api_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor") as patched:
            patched.side_effect = Exception("Not found")
            result = runner.invoke(main, ["monitor", "get", "mon_bad"])

        assert result.exit_code != 0
