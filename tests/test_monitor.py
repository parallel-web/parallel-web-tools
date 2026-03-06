"""Tests for the Monitor web tracking functionality."""

import json
from unittest import mock

import httpx
import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.monitor import (
    BASE_URL,
    MONITOR_CADENCES,
    MONITOR_EVENT_TYPES,
    _request,
    create_monitor,
    delete_monitor,
    get_monitor,
    get_monitor_event_group,
    list_monitor_events,
    list_monitors,
    simulate_monitor_event,
    update_monitor,
)


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_httpx():
    """Mock httpx.request for monitor API calls."""
    with mock.patch("parallel_web_tools.core.monitor.httpx") as m:
        yield m


@pytest.fixture
def mock_resolve_api_key():
    """Mock resolve_api_key to return a test key."""
    with mock.patch("parallel_web_tools.core.monitor.resolve_api_key", return_value="test-key"):
        yield


def _make_response(status_code=200, json_data=None):
    """Helper to build a mock httpx Response."""
    resp = mock.MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = json.dumps(json_data).encode() if json_data is not None else b""
    resp.json.return_value = json_data if json_data is not None else {}
    resp.raise_for_status.return_value = None
    return resp


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module-level constants."""

    def test_cadences_exist(self):
        assert "daily" in MONITOR_CADENCES
        assert "hourly" in MONITOR_CADENCES
        assert len(MONITOR_CADENCES) > 0

    def test_event_types_exist(self):
        assert "monitor.event.detected" in MONITOR_EVENT_TYPES
        assert "monitor.execution.completed" in MONITOR_EVENT_TYPES
        assert "monitor.execution.failed" in MONITOR_EVENT_TYPES
        assert len(MONITOR_EVENT_TYPES) > 0

    def test_base_url(self):
        assert BASE_URL == "https://api.parallel.ai"


# =============================================================================
# Core Function Tests
# =============================================================================


class TestRequest:
    """Tests for the _request helper."""

    def test_sends_auth_header(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"ok": True})

        _request("GET", "/v1alpha/monitors")

        call_kwargs = mock_httpx.request.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert headers["x-api-key"] == "test-key"

    def test_uses_correct_url(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})

        _request("GET", "/v1alpha/monitors")

        call_args = mock_httpx.request.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == f"{BASE_URL}/v1alpha/monitors"

    def test_passes_json_body(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})
        body = {"query": "test", "cadence": "daily"}

        _request("POST", "/v1alpha/monitors", json=body)

        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("json") == body

    def test_passes_query_params(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})

        _request("GET", "/v1alpha/monitors", params={"limit": 10})

        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("params") == {"limit": 10}

    def test_raises_on_http_error(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=mock.MagicMock(), response=mock.MagicMock()
        )

        with pytest.raises(httpx.HTTPStatusError):
            _request("GET", "/v1alpha/monitors/bad_id")


class TestCreateMonitor:
    """Tests for create_monitor."""

    def test_basic_create(self, mock_httpx, mock_resolve_api_key):
        expected = {"monitor_id": "mon_123", "query": "track AI news", "cadence": "daily"}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = create_monitor("track AI news", "daily")

        assert result["monitor_id"] == "mon_123"
        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert body["query"] == "track AI news"
        assert body["cadence"] == "daily"

    def test_create_with_all_options(self, mock_httpx, mock_resolve_api_key):
        expected = {"monitor_id": "mon_456"}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = create_monitor(
            "track stuff",
            "hourly",
            webhook="https://hook.example.com",
            metadata={"project": "test"},
            output_schema={"type": "object"},
        )

        assert result["monitor_id"] == "mon_456"
        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert body["webhook"] == {"url": "https://hook.example.com", "event_types": ["monitor.event.detected"]}
        assert body["metadata"] == {"project": "test"}
        assert body["output_schema"] == {"type": "object"}

    def test_create_omits_none_fields(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitor_id": "mon_x"})

        create_monitor("query", "daily")

        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert "webhook" not in body
        assert "metadata" not in body
        assert "output_schema" not in body


class TestListMonitors:
    """Tests for list_monitors."""

    def test_list_basic(self, mock_httpx, mock_resolve_api_key):
        monitors = [{"monitor_id": "mon_1"}, {"monitor_id": "mon_2"}]
        mock_httpx.request.return_value = _make_response(200, {"monitors": monitors})

        result = list_monitors()

        assert len(result) == 2
        assert result[0]["monitor_id"] == "mon_1"

    def test_list_with_limit(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitors": []})

        list_monitors(limit=5)

        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("params") == {"limit": 5}

    def test_list_with_cursor(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitors": []})

        list_monitors(monitor_id="mon_cursor")

        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("params")["monitor_id"] == "mon_cursor"

    def test_list_handles_array_response(self, mock_httpx, mock_resolve_api_key):
        monitors = [{"monitor_id": "mon_1"}]
        mock_httpx.request.return_value = _make_response(200, monitors)

        result = list_monitors()

        assert len(result) == 1


class TestGetMonitor:
    """Tests for get_monitor."""

    def test_get_by_id(self, mock_httpx, mock_resolve_api_key):
        expected = {"monitor_id": "mon_abc", "query": "test", "cadence": "daily"}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = get_monitor("mon_abc")

        assert result["monitor_id"] == "mon_abc"
        call_args = mock_httpx.request.call_args
        assert "/v1alpha/monitors/mon_abc" in call_args[0][1]


class TestUpdateMonitor:
    """Tests for update_monitor."""

    def test_update_query(self, mock_httpx, mock_resolve_api_key):
        expected = {"monitor_id": "mon_abc", "query": "new query"}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = update_monitor("mon_abc", query="new query")

        assert result["query"] == "new query"
        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert body["query"] == "new query"

    def test_update_cadence(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitor_id": "mon_abc"})

        update_monitor("mon_abc", cadence="hourly")

        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert body["cadence"] == "hourly"

    def test_update_omits_none_fields(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitor_id": "mon_abc"})

        update_monitor("mon_abc", query="only query")

        call_kwargs = mock_httpx.request.call_args
        body = call_kwargs.kwargs.get("json")
        assert "cadence" not in body
        assert "webhook" not in body


class TestDeleteMonitor:
    """Tests for delete_monitor."""

    def test_delete(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"monitor_id": "mon_del", "deleted": True})

        result = delete_monitor("mon_del")

        assert result["deleted"] is True
        call_args = mock_httpx.request.call_args
        assert call_args[0][0] == "DELETE"

    def test_delete_204_no_content(self, mock_httpx, mock_resolve_api_key):
        resp = _make_response(204)
        resp.content = b""
        mock_httpx.request.return_value = resp

        result = delete_monitor("mon_del")

        assert result["monitor_id"] == "mon_del"
        assert result["deleted"] is True


class TestListMonitorEvents:
    """Tests for list_monitor_events."""

    def test_list_events_default_lookback(self, mock_httpx, mock_resolve_api_key):
        expected = {"events": [{"event_id": "ev_1"}]}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = list_monitor_events("mon_abc")

        assert len(result["events"]) == 1
        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("params") == {"lookback_period": "10d"}

    def test_list_events_custom_lookback(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {"events": []})

        list_monitor_events("mon_abc", lookback_period="24h")

        call_kwargs = mock_httpx.request.call_args
        assert call_kwargs.kwargs.get("params") == {"lookback_period": "24h"}


class TestGetMonitorEventGroup:
    """Tests for get_monitor_event_group."""

    def test_get_event_group(self, mock_httpx, mock_resolve_api_key):
        expected = {"event_group_id": "eg_123", "type": "event"}
        mock_httpx.request.return_value = _make_response(200, expected)

        result = get_monitor_event_group("mon_abc", "eg_123")

        assert result["event_group_id"] == "eg_123"
        call_args = mock_httpx.request.call_args
        assert "/v1alpha/monitors/mon_abc/event_groups/eg_123" in call_args[0][1]


class TestSimulateMonitorEvent:
    """Tests for simulate_monitor_event."""

    def test_simulate_basic(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})

        simulate_monitor_event("mon_abc")

        call_args = mock_httpx.request.call_args
        assert call_args[0][0] == "POST"
        assert "/simulate_event" in call_args[0][1]

    def test_simulate_with_event_type(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})

        simulate_monitor_event("mon_abc", event_type="monitor.event.detected")

        call_kwargs = mock_httpx.request.call_args
        params = call_kwargs.kwargs.get("params")
        assert params["event_type"] == "monitor.event.detected"

    def test_simulate_returns_none(self, mock_httpx, mock_resolve_api_key):
        mock_httpx.request.return_value = _make_response(200, {})

        result = simulate_monitor_event("mon_abc")

        assert result is None


# =============================================================================
# CLI Command Tests
# =============================================================================


class TestMonitorGroup:
    """Tests for the monitor command group."""

    def test_monitor_help(self, runner):
        result = runner.invoke(main, ["monitor", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "list" in result.output
        assert "get" in result.output
        assert "update" in result.output
        assert "delete" in result.output
        assert "events" in result.output
        assert "event-group" in result.output
        assert "simulate" in result.output

    def test_monitor_in_main_help(self, runner):
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "monitor" in result.output


class TestMonitorCreateCommand:
    """Tests for the monitor create CLI command."""

    def test_create_help(self, runner):
        result = runner.invoke(main, ["monitor", "create", "--help"])
        assert result.exit_code == 0
        assert "--cadence" in result.output
        assert "--webhook" in result.output
        assert "--json" in result.output

    def test_create_no_query(self, runner):
        result = runner.invoke(main, ["monitor", "create"])
        assert result.exit_code != 0

    def test_create_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.return_value = {
                "monitor_id": "mon_test",
                "query": "track AI news",
                "cadence": "daily",
            }

            result = runner.invoke(main, ["monitor", "create", "track AI news"])

            assert result.exit_code == 0
            assert "mon_test" in result.output
            mock_create.assert_called_once()

    def test_create_with_cadence(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.return_value = {"monitor_id": "mon_hr"}

            result = runner.invoke(main, ["monitor", "create", "track stuff", "--cadence", "hourly"])

            assert result.exit_code == 0
            call_kwargs = mock_create.call_args.kwargs
            assert call_kwargs["cadence"] == "hourly"

    def test_create_json_output(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.return_value = {
                "monitor_id": "mon_json",
                "query": "test",
                "cadence": "daily",
            }

            result = runner.invoke(main, ["monitor", "create", "test", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["monitor_id"] == "mon_json"

    def test_create_with_webhook(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.return_value = {"monitor_id": "mon_wh"}

            result = runner.invoke(main, ["monitor", "create", "test", "--webhook", "https://hook.example.com"])

            assert result.exit_code == 0
            call_kwargs = mock_create.call_args.kwargs
            assert call_kwargs["webhook"] == "https://hook.example.com"

    def test_create_invalid_metadata_json(self, runner):
        result = runner.invoke(main, ["monitor", "create", "test", "--metadata", "not-json"])
        assert result.exit_code != 0

    def test_create_saves_to_output_file(self, runner, tmp_path):
        output_file = tmp_path / "monitor.json"

        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.return_value = {"monitor_id": "mon_file", "query": "test", "cadence": "daily"}

            result = runner.invoke(main, ["monitor", "create", "test", "-o", str(output_file)])

            assert result.exit_code == 0
            assert output_file.exists()
            data = json.loads(output_file.read_text())
            assert data["monitor_id"] == "mon_file"


class TestMonitorListCommand:
    """Tests for the monitor list CLI command."""

    def test_list_help(self, runner):
        result = runner.invoke(main, ["monitor", "list", "--help"])
        assert result.exit_code == 0
        assert "--limit" in result.output

    def test_list_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as mock_list:
            mock_list.return_value = [
                {"monitor_id": "mon_1", "query": "test", "cadence": "daily", "status": "active"},
            ]

            result = runner.invoke(main, ["monitor", "list"])

            assert result.exit_code == 0
            assert "mon_1" in result.output

    def test_list_empty(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as mock_list:
            mock_list.return_value = []

            result = runner.invoke(main, ["monitor", "list"])

            assert result.exit_code == 0
            assert "No monitors found" in result.output

    def test_list_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as mock_list:
            monitors = [{"monitor_id": "mon_1"}, {"monitor_id": "mon_2"}]
            mock_list.return_value = monitors

            result = runner.invoke(main, ["monitor", "list", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert len(output) == 2

    def test_list_with_limit(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as mock_list:
            mock_list.return_value = []

            runner.invoke(main, ["monitor", "list", "--limit", "5"])

            call_kwargs = mock_list.call_args.kwargs
            assert call_kwargs["limit"] == 5


class TestMonitorGetCommand:
    """Tests for the monitor get CLI command."""

    def test_get_help(self, runner):
        result = runner.invoke(main, ["monitor", "get", "--help"])
        assert result.exit_code == 0
        assert "MONITOR_ID" in result.output

    def test_get_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor") as mock_get:
            mock_get.return_value = {
                "monitor_id": "mon_abc",
                "query": "track news",
                "cadence": "daily",
                "status": "active",
            }

            result = runner.invoke(main, ["monitor", "get", "mon_abc"])

            assert result.exit_code == 0
            assert "mon_abc" in result.output
            assert "track news" in result.output

    def test_get_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor") as mock_get:
            mock_get.return_value = {"monitor_id": "mon_abc", "query": "test"}

            result = runner.invoke(main, ["monitor", "get", "mon_abc", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["monitor_id"] == "mon_abc"


class TestMonitorUpdateCommand:
    """Tests for the monitor update CLI command."""

    def test_update_help(self, runner):
        result = runner.invoke(main, ["monitor", "update", "--help"])
        assert result.exit_code == 0
        assert "--query" in result.output
        assert "--cadence" in result.output

    def test_update_no_fields(self, runner):
        result = runner.invoke(main, ["monitor", "update", "mon_abc"])
        assert result.exit_code != 0

    def test_update_query(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.update_monitor") as mock_update:
            mock_update.return_value = {"monitor_id": "mon_abc", "query": "new query"}

            result = runner.invoke(main, ["monitor", "update", "mon_abc", "--query", "new query"])

            assert result.exit_code == 0
            assert "updated" in result.output.lower()

    def test_update_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.update_monitor") as mock_update:
            mock_update.return_value = {"monitor_id": "mon_abc"}

            result = runner.invoke(main, ["monitor", "update", "mon_abc", "--cadence", "hourly", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["monitor_id"] == "mon_abc"


class TestMonitorDeleteCommand:
    """Tests for the monitor delete CLI command."""

    def test_delete_help(self, runner):
        result = runner.invoke(main, ["monitor", "delete", "--help"])
        assert result.exit_code == 0

    def test_delete_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.delete_monitor") as mock_del:
            mock_del.return_value = {"monitor_id": "mon_abc", "deleted": True}

            result = runner.invoke(main, ["monitor", "delete", "mon_abc"])

            assert result.exit_code == 0
            assert "mon_abc" in result.output

    def test_delete_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.delete_monitor") as mock_del:
            mock_del.return_value = {"monitor_id": "mon_abc", "deleted": True}

            result = runner.invoke(main, ["monitor", "delete", "mon_abc", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["deleted"] is True


class TestMonitorEventsCommand:
    """Tests for the monitor events CLI command."""

    def test_events_help(self, runner):
        result = runner.invoke(main, ["monitor", "events", "--help"])
        assert result.exit_code == 0
        assert "--lookback" in result.output

    def test_events_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as mock_events:
            mock_events.return_value = {
                "events": [
                    {
                        "type": "event",
                        "event_group_id": "mevtgrp_abc123",
                        "output": "Price changed",
                        "event_date": "2026-01-01",
                        "source_urls": ["https://example.com"],
                    },
                ]
            }

            result = runner.invoke(main, ["monitor", "events", "mon_abc"])

            assert result.exit_code == 0
            assert "mevtgrp_abc123" in result.output

    def test_events_empty(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as mock_events:
            mock_events.return_value = {"events": []}

            result = runner.invoke(main, ["monitor", "events", "mon_abc"])

            assert result.exit_code == 0
            assert "No events found" in result.output

    def test_events_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as mock_events:
            mock_events.return_value = {"events": [{"event_id": "ev_1"}]}

            result = runner.invoke(main, ["monitor", "events", "mon_abc", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert len(output["events"]) == 1

    def test_events_custom_lookback(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as mock_events:
            mock_events.return_value = {"events": []}

            runner.invoke(main, ["monitor", "events", "mon_abc", "--lookback", "24h"])

            call_kwargs = mock_events.call_args.kwargs
            assert call_kwargs["lookback_period"] == "24h"

    def test_events_saves_to_file(self, runner, tmp_path):
        output_file = tmp_path / "events.json"

        with mock.patch("parallel_web_tools.cli.commands.list_monitor_events") as mock_events:
            mock_events.return_value = {"events": [{"event_id": "ev_1"}]}

            result = runner.invoke(main, ["monitor", "events", "mon_abc", "-o", str(output_file)])

            assert result.exit_code == 0
            assert output_file.exists()


class TestMonitorEventGroupCommand:
    """Tests for the monitor event-group CLI command."""

    def test_event_group_help(self, runner):
        result = runner.invoke(main, ["monitor", "event-group", "--help"])
        assert result.exit_code == 0

    def test_event_group_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor_event_group") as mock_eg:
            mock_eg.return_value = {
                "event_group_id": "eg_123",
                "events": [],
            }

            result = runner.invoke(main, ["monitor", "event-group", "mon_abc", "eg_123"])

            assert result.exit_code == 0
            assert "eg_123" in result.output

    def test_event_group_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor_event_group") as mock_eg:
            mock_eg.return_value = {"event_group_id": "eg_123"}

            result = runner.invoke(main, ["monitor", "event-group", "mon_abc", "eg_123", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["event_group_id"] == "eg_123"


class TestMonitorSimulateCommand:
    """Tests for the monitor simulate CLI command."""

    def test_simulate_help(self, runner):
        result = runner.invoke(main, ["monitor", "simulate", "--help"])
        assert result.exit_code == 0
        assert "--event-type" in result.output

    def test_simulate_basic(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.simulate_monitor_event") as mock_sim:
            mock_sim.return_value = None

            result = runner.invoke(main, ["monitor", "simulate", "mon_abc"])

            assert result.exit_code == 0
            assert "simulated" in result.output.lower()

    def test_simulate_with_event_type(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.simulate_monitor_event") as mock_sim:
            mock_sim.return_value = None

            result = runner.invoke(main, ["monitor", "simulate", "mon_abc", "--event-type", "monitor.event.detected"])

            assert result.exit_code == 0
            call_kwargs = mock_sim.call_args.kwargs
            assert call_kwargs["event_type"] == "monitor.event.detected"

    def test_simulate_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.simulate_monitor_event") as mock_sim:
            mock_sim.return_value = None

            result = runner.invoke(main, ["monitor", "simulate", "mon_abc", "--json"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["simulated"] is True


class TestMonitorErrorHandling:
    """Tests for error handling across monitor commands."""

    def test_create_api_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.side_effect = Exception("API error")

            result = runner.invoke(main, ["monitor", "create", "test"])

            assert result.exit_code != 0

    def test_create_api_error_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.create_monitor") as mock_create:
            mock_create.side_effect = Exception("API error")

            result = runner.invoke(main, ["monitor", "create", "test", "--json"])

            assert result.exit_code != 0
            output = json.loads(result.output)
            assert "error" in output

    def test_get_api_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.get_monitor") as mock_get:
            mock_get.side_effect = Exception("Not found")

            result = runner.invoke(main, ["monitor", "get", "mon_bad"])

            assert result.exit_code != 0

    def test_list_api_error(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.list_monitors") as mock_list:
            mock_list.side_effect = Exception("Unauthorized")

            result = runner.invoke(main, ["monitor", "list"])

            assert result.exit_code != 0
