"""Tests for the Parallel Memory SDK integration and CLI."""

from __future__ import annotations

import datetime
import json
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.memory import (
    MemoryApiError,
    MemoryInputError,
    clear_memory,
    evict_memory,
    retrieve_memory,
)


class FakeModel:
    def __init__(self, data: dict) -> None:
        self.data = data

    def model_dump(self, mode: str = "python") -> dict:
        assert mode == "json"
        return self.data


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def memory_client():
    client = mock.MagicMock()
    client.beta.memory.retrieve.return_value = FakeModel({"results": []})
    with mock.patch("parallel_web_tools.core.memory.create_client", return_value=client) as create:
        yield client, create


class TestMemoryValidation:
    def test_public_package_exports_memory_operations(self):
        import parallel_web_tools

        assert parallel_web_tools.retrieve_memory is retrieve_memory
        assert parallel_web_tools.evict_memory is evict_memory
        assert parallel_web_tools.clear_memory is clear_memory

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"query": "x" * 501}, "query"),
            ({"limit": 0}, "limit"),
            ({"limit": 26}, "limit"),
            ({"since": "2026-07-15T17:30:00"}, "timezone"),
            ({"memory_scope_key": "contains spaces"}, "memory_scope_key"),
        ],
    )
    def test_rejects_invalid_retrieve_fields(self, kwargs, message):
        with pytest.raises(MemoryInputError, match=message):
            retrieve_memory(**kwargs)


class TestMemorySdk:
    def test_retrieve_passes_filtered_payload_to_sdk(self, memory_client):
        client, create = memory_client

        result = retrieve_memory(
            "serverless inference",
            25,
            kind="findall",
            since="2026-07-15T17:30:00Z",
            memory_scope_key="workspace_acme",
            api_key="test-api-key",
            source="cli",
        )

        assert result == {"results": []}
        create.assert_called_once_with("test-api-key", "cli")
        client.beta.memory.retrieve.assert_called_once_with(
            query="serverless inference",
            limit=25,
            kind="findall",
            since="2026-07-15T17:30:00Z",
            memory_scope_key="workspace_acme",
        )

    def test_retrieve_serializes_datetime_for_sdk(self, memory_client):
        client, _ = memory_client
        since = datetime.datetime(2026, 7, 15, 17, 30, tzinfo=datetime.timezone.utc)

        retrieve_memory(since=since)

        assert client.beta.memory.retrieve.call_args.kwargs["since"] == "2026-07-15T17:30:00+00:00"

    def test_retrieve_rejects_malformed_sdk_response(self, memory_client):
        client, _ = memory_client
        client.beta.memory.retrieve.return_value = FakeModel({"unexpected": []})

        with pytest.raises(MemoryApiError, match="unexpected response"):
            retrieve_memory()

    def test_evict_calls_sdk_and_returns_stable_acknowledgement(self, memory_client):
        client, _ = memory_client

        result = evict_memory("task", "trun_example", memory_scope_key="workspace_acme")

        assert result == {"ok": True, "action": "evict"}
        client.beta.memory.evict.assert_called_once_with(
            kind="task",
            id="trun_example",
            memory_scope_key="workspace_acme",
        )

    def test_clear_personal_memory_calls_sdk_without_scope(self, memory_client):
        client, _ = memory_client

        result = clear_memory()

        assert result == {"ok": True, "action": "clear"}
        client.beta.memory.clear.assert_called_once_with()


class TestMemoryCli:
    def test_help_lists_all_operations(self, runner):
        result = runner.invoke(main, ["memory", "--help"])

        assert result.exit_code == 0
        assert "Search and manage saved Task, Monitor, and FindAll entries." in result.output
        assert "clear     Remove all entries from selected Memory." in result.output
        assert "evict     Remove one entry from Memory." in result.output
        assert "retrieve  Search Memory or list recent entries." in result.output

    def test_retrieve_help_marks_query_as_optional(self, runner):
        result = runner.invoke(main, ["memory", "retrieve", "--help"])

        assert result.exit_code == 0
        assert "[OPTIONS] [QUERY]" in result.output

    def test_retrieve_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.retrieve_memory") as retrieve:
            retrieve.return_value = {
                "results": [
                    {
                        "kind": "task",
                        "id": "trun_example",
                        "updated_at": "2026-07-29T18:20:00Z",
                        "input_excerpt": "Research vendors",
                        "output_excerpt": "Prior findings",
                    }
                ]
            }
            result = runner.invoke(
                main,
                [
                    "memory",
                    "retrieve",
                    "--query",
                    "serverless inference",
                    "--limit",
                    "5",
                    "--kind",
                    "task",
                    "--scope-key",
                    "workspace_acme",
                    "--json",
                ],
            )

        assert result.exit_code == 0
        assert json.loads(result.output)["results"][0]["id"] == "trun_example"
        retrieve.assert_called_once_with(
            query="serverless inference",
            limit=5,
            kind="task",
            since=None,
            memory_scope_key="workspace_acme",
            source="cli",
        )

    def test_retrieve_human_output(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.retrieve_memory") as retrieve:
            retrieve.return_value = {
                "results": [
                    {
                        "kind": "findall",
                        "id": "findall_example",
                        "updated_at": "2026-07-29T18:20:00Z",
                        "input_excerpt": "Find vendors",
                        "matched_count": 17,
                    }
                ]
            }
            result = runner.invoke(main, ["memory", "retrieve", "inference vendors"])

        assert result.exit_code == 0
        assert "Found 1 entry in Memory." in result.output
        assert "findall_example" in result.output
        assert "17" in result.output

    def test_retrieve_empty_human_output(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.retrieve_memory") as retrieve:
            retrieve.return_value = {"results": []}
            result = runner.invoke(main, ["memory", "retrieve", "inference vendors"])

        assert result.exit_code == 0
        assert "No Memory entries found." in result.output

    def test_retrieve_renders_monitor_event_id(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.retrieve_memory") as retrieve:
            retrieve.return_value = {
                "results": [
                    {
                        "kind": "monitor",
                        "id": "monitor_example",
                        "updated_at": "2026-07-29T18:20:00Z",
                        "status": "active",
                        "matched_events": [
                            {
                                "event_id": "mevt_example",
                                "detected_at": "2026-07-29T18:15:00Z",
                                "excerpt": "Pricing changed",
                            }
                        ],
                    }
                ]
            }
            result = runner.invoke(main, ["memory", "retrieve", "pricing changes"])

        assert result.exit_code == 0
        assert "Event mevt_example" in result.output
        assert "Event unknown" not in result.output

    def test_evict_json(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.evict_memory") as evict:
            evict.return_value = {"ok": True, "action": "evict"}
            result = runner.invoke(
                main,
                ["memory", "evict", "--kind", "task", "--id", "trun_example", "--json"],
            )

        assert result.exit_code == 0
        assert json.loads(result.output) == {"ok": True, "action": "evict"}

    def test_evict_human_output_uses_entry_terminology(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.evict_memory") as evict:
            evict.return_value = {"ok": True, "action": "evict"}
            result = runner.invoke(
                main,
                ["memory", "evict", "--kind", "task", "--id", "trun_example"],
            )

        assert result.exit_code == 0
        assert "Removed task entry trun_example from personal Memory." in result.output
        assert "underlying Task, Monitor, or FindAll resource was not deleted" in result.output

    def test_clear_requires_explicit_confirmation(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.clear_memory") as clear:
            result = runner.invoke(main, ["memory", "clear", "--scope-key", "workspace_acme", "--json"])

        assert result.exit_code == 2
        assert "confirm-clear" in json.loads(result.output)["error"]["message"]
        clear.assert_not_called()

    def test_clear_confirmed(self, runner):
        with mock.patch("parallel_web_tools.cli.commands.clear_memory") as clear:
            clear.return_value = {"ok": True, "action": "clear"}
            result = runner.invoke(
                main,
                ["memory", "clear", "--scope-key", "workspace_acme", "--confirm-clear", "--json"],
            )

        assert result.exit_code == 0
        assert json.loads(result.output) == {"ok": True, "action": "clear"}
        clear.assert_called_once_with(memory_scope_key="workspace_acme", source="cli")
