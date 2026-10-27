"""Tests for sending feedback to Parallel from the CLI."""

from __future__ import annotations

import json
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.feedback import FeedbackInputError, parse_example, send_feedback

_MESSAGE = "Search results for our eval queries are mostly irrelevant."
_RECEIVED = {
    "status": "received",
    "feedback_id": "feedback_1",
    "duplicate": False,
    "report": {
        "message": _MESSAGE,
        "category": "results_quality",
        "product": "search",
        "severity": "degraded",
        "examples": [{"id": "search_abc", "issue": "Off topic", "expected": None}],
        "expected": None,
        "actual": None,
        "context": {"org_id": "org", "sender": "user u1 (personal credential)", "surface": "cli"},
        "redactions": 0,
        "recipient": "Parallel's team only.",
    },
    "next_step": "Sent.",
}


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def client():
    client = mock.MagicMock()
    with mock.patch("parallel_web_tools.core.feedback.create_client", return_value=client):
        yield client


def _sent_bodies(client) -> list[dict]:
    return [call.kwargs["body"] for call in client.post.call_args_list]


class TestValidation:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"message": "too short"}, "message"),
            ({"category": "praise"}, "category"),
            ({"product": "spreadsheet"}, "product"),
            ({"severity": "urgent"}, "severity"),
            ({"examples": [{"id": f"trun_{n}"} for n in range(51)]}, "examples"),
        ],
    )
    def test_rejects_invalid_reports_before_calling_the_api(self, client, kwargs, message):
        fields: dict[str, Any] = {"message": _MESSAGE, "category": "results_quality", "product": "search", **kwargs}
        with pytest.raises(FeedbackInputError, match=message):
            send_feedback(**fields)
        client.post.assert_not_called()

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("trun_abc", {"id": "trun_abc"}),
            ("search_abc:Returned: nothing useful", {"id": "search_abc", "issue": "Returned: nothing useful"}),
        ],
    )
    def test_parses_examples(self, raw, expected):
        assert parse_example(raw) == expected

    def test_rejects_an_example_that_is_not_an_id(self):
        with pytest.raises(FeedbackInputError, match="example"):
            parse_example("not an id:note")


class TestApi:
    def test_send_posts_only_the_fields_given(self, client):
        client.post.return_value = _RECEIVED
        result = send_feedback(
            message=_MESSAGE,
            category="results_quality",
            product="search",
            examples=[{"id": "search_abc", "issue": "Off topic"}],
            source="cli",
        )
        assert result == _RECEIVED
        client.post.assert_called_once_with(
            path="/v1beta/feedback",
            body={
                "message": _MESSAGE,
                "category": "results_quality",
                "product": "search",
                "severity": "degraded",
                "examples": [{"id": "search_abc", "issue": "Off topic"}],
            },
            cast_to=dict,
        )


class TestSendCommand:
    _ARGS = ["feedback", "send", "-m", _MESSAGE, "--category", "results_quality", "--product", "search"]

    def test_json_sends_once_and_prints_the_response(self, runner, client):
        client.post.return_value = _RECEIVED
        result = runner.invoke(main, [*self._ARGS, "--example", "search_abc:Off topic", "--json"])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["feedback_id"] == "feedback_1"
        assert len(_sent_bodies(client)) == 1

    def test_shows_what_parallel_received(self, runner, client):
        client.post.return_value = _RECEIVED
        result = runner.invoke(main, self._ARGS)
        assert result.exit_code == 0, result.output
        assert len(_sent_bodies(client)) == 1
        assert "feedback_1" in result.output
        assert _MESSAGE in result.output
        assert "search_abc" in result.output

    def test_a_repeat_says_so(self, runner, client):
        client.post.return_value = {**_RECEIVED, "duplicate": True}
        result = runner.invoke(main, self._ARGS)
        assert result.exit_code == 0, result.output
        assert "already received" in result.output

    def test_a_bad_example_is_a_usage_error(self, runner, client):
        result = runner.invoke(main, [*self._ARGS, "--example", "has spaces", "--json"])
        assert result.exit_code != 0
        client.post.assert_not_called()

    def test_there_is_no_consent_command(self, runner, client):
        result = runner.invoke(main, ["feedback", "consent"])
        assert result.exit_code != 0
