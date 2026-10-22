"""Tests for sending feedback to Parallel from the CLI."""

from __future__ import annotations

import json
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner

from parallel_web_tools.cli.commands import main
from parallel_web_tools.core.feedback import (
    FeedbackInputError,
    get_feedback_consent,
    parse_example,
    send_feedback,
    set_feedback_consent,
)

_MESSAGE = "Search results for our eval queries are mostly irrelevant."
_PREVIEW = {
    "status": "consent_required",
    "feedback_id": None,
    "preview_id": "p1",
    "duplicate": False,
    "consent_preference": "ask",
    "preview": {
        "message": _MESSAGE,
        "category": "results_quality",
        "product": "search",
        "severity": "degraded",
        "examples": [{"id": "search_abc", "link": "unchecked", "issue": "Off topic", "expected": None}],
        "expected": None,
        "actual": None,
        "context": {"org_id": "org", "sender": "user u1 (personal credential)", "surface": "cli"},
        "redactions": 0,
        "recipient": "Parallel's team only.",
        "remember_scope": "Parallel user u1, on any of their personal credentials",
    },
    "next_step": "Nothing was sent.",
}
_RECEIVED = {**_PREVIEW, "status": "received", "feedback_id": "feedback_1", "next_step": "Sent."}


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
            ({"user_consent": "maybe"}, "user_consent"),
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

    def test_only_ask_or_always_can_be_stored(self, client):
        with pytest.raises(FeedbackInputError, match="preference"):
            set_feedback_consent("sometimes")
        client.put.assert_not_called()


class TestApi:
    def test_send_posts_only_the_fields_given(self, client):
        client.post.return_value = _PREVIEW
        result = send_feedback(
            message=_MESSAGE,
            category="results_quality",
            product="search",
            examples=[{"id": "search_abc", "issue": "Off topic"}],
            source="cli",
        )
        assert result == _PREVIEW
        client.post.assert_called_once_with(
            path="/v1beta/feedback",
            body={
                "message": _MESSAGE,
                "category": "results_quality",
                "product": "search",
                "severity": "degraded",
                "examples": [{"id": "search_abc", "issue": "Off topic"}],
                "user_consent": "not_asked",
            },
            cast_to=dict,
        )

    def test_consent_round_trip(self, client):
        client.get.return_value = {"preference": "ask", "scope": None}
        client.put.return_value = {"preference": "always", "scope": "Parallel user u1"}
        assert get_feedback_consent()["preference"] == "ask"
        assert set_feedback_consent("always")["preference"] == "always"
        client.put.assert_called_once_with(path="/v1beta/feedback/consent", body={"preference": "always"}, cast_to=dict)


class TestSendCommand:
    _ARGS = ["feedback", "send", "-m", _MESSAGE, "--category", "results_quality", "--product", "search"]

    def test_json_without_consent_returns_the_preview_and_sends_nothing(self, runner, client):
        client.post.return_value = _PREVIEW
        result = runner.invoke(main, [*self._ARGS, "--example", "search_abc:Off topic", "--json"])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["status"] == "consent_required"
        assert [body["user_consent"] for body in _sent_bodies(client)] == ["not_asked"]

    def test_an_agent_sends_with_the_users_consent_and_preview_id(self, runner, client):
        client.post.return_value = _RECEIVED
        result = runner.invoke(main, [*self._ARGS, "--consent", "this_report", "--preview-id", "p1", "--json"])
        assert result.exit_code == 0, result.output
        [body] = _sent_bodies(client)
        assert body["user_consent"] == "this_report"
        assert body["preview_id"] == "p1"

    @pytest.mark.parametrize(("answer", "consent"), [("s", "this_report"), ("a", "always")])
    def test_a_person_approves_the_preview_at_the_prompt(self, runner, client, answer, consent):
        client.post.side_effect = [_PREVIEW, _RECEIVED]
        result = runner.invoke(main, self._ARGS, input=f"{answer}\n")
        assert result.exit_code == 0, result.output
        assert _MESSAGE in result.output
        assert "feedback_1" in result.output
        second = _sent_bodies(client)[1]
        assert second["user_consent"] == consent
        assert second["preview_id"] == "p1"

    def test_declining_at_the_prompt_sends_nothing(self, runner, client):
        client.post.return_value = _PREVIEW
        result = runner.invoke(main, self._ARGS, input="n\n")
        assert result.exit_code == 0, result.output
        assert len(_sent_bodies(client)) == 1
        assert "Nothing was sent" in result.output

    def test_a_stored_always_sends_on_the_first_call(self, runner, client):
        client.post.return_value = {**_RECEIVED, "consent_preference": "always"}
        result = runner.invoke(main, self._ARGS)
        assert result.exit_code == 0, result.output
        assert len(_sent_bodies(client)) == 1
        assert "feedback_1" in result.output

    def test_a_bad_example_is_a_usage_error(self, runner, client):
        result = runner.invoke(main, [*self._ARGS, "--example", "has spaces", "--json"])
        assert result.exit_code != 0
        client.post.assert_not_called()


class TestConsentCommand:
    def test_shows_the_preference(self, runner, client):
        client.get.return_value = {"preference": "always", "scope": "Parallel user u1"}
        result = runner.invoke(main, ["feedback", "consent", "--json"])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["preference"] == "always"

    def test_resets_to_ask(self, runner, client):
        client.put.return_value = {"preference": "ask", "scope": "Parallel user u1"}
        result = runner.invoke(main, ["feedback", "consent", "ask"])
        assert result.exit_code == 0, result.output
        client.put.assert_called_once_with(path="/v1beta/feedback/consent", body={"preference": "ask"}, cast_to=dict)
