"""Tests for the shared polling utility."""

from unittest import mock

import pytest

from parallel_web_tools.core.polling import TERMINAL_STATUSES, poll_until


class TestTerminalStatuses:
    """Tests for the TERMINAL_STATUSES constant."""

    def test_contains_expected_statuses(self):
        assert "completed" in TERMINAL_STATUSES
        assert "failed" in TERMINAL_STATUSES
        assert "cancelled" in TERMINAL_STATUSES

    def test_is_tuple(self):
        assert isinstance(TERMINAL_STATUSES, tuple)


class TestPollUntilImmediateCompletion:
    """Tests for poll_until when the task completes on the first check."""

    def test_returns_result_immediately(self):
        result = poll_until(
            retrieve=lambda: {"status": "completed"},
            extract_status=lambda r: r["status"],
            fetch_result=lambda: {"data": "done"},
            format_error=lambda r, s: f"Error: {s}",
        )
        assert result == {"data": "done"}

    def test_does_not_sleep_on_immediate_completion(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep") as mock_sleep:
            poll_until(
                retrieve=lambda: {"status": "completed"},
                extract_status=lambda r: r["status"],
                fetch_result=lambda: "ok",
                format_error=lambda r, s: "",
            )
        mock_sleep.assert_not_called()


class TestPollUntilRetryThenComplete:
    """Tests for poll_until when the task completes after retries."""

    def test_retries_then_succeeds(self):
        responses = iter(
            [
                {"status": "running"},
                {"status": "running"},
                {"status": "completed"},
            ]
        )

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = poll_until(
                retrieve=lambda: next(responses),
                extract_status=lambda r: r["status"],
                fetch_result=lambda: {"data": "final"},
                format_error=lambda r, s: "",
                poll_interval=1,
                timeout=60,
            )

        assert result == {"data": "final"}

    def test_sleeps_between_retries(self):
        responses = iter(
            [
                {"status": "pending"},
                {"status": "completed"},
            ]
        )

        with mock.patch("parallel_web_tools.core.polling.time.sleep") as mock_sleep:
            poll_until(
                retrieve=lambda: next(responses),
                extract_status=lambda r: r["status"],
                fetch_result=lambda: "ok",
                format_error=lambda r, s: "",
                poll_interval=5,
                timeout=60,
            )

        mock_sleep.assert_called_once_with(5)


class TestPollUntilTimeout:
    """Tests for poll_until timeout behavior."""

    def test_raises_timeout_error(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with mock.patch("parallel_web_tools.core.polling.time.time") as mock_time:
                mock_time.side_effect = [0, 0, 5, 10, 15]

                with pytest.raises(TimeoutError, match="timed out"):
                    poll_until(
                        retrieve=lambda: {"status": "running"},
                        extract_status=lambda r: r["status"],
                        fetch_result=lambda: None,
                        format_error=lambda r, s: "",
                        timeout=10,
                        poll_interval=1,
                        timeout_message="Task timed out",
                    )

    def test_uses_custom_timeout_message(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with mock.patch("parallel_web_tools.core.polling.time.time") as mock_time:
                mock_time.side_effect = [0, 100]

                with pytest.raises(TimeoutError, match="custom message here"):
                    poll_until(
                        retrieve=lambda: {"status": "running"},
                        extract_status=lambda r: r["status"],
                        fetch_result=lambda: None,
                        format_error=lambda r, s: "",
                        timeout=10,
                        timeout_message="custom message here",
                    )


class TestPollUntilFailure:
    """Tests for poll_until when the task fails."""

    def test_raises_runtime_error_on_failure(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="Task failed badly"):
                poll_until(
                    retrieve=lambda: {"status": "failed", "error": "boom"},
                    extract_status=lambda r: r["status"],
                    fetch_result=lambda: None,
                    format_error=lambda r, s: f"Task {s} badly",
                )

    def test_format_error_receives_response_and_status(self):
        captured = {}

        def format_error(response, status):
            captured["response"] = response
            captured["status"] = status
            return "err"

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError):
                poll_until(
                    retrieve=lambda: {"status": "failed", "detail": "info"},
                    extract_status=lambda r: r["status"],
                    fetch_result=lambda: None,
                    format_error=format_error,
                )

        assert captured["status"] == "failed"
        assert captured["response"] == {"status": "failed", "detail": "info"}


class TestPollUntilCancellation:
    """Tests for poll_until when the task is cancelled."""

    def test_raises_runtime_error_on_cancellation(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="was cancelled"):
                poll_until(
                    retrieve=lambda: {"status": "cancelled"},
                    extract_status=lambda r: r["status"],
                    fetch_result=lambda: None,
                    format_error=lambda r, s: f"Task was {s}",
                )


class TestPollUntilCallback:
    """Tests for the on_poll callback."""

    def test_callback_invoked_each_iteration(self):
        responses = iter(
            [
                {"status": "pending", "progress": 0},
                {"status": "running", "progress": 50},
                {"status": "completed", "progress": 100},
            ]
        )
        poll_responses = []

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            poll_until(
                retrieve=lambda: next(responses),
                extract_status=lambda r: r["status"],
                fetch_result=lambda: "done",
                format_error=lambda r, s: "",
                on_poll=lambda r: poll_responses.append(r),
                poll_interval=1,
                timeout=60,
            )

        assert len(poll_responses) == 3
        assert poll_responses[0]["progress"] == 0
        assert poll_responses[1]["progress"] == 50
        assert poll_responses[2]["progress"] == 100

    def test_callback_not_required(self):
        result = poll_until(
            retrieve=lambda: {"status": "completed"},
            extract_status=lambda r: r["status"],
            fetch_result=lambda: "ok",
            format_error=lambda r, s: "",
            on_poll=None,
        )
        assert result == "ok"


class TestPollUntilCustomTerminalStatuses:
    """Tests for custom terminal_statuses parameter."""

    def test_custom_terminal_with_completed_still_succeeds(self):
        """Custom terminal set that includes 'completed' still returns result."""
        result = poll_until(
            retrieve=lambda: {"status": "completed"},
            extract_status=lambda r: r["status"],
            fetch_result=lambda: "custom_result",
            format_error=lambda r, s: "",
            terminal_statuses=("completed", "done", "error"),
        )
        assert result == "custom_result"

    def test_custom_terminal_triggers_error_for_non_completed(self):
        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            with pytest.raises(RuntimeError, match="stopped"):
                poll_until(
                    retrieve=lambda: {"status": "stopped"},
                    extract_status=lambda r: r["status"],
                    fetch_result=lambda: None,
                    format_error=lambda r, s: f"Task {s}",
                    terminal_statuses=("completed", "stopped"),
                )

    def test_non_terminal_status_keeps_polling(self):
        responses = iter(
            [
                {"status": "custom_running"},
                {"status": "completed"},
            ]
        )

        with mock.patch("parallel_web_tools.core.polling.time.sleep"):
            result = poll_until(
                retrieve=lambda: next(responses),
                extract_status=lambda r: r["status"],
                fetch_result=lambda: "got it",
                format_error=lambda r, s: "",
                terminal_statuses=("completed", "failed"),
                poll_interval=1,
                timeout=60,
            )

        assert result == "got it"
