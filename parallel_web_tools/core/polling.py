"""Shared polling utility for deadline-based poll-sleep-check loops.

Used internally by research and findall modules. Not part of the public API.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def poll_until(
    *,
    retrieve: Callable[[], Any],
    extract_status: Callable[[Any], str],
    fetch_result: Callable[[], Any],
    format_error: Callable[[Any, str], str],
    on_poll: Callable[[Any], None] | None = None,
    timeout: int = 3600,
    poll_interval: int = 30,
    timeout_message: str = "Task timed out",
    terminal_statuses: tuple[str, ...] = TERMINAL_STATUSES,
) -> Any:
    """Poll until a task reaches a terminal status.

    Args:
        retrieve: Zero-arg callable that fetches the current run state.
        extract_status: Takes the response from retrieve, returns a status string.
        fetch_result: Zero-arg callable to fetch/format the result on completion.
        format_error: Takes (response, status), returns an error message string.
        on_poll: Optional callback invoked with the response on each iteration.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        timeout_message: Message for the TimeoutError if the deadline is exceeded.
        terminal_statuses: Tuple of statuses that indicate the task is done.

    Returns:
        The result from fetch_result() on successful completion.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    deadline = time.time() + timeout

    while time.time() < deadline:
        response = retrieve()
        status = extract_status(response)

        if on_poll:
            on_poll(response)

        if status in terminal_statuses:
            if status == "completed":
                return fetch_result()

            raise RuntimeError(format_error(response, status))

        time.sleep(poll_interval)

    raise TimeoutError(timeout_message)
