"""Deep Research using the Parallel Task API.

Deep research is designed for open-ended research questions that require
comprehensive multi-step web exploration. Unlike batch enrichment which
processes structured data, deep research takes a natural language query
and returns analyst-grade intelligence reports.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.batch import extract_basis

# Processor tiers for deep research with expected latency
RESEARCH_PROCESSORS = {
    "pro-fast": "1-5 min - exploratory research (default)",
    "pro": "2-10 min - exploratory research, fresher data",
    "ultra-fast": "2-12 min - multi-source deep research",
    "ultra": "5-25 min - advanced deep research, fresher data",
    "ultra2x-fast": "2-25 min - difficult deep research",
    "ultra2x": "5-50 min - difficult deep research, fresher data",
    "ultra4x-fast": "2-45 min - very difficult research",
    "ultra4x": "5-90 min - very difficult research, fresher data",
    "ultra8x-fast": "2-60 min - most challenging research",
    "ultra8x": "5min-2hr - most challenging research, fresher data",
}

TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def create_research_task(
    query: str,
    processor: str = "pro-fast",
    api_key: str | None = None,
) -> dict[str, Any]:
    """Create a deep research task without waiting for results.

    Args:
        query: Research question or topic (max 15,000 chars).
        processor: Processor tier (see RESEARCH_PROCESSORS).
        api_key: Optional API key.

    Returns:
        Dict with run_id, result_url, and other task metadata.
    """
    from parallel import Parallel

    client = Parallel(api_key=resolve_api_key(api_key))

    task = client.task_run.create(
        input=query[:15000],
        processor=processor,
    )

    return {
        "run_id": task.run_id,
        "result_url": getattr(task, "result_url", f"https://platform.parallel.ai/tasks/{task.run_id}"),
        "processor": processor,
        "status": getattr(task, "status", "pending"),
    }


def get_research_status(
    run_id: str,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Get the current status of a research task.

    Args:
        run_id: The task run ID.
        api_key: Optional API key.

    Returns:
        Dict with status and other task info.
    """
    from parallel import Parallel

    client = Parallel(api_key=resolve_api_key(api_key))
    status = client.task_run.retrieve(run_id=run_id)

    return {
        "run_id": run_id,
        "status": status.status,
        "result_url": f"https://platform.parallel.ai/tasks/{run_id}",
    }


def get_research_result(
    run_id: str,
    api_key: str | None = None,
    include_basis: bool = True,
) -> dict[str, Any]:
    """Get the result of a completed research task.

    Args:
        run_id: The task run ID.
        api_key: Optional API key.
        include_basis: Whether to include citations/sources.

    Returns:
        Dict with content, basis (if included), and metadata.
    """
    from parallel import Parallel

    client = Parallel(api_key=resolve_api_key(api_key))
    result = client.task_run.result(run_id=run_id)

    output = result.output if hasattr(result, "output") else {}
    content = _extract_content(output)

    response: dict[str, Any] = {
        "run_id": run_id,
        "status": "completed",
        "content": content,
    }

    if include_basis and hasattr(output, "basis"):
        response["basis"] = extract_basis(output)

    return response


def _poll_until_complete(
    client,
    run_id: str,
    result_url: str,
    timeout: int,
    poll_interval: int,
    include_basis: bool,
    on_status: Callable[[str, str], None] | None,
) -> dict[str, Any]:
    """Poll a research task until completion and return the result.

    This is the shared polling logic used by both run_research and poll_research.

    Args:
        client: Parallel client instance.
        run_id: The task run ID to poll.
        result_url: URL to view results.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        include_basis: Whether to include citations/sources.
        on_status: Optional callback called with (status, run_id) on each poll.

    Returns:
        Dict with content, basis (if included), and metadata.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    deadline = time.time() + timeout

    while time.time() < deadline:
        status = client.task_run.retrieve(run_id=run_id)
        current_status = status.status

        if on_status:
            on_status(current_status, run_id)

        if current_status in TERMINAL_STATUSES:
            if current_status == "completed":
                result = client.task_run.result(run_id=run_id)
                output = result.output if hasattr(result, "output") else {}
                content = _extract_content(output)

                response: dict[str, Any] = {
                    "run_id": run_id,
                    "result_url": result_url,
                    "status": "completed",
                    "content": content,
                }

                if include_basis and hasattr(output, "basis"):
                    response["basis"] = extract_basis(output)

                return response

            error = getattr(status, "error", None) or f"Task {current_status}"
            raise RuntimeError(f"Research {current_status}: {error}")

        time.sleep(poll_interval)

    raise TimeoutError(f"Research task {run_id} timed out after {timeout} seconds")


def run_research(
    query: str,
    processor: str = "pro-fast",
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 45,
    include_basis: bool = True,
    on_status: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    """Run deep research and wait for results.

    This is the main entry point for running research. It creates a task,
    polls for completion, and returns the result.

    Args:
        query: Research question or topic (max 15,000 chars).
        processor: Processor tier (see RESEARCH_PROCESSORS).
        api_key: Optional API key.
        timeout: Maximum wait time in seconds (default: 3600 = 1 hour).
        poll_interval: Seconds between status checks (default: 45).
        include_basis: Whether to include citations/sources.
        on_status: Optional callback called with (status, run_id) on each poll.

    Returns:
        Dict with content, basis (if included), and metadata.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    from parallel import Parallel

    client = Parallel(api_key=resolve_api_key(api_key))

    task = client.task_run.create(
        input=query[:15000],
        processor=processor,
    )
    run_id = task.run_id
    result_url = getattr(task, "result_url", f"https://platform.parallel.ai/tasks/{run_id}")

    if on_status:
        on_status("created", run_id)

    return _poll_until_complete(client, run_id, result_url, timeout, poll_interval, include_basis, on_status)


def poll_research(
    run_id: str,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 45,
    include_basis: bool = True,
    on_status: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    """Resume polling an existing research task.

    Use this to reconnect to a task that was created earlier.

    Args:
        run_id: The task run ID to poll.
        api_key: Optional API key.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        include_basis: Whether to include citations/sources.
        on_status: Optional callback called with (status, run_id) on each poll.

    Returns:
        Dict with content, basis (if included), and metadata.
    """
    from parallel import Parallel

    client = Parallel(api_key=resolve_api_key(api_key))
    result_url = f"https://platform.parallel.ai/tasks/{run_id}"

    if on_status:
        on_status("polling", run_id)

    return _poll_until_complete(client, run_id, result_url, timeout, poll_interval, include_basis, on_status)


def _extract_content(output: Any) -> str:
    """Extract the content string from various output formats."""
    if output is None:
        return ""

    if isinstance(output, str):
        return output

    if isinstance(output, dict):
        # Priority: content > markdown > text > JSON dump
        for key in ("content", "markdown", "text"):
            if key in output:
                return str(output[key])
        return json.dumps(output, indent=2, default=str)

    # Handle SDK response objects
    if hasattr(output, "content"):
        content = output.content
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            return json.dumps(content, indent=2, default=str)
        return str(content)

    if hasattr(output, "markdown"):
        return str(output.markdown)

    if hasattr(output, "text"):
        return str(output.text)

    return str(output)
