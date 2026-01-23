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

# Processor tiers for deep research with expected latency (from docs)
# Fast variants are 2-5x faster but may use slightly less fresh data
RESEARCH_PROCESSORS = {
    # Fast processors (optimized for speed)
    "lite-fast": "10-20s - quick lookups",
    "base-fast": "15-50s - simple questions",
    "core-fast": "15s-100s - moderate research",
    "core2x-fast": "15s-3min - extended research",
    "pro-fast": "30s-5min - exploratory research (default)",
    "ultra-fast": "1-10min - multi-source deep research",
    "ultra2x-fast": "1-20min - difficult deep research",
    "ultra4x-fast": "1-40min - very difficult research",
    "ultra8x-fast": "1min-1hr - most challenging research",
    # Standard processors (fresher data)
    "lite": "10-60s - quick lookups, fresher data",
    "base": "15-100s - simple questions, fresher data",
    "core": "1-5min - moderate research, fresher data",
    "core2x": "1-10min - extended research, fresher data",
    "pro": "2-10min - exploratory research, fresher data",
    "ultra": "5-25min - advanced deep research, fresher data",
    "ultra2x": "5-50min - difficult deep research, fresher data",
    "ultra4x": "5-90min - very difficult research, fresher data",
    "ultra8x": "5min-2hr - most challenging research, fresher data",
}

TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def create_research_task(
    query: str,
    processor: str = "pro-fast",
    api_key: str | None = None,
    output_format: str = "text",
) -> dict[str, Any]:
    """Create a deep research task without waiting for results.

    Args:
        query: Research question or topic (max 15,000 chars).
        processor: Processor tier (see RESEARCH_PROCESSORS).
        api_key: Optional API key.
        output_format: "text" for markdown report (default), "auto" for structured JSON.

    Returns:
        Dict with run_id, result_url, and other task metadata.
    """
    from parallel import Parallel
    from parallel.types import TaskSpecParam, TextSchemaParam

    client = Parallel(api_key=resolve_api_key(api_key))

    # Build task spec based on output format
    task_spec = None
    if output_format == "text":
        task_spec = TaskSpecParam(output_schema=TextSchemaParam(type="text"))

    task = client.task_run.create(
        input=query[:15000],
        processor=processor,
        task_spec=task_spec,
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
    output_format: str = "text",
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
        output_format: "text" for markdown report (default), "auto" for structured JSON.

    Returns:
        Dict with content, basis (if included), and metadata.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    from parallel import Parallel
    from parallel.types import TaskSpecParam, TextSchemaParam

    client = Parallel(api_key=resolve_api_key(api_key))

    # Build task spec based on output format
    task_spec = None
    if output_format == "text":
        task_spec = TaskSpecParam(output_schema=TextSchemaParam(type="text"))

    task = client.task_run.create(
        input=query[:15000],
        processor=processor,
        task_spec=task_spec,
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
    """Extract the content string from various output formats.

    The Parallel API can return content in several formats:
    - Direct string (markdown text)
    - Dict with 'content', 'markdown', or 'text' keys
    - SDK object with .content, .markdown, or .text attributes
    - Nested structures where content contains another dict

    We prioritize finding actual text content over JSON dumping structured data.
    """
    if output is None:
        return ""

    if isinstance(output, str):
        return output

    if isinstance(output, dict):
        # Priority: content > markdown > text
        for key in ("content", "markdown", "text"):
            if key in output:
                value = output[key]
                # Recursively extract if the value is also a dict/object
                if isinstance(value, str):
                    return value
                return _extract_content(value)
        # Fallback to JSON dump if no text keys found
        return json.dumps(output, indent=2, default=str)

    # Handle SDK response objects with attributes
    if hasattr(output, "content"):
        content = output.content
        if isinstance(content, str):
            return content
        # If content is a dict, look for text fields within it
        if isinstance(content, dict):
            for key in ("content", "markdown", "text"):
                if key in content:
                    value = content[key]
                    if isinstance(value, str):
                        return value
                    return _extract_content(value)
            return json.dumps(content, indent=2, default=str)
        return _extract_content(content)

    if hasattr(output, "markdown"):
        return str(output.markdown)

    if hasattr(output, "text"):
        return str(output.text)

    return str(output)
