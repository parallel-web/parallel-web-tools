"""Deep Research using the Parallel Task API.

Deep research is designed for open-ended research questions that require
comprehensive multi-step web exploration. Unlike batch enrichment which
processes structured data, deep research takes a natural language query
and returns analyst-grade intelligence reports.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.polling import poll_until
from parallel_web_tools.core.user_agent import ClientSource

# Base URL for viewing results
PLATFORM_BASE = "https://platform.parallel.ai"

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


def _serialize_output(output: Any) -> dict[str, Any]:
    """Serialize SDK output object to a dictionary.

    The Parallel SDK returns Pydantic-like objects that can be
    serialized via model_dump() or to_dict().
    """
    if output is None:
        return {}

    if isinstance(output, dict):
        return output

    # Try common serialization methods
    if hasattr(output, "model_dump"):
        return output.model_dump()

    if hasattr(output, "to_dict"):
        return output.to_dict()

    if hasattr(output, "__dict__"):
        return output.__dict__

    return {"raw": str(output)}


def create_research_task(
    query: str,
    processor: str = "pro-fast",
    api_key: str | None = None,
    source: ClientSource = "python",
    previous_interaction_id: str | None = None,
) -> dict[str, Any]:
    """Create a deep research task without waiting for results.

    Args:
        query: Research question or topic (max 15,000 chars).
        processor: Processor tier (see RESEARCH_PROCESSORS).
        api_key: Optional API key.
        source: Client source identifier for User-Agent.
        previous_interaction_id: Interaction ID from a previous task to reuse as context.

    Returns:
        Dict with run_id, interaction_id, result_url, and other task metadata.
    """
    client = create_client(api_key, source)

    create_kwargs: dict[str, Any] = {
        "input": query[:15000],
        "processor": processor,
    }
    if previous_interaction_id:
        create_kwargs["previous_interaction_id"] = previous_interaction_id

    task = client.task_run.create(**create_kwargs)

    return {
        "run_id": task.run_id,
        "interaction_id": getattr(task, "interaction_id", task.run_id),
        "result_url": f"{PLATFORM_BASE}/play/deep-research/{task.run_id}",
        "processor": processor,
        "status": getattr(task, "status", "pending"),
    }


def get_research_status(
    run_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Get the current status of a research task.

    Args:
        run_id: The task run ID.
        api_key: Optional API key.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with status, interaction_id, and other task info.
    """
    client = create_client(api_key, source)
    status = client.task_run.retrieve(run_id=run_id)

    return {
        "run_id": run_id,
        "interaction_id": getattr(status, "interaction_id", run_id),
        "status": status.status,
        "result_url": f"{PLATFORM_BASE}/play/deep-research/{run_id}",
    }


def get_research_result(
    run_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Get the result of a completed research task.

    Args:
        run_id: The task run ID.
        api_key: Optional API key.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with output data and metadata.
    """
    client = create_client(api_key, source)
    result = client.task_run.result(run_id=run_id)

    output = result.output if hasattr(result, "output") else {}
    output_data = _serialize_output(output)

    return {
        "run_id": run_id,
        "result_url": f"{PLATFORM_BASE}/play/deep-research/{run_id}",
        "status": "completed",
        "output": output_data,
    }


def _poll_until_complete(
    client,
    run_id: str,
    result_url: str,
    timeout: int,
    poll_interval: int,
    on_status: Callable[[str, str], None] | None,
    interaction_id: str | None = None,
) -> dict[str, Any]:
    """Poll a research task until completion and return the result.

    Args:
        client: Parallel client instance.
        run_id: The task run ID to poll.
        result_url: URL to view results.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        on_status: Optional callback called with (status, run_id) on each poll.
        interaction_id: Known interaction ID (updated from poll responses).

    Returns:
        Dict with content and metadata.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    # Track interaction_id from poll responses
    poll_state = {"interaction_id": interaction_id}

    def retrieve():
        response = client.task_run.retrieve(run_id=run_id)
        # Capture interaction_id from the latest response
        if hasattr(response, "interaction_id") and response.interaction_id:
            poll_state["interaction_id"] = response.interaction_id
        return response

    def extract_status(response):
        return response.status

    def fetch_result():
        result = client.task_run.result(run_id=run_id)
        output = result.output if hasattr(result, "output") else {}
        output_data = _serialize_output(output)
        return {
            "run_id": run_id,
            "interaction_id": poll_state["interaction_id"] or run_id,
            "result_url": result_url,
            "status": "completed",
            "output": output_data,
        }

    def format_error(response, status):
        error = getattr(response, "error", None) or f"Task {status}"
        return f"Research {status}: {error}"

    def _on_poll(response):
        if on_status:
            on_status(response.status, run_id)

    return poll_until(
        retrieve=retrieve,
        extract_status=extract_status,
        fetch_result=fetch_result,
        format_error=format_error,
        on_poll=_on_poll,
        timeout=timeout,
        poll_interval=poll_interval,
        timeout_message=f"Research task {run_id} timed out after {timeout} seconds",
    )


def run_research(
    query: str,
    processor: str = "pro-fast",
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 45,
    on_status: Callable[[str, str], None] | None = None,
    source: ClientSource = "python",
    previous_interaction_id: str | None = None,
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
        on_status: Optional callback called with (status, run_id) on each poll.
        source: Client source identifier for User-Agent.
        previous_interaction_id: Interaction ID from a previous task to reuse as context.

    Returns:
        Dict with content and metadata.

    Raises:
        TimeoutError: If the task doesn't complete within timeout.
        RuntimeError: If the task fails or is cancelled.
    """
    client = create_client(api_key, source)

    create_kwargs: dict[str, Any] = {
        "input": query[:15000],
        "processor": processor,
    }
    if previous_interaction_id:
        create_kwargs["previous_interaction_id"] = previous_interaction_id

    task = client.task_run.create(**create_kwargs)
    run_id = task.run_id
    interaction_id = getattr(task, "interaction_id", run_id)
    result_url = f"{PLATFORM_BASE}/play/deep-research/{run_id}"

    if on_status:
        on_status("created", run_id)

    return _poll_until_complete(
        client, run_id, result_url, timeout, poll_interval, on_status, interaction_id=interaction_id
    )


def poll_research(
    run_id: str,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 45,
    on_status: Callable[[str, str], None] | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Resume polling an existing research task.

    Use this to reconnect to a task that was created earlier.

    Args:
        run_id: The task run ID to poll.
        api_key: Optional API key.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        on_status: Optional callback called with (status, run_id) on each poll.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with content and metadata including interaction_id.
    """
    client = create_client(api_key, source)
    result_url = f"{PLATFORM_BASE}/play/deep-research/{run_id}"

    if on_status:
        on_status("polling", run_id)

    return _poll_until_complete(client, run_id, result_url, timeout, poll_interval, on_status)
