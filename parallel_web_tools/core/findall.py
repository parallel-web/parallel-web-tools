"""FindAll: web-scale entity discovery using the Parallel FindAll API.

FindAll turns natural language queries into structured, enriched databases.
It generates candidates from web data, validates them against match conditions,
and optionally enriches matches with additional structured information.

The typical workflow is:
    1. Ingest: convert a natural language objective into a structured schema
    2. Create: start a run that generates and evaluates candidates
    3. Poll: wait for the run to complete (can take several minutes)
    4. Result: retrieve matched candidates with citations and reasoning
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.polling import poll_until
from parallel_web_tools.core.user_agent import ClientSource

# Generator tiers for FindAll runs
FINDALL_GENERATORS = {
    "preview": "~10 candidates - test queries before committing",
    "base": "moderate pool - broad queries with many expected matches",
    "core": "large pool - specific queries, balanced breadth/depth",
    "pro": "largest pool - highly specific, hard-to-find matches",
}

# Kept as a public alias for backwards compatibility with tests/consumers
FINDALL_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def _serialize(obj: Any) -> Any:
    """Serialize an SDK object to a plain dict/list."""
    if obj is None:
        return None
    if isinstance(obj, (dict, str, int, float, bool)):
        return obj
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "__dict__"):
        return {k: _serialize(v) for k, v in obj.__dict__.items() if not k.startswith("_")}
    return str(obj)


def _extract_status_info(run: Any) -> dict[str, Any]:
    """Extract status, is_active, and metrics from a FindAll run object."""
    status_obj = getattr(run, "status", None)
    if status_obj is None:
        return {"status": "unknown", "is_active": False, "metrics": {}}

    status_str = getattr(status_obj, "status", "unknown")
    is_active = getattr(status_obj, "is_active", False)

    metrics_obj = getattr(status_obj, "metrics", None)
    metrics = {}
    if metrics_obj:
        metrics = {
            "generated_candidates_count": getattr(metrics_obj, "generated_candidates_count", 0),
            "matched_candidates_count": getattr(metrics_obj, "matched_candidates_count", 0),
        }

    return {"status": status_str, "is_active": is_active, "metrics": metrics}


def ingest_findall(
    objective: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Convert a natural language objective into a structured FindAll schema.

    The returned schema contains entity_type, match_conditions, and optionally
    suggested enrichments and generator. Review and modify before passing to
    create_findall_run().

    Args:
        objective: Natural language query (e.g., "Find all AI startups in SF").
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with objective, entity_type, match_conditions, enrichments,
        generator, and match_limit as suggested by the API.
    """
    client = create_client(api_key, source)
    schema = client.beta.findall.ingest(objective=objective)
    return _serialize(schema)


def create_findall_run(
    objective: str,
    entity_type: str,
    match_conditions: list[dict[str, str]],
    generator: str = "core",
    match_limit: int = 10,
    exclude_list: list[dict[str, str]] | None = None,
    metadata: dict[str, Any] | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Create a FindAll run without waiting for results.

    Args:
        objective: Natural language objective.
        entity_type: Type of entities to find (e.g., "companies").
        match_conditions: List of {"name": ..., "description": ...} dicts.
        generator: Generator tier (preview, base, core, pro).
        match_limit: Maximum matched candidates (5-1000).
        exclude_list: Optional list of {"name": ..., "url": ...} to exclude.
        metadata: Optional metadata dict.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id, status, generator, and timestamps.
    """
    client = create_client(api_key, source)

    kwargs: dict[str, Any] = {
        "objective": objective,
        "entity_type": entity_type,
        "match_conditions": match_conditions,
        "generator": generator,
        "match_limit": match_limit,
    }
    if exclude_list:
        kwargs["exclude_list"] = exclude_list
    if metadata:
        kwargs["metadata"] = metadata

    run = client.beta.findall.create(**kwargs)
    status_info = _extract_status_info(run)

    return {
        "findall_id": run.findall_id,
        **status_info,
        "generator": getattr(run, "generator", generator),
        "created_at": getattr(run, "created_at", None),
    }


def cancel_findall_run(
    findall_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Cancel a running FindAll run.

    Args:
        findall_id: The FindAll run ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id and cancellation status.
    """
    client = create_client(api_key, source)
    client.beta.findall.cancel(findall_id=findall_id)
    return {"findall_id": findall_id, "status": "cancelled"}


def get_findall_status(
    findall_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Check the current status of a FindAll run.

    Args:
        findall_id: The FindAll run ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id, status, is_active, metrics, and generator.
    """
    client = create_client(api_key, source)
    run = client.beta.findall.retrieve(findall_id=findall_id)
    status_info = _extract_status_info(run)

    return {
        "findall_id": findall_id,
        **status_info,
        "generator": getattr(run, "generator", None),
        "created_at": getattr(run, "created_at", None),
        "modified_at": getattr(run, "modified_at", None),
    }


def _extract_status_from_result(result: Any) -> dict[str, Any]:
    """Extract status info from a FindAllRunResult (which nests status under .run)."""
    run_obj = getattr(result, "run", None)
    if run_obj:
        return _extract_status_info(run_obj)
    return _extract_status_info(result)


def get_findall_result(
    findall_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve results of a FindAll run.

    Args:
        findall_id: The FindAll run ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id, status, metrics, and candidates list.
    """
    client = create_client(api_key, source)
    result = client.beta.findall.result(findall_id=findall_id)
    status_info = _extract_status_from_result(result)
    candidates = _serialize(getattr(result, "candidates", []))

    return {
        "findall_id": findall_id,
        **status_info,
        "candidates": candidates or [],
    }


# Type for the FindAll status callback
FindAllStatusCallback = Callable[[str, str, dict[str, Any]], None]


def _poll_findall_until_complete(
    client: Any,
    findall_id: str,
    timeout: int,
    poll_interval: int,
    on_status: FindAllStatusCallback | None,
) -> dict[str, Any]:
    """Poll a FindAll run until it reaches a terminal status.

    Args:
        client: Parallel SDK client instance.
        findall_id: The FindAll run ID.
        timeout: Maximum wait time in seconds.
        poll_interval: Seconds between status checks.
        on_status: Optional callback(status, findall_id, metrics) on each poll.

    Returns:
        Dict with findall_id, status, metrics, and candidates.

    Raises:
        TimeoutError: If the run doesn't complete within timeout.
        RuntimeError: If the run fails or is cancelled.
    """

    def retrieve():
        return client.beta.findall.retrieve(findall_id=findall_id)

    def extract_status(response):
        return _extract_status_info(response)["status"]

    def fetch_result():
        result = client.beta.findall.result(findall_id=findall_id)
        candidates = _serialize(getattr(result, "candidates", []))
        result_status = _extract_status_from_result(result)
        return {
            "findall_id": findall_id,
            **result_status,
            "candidates": candidates or [],
        }

    def format_error(response, status):
        termination = getattr(getattr(response, "status", None), "termination_reason", None)
        detail = f" ({termination})" if termination else ""
        return f"FindAll run {status}{detail}"

    def _on_poll(response):
        if on_status:
            status_info = _extract_status_info(response)
            on_status(status_info["status"], findall_id, status_info["metrics"])

    return poll_until(
        retrieve=retrieve,
        extract_status=extract_status,
        fetch_result=fetch_result,
        format_error=format_error,
        on_poll=_on_poll,
        timeout=timeout,
        poll_interval=poll_interval,
        timeout_message=f"FindAll run {findall_id} timed out after {timeout}s. Use 'parallel-cli findall poll {findall_id}' to resume.",
    )


def run_findall(
    objective: str,
    generator: str = "core",
    match_limit: int = 10,
    exclude_list: list[dict[str, str]] | None = None,
    metadata: dict[str, Any] | None = None,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 30,
    on_status: FindAllStatusCallback | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Ingest, create, and poll a FindAll run to completion.

    This is the main all-in-one entry point. It converts the objective to a
    schema via ingest, creates a run, and polls until results are ready.

    Args:
        objective: Natural language query.
        generator: Generator tier (preview/base/core/pro).
        match_limit: Maximum matched candidates (5-1000).
        exclude_list: Optional entities to exclude.
        metadata: Optional run metadata.
        api_key: Optional API key override.
        timeout: Maximum wait time in seconds (default: 3600).
        poll_interval: Seconds between status checks (default: 30).
        on_status: Optional callback(status, findall_id, metrics) on each poll.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id, status, metrics, and candidates.

    Raises:
        TimeoutError: If the run doesn't complete within timeout.
        RuntimeError: If the run fails or is cancelled.
    """
    client = create_client(api_key, source)

    # Step 1: Ingest - convert natural language to structured schema
    schema = client.beta.findall.ingest(objective=objective)

    entity_type = getattr(schema, "entity_type", "entities")
    match_conditions = _serialize(getattr(schema, "match_conditions", []))

    if on_status:
        on_status("ingested", "", {"entity_type": entity_type})

    # Step 2: Create the run
    kwargs: dict[str, Any] = {
        "objective": objective,
        "entity_type": entity_type,
        "match_conditions": match_conditions,
        "generator": generator,
        "match_limit": match_limit,
    }
    if exclude_list:
        kwargs["exclude_list"] = exclude_list
    if metadata:
        kwargs["metadata"] = metadata

    run = client.beta.findall.create(**kwargs)
    findall_id = run.findall_id

    if on_status:
        on_status("created", findall_id, {})

    # Step 3: Poll until complete
    return _poll_findall_until_complete(client, findall_id, timeout, poll_interval, on_status)


def poll_findall(
    findall_id: str,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 30,
    on_status: FindAllStatusCallback | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Resume polling an existing FindAll run.

    Use this to reconnect to a run that was created earlier (e.g., via
    --no-wait or after a timeout).

    Args:
        findall_id: The FindAll run ID.
        api_key: Optional API key override.
        timeout: Maximum wait time in seconds (default: 3600).
        poll_interval: Seconds between status checks (default: 30).
        on_status: Optional callback(status, findall_id, metrics) on each poll.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id, status, metrics, and candidates.
    """
    client = create_client(api_key, source)

    if on_status:
        on_status("polling", findall_id, {})

    return _poll_findall_until_complete(client, findall_id, timeout, poll_interval, on_status)
