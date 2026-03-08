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


def _poll_enrichments_until_complete(
    client: Any,
    findall_id: str,
    enrichment_schemas: list[dict[str, Any]],
    timeout: int,
    poll_interval: int,
    on_status: FindAllStatusCallback | None,
) -> dict[str, Any]:
    """Poll until enrichment data appears on all matched candidates.

    After calling findall.enrich(), the enrichment values are populated
    asynchronously. This polls the result endpoint until all matched
    candidates have the enrichment fields populated in their output.
    """
    import time

    # Collect expected enrichment field names from schemas
    expected_fields: set[str] = set()
    for schema in enrichment_schemas:
        expected_fields.update(schema.get("properties", {}).keys())

    if not expected_fields:
        # Nothing to wait for
        result = client.beta.findall.result(findall_id=findall_id)
        candidates = _serialize(getattr(result, "candidates", []))
        result_status = _extract_status_from_result(result)
        return {"findall_id": findall_id, **result_status, "candidates": candidates or []}

    # Use a shorter timeout for enrichment polling (5 min default, capped at main timeout)
    enrich_timeout = min(300, timeout)
    start = time.time()
    while time.time() - start < enrich_timeout:
        result = client.beta.findall.result(findall_id=findall_id)
        candidates = _serialize(getattr(result, "candidates", []))
        result_status = _extract_status_from_result(result)

        # Bail if the run failed (enrichment can cause run to error)
        if result_status.get("status") == "failed":
            return {"findall_id": findall_id, **result_status, "candidates": candidates or []}

        matched = [c for c in candidates if c.get("match_status") == "matched"]
        if not matched:
            return {"findall_id": findall_id, **result_status, "candidates": candidates or []}

        # Check if all matched candidates have enrichment fields in output
        all_enriched = all(expected_fields.issubset((c.get("output") or {}).keys()) for c in matched)

        if on_status:
            enriched_count = sum(1 for c in matched if expected_fields.issubset((c.get("output") or {}).keys()))
            on_status("enriching", findall_id, {"enriched": enriched_count, "total": len(matched)})

        if all_enriched:
            return {"findall_id": findall_id, **result_status, "candidates": candidates or []}

        time.sleep(poll_interval)

    # Timeout — return what we have
    result = client.beta.findall.result(findall_id=findall_id)
    candidates = _serialize(getattr(result, "candidates", []))
    result_status = _extract_status_from_result(result)
    return {"findall_id": findall_id, **result_status, "candidates": candidates or []}


def _add_enrichments(
    client: Any,
    findall_id: str,
    enrichments: list[dict[str, Any]],
) -> None:
    """Add enrichments to a FindAll run immediately after creation.

    Enrichments must be added while the run is still active so the API
    can process them on matched candidates as they arrive.
    """
    from parallel.types import JsonSchemaParam

    for enrichment in enrichments:
        output_schema = enrichment.get("output_schema", {})
        json_schema = output_schema.get("json_schema")
        if json_schema:
            processor = enrichment.get("processor", "core")
            client.beta.findall.enrich(
                findall_id=findall_id,
                processor=processor,
                output_schema=JsonSchemaParam(type="json", json_schema=json_schema),
            )


def _collect_enrichment_schemas(enrichments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract json_schema dicts from enrichment definitions."""
    schemas = []
    for enrichment in enrichments:
        output_schema = enrichment.get("output_schema", {})
        json_schema = output_schema.get("json_schema")
        if json_schema:
            schemas.append(json_schema)
    return schemas


def _enrich_candidates_via_task_api(
    result: dict[str, Any],
    enrichments: list[dict[str, Any]],
    api_key: str | None,
    source: ClientSource,
    on_status: FindAllStatusCallback | None,
) -> dict[str, Any]:
    """Enrich matched candidates using the Task API (batch enrichment).

    The FindAll-native enrich endpoint is unreliable, so we fall back to
    enriching matched candidates via the standard Task API. Each matched
    candidate is enriched with the schemas suggested by ingest.
    """
    from parallel_web_tools.core.batch import enrich_batch

    candidates = result.get("candidates", [])
    matched_indices = [i for i, c in enumerate(candidates) if c.get("match_status") == "matched"]
    if not matched_indices:
        return result

    # Collect output columns from enrichment schemas
    output_columns: list[str] = []
    processor = "core"
    for enrichment in enrichments:
        output_schema = enrichment.get("output_schema", {})
        json_schema = output_schema.get("json_schema", {})
        processor = enrichment.get("processor", processor)
        for prop_name, prop_def in json_schema.get("properties", {}).items():
            desc = prop_def.get("description", prop_name)
            output_columns.append(desc)

    if not output_columns:
        return result

    if on_status:
        on_status("enriching", result.get("findall_id", ""), {"total": len(matched_indices)})

    # Build inputs from matched candidates
    inputs = []
    for idx in matched_indices:
        c = candidates[idx]
        inputs.append(
            {
                "name": c.get("name", ""),
                "url": c.get("url", ""),
                "description": c.get("description", ""),
            }
        )

    # Run batch enrichment via Task API
    enrichment_results = enrich_batch(
        inputs=inputs,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        include_basis=False,
        source=source,
    )

    # Merge enrichment data into candidate output fields
    for i, idx in enumerate(matched_indices):
        if i >= len(enrichment_results):
            break
        enrich_data = enrichment_results[i]
        if "error" in enrich_data:
            continue

        output = candidates[idx].get("output") or {}
        for key, value in enrich_data.items():
            output[key] = {"type": "enrichment", "value": value}
        candidates[idx]["output"] = output

    result["candidates"] = candidates
    return result


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
    enrich: bool = True,
) -> dict[str, Any]:
    """Ingest, create, and poll a FindAll run to completion.

    This is the main all-in-one entry point. It converts the objective to a
    schema via ingest, creates a run, and polls until results are ready.

    If the ingest step suggests enrichments (non-boolean data fields like
    CEO name, revenue, etc.), they are automatically added to the run so
    matched candidates include enriched data.

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
        enrich: Whether to apply suggested enrichments from ingest. Default True.

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
    enrichments = _serialize(getattr(schema, "enrichments", []))

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

    # Step 3: Add enrichments immediately (while still running)
    # Enrichments must be added before the run completes — the FindAll API
    # runs enrichment tasks on matched candidates as they arrive.
    if enrich and enrichments:
        try:
            _add_enrichments(client, findall_id, enrichments)
        except Exception:
            # If native enrichment fails, we'll fall back to Task API after polling
            enrichments = []

    # Step 4: Poll until the run completes (enrichments run in parallel)
    result = _poll_findall_until_complete(client, findall_id, timeout, poll_interval, on_status)

    # Step 5: Wait for enrichment data to appear on candidates
    if enrich and enrichments:
        enrichment_schemas = _collect_enrichment_schemas(enrichments)
        if enrichment_schemas:
            result = _poll_enrichments_until_complete(
                client, findall_id, enrichment_schemas, timeout, poll_interval, on_status
            )

    return result


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


def enrich_findall(
    findall_id: str,
    output_schema: dict[str, Any],
    processor: str = "core",
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Add enrichments to a FindAll run.

    Enrichments extract additional non-boolean data from matched candidates
    without affecting match conditions. Can be called anytime after a run is
    created, even on completed runs.

    Args:
        findall_id: The FindAll run ID.
        output_schema: JSON schema dict for enrichment fields, e.g.:
            {"type": "object", "properties": {"ceo_name": {"type": "string", ...}}}
        processor: Task API processor (base, core, auto). Default "core".
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with the updated run schema including enrichments.
    """
    from parallel.types import JsonSchemaParam

    client = create_client(api_key, source)
    result = client.beta.findall.enrich(
        findall_id=findall_id,
        processor=processor,
        output_schema=JsonSchemaParam(type="json", json_schema=output_schema),
    )
    return _serialize(result)


def extend_findall(
    findall_id: str,
    additional_match_limit: int,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Extend a FindAll run to get more matches.

    Increases the match limit without re-running the full search. Only pays
    for additional matches beyond the original limit. Cannot be used on
    preview runs.

    Args:
        findall_id: The FindAll run ID.
        additional_match_limit: Number of additional matches to find.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with findall_id and updated status.
    """
    client = create_client(api_key, source)
    result = client.beta.findall.extend(
        findall_id=findall_id,
        additional_match_limit=additional_match_limit,
    )
    return _serialize(result)


def get_findall_schema(
    findall_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve the schema of a FindAll run.

    Useful for refreshing/rerunning searches with the same criteria.
    Returns objective, entity_type, match_conditions, enrichments,
    generator, and match_limit.

    Args:
        findall_id: The FindAll run ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with the run's schema (objective, entity_type, match_conditions,
        enrichments, generator, match_limit).
    """
    client = create_client(api_key, source)
    schema = client.beta.findall.schema(findall_id=findall_id)
    return _serialize(schema)
