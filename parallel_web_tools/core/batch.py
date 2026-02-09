"""Batch enrichment using the Parallel Task Group API."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.user_agent import ClientSource

# Base URL for viewing task groups on the platform
PLATFORM_BASE = "https://platform.parallel.ai"


def build_output_schema(output_columns: list[str]) -> dict[str, Any]:
    """Build a JSON schema from output column descriptions."""
    properties = {}
    for col in output_columns:
        # Extract base name before any annotations like (type), [hint], {note}
        base_name = col.split("(")[0].split("[")[0].split("{")[0].strip()

        # Convert to valid property name
        prop_name = base_name.lower().replace(" ", "_").replace("-", "_")
        prop_name = "".join(c for c in prop_name if c.isalnum() or c == "_")
        if prop_name and not prop_name[0].isalpha():
            prop_name = "col_" + prop_name
        prop_name = prop_name or "column"

        properties[prop_name] = {"type": "string", "description": col}

    return {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
    }


def _parse_content(content) -> dict[str, Any]:
    """Parse API response content into a dictionary."""
    if isinstance(content, dict):
        return dict(content)
    if isinstance(content, str):
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"result": content}
    return {"result": str(content)}


def extract_basis(output) -> list[dict[str, Any]]:
    """Extract basis/citations from a Parallel API output."""
    if not getattr(output, "basis", None):
        return []

    basis_list: list[dict[str, Any]] = []
    for field_basis in output.basis:
        entry: dict[str, Any] = {}

        if field := getattr(field_basis, "field", None):
            entry["field"] = field

        if citations := getattr(field_basis, "citations", None):
            entry["citations"] = [
                {"url": getattr(c, "url", None), "excerpts": getattr(c, "excerpts", [])} for c in citations
            ]

        if reasoning := getattr(field_basis, "reasoning", None):
            entry["reasoning"] = reasoning

        if confidence := getattr(field_basis, "confidence", None):
            entry["confidence"] = confidence

        # Fallback for simpler basis format
        if not entry:
            if url := getattr(field_basis, "url", None):
                entry["url"] = url
            if title := getattr(field_basis, "title", None):
                entry["title"] = title
            if excerpts := getattr(field_basis, "excerpts", None):
                entry["excerpts"] = excerpts

        if entry:
            basis_list.append(entry)

    return basis_list


def enrich_batch(
    inputs: list[dict[str, Any]],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    poll_interval: int = 5,
    include_basis: bool = True,
    source: ClientSource = "python",
) -> list[dict[str, Any]]:
    """Enrich multiple inputs using the Parallel Task Group API.

    Args:
        inputs: List of input dictionaries
        output_columns: List of column descriptions to enrich
        api_key: Optional API key
        processor: Parallel processor (default: lite-fast)
        timeout: Max wait time in seconds
        poll_interval: Seconds between status polls
        include_basis: Whether to include citations
        source: Client source identifier for User-Agent (default: python)

    Returns:
        List of result dictionaries in same order as inputs.
    """
    from parallel.types import JsonSchemaParam, TaskSpecParam
    from parallel.types.beta import BetaRunInputParam

    if not inputs:
        return []

    try:
        client = create_client(api_key, source)
        output_schema = build_output_schema(output_columns)
        task_spec = TaskSpecParam(output_schema=JsonSchemaParam(type="json", json_schema=output_schema))

        # Create task group
        task_group = client.beta.task_group.create()
        taskgroup_id = task_group.task_group_id

        # Add runs - use SDK type for proper typing
        run_inputs: list[BetaRunInputParam] = [{"input": inp, "processor": processor} for inp in inputs]
        response = client.beta.task_group.add_runs(
            taskgroup_id,
            default_task_spec=task_spec,
            inputs=run_inputs,
        )
        run_ids = response.run_ids

        if not run_ids:
            return [{"error": "Failed to add runs to task group"}] * len(inputs)

        # Poll for completion
        time.sleep(3)
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = client.beta.task_group.retrieve(taskgroup_id)
            status_counts = status.status.task_run_status_counts or {}
            completed = status_counts.get("completed", 0)
            failed = status_counts.get("failed", 0)
            total = status.status.num_task_runs

            if completed + failed >= total or not status.status.is_active:
                break
            time.sleep(poll_interval)

        # Collect results
        results_by_id: dict[str, dict[str, Any]] = {}
        runs_stream = client.beta.task_group.get_runs(taskgroup_id, include_input=True, include_output=True)

        for event in runs_stream:
            if event.type == "task_run.state":
                run_id = event.run.run_id
                if content := getattr(event.output, "content", None):
                    result = _parse_content(content)
                    if include_basis:
                        result["basis"] = extract_basis(event.output)
                    results_by_id[run_id] = result
                elif event.run.error:
                    results_by_id[run_id] = {"error": str(event.run.error)}

        return [results_by_id.get(run_id, {"error": "No result"}) for run_id in run_ids]

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception(f"enrich_batch failed for {len(inputs)} inputs: {e}")
        return [{"error": str(e)}] * len(inputs)


def enrich_single(
    input_data: dict[str, Any],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
    include_basis: bool = True,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Enrich a single input using the Parallel API."""
    results = enrich_batch(
        [input_data],
        output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
        source=source,
    )
    return results[0] if results else {"error": "No result"}


def create_task_group(
    input_data: list[dict[str, Any]],
    InputModel,
    OutputModel,
    processor: str = "core-fast",
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Create a task group and add runs without waiting for completion.

    Args:
        input_data: List of input dictionaries.
        InputModel: Pydantic model for input schema.
        OutputModel: Pydantic model for output schema.
        processor: Parallel processor (default: core-fast).
        source: Client source identifier for User-Agent.

    Returns:
        Dict with taskgroup_id, url, and num_runs.
    """
    from parallel.types import JsonSchemaParam, TaskSpecParam
    from parallel.types.beta import BetaRunInputParam

    logger = logging.getLogger(__name__)

    client = create_client(source=source)

    # Build task spec from Pydantic models
    task_spec = TaskSpecParam(
        input_schema=JsonSchemaParam(type="json", json_schema=InputModel.model_json_schema()),
        output_schema=JsonSchemaParam(type="json", json_schema=OutputModel.model_json_schema()),
    )

    # Create task group
    task_group = client.beta.task_group.create()
    taskgroup_id = task_group.task_group_id
    logger.info(f"Created taskgroup id {taskgroup_id}")

    # Add runs in batches
    batch_size = 100
    total_created = 0
    for i in range(0, len(input_data), batch_size):
        batch = input_data[i : i + batch_size]
        run_inputs: list[BetaRunInputParam] = [{"input": row, "processor": processor} for row in batch]
        response = client.beta.task_group.add_runs(
            taskgroup_id,
            default_task_spec=task_spec,
            inputs=run_inputs,
        )
        total_created += len(response.run_ids)
        logger.info(f"Processing {i + len(batch)} entities. Created {total_created} Tasks.")

    return {
        "taskgroup_id": taskgroup_id,
        "url": f"{PLATFORM_BASE}/view/task-run-group/{taskgroup_id}",
        "num_runs": total_created,
    }


def get_task_group_status(
    taskgroup_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Get the current status of a task group.

    Args:
        taskgroup_id: The task group ID.
        api_key: Optional API key.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with taskgroup_id, status_counts, is_active, num_runs, url.
    """
    client = create_client(api_key, source)
    status = client.beta.task_group.retrieve(taskgroup_id)
    status_counts = dict(status.status.task_run_status_counts or {})

    return {
        "taskgroup_id": taskgroup_id,
        "status_counts": status_counts,
        "is_active": status.status.is_active,
        "num_runs": status.status.num_task_runs,
        "url": f"{PLATFORM_BASE}/view/task-run-group/{taskgroup_id}",
    }


def poll_task_group(
    taskgroup_id: str,
    api_key: str | None = None,
    timeout: int = 3600,
    poll_interval: int = 5,
    on_progress: Callable[[int, int, int], None] | None = None,
    source: ClientSource = "python",
) -> list[dict[str, Any]]:
    """Poll a task group until completion and collect results.

    Args:
        taskgroup_id: The task group ID.
        api_key: Optional API key.
        timeout: Max wait time in seconds (default: 3600).
        poll_interval: Seconds between status checks (default: 5).
        on_progress: Optional callback called with (completed, failed, total).
        source: Client source identifier for User-Agent.

    Returns:
        List of result dicts (raw enrichment results, no Pydantic validation).

    Raises:
        TimeoutError: If the task group doesn't complete within timeout.
    """
    client = create_client(api_key, source)
    logger = logging.getLogger(__name__)

    deadline = time.time() + timeout

    while time.time() < deadline:
        status = client.beta.task_group.retrieve(taskgroup_id)
        status_counts = status.status.task_run_status_counts or {}
        completed = status_counts.get("completed", 0)
        failed = status_counts.get("failed", 0)
        total = status.status.num_task_runs

        if on_progress:
            on_progress(completed, failed, total)

        if not status.status.is_active:
            logger.info("Task group completed!")
            break

        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"Task group {taskgroup_id} timed out after {timeout} seconds")

    # Collect results
    results = []
    runs_stream = client.beta.task_group.get_runs(taskgroup_id, include_input=True, include_output=True)

    for event in runs_stream:
        if event.type == "task_run.state":
            result: dict[str, Any] = {}
            if event.input and hasattr(event.input, "input"):
                result["input"] = event.input.input
            if content := getattr(event.output, "content", None):
                result["output"] = _parse_content(content)
            elif event.run.error:
                result["error"] = str(event.run.error)
            else:
                result["error"] = "No result"
            results.append(result)

    return results


def run_tasks(
    input_data: list[dict[str, Any]],
    InputModel,
    OutputModel,
    processor: str = "core-fast",
    source: ClientSource = "python",
    timeout: int = 3600,
) -> list[Any]:
    """Run batch tasks using Pydantic models for schema.

    Uses the Parallel SDK's task group API with proper SSE handling.

    Args:
        timeout: Max seconds to wait for completion (default: 3600 = 1 hour).
    """
    logger = logging.getLogger(__name__)

    batch_id = str(uuid.uuid4())
    logger.info(f"Generated batch_id: {batch_id}")

    # Create task group and add runs
    tg_info = create_task_group(input_data, InputModel, OutputModel, processor, source)
    taskgroup_id = tg_info["taskgroup_id"]

    # Wait for completion
    client = create_client(source=source)
    poll_start = time.time()
    while time.time() - poll_start < timeout:
        status = client.beta.task_group.retrieve(taskgroup_id)
        status_counts = status.status.task_run_status_counts or {}
        logger.info(f"Status: {status_counts}")

        if not status.status.is_active:
            logger.info("All tasks completed!")
            break

        time.sleep(2)
    else:
        logger.warning(f"Timed out after {timeout}s waiting for task group {taskgroup_id}")

    # Get results using SDK's streaming (handles SSE properly)
    results = []
    runs_stream = client.beta.task_group.get_runs(taskgroup_id, include_input=True, include_output=True)

    for event in runs_stream:
        if event.type == "task_run.state" and event.output:
            try:
                input_val = InputModel.model_validate(event.input.input if event.input else {})
                content = _parse_content(event.output.content)
                output_val = OutputModel.model_validate(content)
                results.append(
                    {
                        **input_val.model_dump(),
                        **output_val.model_dump(),
                        "batch_id": batch_id,
                        "insertion_timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to parse result: {e}")

    logger.info(f"Successfully processed {len(results)} entities.")
    return results
