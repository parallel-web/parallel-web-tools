"""Batch enrichment using the Parallel Task Group API."""

from __future__ import annotations

import json
import time
from typing import Any

from parallel_web_tools.core.auth import resolve_api_key


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


def extract_basis(output) -> list[dict[str, Any]]:
    """Extract basis/citations from a Parallel API output."""
    basis_list: list[dict[str, Any]] = []

    if not hasattr(output, "basis") or not output.basis:
        return basis_list

    for field_basis in output.basis:
        basis_entry: dict[str, Any] = {}

        if hasattr(field_basis, "field") and field_basis.field:
            basis_entry["field"] = field_basis.field

        if hasattr(field_basis, "citations") and field_basis.citations:
            basis_entry["citations"] = [
                {
                    "url": c.url if hasattr(c, "url") else None,
                    "excerpts": c.excerpts if hasattr(c, "excerpts") else [],
                }
                for c in field_basis.citations
            ]

        if hasattr(field_basis, "reasoning") and field_basis.reasoning:
            basis_entry["reasoning"] = field_basis.reasoning

        if hasattr(field_basis, "confidence") and field_basis.confidence:
            basis_entry["confidence"] = field_basis.confidence

        # Fallback for simpler basis format
        if not basis_entry:
            if hasattr(field_basis, "url") and field_basis.url:
                basis_entry["url"] = field_basis.url
            if hasattr(field_basis, "title") and field_basis.title:
                basis_entry["title"] = field_basis.title
            if hasattr(field_basis, "excerpts") and field_basis.excerpts:
                basis_entry["excerpts"] = field_basis.excerpts

        if basis_entry:
            basis_list.append(basis_entry)

    return basis_list


def enrich_batch(
    inputs: list[dict[str, Any]],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    poll_interval: int = 10,
    include_basis: bool = True,
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

    Returns:
        List of result dictionaries in same order as inputs.
    """
    from parallel.types import JsonSchemaParam, TaskSpecParam

    if not inputs:
        return []

    try:
        from parallel import Parallel

        client = Parallel(api_key=resolve_api_key(api_key))
        output_schema = build_output_schema(output_columns)
        task_spec = TaskSpecParam(output_schema=JsonSchemaParam(type="json", json_schema=output_schema))

        # Create task group
        task_group = client.beta.task_group.create()
        taskgroup_id = task_group.task_group_id

        # Add runs
        run_inputs = [{"input": inp, "processor": processor} for inp in inputs]
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
                if event.output and hasattr(event.output, "content"):
                    content = event.output.content
                    if isinstance(content, dict):
                        result = dict(content)
                    elif isinstance(content, str):
                        try:
                            result = json.loads(content)
                        except json.JSONDecodeError:
                            result = {"result": content}
                    else:
                        result = {"result": str(content)}

                    if include_basis:
                        result["basis"] = extract_basis(event.output)

                    results_by_id[run_id] = result
                elif event.run.error:
                    results_by_id[run_id] = {"error": str(event.run.error)}

        return [results_by_id.get(run_id, {"error": "No result"}) for run_id in run_ids]

    except Exception as e:
        return [{"error": str(e)}] * len(inputs)


def enrich_single(
    input_data: dict[str, Any],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
    include_basis: bool = True,
) -> dict[str, Any]:
    """Enrich a single input using the Parallel API."""
    results = enrich_batch(
        [input_data],
        output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
    )
    return results[0] if results else {"error": "No result"}


def run_tasks(
    input_data: list[dict[str, Any]],
    InputModel,
    OutputModel,
    processor: str = "core-fast",
) -> list[Any]:
    """Run batch tasks using Pydantic models for schema.

    This is the async-based batch processing using task groups.
    For simpler use cases, use enrich_batch() instead.
    """
    import asyncio
    import logging
    import uuid
    from datetime import UTC, datetime

    from parallel.types import TaskSpecParam

    logger = logging.getLogger(__name__)

    def build_task_spec_param(input_schema, output_schema) -> TaskSpecParam:
        return {
            "input_schema": {"type": "json", "json_schema": input_schema.model_json_schema()},
            "output_schema": {"type": "json", "json_schema": output_schema.model_json_schema()},
        }

    async def run_batch_task(
        input_data: list[dict[str, Any]],
        InputModel,
        OutputModel,
        processor: str,
        batch_size: int = 100,
    ):
        import httpx

        from parallel_web_tools.core.auth import get_api_key

        batch_id = str(uuid.uuid4())
        logger.info(f"Generated batch_id: {batch_id}")

        api_key = get_api_key()
        base_url = "https://api.parallel.ai"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Anthropic-Beta": "tasks-2025-01-15",
        }

        async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=120) as client:
            # Create task group
            response = await client.post("/v1beta/tasks/groups", json={})
            response.raise_for_status()
            group_response = response.json()
            taskgroup_id = group_response["taskgroup_id"]
            logger.info(f"Created taskgroup id {taskgroup_id}")

            total_created = 0

            for i in range(0, len(input_data), batch_size):
                batch = input_data[i : i + batch_size]
                run_inputs = [{"input": row, "processor": processor} for row in batch]
                task_spec = build_task_spec_param(InputModel, OutputModel)

                response = await client.post(
                    f"/v1beta/tasks/groups/{taskgroup_id}/runs",
                    json={"default_task_spec": task_spec, "inputs": run_inputs},
                )
                response.raise_for_status()
                resp_data = response.json()
                total_created += len(resp_data.get("run_ids", []))
                logger.info(f"Processing {i + len(batch)} entities. Created {total_created} Tasks.")

            # Wait for completion
            while True:
                response = await client.get(f"/v1beta/tasks/groups/{taskgroup_id}")
                response.raise_for_status()
                resp_data = response.json()
                status = resp_data.get("status", {})
                logger.info(f"Status: {status.get('task_run_status_counts', {})}")

                if not status.get("is_active", True):
                    logger.info("All tasks completed!")
                    break

                await asyncio.sleep(10)

            # Get results - use streaming endpoint
            results = []
            path = f"/v1beta/tasks/groups/{taskgroup_id}/runs?include_input=true&include_output=true"

            async with client.stream("GET", path) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.strip():
                        continue
                    # Parse SSE data lines
                    if line.startswith("data: "):
                        import json

                        event = json.loads(line[6:])
                        if event.get("type") == "task_run.state" and event.get("output"):
                            input_val = InputModel.model_validate(event["input"]["input"])
                            output_val = OutputModel.model_validate(event["output"]["content"])
                            results.append(
                                {
                                    **input_val.model_dump(),
                                    **output_val.model_dump(),
                                    "batch_id": batch_id,
                                    "insertion_timestamp": datetime.now(UTC).isoformat(),
                                }
                            )

            logger.info(f"Successfully processed {len(results)} entities.")
            return results

    return asyncio.run(run_batch_task(input_data, InputModel, OutputModel, processor))
