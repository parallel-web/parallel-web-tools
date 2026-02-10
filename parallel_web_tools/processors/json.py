"""JSON processor for data enrichment."""

import json
import logging
from typing import Any

from parallel_web_tools.core import InputSchema, parse_input_and_output_models, run_tasks
from parallel_web_tools.core.batch import create_task_group

logger = logging.getLogger(__name__)


def process_json(schema: InputSchema, no_wait: bool = False) -> dict[str, Any] | None:
    """Process JSON file and enrich data."""
    logger.info("Processing JSON file: %s", schema.source)

    InputModel, OutputModel = parse_input_and_output_models(schema)

    # Read all rows from JSON (expects a JSON array of objects)
    with open(schema.source) as f:
        data = json.load(f)

    if no_wait:
        return create_task_group(data, InputModel, OutputModel, schema.processor)

    # Process all rows in batch
    output_rows = run_tasks(data, InputModel, OutputModel, schema.processor)

    # Write results to target JSON
    with open(schema.target, "w") as f:
        json.dump(output_rows, f, indent=2)

    return None
