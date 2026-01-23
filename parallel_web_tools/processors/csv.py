"""CSV processor for data enrichment."""

import csv
import logging

from parallel_web_tools.core import InputSchema, parse_input_and_output_models, run_tasks

logger = logging.getLogger(__name__)


def process_csv(schema: InputSchema):
    """Process CSV file and enrich data."""
    logger.info("Processing CSV file: %s", schema.source)

    InputModel, OutputModel = parse_input_and_output_models(schema)

    # Read all rows from CSV
    data = []
    with open(schema.source) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            data.append(dict(row))

    # Process all rows in batch
    output_rows = run_tasks(data, InputModel, OutputModel, schema.processor)

    # Write results to target CSV
    with open(schema.target, "w", newline="") as f:
        fieldnames = output_rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
