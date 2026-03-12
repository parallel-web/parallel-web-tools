"""Core runner for data enrichment tasks."""

import logging
from pathlib import Path

from parallel_web_tools.core.schema import InputSchema, SourceType, load_schema, parse_schema

logger = logging.getLogger(__name__)


def _run_processor(
    parsed_schema: InputSchema, no_wait: bool = False, previous_interaction_id: str | None = None
) -> dict | None:
    """Run the appropriate processor for the given schema."""
    match parsed_schema.source_type:
        case SourceType.CSV:
            from parallel_web_tools.processors.csv import process_csv

            return process_csv(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)
        case SourceType.JSON:
            from parallel_web_tools.processors.json import process_json

            return process_json(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)
        case SourceType.DUCKDB:
            from parallel_web_tools.processors.duckdb import process_duckdb

            return process_duckdb(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)
        case SourceType.BIGQUERY:
            from parallel_web_tools.processors.bigquery import process_bigquery

            return process_bigquery(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)
        case _:
            raise NotImplementedError(f"{parsed_schema.source_type} is not supported")


def run_enrichment(
    config_file: str | Path, no_wait: bool = False, previous_interaction_id: str | None = None
) -> dict | None:
    """Run data enrichment using a YAML config file.

    Args:
        config_file: Path to YAML configuration file
        no_wait: If True, return taskgroup info without waiting for completion.
        previous_interaction_id: Interaction ID from a previous task to reuse as context.

    Example:
        >>> from parallel_web_tools import run_enrichment
        >>> run_enrichment("my_config.yaml")
    """
    config_path = Path(config_file)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    logger.info(f"Loading configuration from {config_file}")
    schema = load_schema(str(config_path))
    parsed_schema = parse_schema(schema)

    logger.info(f"Running enrichment: {parsed_schema.source} -> {parsed_schema.target}")
    result = _run_processor(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)

    if no_wait:
        return result

    logger.info("Enrichment complete!")
    return None


def run_enrichment_from_dict(
    config: dict, no_wait: bool = False, previous_interaction_id: str | None = None
) -> dict | None:
    """Run data enrichment using a configuration dictionary.

    Args:
        config: Configuration dictionary matching YAML schema
        no_wait: If True, return taskgroup info without waiting for completion.
        previous_interaction_id: Interaction ID from a previous task to reuse as context.

    Example:
        >>> config = {
        ...     "source": "data.csv",
        ...     "target": "enriched.csv",
        ...     "source_type": "csv",
        ...     "source_columns": [{"name": "company", "description": "Company name"}],
        ...     "enriched_columns": [{"name": "revenue", "description": "Annual revenue"}]
        ... }
        >>> run_enrichment_from_dict(config)
    """
    logger.info("Running enrichment from configuration dictionary")
    parsed_schema = parse_schema(config)

    logger.info(f"Running enrichment: {parsed_schema.source} -> {parsed_schema.target}")
    result = _run_processor(parsed_schema, no_wait=no_wait, previous_interaction_id=previous_interaction_id)

    if no_wait:
        return result

    logger.info("Enrichment complete!")
    return None
