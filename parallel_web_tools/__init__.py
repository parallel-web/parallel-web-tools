"""Parallel Data Enrichment package."""

# Re-export everything from core for convenience
from parallel_web_tools.core import (
    # Schema
    AVAILABLE_PROCESSORS,
    Column,
    InputSchema,
    ParseError,
    ProcessorType,
    SourceType,
    # Batch
    enrich_batch,
    enrich_single,
    # Auth
    get_api_key,
    get_async_client,
    get_auth_status,
    get_client,
    load_schema,
    logout,
    parse_input_and_output_models,
    parse_schema,
    # Runner
    run_enrichment,
    run_enrichment_from_dict,
    run_tasks,
)

__version__ = "0.0.11"

__all__ = [
    # Auth
    "get_api_key",
    "get_auth_status",
    "get_client",
    "get_async_client",
    "logout",
    # Schema
    "AVAILABLE_PROCESSORS",
    "Column",
    "InputSchema",
    "ParseError",
    "ProcessorType",
    "SourceType",
    "load_schema",
    "parse_schema",
    "parse_input_and_output_models",
    # Batch
    "enrich_batch",
    "enrich_single",
    "run_tasks",
    # Runner
    "run_enrichment",
    "run_enrichment_from_dict",
]
