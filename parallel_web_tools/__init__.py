"""Parallel Data Enrichment package."""

# Re-export everything from core for convenience
from parallel_web_tools.core import (
    AVAILABLE_PROCESSORS,
    Column,
    DeviceCodeInfo,
    InputSchema,
    ParseError,
    ProcessorType,
    SourceType,
    create_monitor,
    enrich_batch,
    enrich_single,
    get_api_key,
    get_async_client,
    get_auth_status,
    get_client,
    load_schema,
    logout,
    parse_input_and_output_models,
    parse_schema,
    poll_device_token,
    request_device_code,
    run_enrichment,
    run_enrichment_from_dict,
    run_findall,
    run_research,
    run_tasks,
)

__version__ = "0.1.2rc2"

__all__ = [
    # Auth
    "DeviceCodeInfo",
    "get_api_key",
    "get_auth_status",
    "get_client",
    "get_async_client",
    "logout",
    "poll_device_token",
    "request_device_code",
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
    # FindAll
    "run_findall",
    # Monitor
    "create_monitor",
    # Research
    "run_research",
]
