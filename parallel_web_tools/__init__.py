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
    cancel_monitor,
    create_monitor,
    enrich_batch,
    enrich_single,
    entity_search_findall,
    get_api_key,
    get_async_client,
    get_auth_status,
    get_client,
    get_monitor,
    list_monitor_events,
    list_monitors,
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
    trigger_monitor,
    update_monitor,
)

__version__ = "0.7.1"

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
    "entity_search_findall",
    # Monitor
    "cancel_monitor",
    "create_monitor",
    "get_monitor",
    "list_monitor_events",
    "list_monitors",
    "trigger_monitor",
    "update_monitor",
    # Research
    "run_research",
]
