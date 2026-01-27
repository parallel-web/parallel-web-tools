"""Core functionality for Parallel Data."""

from parallel_web_tools.core.auth import (
    get_api_key,
    get_async_client,
    get_auth_status,
    get_client,
    logout,
    resolve_api_key,
)
from parallel_web_tools.core.batch import (
    build_output_schema,
    enrich_batch,
    enrich_single,
    extract_basis,
    run_tasks,
)
from parallel_web_tools.core.research import (
    RESEARCH_PROCESSORS,
    create_research_task,
    get_research_result,
    get_research_status,
    poll_research,
    run_research,
)
from parallel_web_tools.core.result import EnrichmentResult
from parallel_web_tools.core.runner import (
    run_enrichment,
    run_enrichment_from_dict,
)
from parallel_web_tools.core.schema import (
    AVAILABLE_PROCESSORS,
    JSON_SCHEMA_TYPE_MAP,
    TYPE_MAP,
    Column,
    InputSchema,
    ParseError,
    ProcessorType,
    SourceType,
    get_available_types,
    load_schema,
    parse_input_and_output_models,
    parse_schema,
)
from parallel_web_tools.core.user_agent import (
    ClientSource,
    get_default_headers,
    get_user_agent,
)

__all__ = [
    # Auth
    "get_api_key",
    "get_auth_status",
    "get_client",
    "get_async_client",
    "logout",
    "resolve_api_key",
    # User Agent
    "ClientSource",
    "get_default_headers",
    "get_user_agent",
    # Schema
    "AVAILABLE_PROCESSORS",
    "Column",
    "InputSchema",
    "JSON_SCHEMA_TYPE_MAP",
    "ParseError",
    "ProcessorType",
    "SourceType",
    "TYPE_MAP",
    "get_available_types",
    "load_schema",
    "parse_schema",
    "parse_input_and_output_models",
    # Batch
    "build_output_schema",
    "enrich_batch",
    "enrich_single",
    "extract_basis",
    "run_tasks",
    # Runner
    "run_enrichment",
    "run_enrichment_from_dict",
    # Research
    "RESEARCH_PROCESSORS",
    "create_research_task",
    "get_research_result",
    "get_research_status",
    "poll_research",
    "run_research",
    # Result
    "EnrichmentResult",
]
