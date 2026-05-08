"""Monitor: continuously track the web for changes using the Parallel Monitor API.

Monitors run on a fixed frequency and emit events when material changes are
detected. Two monitor types are supported:

- ``event_stream`` (default): tracks a natural-language search query.
- ``snapshot``: tracks the output of a specific Task Run.

Results can be polled via the events endpoint or delivered via webhooks. This
module wraps the ``client.monitor.*`` SDK resource and returns plain dicts so
the CLI and other callers don't have to deal with pydantic models.
"""

from __future__ import annotations

from typing import Any

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.user_agent import ClientSource

# Friendly aliases for SDK frequency strings.
# The SDK accepts "<n><unit>" with unit in {h, d, w}, range 1h-30d (inclusive).
MONITOR_FREQUENCY_PRESETS: dict[str, str] = {
    "hourly": "1h",
    "daily": "1d",
    "weekly": "1w",
    "every_two_weeks": "2w",
}

MONITOR_TYPES: tuple[str, ...] = ("event_stream", "snapshot")
MONITOR_PROCESSORS: tuple[str, ...] = ("lite", "base")
MONITOR_STATUSES: tuple[str, ...] = ("active", "cancelled")

# Webhook event types accepted by the API.
MONITOR_EVENT_TYPES: list[str] = [
    "monitor.event.detected",
    "monitor.execution.completed",
    "monitor.execution.failed",
]


def resolve_frequency(value: str) -> str:
    """Translate a friendly preset (e.g. "daily") to an SDK frequency string ("1d")."""
    return MONITOR_FREQUENCY_PRESETS.get(value, value)


def _build_webhook(url: str, event_types: list[str] | None = None) -> dict[str, Any]:
    return {"url": url, "event_types": event_types or ["monitor.event.detected"]}


def _to_dict(model: Any) -> dict[str, Any]:
    """Convert an SDK pydantic response to a JSON-safe dict."""
    if model is None:
        return {}
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    if isinstance(model, dict):
        return model
    return dict(model)


def create_monitor(
    query: str | None = None,
    frequency: str = "1d",
    *,
    type: str = "event_stream",
    task_run_id: str | None = None,
    webhook: str | None = None,
    metadata: dict[str, str] | None = None,
    output_schema: dict[str, Any] | None = None,
    include_backfill: bool | None = None,
    processor: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Create a new monitor.

    Args:
        query: Search query to monitor (required for ``type="event_stream"``).
        frequency: How often to run the monitor. Format ``<n><unit>`` where unit is
            ``h``, ``d``, or ``w`` (e.g. ``1d``, ``12h``, ``2w``). Friendly aliases
            like ``"daily"`` and ``"hourly"`` are also accepted.
        type: ``event_stream`` to track a search query, or ``snapshot`` to track a
            specific Task Run output.
        task_run_id: Task Run whose output should be tracked (required for
            ``type="snapshot"``).
        webhook: Optional webhook URL for event delivery.
        metadata: Optional metadata dict (max 16 keys, max 512 chars per value).
        output_schema: Optional JSON schema for structured output (event_stream only).
        include_backfill: For event_stream monitors, include a sample of historical
            events on the first run.
        processor: ``lite`` (default, fast/cheap) or ``base`` (more thorough).
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict representation of the created Monitor.
    """
    client = create_client(api_key, source)

    settings: dict[str, Any]
    if type == "event_stream":
        if not query:
            raise ValueError("query is required when type='event_stream'")
        settings = {"query": query}
        if include_backfill is not None:
            settings["include_backfill"] = include_backfill
        if output_schema is not None:
            settings["output_schema"] = {"type": "json", "json_schema": output_schema}
    elif type == "snapshot":
        if not task_run_id:
            raise ValueError("task_run_id is required when type='snapshot'")
        settings = {"task_run_id": task_run_id}
    else:
        raise ValueError(f"Unsupported monitor type: {type!r}")

    kwargs: dict[str, Any] = {
        "frequency": resolve_frequency(frequency),
        "settings": settings,
        "type": type,
    }
    if webhook is not None:
        kwargs["webhook"] = _build_webhook(webhook)
    if metadata is not None:
        kwargs["metadata"] = metadata
    if processor is not None:
        kwargs["processor"] = processor

    return _to_dict(client.monitor.create(**kwargs))


def list_monitors(
    cursor: str | None = None,
    limit: int | None = None,
    *,
    status: list[str] | None = None,
    type: list[str] | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """List monitors with cursor-based pagination.

    Args:
        cursor: Pagination token from ``next_cursor`` of a previous response.
        limit: Maximum number of monitors to return (1-10000, default 100).
        status: Filter by status. Defaults to ``["active"]`` server-side.
        type: Filter by monitor type.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with ``monitors`` (list) and optional ``next_cursor``.
    """
    client = create_client(api_key, source)
    kwargs: dict[str, Any] = {}
    if cursor is not None:
        kwargs["cursor"] = cursor
    if limit is not None:
        kwargs["limit"] = limit
    if status is not None:
        kwargs["status"] = status
    if type is not None:
        kwargs["type"] = type
    return _to_dict(client.monitor.list(**kwargs))


def get_monitor(
    monitor_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve a monitor by ID."""
    client = create_client(api_key, source)
    return _to_dict(client.monitor.retrieve(monitor_id))


def update_monitor(
    monitor_id: str,
    *,
    frequency: str | None = None,
    metadata: dict[str, str] | None = None,
    type: str | None = None,
    webhook: str | None = None,
    advanced_settings: dict[str, Any] | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Update an existing monitor.

    Only ``frequency``, ``metadata``, ``webhook``, and (event_stream-only)
    ``advanced_settings`` can be modified after creation. Query / task_run_id
    are immutable — create a new monitor to change them.

    Args:
        monitor_id: The monitor to update.
        frequency: New frequency (e.g. ``"6h"``, ``"1w"``); aliases accepted.
        metadata: Replacement metadata dict.
        type: Required when ``advanced_settings`` is provided; must be ``event_stream``.
        webhook: Replacement webhook URL.
        advanced_settings: Advanced configuration overrides for event_stream monitors.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict representation of the updated Monitor.
    """
    client = create_client(api_key, source)
    kwargs: dict[str, Any] = {}
    if frequency is not None:
        kwargs["frequency"] = resolve_frequency(frequency)
    if metadata is not None:
        kwargs["metadata"] = metadata
    if webhook is not None:
        kwargs["webhook"] = _build_webhook(webhook)
    if advanced_settings is not None:
        kwargs["settings"] = {"advanced_settings": advanced_settings}
        if type is None:
            type = "event_stream"
    if type is not None:
        kwargs["type"] = type

    if not kwargs:
        raise ValueError("At least one field must be provided to update_monitor")

    return _to_dict(client.monitor.update(monitor_id, **kwargs))


def cancel_monitor(
    monitor_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Cancel a monitor (irreversible). Replaces ``delete_monitor`` from the alpha API."""
    client = create_client(api_key, source)
    return _to_dict(client.monitor.cancel(monitor_id))


def list_monitor_events(
    monitor_id: str,
    *,
    cursor: str | None = None,
    event_group_id: str | None = None,
    include_completions: bool = False,
    limit: int | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """List events for a monitor, newest first.

    Pass ``event_group_id`` to narrow results to a single execution. Pagination
    parameters are ignored when ``event_group_id`` is set.

    Args:
        monitor_id: The monitor whose events to fetch.
        cursor: Pagination token from a previous response.
        event_group_id: Restrict results to a single execution.
        include_completions: Include no-change completion events (audit history).
        limit: Maximum number of events to return (1-100, default 20).
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with ``events`` (list), optional ``next_cursor``, and optional ``warnings``.
    """
    client = create_client(api_key, source)
    kwargs: dict[str, Any] = {}
    if cursor is not None:
        kwargs["cursor"] = cursor
    if event_group_id is not None:
        kwargs["event_group_id"] = event_group_id
    if include_completions:
        kwargs["include_completions"] = True
    if limit is not None:
        kwargs["limit"] = limit
    return _to_dict(client.monitor.events(monitor_id, **kwargs))


def trigger_monitor(
    monitor_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> None:
    """Trigger an immediate one-off monitor run.

    The monitor's regular schedule is unaffected. An event is only emitted if
    a material change is detected. Replaces the alpha ``simulate_event`` flow.
    """
    client = create_client(api_key, source)
    client.monitor.trigger(monitor_id)
