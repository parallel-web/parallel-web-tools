"""Monitor: continuously track the web for changes using the Parallel Monitor API.

Monitors let you define natural-language queries that run on a schedule (cadence).
When changes are detected, events are generated and optionally delivered via webhook.

This module uses httpx directly since the SDK does not yet have high-level
convenience methods for Monitor endpoints.

The typical workflow is:
    1. Create a monitor with a query and cadence
    2. Optionally configure a webhook for real-time notifications
    3. List events to see detected changes
    4. Update or delete monitors as needed
"""

from __future__ import annotations

from typing import Any

import httpx

from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.user_agent import ClientSource, get_default_headers

BASE_URL = "https://api.parallel.ai"

# Supported cadences for monitor scheduling
MONITOR_CADENCES = {
    "5min": "Every 5 minutes",
    "15min": "Every 15 minutes",
    "30min": "Every 30 minutes",
    "hourly": "Every hour",
    "daily": "Once per day",
    "weekly": "Once per week",
}

# Valid webhook event types
MONITOR_EVENT_TYPES = [
    "change_detected",
    "monitor_error",
]


def _request(
    method: str,
    path: str,
    api_key: str | None = None,
    source: ClientSource = "python",
    json: Any | None = None,
    params: dict[str, Any] | None = None,
) -> httpx.Response:
    """Send an authenticated request to the Monitor API.

    Args:
        method: HTTP method (GET, POST, DELETE).
        path: API path (e.g., "/v1alpha/monitors").
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.
        json: Optional JSON body.
        params: Optional query parameters.

    Returns:
        The httpx Response object.

    Raises:
        httpx.HTTPStatusError: If the response indicates an error.
    """
    key = resolve_api_key(api_key)
    headers = {
        **get_default_headers(source),
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    url = f"{BASE_URL}{path}"

    response = httpx.request(method, url, headers=headers, json=json, params=params, timeout=30)
    response.raise_for_status()
    return response


def create_monitor(
    query: str,
    cadence: str,
    webhook: str | None = None,
    metadata: dict[str, Any] | None = None,
    output_schema: dict[str, Any] | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Create a new monitor.

    Args:
        query: Natural language query describing what to track.
        cadence: How often to check (e.g., "daily", "hourly").
        webhook: Optional webhook URL for event delivery.
        metadata: Optional metadata dict.
        output_schema: Optional JSON schema for structured output.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with monitor details including monitor_id.
    """
    body: dict[str, Any] = {"query": query, "cadence": cadence}
    if webhook is not None:
        body["webhook"] = webhook
    if metadata is not None:
        body["metadata"] = metadata
    if output_schema is not None:
        body["output_schema"] = output_schema

    resp = _request("POST", "/v1alpha/monitors", api_key=api_key, source=source, json=body)
    return resp.json()


def list_monitors(
    monitor_id: str | None = None,
    limit: int | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> list[dict[str, Any]]:
    """List monitors with optional pagination.

    Args:
        monitor_id: Cursor for pagination (start after this monitor).
        limit: Maximum number of monitors to return.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        List of monitor dicts.
    """
    params: dict[str, Any] = {}
    if monitor_id is not None:
        params["monitor_id"] = monitor_id
    if limit is not None:
        params["limit"] = limit

    resp = _request("GET", "/v1alpha/monitors", api_key=api_key, source=source, params=params)
    data = resp.json()
    return data.get("monitors", data) if isinstance(data, dict) else data


def get_monitor(
    monitor_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve a single monitor by ID.

    Args:
        monitor_id: The monitor ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with monitor details.
    """
    resp = _request("GET", f"/v1alpha/monitors/{monitor_id}", api_key=api_key, source=source)
    return resp.json()


def update_monitor(
    monitor_id: str,
    query: str | None = None,
    cadence: str | None = None,
    webhook: str | None = None,
    metadata: dict[str, Any] | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Update an existing monitor.

    Args:
        monitor_id: The monitor ID.
        query: Updated query text.
        cadence: Updated cadence.
        webhook: Updated webhook URL.
        metadata: Updated metadata dict.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with updated monitor details.
    """
    body: dict[str, Any] = {}
    if query is not None:
        body["query"] = query
    if cadence is not None:
        body["cadence"] = cadence
    if webhook is not None:
        body["webhook"] = webhook
    if metadata is not None:
        body["metadata"] = metadata

    resp = _request("POST", f"/v1alpha/monitors/{monitor_id}", api_key=api_key, source=source, json=body)
    return resp.json()


def delete_monitor(
    monitor_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Delete a monitor.

    Args:
        monitor_id: The monitor ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with deletion confirmation.
    """
    resp = _request("DELETE", f"/v1alpha/monitors/{monitor_id}", api_key=api_key, source=source)
    if resp.status_code == 204 or not resp.content:
        return {"monitor_id": monitor_id, "deleted": True}
    return resp.json()


def list_monitor_events(
    monitor_id: str,
    lookback_period: str = "10d",
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """List events for a monitor.

    Args:
        monitor_id: The monitor ID.
        lookback_period: How far back to look (e.g., "10d", "24h").
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with events list.
    """
    params = {"lookback_period": lookback_period}
    resp = _request("GET", f"/v1alpha/monitors/{monitor_id}/events", api_key=api_key, source=source, params=params)
    return resp.json()


def get_monitor_event_group(
    monitor_id: str,
    event_group_id: str,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve a specific event group.

    Args:
        monitor_id: The monitor ID.
        event_group_id: The event group ID.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.

    Returns:
        Dict with event group details.
    """
    resp = _request(
        "GET",
        f"/v1alpha/monitors/{monitor_id}/event_groups/{event_group_id}",
        api_key=api_key,
        source=source,
    )
    return resp.json()


def simulate_monitor_event(
    monitor_id: str,
    event_type: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> None:
    """Simulate an event for webhook testing.

    Args:
        monitor_id: The monitor ID.
        event_type: Optional event type to simulate.
        api_key: Optional API key override.
        source: Client source identifier for User-Agent.
    """
    body: dict[str, Any] = {}
    if event_type is not None:
        body["event_type"] = event_type

    _request(
        "POST",
        f"/v1alpha/monitors/{monitor_id}/simulate_event",
        api_key=api_key,
        source=source,
        json=body if body else None,
    )
