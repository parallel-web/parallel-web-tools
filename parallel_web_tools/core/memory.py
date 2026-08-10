"""Retrieve and manage Parallel Memory through the generated Python SDK."""

from __future__ import annotations

import datetime
import re
from typing import Any, Literal

from parallel_web_tools.core.auth import create_client
from parallel_web_tools.core.user_agent import ClientSource

MemoryKind = Literal["task", "monitor", "findall"]

MEMORY_KINDS: tuple[MemoryKind, ...] = ("task", "monitor", "findall")
MAX_MEMORY_QUERY_CHARS = 500
MAX_MEMORY_RESULTS = 25
MEMORY_IDENTIFIER_PATTERN = r"^[a-zA-Z0-9_-]{1,128}$"
_MEMORY_IDENTIFIER_RE = re.compile(MEMORY_IDENTIFIER_PATTERN)


class MemoryInputError(ValueError):
    """Raised when a Memory request is invalid before it reaches the API."""


class MemoryApiError(RuntimeError):
    """Raised when the Memory SDK returns a malformed response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def validate_memory_identifier(value: str, field: str) -> str:
    """Validate a scope key or source ID against the public API contract."""
    if not _MEMORY_IDENTIFIER_RE.fullmatch(value):
        raise MemoryInputError(f"{field} must be 1-128 ASCII letters, digits, underscores, or hyphens")
    return value


def validate_memory_scope_key(memory_scope_key: str | None) -> str | None:
    """Validate and return a Memory scope key, preserving ``None`` for personal memory."""
    if memory_scope_key is None:
        return None
    return validate_memory_identifier(memory_scope_key, "memory_scope_key")


def _normalize_since(value: str | datetime.datetime) -> str:
    """Return an RFC 3339 timestamp after verifying that it has a timezone."""
    if isinstance(value, datetime.datetime):
        parsed = value
        rendered = value.isoformat()
    else:
        rendered = value
        normalized = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
        try:
            parsed = datetime.datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise MemoryInputError("since must be an RFC 3339 timestamp") from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MemoryInputError("since must include a timezone")
    return rendered


def _serialize_sdk_response(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        data = value.model_dump(mode="json")
        if isinstance(data, dict):
            return data
    if hasattr(value, "to_dict"):
        data = value.to_dict()
        if isinstance(data, dict):
            return data
    raise MemoryApiError("Memory SDK returned an unexpected response")


def retrieve_memory(
    query: str | None = "",
    limit: int = 10,
    *,
    kind: MemoryKind | None = None,
    since: str | datetime.datetime | None = None,
    memory_scope_key: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Retrieve relevant or recent saved Task, Monitor, and FindAll runs."""
    if query is not None and len(query) > MAX_MEMORY_QUERY_CHARS:
        raise MemoryInputError(f"query must be at most {MAX_MEMORY_QUERY_CHARS} characters")
    if not 1 <= limit <= MAX_MEMORY_RESULTS:
        raise MemoryInputError(f"limit must be between 1 and {MAX_MEMORY_RESULTS}")
    if kind is not None and kind not in MEMORY_KINDS:
        raise MemoryInputError(f"kind must be one of: {', '.join(MEMORY_KINDS)}")

    kwargs: dict[str, Any] = {"query": query, "limit": limit}
    if kind is not None:
        kwargs["kind"] = kind
    if since is not None:
        kwargs["since"] = _normalize_since(since)
    scope_key = validate_memory_scope_key(memory_scope_key)
    if scope_key is not None:
        kwargs["memory_scope_key"] = scope_key

    client = create_client(api_key, source)
    result = _serialize_sdk_response(client.beta.memory.retrieve(**kwargs))
    if not isinstance(result.get("results"), list):
        raise MemoryApiError("Memory retrieve returned an unexpected response")
    return result


def evict_memory(
    kind: MemoryKind,
    source_id: str,
    *,
    memory_scope_key: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Remove one source from Memory without deleting the underlying resource."""
    if kind not in MEMORY_KINDS:
        raise MemoryInputError(f"kind must be one of: {', '.join(MEMORY_KINDS)}")

    kwargs: dict[str, Any] = {
        "kind": kind,
        "id": validate_memory_identifier(source_id, "source id"),
    }
    scope_key = validate_memory_scope_key(memory_scope_key)
    if scope_key is not None:
        kwargs["memory_scope_key"] = scope_key

    client = create_client(api_key, source)
    client.beta.memory.evict(**kwargs)
    return {"ok": True, "action": "evict"}


def clear_memory(
    *,
    memory_scope_key: str | None = None,
    api_key: str | None = None,
    source: ClientSource = "python",
) -> dict[str, Any]:
    """Permanently clear personal Memory or a scoped Memory."""
    kwargs: dict[str, Any] = {}
    scope_key = validate_memory_scope_key(memory_scope_key)
    if scope_key is not None:
        kwargs["memory_scope_key"] = scope_key

    client = create_client(api_key, source)
    client.beta.memory.clear(**kwargs)
    return {"ok": True, "action": "clear"}
