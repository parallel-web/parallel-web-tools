"""Collect deprecation notices that the Parallel API sends with responses.

The API marks deprecated endpoints with RFC 9745 ``Deprecation`` headers
(plus ``Sunset`` and ``Link`` headers) and with ``deprecation_warning``
entries in a response body's ``warnings`` list. Clients created for the CLI
record these notices so the CLI can update itself or tell the caller to
update after the command finishes.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

DEPRECATION_WARNING_TYPES = frozenset({"deprecation_warning", "deprecation"})

_LINK_RE = re.compile(r"<([^>]*)>\s*((?:;[^,<]*)*)")
_REL_RE = re.compile(r';\s*rel\s*=\s*"?([^";]+)"?', re.IGNORECASE)


@dataclass(frozen=True)
class DeprecationNotice:
    """One deprecated API call seen during this process."""

    endpoint: str | None = None
    message: str | None = None
    link: str | None = None
    successor: str | None = None
    sunset: str | None = None


_lock = threading.Lock()
_notices: list[DeprecationNotice] = []


def record_notice(notice: DeprecationNotice) -> None:
    with _lock:
        if notice not in _notices:
            _notices.append(notice)


def get_notices() -> list[DeprecationNotice]:
    with _lock:
        return list(_notices)


def clear_notices() -> None:
    with _lock:
        _notices.clear()


def _parse_links(values: Iterable[str]) -> dict[str, str]:
    links: dict[str, str] = {}
    for value in values:
        for url, params in _LINK_RE.findall(value):
            for rel_match in _REL_RE.finditer(params):
                for rel in rel_match.group(1).lower().split():
                    links.setdefault(rel, url)
    return links


def notice_from_headers(headers: httpx.Headers, endpoint: str | None = None) -> DeprecationNotice | None:
    """Build a notice from RFC 9745 / RFC 8594 headers, or None if not deprecated."""
    if "deprecation" not in headers:
        return None
    links = _parse_links(headers.get_list("link"))
    return DeprecationNotice(
        endpoint=endpoint,
        link=links.get("deprecation"),
        successor=links.get("successor-version"),
        sunset=headers.get("sunset"),
    )


def _get(obj: Any, key: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key)
    return getattr(obj, key, None)


def record_body_warnings(warnings: Iterable[Any] | None, endpoint: str | None = None) -> None:
    """Record ``deprecation_warning`` entries from a response body's ``warnings`` list."""
    for warning in warnings or []:
        if _get(warning, "type") not in DEPRECATION_WARNING_TYPES:
            continue
        detail = _get(warning, "detail")
        link = _get(detail, "documentation") if detail is not None else None
        successor = _get(detail, "replacement_endpoint") if detail is not None else None
        message = _get(warning, "message")
        record_notice(
            DeprecationNotice(
                endpoint=endpoint,
                message=message if isinstance(message, str) else None,
                link=link if isinstance(link, str) else None,
                successor=successor if isinstance(successor, str) else None,
            )
        )


def _endpoint(response: httpx.Response) -> str:
    request = response.request
    return f"{request.method} {request.url.path}"


def record_response(response: httpx.Response) -> None:
    """httpx response hook: record a notice when the response is marked deprecated."""
    notice = notice_from_headers(response.headers, endpoint=_endpoint(response))
    if notice is not None:
        record_notice(notice)


async def record_response_async(response: httpx.Response) -> None:
    record_response(response)
