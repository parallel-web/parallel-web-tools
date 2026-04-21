"""Service API client for parallel-cli.

Wraps the subset of ``/service/v1/*`` endpoints the CLI needs to provision a
data-API key for the currently-authenticated user:

- ``GET  /service/v1/apps``               — list apps for the caller's org
- ``POST /service/v1/apps/{app_id}/keys`` — create an API key on an app

Request and response shapes are parsed with the Pydantic models in
:mod:`parallel_web_tools.core.service_types` (auto-generated from the OpenAPI
spec; regenerate with ``scripts/generate_service_types.py``).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from pydantic import ValidationError

from parallel_web_tools.core.endpoints import PARALLEL_CLI_APP_NAME, get_service_api_url
from parallel_web_tools.core.service_types import (
    AppItem,
    CreateApiKeyRequestModel,
    CreateKeyResponse,
    GetAppsForOrgResponseModel,
)


class ServiceApiError(Exception):
    """Raised when the service API returns an error or an unexpected payload."""


def _request(
    method: str,
    path: str,
    access_token: str,
    body: dict | None = None,
    timeout: int = 30,
) -> Any:
    url = f"{get_service_api_url()}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise ServiceApiError(f"{method} {path} failed: {e.code} - {body_text}") from e


def list_apps(access_token: str) -> list[AppItem]:
    """Return all apps for the caller's org."""
    data = _request("GET", "/service/v1/apps", access_token)
    try:
        resp = GetAppsForOrgResponseModel.model_validate(data or {})
    except ValidationError as e:
        raise ServiceApiError(f"Unexpected /service/v1/apps response: {e}") from e
    return resp.apps or []


def create_api_key(access_token: str, app_id: str, api_key_name: str) -> CreateKeyResponse:
    """Create an API key on the given app and return the typed result."""
    body = CreateApiKeyRequestModel(api_key_name=api_key_name).model_dump()
    data = _request("POST", f"/service/v1/apps/{app_id}/keys", access_token, body=body)
    try:
        return CreateKeyResponse.model_validate(data)
    except ValidationError as e:
        raise ServiceApiError(f"Unexpected create_api_key response: {e}") from e


def _build_key_name(client_id: str | None = None, now: float | None = None) -> str:
    """Return a CLI-minted key name.

    Uses the registered OAuth ``client_id`` as the high-entropy prefix, with a
    ``YYYY-MM-DD-HHMM`` suffix so the same client can mint multiple keys and
    still distinguish them. Falls back to the plain ``parallel-cli`` prefix
    when no ``client_id`` is available (e.g. registration failed earlier).
    """
    prefix = client_id or "parallel-cli"
    return f"{prefix}-{time.strftime('%Y-%m-%d-%H%M', time.localtime(now))}"


def provision_cli_api_key(access_token: str, client_id: str | None = None) -> tuple[str, str]:
    """Find the ``parallel-cli Users`` app and mint a fresh API key on it.

    Returns ``(raw_api_key, key_name)``. The raw key is only returned once by
    the server — at creation time — so the caller must persist it immediately.
    """
    apps = list_apps(access_token)
    match = next((a for a in apps if a.app_name == PARALLEL_CLI_APP_NAME), None)
    if match is None:
        raise ServiceApiError(
            f"No app named {PARALLEL_CLI_APP_NAME!r} found for this org. "
            "It should be auto-created during device authorization; contact support if missing."
        )
    key_name = _build_key_name(client_id)
    created = create_api_key(access_token, match.app_id, key_name)
    if not created.raw_api_key:
        raise ServiceApiError("Server returned no raw_api_key on key creation; cannot persist a usable key without it.")
    return created.raw_api_key, key_name
