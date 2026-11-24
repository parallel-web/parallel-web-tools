"""Service API client for parallel-cli.

Wraps the subset of ``/service/v1/*`` endpoints the CLI consumes:

- ``GET  /service/v1/apps``                            — list apps for the caller's org
- ``POST /service/v1/apps/{app_id}/oauth-credentials`` — create a product API OAuth credential
- ``GET  /service/v1/balance``                         — read the org's prepaid balance
- ``POST /service/v1/balance/add``                     — charge Stripe and top up balance

Request and response shapes are parsed with the Pydantic models in
:mod:`parallel_web_tools.core.service_types` (auto-generated from the OpenAPI
spec; regenerate with ``scripts/generate_service_types.py``).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from pydantic import ValidationError

from parallel_web_tools.core.endpoints import PARALLEL_CLI_APP_NAME, PRODUCT_API_SCOPES, get_service_api_url
from parallel_web_tools.core.service_types import (
    AddBalanceRequest,
    AppItem,
    BalanceResponse,
    CreateOAuthCredentialRequest,
    CreateOAuthCredentialResponse,
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


def create_oauth_credential(
    access_token: str,
    app_id: str,
    client_id: str,
    audience: str,
    scopes: list[str],
) -> CreateOAuthCredentialResponse:
    """Create a product API OAuth credential on the given app.

    The response holds a rotating refresh token, returned only once, and the
    ``token_endpoint`` to exchange it at.
    """
    body = CreateOAuthCredentialRequest(client_id=client_id, audience=audience, scopes=scopes).model_dump()
    data = _request("POST", f"/service/v1/apps/{app_id}/oauth-credentials", access_token, body=body)
    try:
        return CreateOAuthCredentialResponse.model_validate(data)
    except ValidationError as e:
        raise ServiceApiError(f"Unexpected create_oauth_credential response: {e}") from e


def get_balance(access_token: str) -> BalanceResponse:
    """Return the caller's current org balance."""
    data = _request("GET", "/service/v1/balance", access_token)
    try:
        return BalanceResponse.model_validate(data)
    except ValidationError as e:
        raise ServiceApiError(f"Unexpected /service/v1/balance response: {e}") from e


def add_balance(access_token: str, amount_cents: int, idempotency_key: str) -> BalanceResponse:
    """Charge the org's payment method and top up the prepaid balance.

    Returns the updated :class:`BalanceResponse`. ``idempotency_key`` must be
    high-entropy; the server dedupes repeat charges for at least 24h when the
    same key is submitted.
    """
    body = AddBalanceRequest(amount_cents=amount_cents, idempotency_key=idempotency_key).model_dump()
    data = _request("POST", "/service/v1/balance/add", access_token, body=body)
    try:
        return BalanceResponse.model_validate(data)
    except ValidationError as e:
        raise ServiceApiError(f"Unexpected /service/v1/balance/add response: {e}") from e


def provision_cli_oauth_credential(access_token: str, client_id: str, audience: str) -> CreateOAuthCredentialResponse:
    """Find the ``parallel-cli Users`` app and create a product API OAuth credential on it.

    The refresh token in the result is only returned once, so the caller must
    persist it immediately.
    """
    apps = list_apps(access_token)
    match = next((a for a in apps if a.app_name == PARALLEL_CLI_APP_NAME), None)
    if match is None:
        raise ServiceApiError(
            f"No app named {PARALLEL_CLI_APP_NAME!r} found for this org. "
            "It should be auto-created during device authorization; contact support if missing."
        )
    return create_oauth_credential(access_token, match.app_id, client_id, audience, list(PRODUCT_API_SCOPES))
