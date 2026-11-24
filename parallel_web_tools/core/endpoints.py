"""Endpoint and client-identity configuration for parallel-cli.

Three base URLs are configurable via env vars so the CLI can be pointed at a
local dev stack:

- ``PARALLEL_PLATFORM_URL`` — the platform that serves ``/getServiceKeys/*``
  (device authorization, token exchange, revocation). Default
  ``https://platform.parallel.ai``; for local dev set to ``http://localhost:3000``.

- ``PARALLEL_SERVICE_API_URL`` — the service/account API that serves
  ``/service/v1/*`` (apps, product API credentials, balance). Default
  ``https://api.parallel.ai/account``; for local dev set to
  ``http://localhost:8090``.

- ``PARALLEL_API_URL`` — the data API (search, extract, research, enrich,
  findall, monitor). Default ``https://api.parallel.ai``. When it is set, it
  also overrides the origin a stored OAuth credential was issued for.
"""

from __future__ import annotations

import os

DEFAULT_PLATFORM_URL = "https://platform.parallel.ai"
DEFAULT_SERVICE_API_URL = "https://api.parallel.ai/account"
DEFAULT_API_URL = "https://api.parallel.ai"

CLIENT_ID = "parallel-cli"
DEFAULT_SCOPE = "keys:read keys:create keys:delete apps:read apps:create apps:delete balance:read balance:add"

PARALLEL_CLI_APP_NAME = "parallel-cli Users"

# Scopes requested for the product API OAuth credential. The account service
# accepts only the wildcard.
PRODUCT_API_SCOPES = ("*",)


def get_platform_url() -> str:
    """Return the platform base URL (no trailing slash)."""
    return os.environ.get("PARALLEL_PLATFORM_URL", DEFAULT_PLATFORM_URL).rstrip("/")


def get_service_api_url() -> str:
    """Return the service API base URL (no trailing slash)."""
    return os.environ.get("PARALLEL_SERVICE_API_URL", DEFAULT_SERVICE_API_URL).rstrip("/")


def get_api_url() -> str:
    """Return the data API base URL (no trailing slash)."""
    return os.environ.get("PARALLEL_API_URL", DEFAULT_API_URL).rstrip("/")
