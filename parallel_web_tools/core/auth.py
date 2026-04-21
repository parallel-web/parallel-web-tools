"""Device-flow authentication for parallel-cli.

Authentication happens exclusively via the OAuth 2.0 Device Authorization Grant
(RFC 8628) against the platform's ``/getServiceKeys/*`` endpoints. After a
successful device flow the CLI additionally provisions a data-API key against
the service API so that subsequent commands (search, extract, etc.) have a key
to use.

All endpoints are built from :mod:`parallel_web_tools.core.endpoints`, so a
local dev stack can be reached via ``PARALLEL_PLATFORM_URL`` /
``PARALLEL_SERVICE_API_URL`` env vars.
"""

from __future__ import annotations

import json
import os
import platform as _platform
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass

from parallel import AsyncParallel, Parallel

from parallel_web_tools.core import credentials, service
from parallel_web_tools.core.endpoints import (
    CLIENT_ID,
    DEFAULT_SCOPE,
    get_api_url,
    get_platform_url,
)
from parallel_web_tools.core.user_agent import ClientSource, get_default_headers

DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
REFRESH_TOKEN_GRANT_TYPE = "refresh_token"

# Proactively refresh when the access token is within this many seconds of its
# absolute expiry, so callers don't get a token that dies mid-request under clock
# skew or network latency.
ACCESS_TOKEN_SKEW_SECONDS = 30


class ReauthenticationRequired(Exception):
    """Raised when the control-API grant can no longer be refreshed silently.

    The caller must run ``parallel-cli login`` before any control-API call
    will succeed — the authorization grant, the refresh token, or both have
    expired (or never existed), so no silent refresh is possible.
    """


@dataclass
class DeviceCodeInfo:
    """Response from the device authorization endpoint (RFC 8628)."""

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int
    interval: int


@dataclass
class TokenResponse:
    """Response from ``/getServiceKeys/token`` (device or refresh grant)."""

    access_token: str
    refresh_token: str
    expires_in: int
    refresh_token_expires_in: int
    authorization_expires_in: int
    org_id: str
    scope: str = ""
    token_type: str = "Bearer"

    @property
    def scopes(self) -> list[str]:
        return self.scope.split() if self.scope else []


def _platform_path(path: str) -> str:
    return f"{get_platform_url()}{path}"


def _is_headless() -> bool:
    """Detect if the environment cannot open a browser."""
    if os.environ.get("SSH_CLIENT") or os.environ.get("SSH_TTY"):
        return True
    if os.environ.get("CI"):
        return True
    if sys.platform == "linux" and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        return True
    if os.path.exists("/.dockerenv") or os.environ.get("container"):
        return True
    return False


def _post_form(url: str, data: dict[str, str], headers: dict[str, str] | None = None, timeout: int = 30) -> dict:
    """POST a form-encoded request, return parsed JSON body.

    Raises ``urllib.error.HTTPError`` on HTTP error (body still readable via ``e.read()``).
    """
    body = urllib.parse.urlencode(data).encode()
    req_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=body, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode())


def _post_json(url: str, body: dict, timeout: int = 30) -> dict:
    """POST a JSON body, return parsed JSON response."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode())


def _get_platform_info() -> dict[str, str]:
    """Best-effort OS/arch metadata for the registration payload.

    Mirrors the TS ``ClientPlatform`` type: every field is optional. We drop
    any key whose value is falsy (e.g. ``platform.processor()`` returns ``""``
    on some Linux distros), so the payload only carries meaningful fields.
    """
    raw = {
        "system": _platform.system(),
        "release": _platform.release(),
        "machine": _platform.machine(),
        "processor": _platform.processor(),
        "version": _platform.version(),
        "os_name": os.name,
    }
    return {k: v for k, v in raw.items() if v}


def register_client(client_name: str = "parallel-cli") -> str:
    """Register this CLI install with the platform and return the new ``client_id``.

    POSTs to ``/getServiceKeys/register`` with the client name and OS platform
    metadata. The platform assigns and returns a unique ``client_id`` used on
    subsequent OAuth calls.
    """
    url = _platform_path("/getServiceKeys/register")
    body: dict = {"client_name": client_name, "platform": _get_platform_info()}
    try:
        data = _post_json(url, body)
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise Exception(f"Client registration failed: {e.code} - {err_body}") from e
    return data["client_id"]


def _ensure_client_id() -> str:
    """Return a registered ``client_id``, registering if none is stored yet.

    - If the credentials file already has a ``client_id``, returns it.
    - Otherwise calls :func:`register_client` and persists the result.
    - If registration fails, emits a single-line stderr warning and falls
      back to the hardcoded ``CLIENT_ID``. The stored ``client_id`` stays
      unset so the next login attempt retries transparently.
    """
    creds = credentials.load() or credentials.Credentials()
    if creds.client_id:
        return creds.client_id
    try:
        client_id = register_client()
    except Exception as e:
        print(f"Warning: client registration failed ({e}); using fallback client_id.", file=sys.stderr)
        return CLIENT_ID
    creds.client_id = client_id
    credentials.save(creds)
    return client_id


def _build_verification_uri(base: str, email_hint: str | None) -> str:
    """Append ``agent=true`` and an optional ``login_hint`` to a verification URI.

    The login hint encodes ``login=email,e=<user_email>`` — a compound value the
    platform's SSO page uses to route the user through magic-email login with the
    address pre-filled.
    """
    parsed = urllib.parse.urlparse(base)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("agent", "true"))
    if email_hint:
        query.append(("login_hint", f"login=email,e={email_hint}"))
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))


def request_device_code(scope: str = DEFAULT_SCOPE, client_id: str | None = None) -> DeviceCodeInfo:
    """Request a device code from ``/getServiceKeys/device/code`` (RFC 8628 Step 1)."""
    url = _platform_path("/getServiceKeys/device/code")
    try:
        data = _post_form(url, {"client_id": client_id or CLIENT_ID, "scope": scope})
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f"Device code request failed: {e.code} - {body}") from e

    return DeviceCodeInfo(
        device_code=data["device_code"],
        user_code=data["user_code"],
        verification_uri=data["verification_uri"],
        verification_uri_complete=data.get("verification_uri_complete", data["verification_uri"]),
        expires_in=data.get("expires_in", 600),
        interval=data.get("interval", 5),
    )


def _parse_token_response(data: dict) -> TokenResponse:
    return TokenResponse(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_in=int(data.get("expires_in", 0)),
        refresh_token_expires_in=int(data.get("refresh_token_expires_in", 0)),
        authorization_expires_in=int(data.get("authorization_expires_in", 0)),
        org_id=data["org_id"],
        scope=data.get("scope", ""),
        token_type=data.get("token_type", "Bearer"),
    )


def poll_device_token(info: DeviceCodeInfo, client_id: str | None = None) -> TokenResponse:
    """Poll ``/getServiceKeys/token`` until the user authorizes (RFC 8628 Step 3).

    Polls the token endpoint immediately on entry, then waits ``interval``
    seconds between subsequent polls. RFC 8628 only requires waiting *between*
    requests, so polling right away makes fast authorizations feel snappy
    instead of eating a silent ``interval``-second delay.
    """
    url = _platform_path("/getServiceKeys/token")
    interval = info.interval
    deadline = time.monotonic() + info.expires_in

    while time.monotonic() < deadline:
        try:
            data = _post_form(
                url,
                {
                    "grant_type": DEVICE_CODE_GRANT_TYPE,
                    "device_code": info.device_code,
                    "client_id": client_id or CLIENT_ID,
                },
            )
            return _parse_token_response(data)
        except urllib.error.HTTPError as e:
            body = json.loads(e.read().decode())
            error_code = body.get("error", "")
            if error_code == "authorization_pending":
                pass
            elif error_code == "slow_down":
                interval += 5
            elif error_code == "expired_token":
                raise Exception("Device code expired. Please try again.") from e
            elif error_code == "access_denied":
                raise Exception("Authorization denied by user.") from e
            else:
                raise Exception(f"Token exchange failed: {body.get('error_description', error_code)}") from e
        time.sleep(interval)

    raise Exception("Device code expired (timeout). Please try again.")


def refresh_access_token(refresh_token: str, client_id: str | None = None) -> TokenResponse:
    """Exchange a refresh token for a new access+refresh token pair."""
    url = _platform_path("/getServiceKeys/token")
    try:
        data = _post_form(
            url,
            {
                "grant_type": REFRESH_TOKEN_GRANT_TYPE,
                "refresh_token": refresh_token,
                "client_id": client_id or CLIENT_ID,
            },
        )
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise Exception(f"Token refresh failed: {e.code} - {body}") from e
    return _parse_token_response(data)


def revoke_token(refresh_token: str) -> None:
    """Revoke a refresh token via form-encoded POST.

    Body shape: ``refresh_token=<token>`` (application/x-www-form-urlencoded).
    The endpoint identifies the caller from the token itself — no bearer auth.
    """
    url = _platform_path("/getServiceKeys/token/revoke")
    body = urllib.parse.urlencode({"refresh_token": refresh_token}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30):
            pass
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise Exception(f"Token revocation failed: {e.code} - {err_body}") from e


def _do_device_flow(
    email_hint: str | None = None,
    on_device_code: Callable[[DeviceCodeInfo], None] | None = None,
    client_id: str | None = None,
) -> TokenResponse:
    """Run the full device authorization flow (request + poll) and return tokens."""
    info = request_device_code(client_id=client_id)

    enriched_uri = _build_verification_uri(info.verification_uri_complete, email_hint)

    if on_device_code:
        on_device_code(info)
    else:
        print(f"\nTo authenticate, visit: {info.verification_uri}", file=sys.stderr)
        print(f"And enter code: {info.user_code}\n", file=sys.stderr)
        print(f"Or open: {enriched_uri}\n", file=sys.stderr)
        print(f"Waiting for authorization (expires in {info.expires_in // 60} minutes)...", file=sys.stderr)

        if not _is_headless():
            try:
                webbrowser.open(enriched_uri)
            except Exception:
                pass

    return poll_device_token(info, client_id=client_id)


def _persist_token_response(resp: TokenResponse) -> None:
    """Write a TokenResponse into credentials under its org_id, selecting it."""
    now = int(time.time())
    creds = credentials.load() or credentials.Credentials()
    org = creds.orgs.get(resp.org_id) or credentials.OrgCredentials()
    org.control_api = credentials.ControlApiTokens(
        access_token=resp.access_token,
        access_token_expires_at=now + resp.expires_in,
        access_token_scopes=resp.scopes,
        refresh_token=resp.refresh_token,
        refresh_token_expires_at=now + resp.refresh_token_expires_in,
        authorization_expires_at=now + resp.authorization_expires_in,
    )
    creds.orgs[resp.org_id] = org
    creds.selected_org_id = resp.org_id
    credentials.save(creds)


def get_control_api_access_token() -> str:
    """Return a currently-valid control-API access token for the selected org.

    Transparently refreshes the access token when it has expired (or is about
    to expire within ``ACCESS_TOKEN_SKEW_SECONDS``), persisting the refreshed
    tokens back to the credentials file.

    Raises:
        ReauthenticationRequired: The caller must run ``parallel-cli login``.
            Reasons: no stored credentials, no control-API tokens for the
            selected org, ``authorization_expires_at`` in the past, or
            ``refresh_token_expires_at`` in the past.
    """
    creds = credentials.load()
    org = creds.selected_org() if creds else None
    if org is None:
        raise ReauthenticationRequired("not logged in; run 'parallel-cli login'")

    tokens = org.control_api
    access_token = tokens.access_token
    if not access_token:
        raise ReauthenticationRequired("not logged in; run 'parallel-cli login'")

    now = int(time.time())

    if tokens.authorization_expires_at is not None and now >= tokens.authorization_expires_at:
        raise ReauthenticationRequired("authorization grant has expired; run 'parallel-cli login'")

    # Fast path: current access token still valid beyond the skew buffer.
    if tokens.access_token_expires_at is None or now < tokens.access_token_expires_at - ACCESS_TOKEN_SKEW_SECONDS:
        return access_token

    # Access token is (about to be) expired. Can we refresh?
    refresh_token_value = tokens.refresh_token
    if not refresh_token_value:
        raise ReauthenticationRequired("no refresh token available; run 'parallel-cli login'")
    if tokens.refresh_token_expires_at is not None and now >= tokens.refresh_token_expires_at:
        raise ReauthenticationRequired("refresh token has expired; run 'parallel-cli login'")

    new_tokens = refresh_access_token(refresh_token_value, client_id=_ensure_client_id())
    _persist_token_response(new_tokens)
    return new_tokens.access_token


def login_flow(
    email: str | None = None,
    on_device_code: Callable[[DeviceCodeInfo], None] | None = None,
) -> str:
    """Run the full CLI login: register client → device flow → persist tokens → auto-mint data API key.

    Returns the newly-minted data API key.
    """
    client_id = _ensure_client_id()
    token_resp = _do_device_flow(email_hint=email, on_device_code=on_device_code, client_id=client_id)
    _persist_token_response(token_resp)

    api_key, key_name = service.provision_cli_api_key(token_resp.access_token, client_id=client_id)

    creds = credentials.load()
    assert creds is not None and creds.selected_org_id == token_resp.org_id
    creds.orgs[token_resp.org_id].api_key = api_key
    # Drop the v0→v1 legacy placeholder org now that the user is properly
    # authenticated against a real org. It only existed for backwards compat
    # during migration; keeping it around after login would be dead state.
    if credentials.LEGACY_ORG_ID != token_resp.org_id:
        creds.orgs.pop(credentials.LEGACY_ORG_ID, None)
    credentials.save(creds)

    if not on_device_code:
        print(f"Authentication successful! Provisioned data API key: {key_name}", file=sys.stderr)

    return api_key


def resolve_api_key(api_key: str | None = None) -> str:
    """Resolve API key from parameter, stored credentials, or environment.

    Priority: explicit ``api_key`` argument → stored credentials → ``PARALLEL_API_KEY``.
    Raises ``ValueError`` if no key is available.
    """
    if api_key:
        return api_key
    stored = credentials.get_selected_api_key()
    if stored:
        return stored
    env_key = os.environ.get("PARALLEL_API_KEY")
    if env_key:
        return env_key
    raise ValueError(
        "Parallel API key required. Run 'parallel-cli login', set the "
        "PARALLEL_API_KEY environment variable, or pass api_key explicitly."
    )


def get_api_key(
    force_login: bool = False,
    on_device_code: Callable[[DeviceCodeInfo], None] | None = None,
    email: str | None = None,
) -> str:
    """Get API key, triggering device-flow login + auto-mint as a fallback.

    Priority (when not ``force_login``): stored credentials → ``PARALLEL_API_KEY``
    → service-API key provisioning from stored control-API tokens.
    """
    if not force_login:
        stored = credentials.get_selected_api_key()
        if stored:
            return stored
        env_key = os.environ.get("PARALLEL_API_KEY")
        if env_key:
            return env_key

        # If we still have valid control-API auth but no data API key saved,
        # mint a new data key via service API before forcing an interactive
        # device-authorization flow.
        try:
            access_token = get_control_api_access_token()
            client_id = _ensure_client_id()
            minted_api_key, _ = service.provision_cli_api_key(access_token, client_id=client_id)
            creds = credentials.load()
            if creds is not None:
                org = creds.selected_org()
                if org is not None:
                    org.api_key = minted_api_key
                    credentials.save(creds)
            return minted_api_key
        except ReauthenticationRequired:
            pass
        except service.ServiceApiError:
            pass

    if not on_device_code:
        print("Starting device authorization...", file=sys.stderr)
    return login_flow(email=email, on_device_code=on_device_code)


def create_client(api_key: str | None = None, source: ClientSource = "python") -> Parallel:
    """Create a configured Parallel client, resolving the API key if not provided."""
    return Parallel(
        base_url=get_api_url(),
        api_key=resolve_api_key(api_key),
        default_headers=get_default_headers(source),
    )


def get_client(force_login: bool = False, source: ClientSource = "python") -> Parallel:
    """Get a configured Parallel client with interactive device-flow fallback."""
    return Parallel(
        base_url=get_api_url(),
        api_key=get_api_key(force_login=force_login),
        default_headers=get_default_headers(source),
    )


def get_async_client(force_login: bool = False, source: ClientSource = "python") -> AsyncParallel:
    """Get a configured async Parallel client."""
    return AsyncParallel(
        base_url=get_api_url(),
        api_key=get_api_key(force_login=force_login),
        default_headers=get_default_headers(source),
    )


def logout() -> bool:
    """Revoke the stored refresh token (best-effort) and remove the credentials file."""
    creds = credentials.load()
    if creds is not None:
        org = creds.selected_org()
        refresh_token = org.control_api.refresh_token if org else None
        if refresh_token:
            try:
                revoke_token(refresh_token)
            except Exception as e:
                print(
                    f"Warning: refresh token revocation failed ({e}); removing local credentials anyway.",
                    file=sys.stderr,
                )
    return credentials.delete()


def get_auth_status() -> dict:
    """Get current authentication status.

    Priority matches :func:`resolve_api_key`: stored credentials beat the
    ``PARALLEL_API_KEY`` env var.
    """
    creds = credentials.load()
    if creds is not None:
        org = creds.selected_org()
        if org and org.api_key:
            return {
                "authenticated": True,
                "method": "oauth",
                "token_file": str(credentials.CREDENTIALS_FILE),
                "version": creds.version,
                "selected_org_id": creds.selected_org_id,
                "has_control_api_tokens": bool(org.control_api.refresh_token),
            }

    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key:
        return {"authenticated": True, "method": "environment", "token_file": None}

    return {"authenticated": False, "method": None, "token_file": None}
