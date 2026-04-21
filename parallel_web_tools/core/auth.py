"""OAuth Authentication for Parallel API."""

import base64
import hashlib
import html
import http.server
import json
import os
import secrets
import socketserver
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass

from parallel import AsyncParallel, Parallel

from parallel_web_tools.core import credentials
from parallel_web_tools.core.user_agent import ClientSource, get_default_headers

# OAuth Configuration
OAUTH_PROVIDER_HOST = "platform.parallel.ai"
OAUTH_PROVIDER_PATH_PREFIX = "/getKeys"
OAUTH_SCOPE = "key:read"

# Device flow grant type (RFC 8628)
DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"


@dataclass
class DeviceCodeInfo:
    """Response from the device authorization endpoint (RFC 8628)."""

    device_code: str
    """Opaque code used to poll the token endpoint. Never shown to user."""

    user_code: str
    """Human-readable code the user enters at the verification URL (e.g. BCDF-GHJK)."""

    verification_uri: str
    """URL the user visits to enter the code."""

    verification_uri_complete: str
    """URL with user_code pre-filled as a query parameter."""

    expires_in: int
    """Seconds until the device code expires (default 600)."""

    interval: int
    """Minimum polling interval in seconds (default 5)."""


def _generate_code_verifier() -> str:
    """Generate a random code verifier for PKCE."""
    return secrets.token_urlsafe(32)


def _generate_code_challenge(verifier: str) -> str:
    """Generate code challenge from verifier using S256."""
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _load_stored_token() -> str | None:
    """Load stored API key for the currently selected org."""
    return credentials.get_selected_api_key()


def _save_token(access_token: str) -> None:
    """Save a token minted by the existing OAuth flow.

    The flow today doesn't know the real org id, so tokens land in the
    ``legacy`` placeholder org. The future control-API login flow will write
    into a properly-keyed org and flip ``selected_org_id``.
    """
    credentials.set_api_key_for_org(credentials.LEGACY_ORG_ID, access_token)


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler to receive OAuth callback."""

    auth_code: str | None = None
    error: str | None = None

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            OAuthCallbackHandler.auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"""
                <html><body style="font-family: system-ui; text-align: center; padding: 50px;">
                <h1>Authentication Successful!</h1>
                <p>You can close this window and return to the terminal.</p>
                </body></html>
            """
            )
        elif "error" in params:
            OAuthCallbackHandler.error = params.get("error_description", params["error"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"""
                <html><body style="font-family: system-ui; text-align: center; padding: 50px;">
                <h1>Authentication Failed</h1>
                <p>{html.escape(OAuthCallbackHandler.error)}</p>
                </body></html>
            """.encode()
            )
        else:
            self.send_response(404)
            self.end_headers()


def _do_oauth_flow() -> str:
    """Perform OAuth authorization code flow with PKCE."""
    OAuthCallbackHandler.auth_code = None
    OAuthCallbackHandler.error = None

    with socketserver.TCPServer(("127.0.0.1", 0), OAuthCallbackHandler) as httpd:
        port = httpd.server_address[1]
        redirect_uri = f"http://localhost:{port}/callback"

        code_verifier = _generate_code_verifier()
        code_challenge = _generate_code_challenge(code_verifier)

        auth_params = {
            "client_id": "localhost",
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": OAUTH_SCOPE,
            "resource": f"http://localhost:{port}",
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        auth_url = f"https://{OAUTH_PROVIDER_HOST}{OAUTH_PROVIDER_PATH_PREFIX}/authorize?" + urllib.parse.urlencode(
            auth_params
        )

        print("\nOpening browser for authentication...", file=sys.stderr)
        print(f"If browser doesn't open, visit: {auth_url}", file=sys.stderr)

        webbrowser.open(auth_url)
        httpd.timeout = 300

        while OAuthCallbackHandler.auth_code is None and OAuthCallbackHandler.error is None:
            httpd.handle_request()

        if OAuthCallbackHandler.error:
            raise Exception(f"OAuth error: {OAuthCallbackHandler.error}")

        auth_code = OAuthCallbackHandler.auth_code

    token_url = f"https://{OAUTH_PROVIDER_HOST}{OAUTH_PROVIDER_PATH_PREFIX}/token"
    token_data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": auth_code,
            "client_id": "localhost",
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
            "resource": f"http://localhost:{port}",
        }
    ).encode()

    req = urllib.request.Request(
        token_url,
        data=token_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            token_response = json.loads(response.read().decode())
            access_token = token_response.get("access_token")
            if not access_token:
                raise Exception("No access token in response")
            return access_token
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"Token exchange failed: {e.code} - {error_body}") from e


def _is_headless() -> bool:
    """Detect if the environment cannot open a browser for OAuth.

    Returns True for SSH sessions, containers, CI, and other headless
    environments where the authorization code flow won't work.
    """
    # SSH session
    if os.environ.get("SSH_CLIENT") or os.environ.get("SSH_TTY"):
        return True

    # CI environments
    if os.environ.get("CI"):
        return True

    # No display on Linux
    if sys.platform == "linux" and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        return True

    # Container indicators
    if os.path.exists("/.dockerenv") or os.environ.get("container"):
        return True

    return False


def request_device_code() -> DeviceCodeInfo:
    """Request a device code from the authorization server (RFC 8628 Step 1).

    Returns a DeviceCodeInfo with the user_code, verification URL, and device_code
    needed for the rest of the flow. Callers should present the verification_uri and
    user_code to the user, then call poll_device_token() to wait for authorization.

    Example::

        info = request_device_code()
        print(f"Visit {info.verification_uri} and enter code: {info.user_code}")
        token = poll_device_token(info)
    """
    device_code_url = f"https://{OAUTH_PROVIDER_HOST}{OAUTH_PROVIDER_PATH_PREFIX}/device/code"

    request_data = urllib.parse.urlencode({"client_id": "localhost", "scope": OAUTH_SCOPE}).encode()
    req = urllib.request.Request(
        device_code_url,
        data=request_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"Device code request failed: {e.code} - {error_body}") from e

    return DeviceCodeInfo(
        device_code=data["device_code"],
        user_code=data["user_code"],
        verification_uri=data["verification_uri"],
        verification_uri_complete=data.get("verification_uri_complete", data["verification_uri"]),
        expires_in=data.get("expires_in", 600),
        interval=data.get("interval", 5),
    )


def poll_device_token(info: DeviceCodeInfo) -> str:
    """Poll the token endpoint until the user authorizes (RFC 8628 Step 3).

    Args:
        info: DeviceCodeInfo from request_device_code().

    Returns:
        The access token string.

    Raises:
        Exception: On expiry, denial, or other errors.
    """
    token_url = f"https://{OAUTH_PROVIDER_HOST}{OAUTH_PROVIDER_PATH_PREFIX}/token"
    interval = info.interval
    deadline = time.monotonic() + info.expires_in

    while time.monotonic() < deadline:
        time.sleep(interval)

        poll_data = urllib.parse.urlencode(
            {
                "grant_type": DEVICE_CODE_GRANT_TYPE,
                "device_code": info.device_code,
                "client_id": "localhost",
            }
        ).encode()

        poll_req = urllib.request.Request(
            token_url,
            data=poll_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with urllib.request.urlopen(poll_req, timeout=30) as response:
                token_response = json.loads(response.read().decode())
                access_token = token_response.get("access_token")
                if access_token:
                    return access_token
                raise Exception("No access token in response")
        except urllib.error.HTTPError as e:
            error_body = json.loads(e.read().decode())
            error_code = error_body.get("error", "")

            if error_code == "authorization_pending":
                continue
            elif error_code == "slow_down":
                interval += 5
                continue
            elif error_code == "expired_token":
                raise Exception("Device code expired. Please try again.") from e
            elif error_code == "access_denied":
                raise Exception("Authorization denied by user.") from e
            else:
                raise Exception(f"Token exchange failed: {error_body.get('error_description', error_code)}") from e

    raise Exception("Device code expired (timeout). Please try again.")


def _do_device_flow(on_device_code: Callable[[DeviceCodeInfo], None] | None = None) -> str:
    """Perform the full device authorization flow (request + poll).

    Args:
        on_device_code: Optional callback invoked with the DeviceCodeInfo after requesting
            the device code. Use this to present the verification URL and user code to the
            user in a custom way (e.g., in a chat message). If not provided, prints
            instructions to stderr and attempts to open the browser.
    """
    info = request_device_code()

    if on_device_code:
        on_device_code(info)
    else:
        # Default: print to stderr and try to open browser
        print(f"\nTo authenticate, visit: {info.verification_uri}", file=sys.stderr)
        print(f"And enter code: {info.user_code}\n", file=sys.stderr)
        print(f"Or open: {info.verification_uri_complete}\n", file=sys.stderr)
        print(f"Waiting for authorization (expires in {info.expires_in // 60} minutes)...", file=sys.stderr)

        try:
            webbrowser.open(info.verification_uri_complete)
        except Exception:
            pass

    return poll_device_token(info)


def resolve_api_key(api_key: str | None = None) -> str:
    """Resolve API key from parameter, environment, or stored credentials.

    This is the non-interactive version that raises an error if no key is found.
    Use get_api_key() if you want interactive OAuth flow as a fallback.

    Args:
        api_key: Optional API key. If provided, returns it directly.

    Returns:
        The resolved API key string.

    Raises:
        ValueError: If no API key can be found.
    """
    if api_key:
        return api_key

    env_key = os.environ.get("PARALLEL_API_KEY")
    if env_key:
        return env_key

    stored_token = _load_stored_token()
    if stored_token:
        return stored_token

    raise ValueError(
        "Parallel API key required. Provide via api_key parameter, "
        "PARALLEL_API_KEY environment variable, or run 'parallel-cli login'."
    )


def get_api_key(
    force_login: bool = False,
    device: bool = False,
    on_device_code: Callable[[DeviceCodeInfo], None] | None = None,
) -> str:
    """Get API key/token for Parallel API with interactive OAuth fallback.

    Priority:
    1. PARALLEL_API_KEY environment variable
    2. Stored OAuth token
    3. Interactive OAuth flow (or device flow if headless/requested)

    Args:
        force_login: Force a new login flow, ignoring stored credentials.
        device: Force device authorization flow instead of browser-based PKCE.
        on_device_code: Callback invoked with DeviceCodeInfo when using device flow.
            Use this to present the verification URL and user code to the user
            programmatically (e.g., in a chat message from an AI agent). If not
            provided, instructions are printed to stderr.
    """
    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key and not force_login:
        return api_key

    if not force_login:
        stored_token = _load_stored_token()
        if stored_token:
            return stored_token

    use_device = device or _is_headless()

    if use_device:
        if not on_device_code:
            print("Starting device authorization...", file=sys.stderr)
        access_token = _do_device_flow(on_device_code=on_device_code)
    else:
        print("Starting authentication...", file=sys.stderr)
        access_token = _do_oauth_flow()

    _save_token(access_token)
    if not on_device_code:
        print("Authentication successful! Credentials saved.", file=sys.stderr)

    return access_token


def create_client(
    api_key: str | None = None,
    source: ClientSource = "python",
) -> Parallel:
    """Create a configured Parallel client, resolving the API key if not provided.

    Unlike get_client(), this uses resolve_api_key() which raises ValueError
    instead of triggering interactive OAuth if no key is found.

    Args:
        api_key: Optional API key. Resolved from env/stored credentials if not provided.
        source: Source identifier for User-Agent (cli, duckdb, bigquery, etc.)

    Returns:
        A configured Parallel client.
    """
    return Parallel(
        api_key=resolve_api_key(api_key),
        default_headers=get_default_headers(source),
    )


def get_client(
    force_login: bool = False,
    source: ClientSource = "python",
) -> Parallel:
    """Get a configured Parallel client with interactive OAuth fallback.

    Args:
        force_login: Force a new OAuth login flow.
        source: Source identifier for User-Agent (cli, duckdb, bigquery, etc.)

    Returns:
        A configured Parallel client.
    """
    api_key = get_api_key(force_login=force_login)
    return Parallel(
        api_key=api_key,
        default_headers=get_default_headers(source),
    )


def get_async_client(
    force_login: bool = False,
    source: ClientSource = "python",
) -> AsyncParallel:
    """Get a configured async Parallel client with User-Agent header.

    Args:
        force_login: Force a new OAuth login flow.
        source: Source identifier for User-Agent (cli, duckdb, bigquery, etc.)

    Returns:
        A configured async Parallel client.
    """
    api_key = get_api_key(force_login=force_login)
    return AsyncParallel(
        base_url="https://api.parallel.ai",
        api_key=api_key,
        default_headers=get_default_headers(source),
    )


def logout() -> bool:
    """Remove stored credentials."""
    return credentials.delete()


def get_auth_status() -> dict:
    """Get current authentication status."""
    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key:
        return {"authenticated": True, "method": "environment", "token_file": None}

    creds = credentials.load()
    org = creds.selected_org() if creds else None
    if org and org.api_key:
        return {
            "authenticated": True,
            "method": "oauth",
            "token_file": str(credentials.CREDENTIALS_FILE),
            "version": creds.version,
            "selected_org_id": creds.selected_org_id,
        }

    return {"authenticated": False, "method": None, "token_file": None}
