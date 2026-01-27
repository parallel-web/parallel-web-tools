"""OAuth Authentication for Parallel API."""

import base64
import hashlib
import http.server
import json
import os
import secrets
import socketserver
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING

from parallel import AsyncParallel, Parallel

from parallel_web_tools.core.user_agent import ClientSource, get_default_headers

if TYPE_CHECKING:
    pass

# OAuth Configuration
OAUTH_PROVIDER_HOST = "platform.parallel.ai"
OAUTH_PROVIDER_PATH_PREFIX = "/getKeys"
OAUTH_SCOPE = "key:read"
TOKEN_FILE = Path.home() / ".config" / "parallel-web-tools" / "credentials.json"


def _generate_code_verifier() -> str:
    """Generate a random code verifier for PKCE."""
    return secrets.token_urlsafe(32)


def _generate_code_challenge(verifier: str) -> str:
    """Generate code challenge from verifier using S256."""
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _load_stored_token() -> str | None:
    """Load stored OAuth token from file."""
    if not TOKEN_FILE.exists():
        return None
    try:
        with open(TOKEN_FILE) as f:
            data = json.load(f)
            return data.get("access_token")
    except (OSError, json.JSONDecodeError):
        return None


def _save_token(access_token: str) -> None:
    """Save OAuth token to file with secure permissions."""
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token}, f)
    os.chmod(TOKEN_FILE, 0o600)


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
                <p>{OAuthCallbackHandler.error}</p>
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


def get_api_key(force_login: bool = False) -> str:
    """Get API key/token for Parallel API with interactive OAuth fallback.

    Priority:
    1. PARALLEL_API_KEY environment variable
    2. Stored OAuth token
    3. Interactive OAuth flow
    """
    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key and not force_login:
        return api_key

    if not force_login:
        stored_token = _load_stored_token()
        if stored_token:
            return stored_token

    print("Starting authentication...", file=sys.stderr)
    access_token = _do_oauth_flow()
    _save_token(access_token)
    print("Authentication successful! Credentials saved.", file=sys.stderr)

    return access_token


def get_client(
    force_login: bool = False,
    source: ClientSource = "python",
) -> Parallel:
    """Get a configured Parallel client with User-Agent header.

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
    """Remove stored OAuth token."""
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        return True
    return False


def get_auth_status() -> dict:
    """Get current authentication status."""
    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key:
        return {"authenticated": True, "method": "environment", "token_file": None}

    stored_token = _load_stored_token()
    if stored_token:
        return {"authenticated": True, "method": "oauth", "token_file": str(TOKEN_FILE)}

    return {"authenticated": False, "method": None, "token_file": None}
