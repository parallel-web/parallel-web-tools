"""End-to-end tests for the product API OAuth credential.

The CLI and the SDK clients run unchanged against ``FakeParallel``, an
in-memory stand-in for the three services they call: the platform (device
flow, token endpoint, revocation), the account service (``/service/v1/*``) and
the product API. Only the transports are replaced: ``urllib.request.urlopen``
for platform and service calls, ``httpx.Client.send`` and
``httpx.AsyncClient.send`` for SDK calls. The auth file is real, in a tmp dir.

Like the real token endpoint, the fake spends a refresh token on use: a client
that sends a spent one gets ``400 invalid_grant``.
"""

from __future__ import annotations

import asyncio
import io
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import Message
from itertools import count
from typing import Any

import httpx
import pytest
from click.testing import CliRunner, Result

from parallel_web_tools.cli.commands import EXIT_API_ERROR, EXIT_AUTH_ERROR, main
from parallel_web_tools.core import auth, credentials
from parallel_web_tools.core.auth import (
    ACCESS_TOKEN_SKEW_SECONDS,
    DEVICE_CODE_GRANT_TYPE,
    create_client,
    get_async_client,
    resolve_api_key,
)
from parallel_web_tools.core.endpoints import DEFAULT_SCOPE, PARALLEL_CLI_APP_NAME

PLATFORM_URL = "https://platform.parallel.ai"
SERVICE_API_URL = "https://api.parallel.ai/account"
API_URL = "https://api.parallel.ai"
AWS_API_URL = "https://api-aws.parallel.ai"
TOKEN_ENDPOINT = f"{PLATFORM_URL}/getServiceKeys/token"

START = 1_800_000_000
ORG_ID = "org_acme"
ORG_NAME = "Acme"
CLI_APP_ID = "app_cli"
CONTROL_ACCESS_TTL = 3600
CONTROL_REFRESH_TTL = 7 * 24 * 3600
AUTHORIZATION_TTL = 90 * 24 * 3600
PRODUCT_ACCESS_TTL = 600
PRODUCT_REFRESH_TTL = 30 * 24 * 3600


# ---------------------------------------------------------------------------
# Fake services
# ---------------------------------------------------------------------------


class FakeClock:
    """Stands in for the ``time`` module inside ``auth``; only ``time()`` is fake."""

    def __init__(self, now: int) -> None:
        self.now = now

    def time(self) -> float:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += seconds

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


@dataclass
class PlatformCall:
    """One urllib request to the platform or the account service."""

    method: str
    url: str
    body: dict[str, Any]
    authorization: str | None

    @property
    def path(self) -> str:
        return urllib.parse.urlsplit(self.url).path


@dataclass
class ProductAccessToken:
    audience: str
    expires_at: int


def _http_error(url: str, status: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, status, "Error", Message(), io.BytesIO(json.dumps(body).encode()))


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _origin(url: httpx.URL) -> str:
    port = f":{url.port}" if url.port else ""
    return f"{url.scheme}://{url.host}{port}"


class FakeParallel:
    """In-memory platform, account service and product API."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self._lock = threading.Lock()
        self._counters: defaultdict[str, count] = defaultdict(lambda: count(1))
        self.client_id = "pcli_registered"
        self.device_code = "d" * 48
        # When set, a new credential gets this audience whatever the client
        # asked for, like an org on an AWS Marketplace listing.
        self.forced_audience: str | None = None
        # Real seconds a refresh grant stalls before it is served, to widen
        # the window in which two clients could spend the same token.
        self.refresh_delay = 0.0
        # Real seconds each credential creation stalls, in call order.
        self.create_delays: list[float] = []
        # Error answers for the next refresh grants and credential creations,
        # in call order. A refresh answered with an error does not spend the token.
        self.refresh_errors: list[tuple[int, dict[str, Any]]] = []
        self.create_errors: list[tuple[int, dict[str, Any]]] = []

        self.api_keys: set[str] = set()
        self.control_access: dict[str, int] = {}
        self.control_refresh: set[str] = set()
        self.product_refresh: dict[str, str] = {}  # live refresh token -> credential_id
        self.credential_audience: dict[str, str] = {}
        self.product_access: dict[str, ProductAccessToken] = {}
        self.revoked: list[str] = []

        self.calls: list[PlatformCall] = []
        self.refresh_attempts: list[str] = []
        self.created_credentials: list[dict[str, Any]] = []
        self.api_requests: list[httpx.Request] = []

    def _new(self, prefix: str) -> str:
        return f"{prefix}_{next(self._counters[prefix])}"

    @property
    def product_refresh_attempts(self) -> list[str]:
        return [t for t in self.refresh_attempts if t.startswith("prt_")]

    def issue_control_tokens(self) -> dict[str, Any]:
        access, refresh = self._new("cat"), self._new("crt")
        self.control_access[access] = self.clock.now + CONTROL_ACCESS_TTL
        self.control_refresh.add(refresh)
        return {
            "access_token": access,
            "token_type": "Bearer",
            "expires_in": CONTROL_ACCESS_TTL,
            "refresh_token": refresh,
            "refresh_token_expires_in": CONTROL_REFRESH_TTL,
            "authorization_expires_in": AUTHORIZATION_TTL,
            "org_id": ORG_ID,
            "org_name": ORG_NAME,
            "scope": DEFAULT_SCOPE,
        }

    def revoke(self, token: str) -> None:
        """Revoke a refresh token server-side, as a dashboard user could."""
        with self._lock:
            self.control_refresh.discard(token)
            self.product_refresh.pop(token, None)

    # -- urllib transport (platform + account service) ----------------------

    def urlopen(self, req: urllib.request.Request, timeout: float | None = None) -> io.BytesIO:
        content_type = req.get_header("Content-type") or ""
        raw = req.data.decode() if isinstance(req.data, bytes) else ""
        if "form-urlencoded" in content_type:
            body: dict[str, Any] = dict(urllib.parse.parse_qsl(raw))
        else:
            body = json.loads(raw) if raw else {}
        call = PlatformCall(req.get_method(), req.full_url, body, req.get_header("Authorization"))

        if call.method == "POST" and call.url == TOKEN_ENDPOINT and body.get("grant_type") == "refresh_token":
            time.sleep(self.refresh_delay)
        elif call.method == "POST" and call.url.endswith("/oauth-credentials"):
            with self._lock:
                delay = self.create_delays.pop(0) if self.create_delays else 0.0
            time.sleep(delay)

        with self._lock:
            self.calls.append(call)
            status, payload = self._route(call)
        if status >= 400:
            raise _http_error(call.url, status, payload or {})
        return io.BytesIO(json.dumps(payload).encode() if payload is not None else b"")

    def _route(self, call: PlatformCall) -> tuple[int, dict[str, Any] | None]:
        key = (call.method, call.url)
        if key == ("POST", f"{PLATFORM_URL}/getServiceKeys/register"):
            return 200, {"client_id": self.client_id}
        if key == ("POST", f"{PLATFORM_URL}/getServiceKeys/device/code"):
            return 200, {
                "device_code": self.device_code,
                "user_code": "BCDF-GHJK",
                "verification_uri": f"{PLATFORM_URL}/getServiceKeys/device",
                "verification_uri_complete": f"{PLATFORM_URL}/getServiceKeys/device?user_code=BCDF-GHJK",
                "expires_in": 600,
                "interval": 5,
            }
        if key == ("POST", TOKEN_ENDPOINT):
            return self._token(call.body)
        if key == ("POST", f"{PLATFORM_URL}/getServiceKeys/token/revoke"):
            token = call.body["refresh_token"]
            self.revoked.append(token)
            self.control_refresh.discard(token)
            self.product_refresh.pop(token, None)
            return 200, None
        if call.url.startswith(f"{SERVICE_API_URL}/service/v1/"):
            return self._service(call)
        raise AssertionError(f"unexpected request: {call.method} {call.url}")

    def _token(self, form: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        grant_type = form.get("grant_type")
        if grant_type == DEVICE_CODE_GRANT_TYPE:
            if form.get("device_code") != self.device_code or form.get("client_id") != self.client_id:
                return 400, {"error": "invalid_grant"}
            return 200, self.issue_control_tokens()
        if grant_type != "refresh_token":
            return 400, {"error": "unsupported_grant_type"}

        token = form.get("refresh_token", "")
        self.refresh_attempts.append(token)
        if self.refresh_errors:
            return self.refresh_errors.pop(0)
        if token in self.control_refresh:
            self.control_refresh.discard(token)
            return 200, self.issue_control_tokens()
        if token in self.product_refresh:
            credential_id = self.product_refresh.pop(token)
            access, refresh = self._new("pat"), self._new("prt")
            self.product_access[access] = ProductAccessToken(
                audience=self.credential_audience[credential_id],
                expires_at=self.clock.now + PRODUCT_ACCESS_TTL,
            )
            self.product_refresh[refresh] = credential_id
            # Field set of the platform /getServiceKeys/token refresh response.
            return 200, {
                "access_token": access,
                "token_type": "Bearer",
                "expires_in": PRODUCT_ACCESS_TTL,
                "refresh_token": refresh,
                "refresh_token_expires_in": PRODUCT_REFRESH_TTL,
                "org_id": ORG_ID,
                "scope": "*",
            }
        # Spent, revoked or unknown.
        return 400, {"error": "invalid_grant", "error_description": "Refresh token is invalid"}

    def _service(self, call: PlatformCall) -> tuple[int, dict[str, Any]]:
        bearer = (call.authorization or "").removeprefix("Bearer ")
        if self.control_access.get(bearer, 0) <= self.clock.now:
            return 401, {"detail": "Invalid or expired token"}
        path = call.path
        if call.method == "GET" and path == "/account/service/v1/apps":
            return 200, {
                "apps": [
                    {"app_name": PARALLEL_CLI_APP_NAME, "org_name": ORG_NAME, "app_id": CLI_APP_ID, "org_id": ORG_ID}
                ]
            }
        if call.method == "POST" and path == f"/account/service/v1/apps/{CLI_APP_ID}/oauth-credentials":
            if self.create_errors:
                return self.create_errors.pop(0)
            self.created_credentials.append(call.body)
            credential_id, refresh = self._new("cred"), self._new("prt")
            audience = self.forced_audience or call.body["audience"]
            self.credential_audience[credential_id] = audience
            self.product_refresh[refresh] = credential_id
            return 201, {
                "credential_id": credential_id,
                "client_id": call.body["client_id"],
                "app_id": CLI_APP_ID,
                "app_name": PARALLEL_CLI_APP_NAME,
                "created_by_user_id": "user_1",
                "is_service_scoped": False,
                "audience": audience,
                "scopes": call.body["scopes"],
                "token_endpoint": TOKEN_ENDPOINT,
                "refresh_token": refresh,
                "refresh_token_expires_at": _iso(self.clock.now + PRODUCT_REFRESH_TTL),
                "created_at": _iso(self.clock.now),
            }
        return 404, {"detail": "Not Found"}

    # -- httpx transport (product API) --------------------------------------

    def handle_api(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.api_requests.append(request)
            if not self._api_request_authorized(request):
                return httpx.Response(401, json={"error": {"message": "Unauthorized"}}, request=request)
        route = (request.method, request.url.path)
        if route == ("POST", "/v1/search"):
            payload: dict[str, Any] = {
                "search_id": "search_1",
                "session_id": "session_1",
                "results": [{"url": "https://example.com", "title": "Example", "excerpts": ["An excerpt"]}],
                "warnings": [],
                "usage": [],
            }
        elif route == ("POST", "/v1beta/tasks/suggest"):
            payload = {
                "title": "Find CEO",
                "output_schema": {"properties": {"ceo": {"type": "string", "description": "CEO name"}}},
                "warnings": [],
            }
        elif route == ("POST", "/v1beta/tasks/suggest-processor"):
            payload = {"recommended_processors": ["pro"]}
        else:
            raise AssertionError(f"unexpected API request: {request.method} {request.url}")
        return httpx.Response(200, json=payload, request=request)

    def _api_request_authorized(self, request: httpx.Request) -> bool:
        authorization = request.headers.get("authorization", "")
        if authorization.startswith("Bearer "):
            issued = self.product_access.get(authorization.removeprefix("Bearer "))
            return issued is not None and issued.expires_at > self.clock.now and issued.audience == _origin(request.url)
        return request.headers.get("x-api-key") in self.api_keys


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    """Point the auth file (and the legacy file) at a tmp dir."""
    path = tmp_path / "auth.json"
    monkeypatch.setattr(credentials, "AUTH_FILE", path)
    monkeypatch.setattr(credentials, "CREDENTIALS_FILE", path)
    monkeypatch.setattr(credentials, "LEGACY_CREDENTIALS_FILE", tmp_path / "credentials.json")
    return path


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock(START)
    monkeypatch.setattr(auth, "time", fake)
    return fake


@pytest.fixture
def backend(clock, creds_file, monkeypatch) -> FakeParallel:
    for var in (
        "PARALLEL_API_KEY",
        "PARALLEL_API_URL",
        "PARALLEL_PLATFORM_URL",
        "PARALLEL_SERVICE_API_URL",
        "VERCEL_PROTECTION_BYPASS_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)
    fake = FakeParallel(clock)

    async def async_send(client, request, **kwargs):
        return fake.handle_api(request)

    monkeypatch.setattr(urllib.request, "urlopen", fake.urlopen)
    monkeypatch.setattr(httpx.Client, "send", lambda client, request, **kwargs: fake.handle_api(request))
    monkeypatch.setattr(httpx.AsyncClient, "send", async_send)
    return fake


def cli(*args: str) -> Result:
    return CliRunner().invoke(main, list(args), catch_exceptions=False)


def login() -> None:
    result = cli("login", "--json", "--no-browser")
    assert result.exit_code == 0, result.output
    events = [json.loads(line)["event"] for line in result.stdout.splitlines()]
    assert events == ["auth_start", "device_code", "auth_waiting", "auth_success"]


def search() -> dict[str, Any]:
    result = cli("search", "parallel web", "--json")
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


def stored_org() -> credentials.OrgCredentials:
    creds = credentials.load()
    assert creds is not None
    org = creds.selected_org()
    assert org is not None
    return org


def bearer(request: httpx.Request) -> str | None:
    return request.headers.get("authorization")


def search_in_threads(client: Any, threads: int = 2) -> list[BaseException]:
    """Run one search per thread on ``client``, all at once; return the errors."""
    start = threading.Barrier(threads)
    errors: list[BaseException] = []

    def run() -> None:
        start.wait()
        try:
            client.search(search_queries=["parallel web"])
        except BaseException as e:
            errors.append(e)

    workers = [threading.Thread(target=run, daemon=True) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
    assert not any(worker.is_alive() for worker in workers), "a search is still running: deadlock on the auth lock?"
    return errors


def write_pre_oauth_auth_file(creds_file, backend: FakeParallel, *, authorization_expires_at: int) -> None:
    """Write auth.json as a release that minted API keys left it.

    It has an API key and control-API tokens, and no ``product_api`` entry. The
    control access token has expired, so using the control API needs a refresh.
    """
    control = backend.issue_control_tokens()
    backend.api_keys.add("sk_legacy")
    creds_file.write_text(
        json.dumps(
            {
                "version": 1,
                "selected_org_id": ORG_ID,
                "client_id": backend.client_id,
                "orgs": {
                    ORG_ID: {
                        "api_key": "sk_legacy",
                        "org_name": ORG_NAME,
                        "control_api": {
                            "access_token": control["access_token"],
                            "access_token_expires_at": START - 60,
                            "access_token_scopes": DEFAULT_SCOPE.split(),
                            "refresh_token": control["refresh_token"],
                            "refresh_token_expires_at": START + CONTROL_REFRESH_TTL,
                            "authorization_expires_at": authorization_expires_at,
                        },
                    }
                },
            }
        )
    )


# ---------------------------------------------------------------------------
# Login creates an OAuth credential
# ---------------------------------------------------------------------------


class TestLogin:
    def test_login_creates_oauth_credential_instead_of_api_key(self, backend, creds_file):
        login()

        assert [(c.method, c.path) for c in backend.calls] == [
            ("POST", "/getServiceKeys/register"),
            ("POST", "/getServiceKeys/device/code"),
            ("POST", "/getServiceKeys/token"),
            ("GET", "/account/service/v1/apps"),
            ("POST", f"/account/service/v1/apps/{CLI_APP_ID}/oauth-credentials"),
        ]
        assert backend.created_credentials == [
            {"client_id": backend.client_id, "audience": API_URL, "scopes": ["*"], "is_service_scoped": False}
        ]

        org = json.loads(creds_file.read_text())["orgs"][ORG_ID]
        assert org["api_key"] is None
        assert org["product_api"] == {
            "credential_id": "cred_1",
            "audience": API_URL,
            "token_endpoint": TOKEN_ENDPOINT,
            "access_token": None,
            "access_token_expires_at": None,
            "refresh_token": "prt_1",
            "refresh_token_expires_at": START + PRODUCT_REFRESH_TTL,
        }

    def test_oauth_login_provides_no_api_key_for_integrations(self, backend):
        login()
        calls_after_login = len(backend.calls)

        with pytest.raises(ValueError, match="does not provide an API key"):
            resolve_api_key()

        # The deployed function stores a key, so deploy must refuse up front.
        result = cli("enrich", "deploy", "--system", "bigquery", "--project", "p")
        assert result.exit_code == 2
        assert "--api-key" in result.output
        assert len(backend.calls) == calls_after_login
        assert backend.api_requests == []


# ---------------------------------------------------------------------------
# SDK commands use and refresh the credential
# ---------------------------------------------------------------------------


class TestCommandsAfterLogin:
    def test_search_exchanges_refresh_token_and_sends_bearer(self, backend):
        login()

        output = search()

        assert output["search_id"] == "search_1"
        assert backend.product_refresh_attempts == ["prt_1"]
        [request] = backend.api_requests
        assert str(request.url) == f"{API_URL}/v1/search"
        assert bearer(request) == "Bearer pat_1"
        assert "x-api-key" not in request.headers
        assert "(cli)" in request.headers["user-agent"]

        product = stored_org().product_api
        assert product.access_token == "pat_1"
        assert product.access_token_expires_at == START + PRODUCT_ACCESS_TTL
        assert product.refresh_token == "prt_2"

    def test_refreshes_near_expiry_with_newest_refresh_token(self, backend, clock):
        login()
        search()
        search()  # access token still fresh: no second exchange
        assert backend.product_refresh_attempts == ["prt_1"]

        # Inside the skew window the token counts as expired.
        clock.advance(PRODUCT_ACCESS_TTL - ACCESS_TOKEN_SKEW_SECONDS + 1)
        search()
        clock.advance(PRODUCT_ACCESS_TTL)
        search()

        # Each exchange spends the token the previous one returned; a spent
        # token would get invalid_grant and fail the search.
        assert backend.product_refresh_attempts == ["prt_1", "prt_2", "prt_3"]
        assert [bearer(r) for r in backend.api_requests] == [
            "Bearer pat_1",
            "Bearer pat_1",
            "Bearer pat_2",
            "Bearer pat_3",
        ]
        product = stored_org().product_api
        assert product.refresh_token == "prt_4"
        # Each refresh extends the refresh token's lifetime.
        assert product.refresh_token_expires_at == clock.now + PRODUCT_REFRESH_TTL
        assert len(backend.created_credentials) == 1

    def test_enrich_suggest_goes_through_oauth_client(self, backend):
        login()

        result = cli("enrich", "suggest", "Find the CEO", "--json")

        assert result.exit_code == 0, result.output
        output = json.loads(result.stdout)
        assert output["processor"] == "pro"
        assert output["enriched_columns"] == [{"name": "ceo", "description": "CEO name", "type": "str"}]
        assert [str(r.url) for r in backend.api_requests] == [
            f"{API_URL}/v1beta/tasks/suggest",
            f"{API_URL}/v1beta/tasks/suggest-processor",
        ]
        assert all(bearer(r) == "Bearer pat_1" for r in backend.api_requests)
        assert all("x-api-key" not in r.headers for r in backend.api_requests)

    def test_aws_audience_is_used_as_base_url(self, backend):
        backend.forced_audience = AWS_API_URL
        login()

        search()

        assert backend.created_credentials[0]["audience"] == API_URL
        assert stored_org().product_api.audience == AWS_API_URL
        [request] = backend.api_requests
        assert str(request.url) == f"{AWS_API_URL}/v1/search"
        assert bearer(request) == "Bearer pat_1"

    def test_parallel_api_url_at_login_sets_audience_and_base_url(self, backend, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_URL", "http://localhost:9000")
        login()
        monkeypatch.delenv("PARALLEL_API_URL")

        search()

        assert backend.created_credentials[0]["audience"] == "http://localhost:9000"
        [request] = backend.api_requests
        assert str(request.url) == "http://localhost:9000/v1/search"


# ---------------------------------------------------------------------------
# Concurrent refresh
# ---------------------------------------------------------------------------


class TestConcurrentRefresh:
    def test_two_threads_spend_the_refresh_token_once(self, backend):
        login()
        backend.refresh_delay = 0.2

        errors = search_in_threads(create_client(source="cli"))

        assert errors == []
        assert backend.product_refresh_attempts == ["prt_1"]
        assert [bearer(r) for r in backend.api_requests] == ["Bearer pat_1", "Bearer pat_1"]
        assert len(backend.created_credentials) == 1
        assert stored_org().product_api.refresh_token == "prt_2"

    def test_async_client_spends_the_refresh_token_once(self, backend):
        login()
        backend.refresh_delay = 0.2

        async def run() -> None:
            client = get_async_client(source="cli")
            await asyncio.gather(
                client.search(search_queries=["first"]),
                client.search(search_queries=["second"]),
            )

        asyncio.run(run())

        assert backend.product_refresh_attempts == ["prt_1"]
        assert [bearer(r) for r in backend.api_requests] == ["Bearer pat_1", "Bearer pat_1"]
        assert stored_org().product_api.refresh_token == "prt_2"


# ---------------------------------------------------------------------------
# Rejected refresh
# ---------------------------------------------------------------------------


class TestRejectedRefresh:
    def test_revoked_credential_is_replaced_from_control_tokens(self, backend, clock):
        login()
        search()
        backend.revoke("prt_2")
        # Both the product and the control access tokens expire.
        clock.advance(CONTROL_ACCESS_TTL)

        search()

        assert backend.product_refresh_attempts == ["prt_1", "prt_2", "prt_3"]
        assert len(backend.created_credentials) == 2
        assert bearer(backend.api_requests[-1]) == "Bearer pat_2"
        org = stored_org()
        assert org.product_api.credential_id == "cred_2"
        assert org.product_api.refresh_token == "prt_4"
        assert org.control_api.refresh_token == "crt_2"

    def test_transient_refresh_failure_keeps_credential(self, backend, clock):
        login()
        search()
        clock.advance(PRODUCT_ACCESS_TTL)
        backend.refresh_errors = [(503, {"error": "temporarily_unavailable"})]

        result = cli("search", "parallel web", "--json")

        assert result.exit_code == EXIT_API_ERROR
        assert "503" in json.loads(result.stdout)["error"]["message"]
        assert stored_org().product_api.refresh_token == "prt_2"

        # The next run exchanges the same, unspent token.
        search()
        assert backend.product_refresh_attempts == ["prt_1", "prt_2", "prt_2"]
        assert len(backend.created_credentials) == 1

    def test_revoked_credential_without_valid_control_tokens_asks_for_login(self, backend, clock):
        login()
        search()
        backend.revoke("prt_2")
        backend.revoke("crt_1")
        clock.advance(CONTROL_ACCESS_TTL)

        result = cli("search", "parallel web", "--json")

        assert result.exit_code == EXIT_AUTH_ERROR
        error = json.loads(result.stdout)["error"]
        assert error["type"] == "ReauthenticationRequired"
        assert "parallel-cli login" in error["message"]
        assert len(backend.created_credentials) == 1
        # The dead credential is dropped, so the next run does not retry it.
        assert stored_org().product_api.refresh_token is None
        assert len(backend.api_requests) == 1


class TestConcurrentRecovery:
    """Two callers find the credential revoked at the same time.

    The replacement must be created once, under the auth-file lock: otherwise
    both callers spend the single-use control refresh token, or each creates a
    credential and revokes the other's while it is in use.
    """

    def test_control_refresh_token_is_spent_once(self, backend, clock):
        login()
        search()
        backend.revoke("prt_2")
        clock.advance(CONTROL_ACCESS_TTL)  # product and control access tokens expire
        backend.refresh_delay = 0.2

        errors = search_in_threads(create_client(source="cli"))

        assert errors == []
        assert backend.refresh_attempts.count("crt_1") == 1
        assert len(backend.created_credentials) == 2

    def test_one_replacement_credential_serves_both_callers(self, backend, clock):
        login()
        search()
        backend.revoke("prt_2")
        clock.advance(PRODUCT_ACCESS_TTL)  # control access token stays valid
        backend.refresh_delay = 0.2
        # Stagger the creations so a second one would land while the first
        # caller is exchanging its new refresh token.
        backend.create_delays = [0.05, 0.15]

        errors = search_in_threads(create_client(source="cli"))

        assert errors == []
        assert len(backend.created_credentials) == 2
        assert backend.revoked == []
        assert stored_org().product_api.refresh_token in backend.product_refresh


# ---------------------------------------------------------------------------
# Migration from a stored API key
# ---------------------------------------------------------------------------


class TestApiKeyMigration:
    def test_stored_api_key_is_replaced_on_next_command(self, backend, creds_file):
        write_pre_oauth_auth_file(creds_file, backend, authorization_expires_at=START + AUTHORIZATION_TTL)

        search()

        assert [(c.method, c.path, c.body.get("refresh_token")) for c in backend.calls] == [
            ("POST", "/getServiceKeys/token", "crt_1"),
            ("GET", "/account/service/v1/apps", None),
            ("POST", f"/account/service/v1/apps/{CLI_APP_ID}/oauth-credentials", None),
            ("POST", "/getServiceKeys/token", "prt_1"),
        ]
        assert backend.created_credentials[0]["client_id"] == backend.client_id
        [request] = backend.api_requests
        assert bearer(request) == "Bearer pat_1"
        assert "x-api-key" not in request.headers

        org = stored_org()
        assert org.api_key is None
        assert org.product_api.refresh_token == "prt_2"
        assert org.control_api.refresh_token == "crt_2"

        status = json.loads(cli("auth", "--json").stdout)
        assert status["stored_credential_type"] == "oauth_credential"

    def test_stored_api_key_is_used_when_creation_fails_then_replaced(self, backend, creds_file):
        write_pre_oauth_auth_file(creds_file, backend, authorization_expires_at=START + AUTHORIZATION_TTL)
        backend.create_errors = [(500, {"detail": "Internal Server Error"})]

        search()

        [request] = backend.api_requests
        assert request.headers["x-api-key"] == "sk_legacy"
        assert stored_org().api_key == "sk_legacy"
        assert stored_org().product_api.refresh_token is None

        search()

        assert bearer(backend.api_requests[-1]) == "Bearer pat_1"
        assert stored_org().api_key is None

    def test_stored_api_key_is_kept_when_control_grant_has_expired(self, backend, creds_file):
        write_pre_oauth_auth_file(creds_file, backend, authorization_expires_at=START - 1)

        search()

        assert backend.calls == []
        [request] = backend.api_requests
        assert request.headers["x-api-key"] == "sk_legacy"
        assert "authorization" not in request.headers
        assert stored_org().api_key == "sk_legacy"


# ---------------------------------------------------------------------------
# auth status and logout
# ---------------------------------------------------------------------------


class TestAuthAndLogout:
    def test_auth_json_reports_oauth_credential_without_secrets(self, backend, creds_file):
        login()
        search()

        result = cli("auth", "--json")

        assert result.exit_code == 0
        status = json.loads(result.stdout)
        assert status == {
            "authenticated": True,
            "method": "oauth",
            "env_var_set": False,
            "has_stored_credentials": True,
            "stored_credential_type": "oauth_credential",
            "stored_overridden_by_env": False,
            "token_file": str(creds_file),
            "version": 1,
            "selected_org_id": ORG_ID,
            "selected_org_name": ORG_NAME,
            "has_control_api_tokens": True,
            "oauth_credential_id": "cred_1",
        }
        for secret in ("prt_", "pat_", "crt_", "cat_"):
            assert secret not in result.stdout

        human = cli("auth")
        assert "Product API: OAuth credential" in human.output

    def test_logout_revokes_newest_control_and_product_refresh_tokens(self, backend, creds_file):
        login()
        search()  # rotates prt_1 -> prt_2

        result = cli("logout", "--json")

        assert json.loads(result.stdout) == {"status": "logged_out"}
        revokes = [c for c in backend.calls if c.path == "/getServiceKeys/token/revoke"]
        assert sorted(c.body["refresh_token"] for c in revokes) == ["crt_1", "prt_2"]
        assert backend.product_refresh == {}
        assert backend.control_refresh == set()
        assert not creds_file.exists()
