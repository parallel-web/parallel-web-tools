"""Tests for the auth module (device flow against /getServiceKeys)."""

import io
import json
import os
import urllib.error
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from email.message import Message
from typing import Any
from unittest import mock

import httpx
import pytest

from parallel_web_tools.core import credentials, service
from parallel_web_tools.core.auth import (
    ACCESS_TOKEN_SKEW_SECONDS,
    AsyncOAuthParallel,
    DeviceCodeInfo,
    OAuthParallel,
    ReauthenticationRequired,
    TokenResponse,
    _do_device_flow,
    _persist_token_response,
    build_verification_uri,
    create_client,
    ensure_client_id,
    get_api_key,
    get_async_client,
    get_auth_status,
    get_client,
    get_control_api_access_token,
    get_product_api_access_token,
    is_headless,
    login_flow,
    logout,
    poll_device_token,
    refresh_access_token,
    register_client,
    request_device_code,
    resolve_api_key,
    revoke_token,
)
from parallel_web_tools.core.service_types import CreateOAuthCredentialResponse

# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    """Patch auth.json path to a tmp path for isolation."""
    path = tmp_path / "auth.json"
    monkeypatch.setattr(credentials, "AUTH_FILE", path)
    monkeypatch.setattr(credentials, "CREDENTIALS_FILE", path)
    monkeypatch.setattr(credentials, "LEGACY_CREDENTIALS_FILE", tmp_path / "credentials.json")
    return path


@pytest.fixture
def legacy_file(tmp_path):
    return tmp_path / "credentials.json"


@pytest.fixture
def no_sleep(monkeypatch):
    """Skip real sleeps in the device-code poll loop."""
    monkeypatch.setattr("parallel_web_tools.core.auth.time.sleep", mock.MagicMock())


@pytest.fixture
def mock_ensure_client_id(monkeypatch):
    """Stub out ensure_client_id to avoid real /getServiceKeys/register calls.

    Returns the value the stub will produce so tests can assert on it.
    """
    value = "cid_test"
    monkeypatch.setattr("parallel_web_tools.core.auth.ensure_client_id", lambda: value)
    return value


def _http_error(status: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://example.com",
        code=status,
        msg="Error",
        hdrs=Message(),
        fp=io.BytesIO(json.dumps(body).encode()),
    )


def _urlopen_stub(responses, capture: dict | None = None):
    """Build a urlopen side_effect that yields each response in order.

    Each entry in ``responses`` is a dict (JSON-encoded body), bytes (raw body),
    or pre-built HTTPError. A single value may be passed directly. When
    ``capture`` is provided it is populated on each call with url/body/headers/method.
    """
    if not isinstance(responses, list):
        responses = [responses]
    idx = [0]

    @contextmanager
    def impl(req, timeout=None):
        if capture is not None:
            capture["url"] = req.full_url
            capture["body"] = req.data.decode() if req.data else ""
            capture["headers"] = dict(req.header_items())
            capture["method"] = req.get_method()
        i = min(idx[0], len(responses) - 1)
        idx[0] += 1
        r = responses[i]
        if isinstance(r, urllib.error.HTTPError):
            raise r
        payload = r if isinstance(r, (bytes, bytearray)) else json.dumps(r).encode()
        yield io.BytesIO(bytes(payload))

    return impl


def _patch_auth_urlopen(responses, capture: dict | None = None):
    return mock.patch(
        "parallel_web_tools.core.auth.urllib.request.urlopen",
        side_effect=_urlopen_stub(responses, capture),
    )


DEVICE_RESPONSE = {
    "device_code": "a" * 48,
    "user_code": "BCDF-GHJK",
    "verification_uri": "http://localhost:3000/getServiceKeys/device",
    "verification_uri_complete": "http://localhost:3000/getServiceKeys/device?user_code=BCDF-GHJK",
    "expires_in": 600,
    "interval": 5,
}


TOKEN_RESPONSE_JSON = {
    "access_token": "at_123",
    "refresh_token": "rt_123",
    "expires_in": 600,
    "refresh_token_expires_in": 604800,
    "authorization_expires_in": 7776000,
    "org_id": "org_abc",
    "org_name": "Acme Org",
    "scope": "keys:read balance:write",
    "token_type": "Bearer",
}


SAMPLE_DEVICE_CODE_INFO = DeviceCodeInfo(
    device_code="a" * 48,
    user_code="BCDF-GHJK",
    verification_uri="http://localhost:3000/getServiceKeys/device",
    verification_uri_complete="http://localhost:3000/getServiceKeys/device?user_code=BCDF-GHJK",
    expires_in=600,
    interval=5,
)


_TOKEN_RESPONSE_DEFAULT = TokenResponse(
    access_token="at_123",
    refresh_token="rt_123",
    expires_in=600,
    refresh_token_expires_in=604800,
    authorization_expires_in=7776000,
    org_id="org_abc",
    org_name="Acme Org",
    scope="keys:read balance:write",
)


def _token_response(**overrides) -> TokenResponse:
    """Build a TokenResponse with test defaults."""
    return replace(_TOKEN_RESPONSE_DEFAULT, **overrides)


TOKEN_ENDPOINT = "https://platform.parallel.ai/getServiceKeys/token"


def _created_credential(
    refresh_token: str = "rt_product", audience: str = "https://api.parallel.ai"
) -> CreateOAuthCredentialResponse:
    """Build the service API's create-credential response with test defaults."""
    return CreateOAuthCredentialResponse(
        credential_id="cred_1",
        client_id="cid_test",
        app_id="app_cli",
        app_name="parallel-cli Users",
        created_by_user_id="user_1",
        is_service_scoped=False,
        audience=audience,
        scopes=["*"],
        token_endpoint=TOKEN_ENDPOINT,
        refresh_token=refresh_token,
        refresh_token_expires_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
        created_at=datetime(2026, 11, 20, tzinfo=timezone.utc),
    )


def _save_oauth_org(
    org_id: str = "org_a",
    *,
    access_token: str | None = None,
    access_token_expires_at: int | None = None,
    refresh_token: str = "rt_product",
    audience: str = "https://api.parallel.ai",
    control_refresh_token: str | None = None,
) -> None:
    """Store a selected org that holds a product API OAuth credential."""
    credentials.save(
        credentials.Credentials(
            selected_org_id=org_id,
            orgs={
                org_id: credentials.OrgCredentials(
                    control_api=credentials.ControlApiTokens(refresh_token=control_refresh_token),
                    product_api=credentials.ProductApiTokens(
                        credential_id="cred_1",
                        audience=audience,
                        token_endpoint=TOKEN_ENDPOINT,
                        access_token=access_token,
                        access_token_expires_at=access_token_expires_at,
                        refresh_token=refresh_token,
                        refresh_token_expires_at=4102444800,
                    ),
                )
            },
        )
    )


# ---------------------------------------------------------------------------
# build_verification_uri
# ---------------------------------------------------------------------------


class TestBuildVerificationUri:
    def test_appends_agent_true(self):
        url = build_verification_uri("http://localhost:3000/getServiceKeys/device?user_code=ABCD", None)
        assert "user_code=ABCD" in url

    def test_passes_login_hint_through_url_encoded(self):
        url = build_verification_uri(
            "http://localhost:3000/getServiceKeys/device?user_code=ABCD",
            "login=email,e=user@example.com",
        )
        # The hint value is URL-encoded: ','→'%2C', '='→'%3D', '@'→'%40'
        assert "login_hint=login%3Demail%2Ce%3Duser%40example.com" in url

    def test_supports_non_email_methods(self):
        # google: no email needed
        google_url = build_verification_uri("http://localhost:3000/d", "login=google")
        assert "login_hint=login%3Dgoogle" in google_url
        # sso: email carried as ,e=…
        sso_url = build_verification_uri("http://localhost:3000/d", "login=sso,e=u@example.com")
        assert "login_hint=login%3Dsso%2Ce%3Du%40example.com" in sso_url

    def test_no_hint_omits_login_hint(self):
        url = build_verification_uri("http://localhost:3000/getServiceKeys/device", None)
        assert "login_hint" not in url


# ---------------------------------------------------------------------------
# register_client / ensure_client_id
# ---------------------------------------------------------------------------


class TestRegisterClient:
    def test_returns_client_id_from_response(self):
        with _patch_auth_urlopen({"client_id": "cid_xyz"}):
            assert register_client() == "cid_xyz"

    def test_posts_json_with_expected_payload(self):
        captured: dict = {}
        with _patch_auth_urlopen({"client_id": "cid_xyz"}, capture=captured):
            register_client()

        assert "/getServiceKeys/register" in captured["url"]
        assert captured["method"] == "POST"
        body = json.loads(captured["body"])
        assert body["client_name"] == "parallel-cli"
        # Per user request, no redirect_uris field is sent.
        assert "redirect_uris" not in body
        # Platform block present with at least system/machine (always populated
        # by the stdlib platform module).
        assert "system" in body["platform"]
        assert "machine" in body["platform"]
        assert body["platform"]["os_name"] == os.name

    def test_raises_on_http_error(self):
        with _patch_auth_urlopen(_http_error(500, {"error": "internal"})):
            with pytest.raises(Exception, match="Client registration failed"):
                register_client()

    def test_ignores_platform_processor_failures(self):
        with (
            mock.patch("parallel_web_tools.core.auth._platform.processor", side_effect=AssertionError("blocked")),
            _patch_auth_urlopen({"client_id": "cid_xyz"}),
        ):
            assert register_client() == "cid_xyz"


class TestEnsureClientId:
    def test_returns_stored_client_id_without_registering(self, creds_file):
        credentials.save(credentials.Credentials(client_id="cid_stored"))
        with mock.patch("parallel_web_tools.core.auth.register_client") as mock_reg:
            assert ensure_client_id() == "cid_stored"
        mock_reg.assert_not_called()

    def test_registers_and_persists_when_missing(self, creds_file):
        with mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_fresh"):
            assert ensure_client_id() == "cid_fresh"

        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_fresh"

    def test_registers_again_when_stored_client_id_is_none(self, creds_file):
        # Simulate a prior registration failure: file exists but client_id is None.
        credentials.save(credentials.Credentials(selected_org_id="x", orgs={"x": credentials.OrgCredentials()}))
        with mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_new") as mock_reg:
            assert ensure_client_id() == "cid_new"
        mock_reg.assert_called_once()

        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_new"

    def test_falls_back_to_hardcoded_on_registration_failure(self, creds_file, capsys):
        with mock.patch(
            "parallel_web_tools.core.auth.register_client",
            side_effect=Exception("server down"),
        ):
            assert ensure_client_id() == "parallel-cli"

        # Failure leaves client_id unset so the next call retries.
        creds = credentials.load()
        assert creds is None or creds.client_id is None
        err = capsys.readouterr().err
        assert "client registration failed" in err


# ---------------------------------------------------------------------------
# request_device_code
# ---------------------------------------------------------------------------


class TestRequestDeviceCode:
    def test_returns_device_code_info(self):
        with _patch_auth_urlopen(DEVICE_RESPONSE):
            info = request_device_code()
        assert isinstance(info, DeviceCodeInfo)
        assert info.user_code == "BCDF-GHJK"
        assert info.expires_in == 600

    def test_hits_get_service_keys_endpoint(self):
        from parallel_web_tools.core.endpoints import DEFAULT_SCOPE

        captured: dict = {}
        with _patch_auth_urlopen(DEVICE_RESPONSE, capture=captured):
            request_device_code()

        assert "/getServiceKeys/device/code" in captured["url"]
        assert "client_id=parallel-cli" in captured["body"]
        # Scope must be present and URL-form-encoded — check for its head and a colon-encoded marker.
        first_scope = DEFAULT_SCOPE.split()[0]  # e.g. "keys:read"
        assert first_scope.replace(":", "%3A") in captured["body"] or first_scope in captured["body"]

    def test_respects_platform_url_env_var(self, monkeypatch):
        monkeypatch.setenv("PARALLEL_PLATFORM_URL", "http://localhost:3000")
        captured: dict = {}
        with _patch_auth_urlopen(DEVICE_RESPONSE, capture=captured):
            request_device_code()
        assert captured["url"].startswith("http://localhost:3000/")

    def test_raises_on_http_error(self):
        with _patch_auth_urlopen(_http_error(500, {"error": "internal"})):
            with pytest.raises(Exception, match="Device code request failed"):
                request_device_code()


# ---------------------------------------------------------------------------
# poll_device_token
# ---------------------------------------------------------------------------


class TestPollDeviceToken:
    def test_returns_token_response_on_success(self, no_sleep):
        with _patch_auth_urlopen(TOKEN_RESPONSE_JSON):
            resp = poll_device_token(SAMPLE_DEVICE_CODE_INFO)
        assert isinstance(resp, TokenResponse)
        assert resp.access_token == "at_123"
        assert resp.refresh_token == "rt_123"
        assert resp.org_id == "org_abc"
        assert resp.scopes == ["keys:read", "balance:write"]

    def test_polls_through_pending(self, monkeypatch):
        sleep_mock = mock.MagicMock()
        monkeypatch.setattr("parallel_web_tools.core.auth.time.sleep", sleep_mock)
        responses = [
            _http_error(400, {"error": "authorization_pending"}),
            _http_error(400, {"error": "authorization_pending"}),
            TOKEN_RESPONSE_JSON,
        ]
        with _patch_auth_urlopen(responses):
            resp = poll_device_token(SAMPLE_DEVICE_CODE_INFO)
        assert resp.access_token == "at_123"
        # Polls first, then sleeps between polls — 3 polls means 2 sleeps.
        assert sleep_mock.call_count == 2

    def test_slow_down_increases_interval(self, monkeypatch):
        sleep_mock = mock.MagicMock()
        monkeypatch.setattr("parallel_web_tools.core.auth.time.sleep", sleep_mock)
        responses = [_http_error(400, {"error": "slow_down"}), TOKEN_RESPONSE_JSON]
        with _patch_auth_urlopen(responses):
            poll_device_token(SAMPLE_DEVICE_CODE_INFO)
        # First poll returns slow_down; we bump interval to 10, sleep 10, poll again.
        assert sleep_mock.call_args_list == [mock.call(10)]

    def test_polls_immediately_without_initial_sleep(self, monkeypatch):
        """Happy path: auth already granted at entry → return on first poll, zero sleeps."""
        sleep_mock = mock.MagicMock()
        monkeypatch.setattr("parallel_web_tools.core.auth.time.sleep", sleep_mock)
        with _patch_auth_urlopen(TOKEN_RESPONSE_JSON):
            poll_device_token(SAMPLE_DEVICE_CODE_INFO)
        sleep_mock.assert_not_called()

    def test_raises_on_access_denied(self, no_sleep):
        with _patch_auth_urlopen(_http_error(400, {"error": "access_denied"})):
            with pytest.raises(Exception, match="Authorization denied"):
                poll_device_token(SAMPLE_DEVICE_CODE_INFO)

    def test_raises_on_expired_token(self, no_sleep):
        with _patch_auth_urlopen(_http_error(400, {"error": "expired_token"})):
            with pytest.raises(Exception, match="expired"):
                poll_device_token(SAMPLE_DEVICE_CODE_INFO)


# ---------------------------------------------------------------------------
# refresh_access_token / revoke_token
# ---------------------------------------------------------------------------


class TestRefreshAccessToken:
    def test_returns_new_token_response(self):
        with _patch_auth_urlopen(TOKEN_RESPONSE_JSON):
            resp = refresh_access_token("rt_old")
        assert resp.access_token == "at_123"

    def test_hits_token_endpoint_with_refresh_grant(self):
        captured: dict = {}
        with _patch_auth_urlopen(TOKEN_RESPONSE_JSON, capture=captured):
            refresh_access_token("rt_old")

        assert "/getServiceKeys/token" in captured["url"]
        assert "grant_type=refresh_token" in captured["body"]
        assert "refresh_token=rt_old" in captured["body"]


class TestRevokeToken:
    def test_sends_form_encoded_refresh_token(self):
        captured: dict = {}
        with _patch_auth_urlopen(b"", capture=captured):
            revoke_token("rt_xyz")

        assert "/getServiceKeys/token/revoke" in captured["url"]
        assert captured["method"] == "POST"
        # Body is form-encoded refresh_token=<token>; no bearer header.
        assert captured["body"] == "refresh_token=rt_xyz"
        assert any(v == "application/x-www-form-urlencoded" for v in captured["headers"].values())
        assert not any(k.lower() == "authorization" for k in captured["headers"])

    def test_raises_on_http_error(self):
        with _patch_auth_urlopen(_http_error(400, {"error": "invalid_request"})):
            with pytest.raises(Exception, match="Token revocation failed: 400"):
                revoke_token("rt_bad")


# ---------------------------------------------------------------------------
# _do_device_flow
# ---------------------------------------------------------------------------


class TestDoDeviceFlow:
    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth.is_headless", return_value=False)
    def test_opens_browser_when_not_headless(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            resp = _do_device_flow()
        assert isinstance(resp, TokenResponse)
        mock_browser_open.assert_called_once()

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth.is_headless", return_value=True)
    def test_skips_browser_when_headless(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow()
        mock_browser_open.assert_not_called()

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth.is_headless", return_value=False)
    def test_opens_browser_with_login_hint(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow(login_hint="login=email,e=user@example.com")
        opened_url = mock_browser_open.call_args.args[0]
        assert "login_hint=login%3Demail%2Ce%3Duser%40example.com" in opened_url

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    def test_callback_receives_device_code_info(self, mock_browser_open, no_sleep):
        captured = []
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow(on_device_code=lambda info: captured.append(info))
        assert len(captured) == 1
        assert isinstance(captured[0], DeviceCodeInfo)
        # Browser should NOT be opened when callback is provided.
        mock_browser_open.assert_not_called()

    def test_reregisters_and_retries_when_client_id_is_unknown(self, creds_file, no_sleep):
        invalid_client = Exception(
            'Device code request failed: 401 - {"error":"invalid_client","error_description":"Unknown client_id. Register the client first."}'
        )
        credentials.save(credentials.Credentials(client_id="cid_stale"))

        with (
            mock.patch(
                "parallel_web_tools.core.auth.request_device_code",
                side_effect=[invalid_client, SAMPLE_DEVICE_CODE_INFO],
            ) as mock_request,
            mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_fresh") as mock_register,
            mock.patch("parallel_web_tools.core.auth.poll_device_token", return_value=_token_response()) as mock_poll,
        ):
            _do_device_flow(client_id="cid_stale")

        assert mock_request.call_args_list[0].kwargs["client_id"] == "cid_stale"
        assert mock_request.call_args_list[1].kwargs["client_id"] == "cid_fresh"
        mock_register.assert_called_once_with()
        mock_poll.assert_called_once_with(SAMPLE_DEVICE_CODE_INFO, client_id="cid_fresh")
        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_fresh"


# ---------------------------------------------------------------------------
# _persist_token_response
# ---------------------------------------------------------------------------


class TestPersistTokenResponse:
    def test_writes_control_api_tokens_to_selected_org(self, creds_file):
        _persist_token_response(_token_response(access_token="at_new", refresh_token="rt_new", org_id="org_real"))
        creds = credentials.load()
        assert creds is not None
        assert creds.selected_org_id == "org_real"
        assert creds.orgs["org_real"].org_name == "Acme Org"
        control = creds.orgs["org_real"].control_api
        assert control.access_token == "at_new"
        assert control.refresh_token == "rt_new"
        assert control.access_token_scopes == ["keys:read", "balance:write"]
        # Expiries are absolute timestamps ordered access < refresh ≤ authorization.
        assert control.access_token_expires_at is not None
        assert control.refresh_token_expires_at is not None
        assert control.authorization_expires_at is not None
        assert control.access_token_expires_at > 0
        assert control.refresh_token_expires_at > control.access_token_expires_at
        assert control.authorization_expires_at >= control.refresh_token_expires_at


# ---------------------------------------------------------------------------
# login_flow + get_api_key
# ---------------------------------------------------------------------------


class TestLoginFlow:
    def test_creates_oauth_credential_and_stores(self, creds_file, mock_ensure_client_id):
        token_resp = _token_response(access_token="at_x", refresh_token="rt_x", org_id="org_real")
        with (
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp) as mock_flow,
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(refresh_token="rt_product"),
            ) as mock_provision,
        ):
            login_flow(login_hint="login=email,e=user@example.com")

        # The registered client_id must be threaded into both the device flow
        # and the credential creation call, which asks for the data API origin.
        assert mock_flow.call_args.kwargs.get("client_id") == mock_ensure_client_id
        mock_provision.assert_called_once_with(
            "at_x", client_id=mock_ensure_client_id, audience="https://api.parallel.ai"
        )

        creds = credentials.load()
        assert creds is not None
        org = creds.orgs["org_real"]
        assert creds.selected_org_id == "org_real"
        assert org.api_key is None
        assert org.org_name == "Acme Org"
        assert org.control_api.access_token == "at_x"
        assert org.product_api.refresh_token == "rt_product"
        assert org.product_api.token_endpoint == TOKEN_ENDPOINT
        assert org.product_api.audience == "https://api.parallel.ai"
        assert org.product_api.credential_id == "cred_1"
        assert org.product_api.refresh_token_expires_at == 4070908800

    def test_relogin_revokes_replaced_product_credential(self, creds_file, mock_ensure_client_id):
        _save_oauth_org("org_real", refresh_token="rt_old")
        with (
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=_token_response(org_id="org_real")),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(refresh_token="rt_new"),
            ),
            mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke,
        ):
            login_flow(on_device_code=lambda info: None)

        mock_revoke.assert_called_once_with("rt_old")
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_real"].product_api.refresh_token == "rt_new"

    def test_removes_legacy_org_after_successful_login(self, creds_file, mock_ensure_client_id):
        # Seed credentials with a v0-style legacy entry, as if the user was upgraded
        # from an older credentials file before running their first real login.
        credentials.save(
            credentials.Credentials(
                selected_org_id=credentials.LEGACY_ORG_ID,
                orgs={credentials.LEGACY_ORG_ID: credentials.OrgCredentials(api_key="legacy_key")},
            )
        )

        token_resp = _token_response(access_token="at_new", refresh_token="rt_new", org_id="org_real")
        with (
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(),
            ),
        ):
            login_flow()

        creds = credentials.load()
        assert creds is not None
        assert creds.selected_org_id == "org_real"
        assert "org_real" in creds.orgs
        # Legacy placeholder must be purged after a successful login.
        assert credentials.LEGACY_ORG_ID not in creds.orgs

    def test_registers_client_when_missing(self, creds_file):
        """First-boot login triggers /getServiceKeys/register and persists the id."""
        token_resp = _token_response(org_id="org_real")
        with (
            mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_fresh") as mock_reg,
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp) as mock_flow,
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(),
            ),
        ):
            login_flow()

        mock_reg.assert_called_once()
        assert mock_flow.call_args.kwargs.get("client_id") == "cid_fresh"

        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_fresh"

    def test_skips_registration_when_client_id_already_stored(self, creds_file):
        credentials.save(credentials.Credentials(client_id="cid_existing"))
        token_resp = _token_response(org_id="org_real")
        with (
            mock.patch("parallel_web_tools.core.auth.register_client") as mock_reg,
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp) as mock_flow,
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(),
            ),
        ):
            login_flow()

        mock_reg.assert_not_called()
        assert mock_flow.call_args.kwargs.get("client_id") == "cid_existing"


class TestGetApiKey:
    def test_env_var_first_priority(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        credentials.set_api_key_for_org("org_a", "stored_key")
        # Env var must win over stored credentials (operator override).
        assert get_api_key() == "env_key"

    def test_env_var_used_when_no_stored_key(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        assert get_api_key() == "env_key"

    def test_stored_only_without_env(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.set_api_key_for_org("org_a", "stored_key")
        assert get_api_key() == "stored_key"

    def test_oauth_login_does_not_provide_a_raw_key(self, creds_file, monkeypatch):
        # Integrations send x-api-key; an OAuth credential can't satisfy them,
        # and asking for a key must not create one or touch the network.
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        _save_oauth_org()
        with (
            mock.patch("parallel_web_tools.core.auth.service.provision_cli_oauth_credential") as mock_provision,
            mock.patch("parallel_web_tools.core.auth.login_flow") as mock_login,
        ):
            with pytest.raises(ValueError, match="does not provide an API key"):
                get_api_key()
        mock_provision.assert_not_called()
        mock_login.assert_not_called()

    def test_login_arguments_are_deprecated_and_do_not_log_in(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        with mock.patch("parallel_web_tools.core.auth.login_flow") as mock_flow:
            with pytest.warns(DeprecationWarning, match="no longer logs in"):
                result = get_api_key(force_login=True, login_hint="login=google")
        assert result == "env_key"
        mock_flow.assert_not_called()


# ---------------------------------------------------------------------------
# Product API OAuth credential
# ---------------------------------------------------------------------------


class TestClientAuthSelection:
    def test_env_var_short_circuits_credential_creation(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        credentials.save(credentials.Credentials(selected_org_id="org_a", orgs={"org_a": credentials.OrgCredentials()}))
        with (
            mock.patch("parallel_web_tools.core.auth.get_control_api_access_token") as mock_at,
            mock.patch("parallel_web_tools.core.auth.login_flow") as mock_login,
        ):
            client = get_client()
        assert type(client) is not OAuthParallel
        assert client.api_key == "env_key"
        mock_at.assert_not_called()
        mock_login.assert_not_called()

    def test_stored_credential_builds_oauth_client_on_its_audience(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        monkeypatch.delenv("PARALLEL_API_URL", raising=False)
        _save_oauth_org(audience="https://api-aws.parallel.ai")
        client = create_client(source="cli")
        assert isinstance(client, OAuthParallel)
        assert str(client.base_url).rstrip("/") == "https://api-aws.parallel.ai"
        assert "x-api-key" not in client.default_headers

    def test_parallel_api_url_overrides_audience(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        monkeypatch.setenv("PARALLEL_API_URL", "http://localhost:9000")
        _save_oauth_org(audience="https://api-aws.parallel.ai")
        client = create_client()
        assert str(client.base_url).rstrip("/") == "http://localhost:9000"

    def test_async_client_uses_oauth(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        _save_oauth_org()
        assert isinstance(get_async_client(), AsyncOAuthParallel)

    def test_migrates_stored_api_key_when_control_tokens_are_valid(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.set_api_key_for_org("org_a", "sk_old")
        with (
            mock.patch("parallel_web_tools.core.auth.get_control_api_access_token", return_value="at_ctrl"),
            mock.patch("parallel_web_tools.core.auth.ensure_client_id", return_value="cid_x"),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(refresh_token="rt_migrated"),
            ) as mock_provision,
        ):
            client = create_client()

        assert isinstance(client, OAuthParallel)
        mock_provision.assert_called_once_with("at_ctrl", client_id="cid_x", audience="https://api.parallel.ai")
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].api_key is None
        assert creds.orgs["org_a"].product_api.refresh_token == "rt_migrated"

    def test_keeps_stored_api_key_when_migration_fails(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.set_api_key_for_org("org_a", "sk_old")
        with mock.patch(
            "parallel_web_tools.core.auth.get_control_api_access_token",
            side_effect=ReauthenticationRequired("no control tokens"),
        ):
            client = create_client()
        assert type(client) is not OAuthParallel
        assert client.api_key == "sk_old"

    def test_creation_failure_falls_back_to_stored_key_with_warning(self, creds_file, monkeypatch, caplog):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.set_api_key_for_org("org_a", "sk_old")
        with (
            mock.patch("parallel_web_tools.core.auth.get_control_api_access_token", return_value="at_ctrl"),
            mock.patch("parallel_web_tools.core.auth.ensure_client_id", return_value="cid_x"),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                side_effect=service.ServiceApiError("POST failed: 422"),
            ),
            caplog.at_level("WARNING", logger="parallel_web_tools.core.auth"),
        ):
            client = create_client()
        assert client.api_key == "sk_old"
        assert "422" in caplog.text

    def test_creation_failure_without_stored_key_raises_instead_of_login(self, creds_file, monkeypatch):
        # A device flow would hit the same failure after the user approves it.
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.save(credentials.Credentials(selected_org_id="org_a", orgs={"org_a": credentials.OrgCredentials()}))
        with (
            mock.patch("parallel_web_tools.core.auth.get_control_api_access_token", return_value="at_ctrl"),
            mock.patch("parallel_web_tools.core.auth.ensure_client_id", return_value="cid_x"),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                side_effect=service.ServiceApiError("No app named 'parallel-cli Users'"),
            ),
            mock.patch("parallel_web_tools.core.auth.login_flow") as mock_login,
        ):
            with pytest.raises(service.ServiceApiError, match="No app named"):
                get_client()
        mock_login.assert_not_called()

    def test_get_client_falls_back_to_login_when_nothing_stored(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)

        def fake_login(**kwargs):
            _save_oauth_org()

        with mock.patch("parallel_web_tools.core.auth.login_flow", side_effect=fake_login) as mock_login:
            client = get_client()
        mock_login.assert_called_once()
        assert isinstance(client, OAuthParallel)

    def test_create_client_never_logs_in(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        with mock.patch("parallel_web_tools.core.auth.login_flow") as mock_login:
            with pytest.raises(ValueError, match="Parallel credentials required"):
                create_client()
        mock_login.assert_not_called()


class TestGetProductApiAccessToken:
    def test_returns_fresh_access_token_without_refresh(self, creds_file):
        _save_oauth_org(access_token="pat_live", access_token_expires_at=4102444800)
        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen") as mock_urlopen:
            assert get_product_api_access_token() == "pat_live"
        mock_urlopen.assert_not_called()

    def test_refreshes_and_persists_rotated_refresh_token(self, creds_file):
        _save_oauth_org(refresh_token="rt_1")
        captured: dict = {}
        refreshed = {
            "access_token": "pat_new",
            "token_type": "Bearer",
            "expires_in": 600,
            "scope": "*",
            "org_id": "org_a",
            "org_name": "Acme",
            "refresh_token": "rt_2",
            "refresh_token_expires_in": 2592000,
        }
        with _patch_auth_urlopen(refreshed, capture=captured):
            assert get_product_api_access_token() == "pat_new"

        assert captured["url"] == TOKEN_ENDPOINT
        assert "grant_type=refresh_token" in captured["body"]
        assert "refresh_token=rt_1" in captured["body"]
        creds = credentials.load()
        assert creds is not None
        product = creds.orgs["org_a"].product_api
        assert product.refresh_token == "rt_2"
        assert product.access_token == "pat_new"
        assert product.access_token_expires_at is not None

    def test_refreshes_when_access_token_is_within_skew(self, creds_file):
        import time as _time

        _save_oauth_org(
            access_token="pat_old", access_token_expires_at=int(_time.time()) + ACCESS_TOKEN_SKEW_SECONDS - 1
        )
        refreshed = {"access_token": "pat_new", "expires_in": 600, "org_id": "org_a", "refresh_token": "rt_2"}
        with _patch_auth_urlopen(refreshed):
            assert get_product_api_access_token() == "pat_new"

    def test_rejected_refresh_creates_new_credential_from_control_tokens(self, creds_file):
        _save_oauth_org(refresh_token="rt_revoked")
        refreshed = {"access_token": "pat_new", "expires_in": 600, "org_id": "org_a", "refresh_token": "rt_3"}
        with (
            _patch_auth_urlopen([_http_error(401, {"error": "invalid_grant"}), refreshed]),
            mock.patch("parallel_web_tools.core.auth.get_control_api_access_token", return_value="at_ctrl"),
            mock.patch("parallel_web_tools.core.auth.ensure_client_id", return_value="cid_x"),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(refresh_token="rt_fresh"),
            ) as mock_provision,
        ):
            assert get_product_api_access_token() == "pat_new"
        mock_provision.assert_called_once()
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].product_api.refresh_token == "rt_3"

    def test_rejected_refresh_without_control_tokens_requires_login(self, creds_file):
        _save_oauth_org(refresh_token="rt_revoked")
        with (
            _patch_auth_urlopen(_http_error(400, {"error": "invalid_grant"})),
            mock.patch(
                "parallel_web_tools.core.auth.get_control_api_access_token",
                side_effect=ReauthenticationRequired("expired"),
            ),
        ):
            with pytest.raises(ReauthenticationRequired, match="no longer valid"):
                get_product_api_access_token()
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].product_api.refresh_token is None

    def test_non_invalid_grant_4xx_keeps_credential(self, creds_file):
        # A 403/429 from a firewall or rate limit says nothing about the token.
        _save_oauth_org(refresh_token="rt_keep")
        with _patch_auth_urlopen(_http_error(403, {"error": "invalid_request"})):
            with pytest.raises(Exception, match="Token refresh failed: 403"):
                get_product_api_access_token()
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].product_api.refresh_token == "rt_keep"

    def test_server_error_keeps_credential(self, creds_file):
        _save_oauth_org(refresh_token="rt_keep")
        with _patch_auth_urlopen(_http_error(503, {"error": "unavailable"})):
            with pytest.raises(Exception, match="Token refresh failed: 503"):
                get_product_api_access_token()
        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].product_api.refresh_token == "rt_keep"

    def test_no_credential_requires_login(self, creds_file):
        with pytest.raises(ReauthenticationRequired):
            get_product_api_access_token()


class TestOAuthParallelRequests:
    @pytest.fixture
    def sent(self, monkeypatch) -> list[httpx.Request]:
        """Capture requests at httpx's send layer instead of the network."""
        seen: list[httpx.Request] = []

        def fake_send(self, request, **kwargs):
            seen.append(request)
            return httpx.Response(200, json={"ok": True}, request=request)

        async def fake_async_send(self, request, **kwargs):
            seen.append(request)
            return httpx.Response(200, json={"ok": True}, request=request)

        monkeypatch.setattr(httpx.Client, "send", fake_send)
        monkeypatch.setattr(httpx.AsyncClient, "send", fake_async_send)
        return seen

    def test_sends_bearer_token_and_no_api_key(self, creds_file, sent):
        _save_oauth_org(access_token="pat_live", access_token_expires_at=4102444800)
        OAuthParallel(base_url="https://api.parallel.ai").post("/v1beta/tasks/suggest", cast_to=dict[str, Any], body={})
        assert sent[0].headers["Authorization"] == "Bearer pat_live"
        assert "x-api-key" not in sent[0].headers

    def test_reauth_error_reaches_caller_unwrapped(self, creds_file, sent):
        client = OAuthParallel(base_url="https://api.parallel.ai")
        with pytest.raises(ReauthenticationRequired):
            client.post("/v1beta/tasks/suggest", cast_to=dict[str, Any], body={})
        assert sent == []

    def test_async_client_sends_bearer_token(self, creds_file, sent):
        import asyncio

        _save_oauth_org(access_token="pat_async", access_token_expires_at=4102444800)

        async def run():
            client = AsyncOAuthParallel(base_url="https://api.parallel.ai")
            await client.post("/v1beta/tasks/suggest", cast_to=dict[str, Any], body={})

        asyncio.run(run())
        assert sent[0].headers["Authorization"] == "Bearer pat_async"


# ---------------------------------------------------------------------------
# get_auth_status / logout
# ---------------------------------------------------------------------------


class TestAuthStatus:
    def test_status_with_env_var(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "test_key")
        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "environment"

    def test_env_var_overrides_stored_in_status(self, creds_file, monkeypatch):
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={"org_a": credentials.OrgCredentials(api_key="stored_key", org_name="Acme Org")},
            )
        )
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        status = get_auth_status()
        assert status["authenticated"] is True
        # Env var wins; stored creds are reported but flagged as overridden.
        assert status["method"] == "environment"
        assert status["env_var_set"] is True
        assert status["has_stored_credentials"] is True
        assert status["stored_overridden_by_env"] is True
        assert status["selected_org_name"] == "Acme Org"

    def test_status_with_stored_token(self, creds_file, legacy_file, monkeypatch):
        legacy_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_file.write_text(json.dumps({"access_token": "stored_token"}))
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)

        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "oauth"
        assert status["version"] == 1
        assert status["selected_org_id"] == "legacy"
        assert status["selected_org_name"] is None
        assert status["has_control_api_tokens"] is False
        assert status["token_file"] == str(creds_file)
        assert creds_file.exists()

    def test_status_with_oauth_credential(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        _save_oauth_org(control_refresh_token="rt_ctrl")
        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "oauth"
        assert status["stored_credential_type"] == "oauth_credential"
        assert status["has_control_api_tokens"] is True

    def test_status_not_authenticated(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        status = get_auth_status()
        assert status["authenticated"] is False
        assert status["method"] is None


class TestLogout:
    def test_logout_removes_token_no_revoke_when_missing(self, creds_file, legacy_file):
        legacy_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_file.write_text(json.dumps({"access_token": "test"}))

        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
            # Legacy tokens have no refresh_token — revoke should be skipped.
            mock_revoke.assert_not_called()
            assert not legacy_file.exists()

    def test_login_flow_writes_structured_auth_and_removes_legacy_file(
        self, creds_file, legacy_file, mock_ensure_client_id
    ):
        legacy_file.write_text(json.dumps({"access_token": "legacy_key"}))
        token_resp = _token_response(access_token="at_x", refresh_token="rt_x", org_id="org_real")

        with (
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp),
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_oauth_credential",
                return_value=_created_credential(),
            ),
        ):
            login_flow()

        auth_disk = json.loads(creds_file.read_text())
        assert auth_disk["version"] == 1
        assert auth_disk["selected_org_id"] == "org_real"
        # Migration removes the legacy file once auth.json is durably written.
        assert not legacy_file.exists()

    def test_logout_revokes_refresh_token_when_present(self, creds_file):
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={
                    "org_a": credentials.OrgCredentials(
                        api_key="sk",
                        control_api=credentials.ControlApiTokens(refresh_token="rt_keep"),
                    )
                },
            )
        )

        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
            mock_revoke.assert_called_once_with("rt_keep")
            assert not creds_file.exists()

    def test_logout_revokes_refresh_tokens_for_all_orgs(self, creds_file):
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={
                    "org_a": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token="rt_a"),
                    ),
                    "org_b": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token="rt_b"),
                    ),
                    "org_c": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token=None),
                    ),
                },
            )
        )

        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
            assert mock_revoke.call_count == 2
            mock_revoke.assert_any_call("rt_a")
            mock_revoke.assert_any_call("rt_b")
            assert not creds_file.exists()

    def test_logout_deduplicates_shared_refresh_tokens(self, creds_file):
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={
                    "org_a": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token="rt_shared"),
                    ),
                    "org_b": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token="rt_shared"),
                    ),
                },
            )
        )

        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
            mock_revoke.assert_called_once_with("rt_shared")

    def test_logout_best_effort_on_revoke_failure(self, creds_file):
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={
                    "org_a": credentials.OrgCredentials(
                        control_api=credentials.ControlApiTokens(refresh_token="rt_bad"),
                    )
                },
            )
        )

        with mock.patch(
            "parallel_web_tools.core.auth.revoke_token",
            side_effect=Exception("server down"),
        ):
            assert logout() is True
            assert not creds_file.exists()

    def test_logout_revokes_product_credential(self, creds_file):
        _save_oauth_org(refresh_token="rt_product", control_refresh_token="rt_ctrl")
        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
        assert mock_revoke.call_count == 2
        mock_revoke.assert_any_call("rt_ctrl")
        mock_revoke.assert_any_call("rt_product")

    def test_logout_no_token(self, creds_file):
        assert logout() is False


# ---------------------------------------------------------------------------
# Client creation
# ---------------------------------------------------------------------------


class TestCreateClient:
    def test_creates_client_with_explicit_key(self):
        with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
            create_client(api_key="test-key-123", source="cli")
            mock_parallel.assert_called_once()
            kwargs = mock_parallel.call_args.kwargs
            assert kwargs["api_key"] == "test-key-123"
            assert "(cli)" in kwargs["default_headers"]["User-Agent"]

    def test_creates_client_with_env_key(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env-key")
        with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
            create_client(source="duckdb")
            assert mock_parallel.call_args.kwargs["api_key"] == "env-key"

    def test_raises_without_key(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        with pytest.raises(ValueError, match="Parallel credentials required"):
            create_client()

    def test_passes_default_base_url(self):
        with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
            create_client(api_key="k", source="cli")
            assert mock_parallel.call_args.kwargs["base_url"] == "https://api.parallel.ai"

    def test_respects_parallel_api_url_env(self, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_URL", "http://localhost:9000")
        with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
            create_client(api_key="k", source="cli")
            assert mock_parallel.call_args.kwargs["base_url"] == "http://localhost:9000"


class TestResolveApiKey:
    def test_empty_string_key_is_falsy(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env-key")
        assert resolve_api_key(api_key="") == "env-key"

    def test_stored_token_used_as_fallback(self, creds_file, legacy_file, monkeypatch):
        legacy_file.write_text(json.dumps({"access_token": "stored-token"}))
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        assert resolve_api_key() == "stored-token"
        assert creds_file.exists()

    def test_env_var_beats_stored(self, creds_file, monkeypatch):
        credentials.set_api_key_for_org("org_a", "stored-key")
        monkeypatch.setenv("PARALLEL_API_KEY", "env-key")
        assert resolve_api_key() == "env-key"


# ---------------------------------------------------------------------------
# is_headless
# ---------------------------------------------------------------------------


class TestIsHeadless:
    def test_ssh_client_detected(self):
        with mock.patch.dict(os.environ, {"SSH_CLIENT": "1.2.3.4 54321 22"}):
            assert is_headless() is True

    def test_ssh_tty_detected(self):
        with mock.patch.dict(os.environ, {"SSH_TTY": "/dev/pts/0"}):
            assert is_headless() is True

    def test_ci_detected(self):
        with mock.patch.dict(os.environ, {"CI": "true"}):
            assert is_headless() is True

    def test_docker_detected(self):
        with mock.patch("os.path.exists", return_value=True):
            assert is_headless() is True

    def test_container_env_detected(self):
        with mock.patch.dict(os.environ, {"container": "podman"}):
            with mock.patch("os.path.exists", return_value=False):
                assert is_headless() is True

    def test_normal_env_not_headless(self):
        env = {k: v for k, v in os.environ.items() if k not in ("SSH_CLIENT", "SSH_TTY", "CI", "container")}
        env["DISPLAY"] = ":0"
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("os.path.exists", return_value=False):
                assert is_headless() is False


# ---------------------------------------------------------------------------
# get_control_api_access_token
# ---------------------------------------------------------------------------


NOW_FIXED = 1_800_000_000  # arbitrary "now" for clock-controlled tests


def _seed_control_api(
    creds_file,
    *,
    access_token: str | None = "at_current",
    access_token_expires_at: int | None = NOW_FIXED + 600,
    refresh_token: str | None = "rt_current",
    refresh_token_expires_at: int | None = NOW_FIXED + 604800,
    authorization_expires_at: int | None = NOW_FIXED + 7776000,
    org_id: str = "org_abc",
) -> None:
    """Write a credentials file with a specific control_api state for testing."""
    credentials.save(
        credentials.Credentials(
            selected_org_id=org_id,
            orgs={
                org_id: credentials.OrgCredentials(
                    api_key="sk_data",
                    control_api=credentials.ControlApiTokens(
                        access_token=access_token,
                        access_token_expires_at=access_token_expires_at,
                        access_token_scopes=["keys:read", "balance:write"],
                        refresh_token=refresh_token,
                        refresh_token_expires_at=refresh_token_expires_at,
                        authorization_expires_at=authorization_expires_at,
                    ),
                )
            },
        )
    )


@pytest.fixture
def frozen_now(monkeypatch):
    """Freeze auth.time.time() to NOW_FIXED."""
    monkeypatch.setattr("parallel_web_tools.core.auth.time.time", lambda: NOW_FIXED)


class TestGetControlApiAccessToken:
    def test_returns_cached_when_valid(self, creds_file, frozen_now):
        _seed_control_api(creds_file)
        with mock.patch("parallel_web_tools.core.auth.refresh_access_token") as mock_refresh:
            assert get_control_api_access_token() == "at_current"
        mock_refresh.assert_not_called()

    def test_refreshes_when_access_token_expired(self, creds_file, frozen_now, mock_ensure_client_id):
        _seed_control_api(creds_file, access_token_expires_at=NOW_FIXED - 10)
        with mock.patch(
            "parallel_web_tools.core.auth.refresh_access_token",
            return_value=_token_response(access_token="at_refreshed", refresh_token="rt_new"),
        ) as mock_refresh:
            assert get_control_api_access_token() == "at_refreshed"
        mock_refresh.assert_called_once_with("rt_current", client_id=mock_ensure_client_id)

        # Refreshed tokens must be persisted — loaded file reflects new state.
        creds = credentials.load()
        assert creds is not None
        org = creds.selected_org()
        assert org is not None
        assert org.control_api.access_token == "at_refreshed"
        assert org.control_api.refresh_token == "rt_new"
        # New expiries are computed relative to the mocked "now".
        assert org.control_api.access_token_expires_at == NOW_FIXED + 600

    def test_skew_buffer_triggers_early_refresh(self, creds_file, frozen_now, mock_ensure_client_id):
        # Access token technically valid but within the skew buffer — refresh.
        _seed_control_api(creds_file, access_token_expires_at=NOW_FIXED + ACCESS_TOKEN_SKEW_SECONDS - 1)
        with mock.patch(
            "parallel_web_tools.core.auth.refresh_access_token",
            return_value=_token_response(access_token="at_refreshed"),
        ) as mock_refresh:
            assert get_control_api_access_token() == "at_refreshed"
        mock_refresh.assert_called_once()

    def test_raises_reauth_when_no_credentials(self, creds_file):
        with pytest.raises(ReauthenticationRequired, match="not logged in"):
            get_control_api_access_token()

    def test_raises_reauth_when_no_control_api_tokens(self, creds_file):
        # Org exists but has no control_api.access_token (e.g. legacy-migrated org).
        _seed_control_api(
            creds_file,
            access_token=None,
            access_token_expires_at=None,
            refresh_token=None,
            refresh_token_expires_at=None,
            authorization_expires_at=None,
        )
        with mock.patch("parallel_web_tools.core.auth.refresh_access_token") as mock_refresh:
            with pytest.raises(ReauthenticationRequired, match="not logged in"):
                get_control_api_access_token()
        mock_refresh.assert_not_called()

    def test_raises_reauth_when_authorization_expired(self, creds_file, frozen_now):
        _seed_control_api(creds_file, authorization_expires_at=NOW_FIXED - 1)
        with mock.patch("parallel_web_tools.core.auth.refresh_access_token") as mock_refresh:
            with pytest.raises(ReauthenticationRequired, match="authorization grant"):
                get_control_api_access_token()
        mock_refresh.assert_not_called()

    def test_raises_reauth_when_refresh_token_expired(self, creds_file, frozen_now):
        _seed_control_api(
            creds_file,
            access_token_expires_at=NOW_FIXED - 10,
            refresh_token_expires_at=NOW_FIXED - 1,
        )
        with mock.patch("parallel_web_tools.core.auth.refresh_access_token") as mock_refresh:
            with pytest.raises(ReauthenticationRequired, match="refresh token"):
                get_control_api_access_token()
        mock_refresh.assert_not_called()

    def test_bubbles_up_refresh_http_error(self, creds_file, frozen_now):
        _seed_control_api(creds_file, access_token_expires_at=NOW_FIXED - 10)
        with mock.patch(
            "parallel_web_tools.core.auth.refresh_access_token",
            side_effect=Exception("500 Internal Server Error"),
        ):
            with pytest.raises(Exception, match="500 Internal Server Error"):
                get_control_api_access_token()
