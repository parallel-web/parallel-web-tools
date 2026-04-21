"""Tests for the auth module (device flow against /getServiceKeys)."""

import io
import json
import os
import urllib.error
from contextlib import contextmanager
from dataclasses import replace
from email.message import Message
from unittest import mock

import pytest

from parallel_web_tools.core import credentials
from parallel_web_tools.core.auth import (
    ACCESS_TOKEN_SKEW_SECONDS,
    DeviceCodeInfo,
    ReauthenticationRequired,
    TokenResponse,
    _build_verification_uri,
    _do_device_flow,
    _ensure_client_id,
    _is_headless,
    _persist_token_response,
    create_client,
    get_api_key,
    get_auth_status,
    get_control_api_access_token,
    login_flow,
    logout,
    poll_device_token,
    refresh_access_token,
    register_client,
    request_device_code,
    resolve_api_key,
    revoke_token,
)

# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    """Patch CREDENTIALS_FILE to a tmp path for isolation."""
    path = tmp_path / "credentials.json"
    monkeypatch.setattr(credentials, "CREDENTIALS_FILE", path)
    return path


@pytest.fixture
def no_sleep(monkeypatch):
    """Skip real sleeps in the device-code poll loop."""
    monkeypatch.setattr("parallel_web_tools.core.auth.time.sleep", mock.MagicMock())


@pytest.fixture
def mock_ensure_client_id(monkeypatch):
    """Stub out _ensure_client_id to avoid real /getServiceKeys/register calls.

    Returns the value the stub will produce so tests can assert on it.
    """
    value = "cid_test"
    monkeypatch.setattr("parallel_web_tools.core.auth._ensure_client_id", lambda: value)
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
    scope="keys:read balance:write",
)


def _token_response(**overrides) -> TokenResponse:
    """Build a TokenResponse with test defaults."""
    return replace(_TOKEN_RESPONSE_DEFAULT, **overrides)


# ---------------------------------------------------------------------------
# _build_verification_uri
# ---------------------------------------------------------------------------


class TestBuildVerificationUri:
    def test_appends_agent_true(self):
        url = _build_verification_uri("http://localhost:3000/getServiceKeys/device?user_code=ABCD", None)
        assert "agent=true" in url
        assert "user_code=ABCD" in url

    def test_appends_login_hint_with_email(self):
        url = _build_verification_uri(
            "http://localhost:3000/getServiceKeys/device?user_code=ABCD",
            "user@example.com",
        )
        assert "agent=true" in url
        # The hint value is URL-encoded: ','→'%2C', '='→'%3D'
        assert "login_hint=login%3Demail%2Ce%3Duser%40example.com" in url

    def test_no_email_omits_login_hint(self):
        url = _build_verification_uri("http://localhost:3000/getServiceKeys/device", None)
        assert "login_hint" not in url


# ---------------------------------------------------------------------------
# register_client / _ensure_client_id
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


class TestEnsureClientId:
    def test_returns_stored_client_id_without_registering(self, creds_file):
        credentials.save(credentials.Credentials(client_id="cid_stored"))
        with mock.patch("parallel_web_tools.core.auth.register_client") as mock_reg:
            assert _ensure_client_id() == "cid_stored"
        mock_reg.assert_not_called()

    def test_registers_and_persists_when_missing(self, creds_file):
        with mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_fresh"):
            assert _ensure_client_id() == "cid_fresh"

        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_fresh"

    def test_registers_again_when_stored_client_id_is_none(self, creds_file):
        # Simulate a prior registration failure: file exists but client_id is None.
        credentials.save(credentials.Credentials(selected_org_id="x", orgs={"x": credentials.OrgCredentials()}))
        with mock.patch("parallel_web_tools.core.auth.register_client", return_value="cid_new") as mock_reg:
            assert _ensure_client_id() == "cid_new"
        mock_reg.assert_called_once()

        creds = credentials.load()
        assert creds is not None
        assert creds.client_id == "cid_new"

    def test_falls_back_to_hardcoded_on_registration_failure(self, creds_file, capsys):
        with mock.patch(
            "parallel_web_tools.core.auth.register_client",
            side_effect=Exception("server down"),
        ):
            assert _ensure_client_id() == "parallel-cli"

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
    @mock.patch("parallel_web_tools.core.auth._is_headless", return_value=False)
    def test_opens_browser_when_not_headless(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            resp = _do_device_flow()
        assert isinstance(resp, TokenResponse)
        mock_browser_open.assert_called_once()
        assert "agent=true" in mock_browser_open.call_args.args[0]

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth._is_headless", return_value=True)
    def test_skips_browser_when_headless(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow()
        mock_browser_open.assert_not_called()

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth._is_headless", return_value=False)
    def test_opens_browser_with_email_hint(self, _headless, mock_browser_open, no_sleep):
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow(email_hint="user@example.com")
        opened_url = mock_browser_open.call_args.args[0]
        assert "login_hint=login%3Demail%2Ce%3Duser%40example.com" in opened_url
        assert "agent=true" in opened_url

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    def test_callback_receives_device_code_info(self, mock_browser_open, no_sleep):
        captured = []
        with _patch_auth_urlopen([DEVICE_RESPONSE, TOKEN_RESPONSE_JSON]):
            _do_device_flow(on_device_code=lambda info: captured.append(info))
        assert len(captured) == 1
        assert isinstance(captured[0], DeviceCodeInfo)
        # Browser should NOT be opened when callback is provided.
        mock_browser_open.assert_not_called()


# ---------------------------------------------------------------------------
# _persist_token_response
# ---------------------------------------------------------------------------


class TestPersistTokenResponse:
    def test_writes_control_api_tokens_to_selected_org(self, creds_file):
        _persist_token_response(_token_response(access_token="at_new", refresh_token="rt_new", org_id="org_real"))
        creds = credentials.load()
        assert creds is not None
        assert creds.selected_org_id == "org_real"
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
    def test_provisions_api_key_and_stores(self, creds_file, mock_ensure_client_id):
        token_resp = _token_response(access_token="at_x", refresh_token="rt_x", org_id="org_real")
        with (
            mock.patch("parallel_web_tools.core.auth._do_device_flow", return_value=token_resp) as mock_flow,
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_api_key",
                return_value=("sk_minted", "cid_test-2026-04-21-1432"),
            ) as mock_provision,
        ):
            api_key = login_flow(email="user@example.com")

        assert api_key == "sk_minted"
        # The registered client_id must be threaded into both the device flow
        # and the data-API key provisioning call.
        assert mock_flow.call_args.kwargs.get("client_id") == mock_ensure_client_id
        mock_provision.assert_called_once_with("at_x", client_id=mock_ensure_client_id)

        creds = credentials.load()
        assert creds is not None
        assert creds.selected_org_id == "org_real"
        assert creds.orgs["org_real"].api_key == "sk_minted"
        assert creds.orgs["org_real"].control_api.access_token == "at_x"

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
                "parallel_web_tools.core.auth.service.provision_cli_api_key",
                return_value=("sk_minted", "name"),
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
                "parallel_web_tools.core.auth.service.provision_cli_api_key",
                return_value=("sk_minted", "name"),
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
                "parallel_web_tools.core.auth.service.provision_cli_api_key",
                return_value=("sk_minted", "name"),
            ),
        ):
            login_flow()

        mock_reg.assert_not_called()
        assert mock_flow.call_args.kwargs.get("client_id") == "cid_existing"


class TestGetApiKey:
    def test_stored_token_first_priority(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        credentials.set_api_key_for_org("org_a", "stored_key")
        # Stored credentials must win over the env var.
        assert get_api_key() == "stored_key"

    def test_env_var_used_when_no_stored_key(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        assert get_api_key() == "env_key"

    def test_stored_only_without_env(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.set_api_key_for_org("org_a", "stored_key")
        assert get_api_key() == "stored_key"

    def test_force_login_runs_login_flow(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")  # should still be ignored with force_login
        with mock.patch("parallel_web_tools.core.auth.login_flow", return_value="minted_key") as mock_flow:
            result = get_api_key(force_login=True, email="u@example.com")
        assert result == "minted_key"
        assert mock_flow.call_args.kwargs.get("email") == "u@example.com"

    def test_provisions_via_service_api_when_stored_api_key_missing(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        credentials.save(
            credentials.Credentials(
                selected_org_id="org_a",
                orgs={"org_a": credentials.OrgCredentials()},
            )
        )

        with (
            mock.patch(
                "parallel_web_tools.core.auth.get_control_api_access_token", return_value="at_existing"
            ) as mock_at,
            mock.patch("parallel_web_tools.core.auth._ensure_client_id", return_value="cid_existing") as mock_client_id,
            mock.patch(
                "parallel_web_tools.core.auth.service.provision_cli_api_key",
                return_value=("sk_minted", "cid_existing-2026-04-23-1212"),
            ) as mock_provision,
            mock.patch("parallel_web_tools.core.auth.login_flow") as mock_login,
        ):
            assert get_api_key() == "sk_minted"

        mock_at.assert_called_once_with()
        mock_client_id.assert_called_once_with()
        mock_provision.assert_called_once_with("at_existing", client_id="cid_existing")
        mock_login.assert_not_called()

        creds = credentials.load()
        assert creds is not None
        assert creds.orgs["org_a"].api_key == "sk_minted"

    def test_falls_back_to_login_when_control_api_requires_reauth(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        with (
            mock.patch(
                "parallel_web_tools.core.auth.get_control_api_access_token",
                side_effect=ReauthenticationRequired("not logged in; run 'parallel-cli login'"),
            ),
            mock.patch("parallel_web_tools.core.auth.login_flow", return_value="sk_from_login") as mock_login,
        ):
            assert get_api_key(email="user@example.com") == "sk_from_login"

        assert mock_login.call_args.kwargs.get("email") == "user@example.com"


# ---------------------------------------------------------------------------
# get_auth_status / logout
# ---------------------------------------------------------------------------


class TestAuthStatus:
    def test_status_with_env_var(self, creds_file, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "test_key")
        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "environment"

    def test_stored_beats_env_var_in_status(self, creds_file, monkeypatch):
        credentials.set_api_key_for_org("org_a", "stored_key")
        monkeypatch.setenv("PARALLEL_API_KEY", "env_key")
        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "oauth"  # stored credentials win

    def test_status_with_stored_token(self, creds_file, monkeypatch):
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text(json.dumps({"access_token": "stored_token"}))
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)

        status = get_auth_status()
        assert status["authenticated"] is True
        assert status["method"] == "oauth"
        assert status["version"] == 1
        assert status["selected_org_id"] == "legacy"
        assert status["has_control_api_tokens"] is False

    def test_status_not_authenticated(self, creds_file, monkeypatch):
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        status = get_auth_status()
        assert status["authenticated"] is False
        assert status["method"] is None


class TestLogout:
    def test_logout_removes_token_no_revoke_when_missing(self, creds_file):
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text(json.dumps({"access_token": "test"}))

        with mock.patch("parallel_web_tools.core.auth.revoke_token") as mock_revoke:
            assert logout() is True
            # Legacy tokens have no refresh_token — revoke should be skipped.
            mock_revoke.assert_not_called()
            assert not creds_file.exists()

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
        with pytest.raises(ValueError, match="Parallel API key required"):
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

    def test_stored_token_used_as_fallback(self, creds_file, monkeypatch):
        creds_file.write_text(json.dumps({"access_token": "stored-token"}))
        monkeypatch.delenv("PARALLEL_API_KEY", raising=False)
        assert resolve_api_key() == "stored-token"

    def test_stored_beats_env_var(self, creds_file, monkeypatch):
        credentials.set_api_key_for_org("org_a", "stored-key")
        monkeypatch.setenv("PARALLEL_API_KEY", "env-key")
        assert resolve_api_key() == "stored-key"


# ---------------------------------------------------------------------------
# _is_headless
# ---------------------------------------------------------------------------


class TestIsHeadless:
    def test_ssh_client_detected(self):
        with mock.patch.dict(os.environ, {"SSH_CLIENT": "1.2.3.4 54321 22"}):
            assert _is_headless() is True

    def test_ssh_tty_detected(self):
        with mock.patch.dict(os.environ, {"SSH_TTY": "/dev/pts/0"}):
            assert _is_headless() is True

    def test_ci_detected(self):
        with mock.patch.dict(os.environ, {"CI": "true"}):
            assert _is_headless() is True

    def test_docker_detected(self):
        with mock.patch("os.path.exists", return_value=True):
            assert _is_headless() is True

    def test_container_env_detected(self):
        with mock.patch.dict(os.environ, {"container": "podman"}):
            with mock.patch("os.path.exists", return_value=False):
                assert _is_headless() is True

    def test_normal_env_not_headless(self):
        env = {k: v for k, v in os.environ.items() if k not in ("SSH_CLIENT", "SSH_TTY", "CI", "container")}
        env["DISPLAY"] = ":0"
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("os.path.exists", return_value=False):
                assert _is_headless() is False


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
