"""Tests for the service API client (apps + OAuth credentials + balance)."""

import io
import json
import urllib.error
from contextlib import contextmanager
from email.message import Message
from unittest import mock

import pytest

from parallel_web_tools.core import service
from parallel_web_tools.core.service import (
    ServiceApiError,
    add_balance,
    create_oauth_credential,
    get_balance,
    list_apps,
    provision_cli_oauth_credential,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _http_error(status: int, body: dict) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://example.com",
        code=status,
        msg="Error",
        hdrs=Message(),
        fp=io.BytesIO(json.dumps(body).encode()),
    )


def _patch_urlopen(responses, capture: dict | None = None):
    """Patch service.urllib.request.urlopen to yield each response in order.

    ``responses`` may be a single value or a list; each entry is a dict
    (JSON-encoded body), bytes (raw body), pre-built HTTPError, or a callable
    ``req -> value`` to dispatch by request. When ``capture`` is provided it
    is populated on each call with url/body/headers/method.
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
        if callable(r):
            r = r(req)
        if isinstance(r, urllib.error.HTTPError):
            raise r
        payload = r if isinstance(r, (bytes, bytearray)) else json.dumps(r).encode()
        yield io.BytesIO(bytes(payload))

    return mock.patch("parallel_web_tools.core.service.urllib.request.urlopen", side_effect=impl)


def _app_item(app_name: str, app_id: str = "app_x", org_id: str = "org_x") -> dict:
    return {"app_name": app_name, "org_name": None, "app_id": app_id, "org_id": org_id}


def _oauth_credential_response(refresh_token: str = "rt_minted", audience: str = "https://api.parallel.ai") -> dict:
    """Build a full CreateOAuthCredentialResponse payload."""
    return {
        "credential_id": "8f1c1f4e-0000-4000-8000-000000000001",
        "client_id": "cid_test",
        "app_id": "app_cli",
        "app_name": service.PARALLEL_CLI_APP_NAME,
        "created_by_user_id": "user_1",
        "is_service_scoped": False,
        "audience": audience,
        "scopes": ["*"],
        "token_endpoint": "https://platform.parallel.ai/getServiceKeys/token",
        "refresh_token": refresh_token,
        "refresh_token_expires_at": "2026-12-20T00:00:00Z",
        "created_at": "2026-11-20T00:00:00Z",
    }


def _apps(*names_and_ids: tuple[str, str]) -> dict:
    """Shorthand for a GetAppsForOrgResponseModel payload."""
    return {"apps": [_app_item(name, app_id=app_id) for name, app_id in names_and_ids]}


def _balance_response(**overrides) -> dict:
    """Build a BalanceResponse payload."""
    base = {
        "org_id": "org_abc",
        "credit_balance_cents": 1500,
        "pending_debit_balance_cents": 0,
        "will_invoice": False,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# list_apps
# ---------------------------------------------------------------------------


class TestListApps:
    def test_parses_apps_list(self):
        with _patch_urlopen(_apps(("parallel-cli Users", "app_1"))):
            apps = list_apps("at_123")
        assert len(apps) == 1
        assert apps[0].app_name == "parallel-cli Users"
        assert apps[0].app_id == "app_1"

    def test_empty_apps_list(self):
        with _patch_urlopen({"apps": []}):
            assert list_apps("at_123") == []

    def test_missing_apps_field_returns_empty(self):
        # GetAppsForOrgResponseModel.apps is Optional; an omitted key is legal.
        with _patch_urlopen({}):
            assert list_apps("at_123") == []

    def test_sends_bearer_auth(self):
        captured: dict = {}
        with _patch_urlopen({"apps": []}, capture=captured):
            list_apps("at_xyz")

        assert captured["method"] == "GET"
        assert "/service/v1/apps" in captured["url"]
        assert any(v == "Bearer at_xyz" for v in captured["headers"].values())

    def test_respects_service_api_url_env(self, monkeypatch):
        monkeypatch.setenv("PARALLEL_SERVICE_API_URL", "http://localhost:8090")
        captured: dict = {}
        with _patch_urlopen({"apps": []}, capture=captured):
            list_apps("at_xyz")
        assert captured["url"].startswith("http://localhost:8090/")

    def test_raises_on_malformed_apps_shape(self):
        # apps must be a list; a string is invalid.
        with _patch_urlopen({"apps": "nope"}):
            with pytest.raises(ServiceApiError, match="Unexpected /service/v1/apps response"):
                list_apps("at_xyz")

    def test_raises_on_http_error(self):
        with _patch_urlopen(_http_error(401, {"error": "unauthorized"})):
            with pytest.raises(ServiceApiError, match="failed: 401"):
                list_apps("at_xyz")


# ---------------------------------------------------------------------------
# create_oauth_credential
# ---------------------------------------------------------------------------


class TestCreateOAuthCredential:
    def test_returns_typed_credential_model(self):
        with _patch_urlopen(_oauth_credential_response()):
            result = create_oauth_credential("at_xyz", "app_1", "cid_test", "https://api.parallel.ai", ["*"])
        assert result.refresh_token == "rt_minted"
        assert result.token_endpoint == "https://platform.parallel.ai/getServiceKeys/token"
        assert result.audience == "https://api.parallel.ai"

    def test_posts_to_oauth_credentials_route_with_bearer(self):
        captured: dict = {}
        with _patch_urlopen(_oauth_credential_response(), capture=captured):
            create_oauth_credential("at_xyz", "app_42", "cid_test", "https://api.parallel.ai", ["*"])

        assert captured["method"] == "POST"
        assert captured["url"].endswith("/service/v1/apps/app_42/oauth-credentials")
        assert captured["headers"]["Authorization"] == "Bearer at_xyz"
        assert json.loads(captured["body"]) == {
            "client_id": "cid_test",
            "audience": "https://api.parallel.ai",
            "scopes": ["*"],
            "is_service_scoped": False,
        }

    def test_raises_on_malformed_response(self):
        with _patch_urlopen({"credential_id": "c1"}):
            with pytest.raises(ServiceApiError, match="Unexpected create_oauth_credential response"):
                create_oauth_credential("at_xyz", "app_1", "cid_test", "https://api.parallel.ai", ["*"])

    def test_raises_on_http_error(self):
        with _patch_urlopen(_http_error(400, {"detail": "bad audience"})):
            with pytest.raises(ServiceApiError, match="400"):
                create_oauth_credential("at_xyz", "app_1", "cid_test", "https://nope.example", ["*"])


# ---------------------------------------------------------------------------
# provision_cli_oauth_credential
# ---------------------------------------------------------------------------


class TestProvisionCliOAuthCredential:
    def test_targets_cli_app_with_wildcard_scope(self):
        apps_payload = _apps(("Some Other App", "app_other"), (service.PARALLEL_CLI_APP_NAME, "app_cli"))
        posted: dict = {}

        def dispatch(req):
            if req.get_method() == "GET":
                return apps_payload
            posted["url"] = req.full_url
            posted["body"] = json.loads(req.data.decode())
            return _oauth_credential_response(refresh_token="rt_provisioned")

        with _patch_urlopen([dispatch, dispatch]):
            created = provision_cli_oauth_credential("at_xyz", client_id="cid_abc", audience="https://api.parallel.ai")

        assert created.refresh_token == "rt_provisioned"
        assert posted["url"].endswith("/apps/app_cli/oauth-credentials")
        assert posted["body"]["client_id"] == "cid_abc"
        assert posted["body"]["scopes"] == ["*"]

    def test_raises_when_app_not_found(self):
        with _patch_urlopen(_apps(("Some Other App", "app_other"))):
            with pytest.raises(ServiceApiError, match="No app named"):
                provision_cli_oauth_credential("at_xyz", client_id="cid", audience="https://api.parallel.ai")


# ---------------------------------------------------------------------------
# get_balance
# ---------------------------------------------------------------------------


class TestGetBalance:
    def test_parses_balance_response(self):
        payload = _balance_response(
            credit_balance_cents=1234,
            pending_debit_balance_cents=56,
            will_invoice=False,
        )
        with _patch_urlopen(payload):
            resp = get_balance("at_xyz")
        assert resp.org_id == "org_abc"
        assert resp.credit_balance_cents == 1234
        assert resp.pending_debit_balance_cents == 56
        assert resp.will_invoice is False

    def test_defaults_optional_fields_when_omitted(self):
        # pending_debit_balance_cents and will_invoice are optional.
        with _patch_urlopen({"org_id": "org_x", "credit_balance_cents": 0}):
            resp = get_balance("at_xyz")
        assert resp.pending_debit_balance_cents == 0
        assert resp.will_invoice is False

    def test_sends_bearer_auth_to_balance_endpoint(self):
        captured: dict = {}
        with _patch_urlopen(_balance_response(), capture=captured):
            get_balance("at_xyz")

        assert captured["method"] == "GET"
        assert captured["url"].endswith("/service/v1/balance")
        assert any(v == "Bearer at_xyz" for v in captured["headers"].values())

    def test_respects_service_api_url_env(self, monkeypatch):
        monkeypatch.setenv("PARALLEL_SERVICE_API_URL", "http://localhost:8090")
        captured: dict = {}
        with _patch_urlopen(_balance_response(), capture=captured):
            get_balance("at_xyz")
        assert captured["url"].startswith("http://localhost:8090/")

    def test_raises_on_http_error(self):
        with _patch_urlopen(_http_error(500, {"error": "internal"})):
            with pytest.raises(ServiceApiError, match="failed: 500"):
                get_balance("at_xyz")

    def test_raises_on_malformed_payload(self):
        # Missing required field org_id.
        with _patch_urlopen({"credit_balance_cents": 10}):
            with pytest.raises(ServiceApiError, match="Unexpected /service/v1/balance response"):
                get_balance("at_xyz")


# ---------------------------------------------------------------------------
# add_balance
# ---------------------------------------------------------------------------


class TestAddBalance:
    def test_posts_expected_body_and_parses_response(self):
        captured: dict = {}
        with _patch_urlopen(_balance_response(credit_balance_cents=1600), capture=captured):
            resp = add_balance("at_xyz", amount_cents=100, idempotency_key="key_1234")

        assert captured["method"] == "POST"
        assert captured["url"].endswith("/service/v1/balance/add")
        assert json.loads(captured["body"]) == {"amount_cents": 100, "idempotency_key": "key_1234"}
        assert any(v == "Bearer at_xyz" for v in captured["headers"].values())
        assert resp.credit_balance_cents == 1600

    def test_raises_on_http_error(self):
        with _patch_urlopen(_http_error(402, {"error": "card_declined"})):
            with pytest.raises(ServiceApiError, match="failed: 402"):
                add_balance("at_xyz", amount_cents=100, idempotency_key="test-key-1")

    def test_raises_on_malformed_response(self):
        with _patch_urlopen({"credit_balance_cents": 10}):
            with pytest.raises(ServiceApiError, match="Unexpected /service/v1/balance/add response"):
                add_balance("at_xyz", amount_cents=100, idempotency_key="test-key-2")
