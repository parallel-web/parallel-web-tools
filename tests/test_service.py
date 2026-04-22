"""Tests for the service API client (apps + keys + balance)."""

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
    _build_key_name,
    add_balance,
    create_api_key,
    get_balance,
    list_apps,
    provision_cli_api_key,
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


def _api_key_response(raw_api_key: str | None = "sk_minted", name: str = "parallel-cli-2026-04-21-1432") -> dict:
    """Build a full CreateKeyResponse payload (flat — no ``api_key`` wrapper)."""
    return {
        "api_key_id": "key_1",
        "api_key_name": name,
        "app_id": "app_cli",
        "app_name": service.PARALLEL_CLI_APP_NAME,
        "created_by_user_id": "user_1",
        "created_by_user_email": "user@example.com",
        "display_value": "sk_***1234",
        "raw_api_key": raw_api_key,
        "created_at": 1776800731,
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
# create_api_key
# ---------------------------------------------------------------------------


class TestCreateApiKey:
    def test_returns_typed_api_key_model(self):
        with _patch_urlopen(_api_key_response()):
            result = create_api_key("at_xyz", "app_1", "parallel-cli-2026-04-21-1432")
        assert result.raw_api_key == "sk_minted"
        assert result.api_key_name == "parallel-cli-2026-04-21-1432"
        assert result.display_value == "sk_***1234"

    def test_request_body_has_only_api_key_name(self):
        captured: dict = {}
        with _patch_urlopen(_api_key_response(), capture=captured):
            create_api_key("at_xyz", "app_42", "parallel-cli-2026-04-21-1432")

        assert captured["method"] == "POST"
        assert captured["url"].endswith("/service/v1/apps/app_42/keys")
        assert json.loads(captured["body"]) == {"api_key_name": "parallel-cli-2026-04-21-1432"}

    def test_raises_on_malformed_response(self):
        # Missing required fields (e.g. api_key_id) — pydantic validation fails.
        with _patch_urlopen({"display_value": "sk_***"}):
            with pytest.raises(ServiceApiError, match="Unexpected create_api_key response"):
                create_api_key("at_xyz", "app_1", "name_1")


# ---------------------------------------------------------------------------
# provision_cli_api_key
# ---------------------------------------------------------------------------


class TestProvisionCliApiKey:
    def test_happy_path(self):
        apps_payload = _apps(("Some Other App", "app_other"), (service.PARALLEL_CLI_APP_NAME, "app_cli"))
        captured_paths: list[str] = []

        def dispatch(req):
            captured_paths.append(req.full_url)
            if req.get_method() == "GET":
                return apps_payload
            return _api_key_response(raw_api_key="sk_provisioned")

        with _patch_urlopen([dispatch, dispatch]):
            key, name = provision_cli_api_key("at_xyz")

        assert key == "sk_provisioned"
        assert name.startswith("parallel-cli-")
        # The created key must target the CLI app, not the other one.
        assert any("/apps/app_cli/keys" in p for p in captured_paths)

    def test_raises_when_app_not_found(self):
        with _patch_urlopen(_apps(("Some Other App", "app_other"))):
            with pytest.raises(ServiceApiError, match="No app named"):
                provision_cli_api_key("at_xyz")

    def test_raises_when_raw_api_key_missing(self):
        apps_payload = _apps((service.PARALLEL_CLI_APP_NAME, "app_cli"))
        with _patch_urlopen([apps_payload, _api_key_response(raw_api_key=None)]):
            with pytest.raises(ServiceApiError, match="no raw_api_key"):
                provision_cli_api_key("at_xyz")

    def test_client_id_is_used_in_created_key_name(self):
        apps_payload = _apps((service.PARALLEL_CLI_APP_NAME, "app_cli"))
        sent_body: dict = {}

        def dispatch(req):
            if req.get_method() == "GET":
                return apps_payload
            sent_body.update(json.loads(req.data.decode()))
            return _api_key_response(raw_api_key="sk_ok")

        with _patch_urlopen([dispatch, dispatch]):
            _, name = provision_cli_api_key("at_xyz", client_id="cid_abc")

        assert name.startswith("cid_abc-")
        assert sent_body["api_key_name"] == name


# ---------------------------------------------------------------------------
# _build_key_name
# ---------------------------------------------------------------------------


class TestBuildKeyName:
    def test_falls_back_to_parallel_cli_prefix_without_client_id(self):
        import re

        name = _build_key_name()
        # parallel-cli-YYYY-MM-DD-HHMM (HHMM is 4 digits, no colon)
        assert re.match(r"^parallel-cli-\d{4}-\d{2}-\d{2}-\d{4}$", name), name

    def test_uses_client_id_as_prefix_when_provided(self):
        import re

        name = _build_key_name(client_id="cid_abc123")
        # Same date suffix, but the client_id now carries the entropy.
        assert re.match(r"^cid_abc123-\d{4}-\d{2}-\d{2}-\d{4}$", name), name


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
            resp = add_balance("at_xyz", amount_cents=100, idempotency_key="key_123")

        assert captured["method"] == "POST"
        assert captured["url"].endswith("/service/v1/balance/add")
        assert json.loads(captured["body"]) == {"amount_cents": 100, "idempotency_key": "key_123"}
        assert any(v == "Bearer at_xyz" for v in captured["headers"].values())
        assert resp.credit_balance_cents == 1600

    def test_raises_on_http_error(self):
        with _patch_urlopen(_http_error(402, {"error": "card_declined"})):
            with pytest.raises(ServiceApiError, match="failed: 402"):
                add_balance("at_xyz", amount_cents=100, idempotency_key="k")

    def test_raises_on_malformed_response(self):
        with _patch_urlopen({"credit_balance_cents": 10}):
            with pytest.raises(ServiceApiError, match="Unexpected /service/v1/balance/add response"):
                add_balance("at_xyz", amount_cents=100, idempotency_key="k")
