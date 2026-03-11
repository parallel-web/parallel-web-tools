"""Tests for the auth module."""

import json
import os
import urllib.error
from unittest import mock

import pytest

from parallel_web_tools.core.auth import (
    DeviceCodeInfo,
    _do_device_flow,
    _generate_code_challenge,
    _generate_code_verifier,
    _is_headless,
    _load_stored_token,
    _save_token,
    create_client,
    get_api_key,
    get_auth_status,
    logout,
    poll_device_token,
    request_device_code,
    resolve_api_key,
)


class TestPKCE:
    """Tests for PKCE code generation."""

    def test_generate_code_verifier_length(self):
        """Code verifier should be URL-safe base64."""
        verifier = _generate_code_verifier()
        assert len(verifier) >= 43  # Base64 encoded 32 bytes
        assert verifier.replace("-", "").replace("_", "").isalnum()

    def test_generate_code_verifier_unique(self):
        """Each code verifier should be unique."""
        verifiers = [_generate_code_verifier() for _ in range(10)]
        assert len(set(verifiers)) == 10

    def test_generate_code_challenge(self):
        """Code challenge should be SHA256 of verifier, base64 encoded."""
        verifier = "test_verifier_12345"
        challenge = _generate_code_challenge(verifier)

        # Challenge should be URL-safe base64 without padding
        assert "=" not in challenge
        assert challenge.replace("-", "").replace("_", "").isalnum()


class TestTokenStorage:
    """Tests for token storage functions."""

    def test_save_and_load_token(self, tmp_path):
        """Token should be saveable and loadable."""
        test_token = "test_token_12345"
        token_file = tmp_path / "tokens.json"

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            _save_token(test_token)

            # File should exist with correct permissions
            assert token_file.exists()
            assert oct(token_file.stat().st_mode)[-3:] == "600"

            # Token should be loadable
            loaded = _load_stored_token()
            assert loaded == test_token

    def test_load_nonexistent_token(self, tmp_path):
        """Loading from nonexistent file should return None."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            loaded = _load_stored_token()
            assert loaded is None

    def test_load_corrupted_token(self, tmp_path):
        """Loading corrupted JSON should return None."""
        token_file = tmp_path / "corrupted.json"
        token_file.write_text("not valid json {{{")

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            loaded = _load_stored_token()
            assert loaded is None


class TestGetApiKey:
    """Tests for get_api_key function."""

    def test_env_var_priority(self, tmp_path):
        """Environment variable should take priority."""
        env_key = "test_env_key_12345"
        token_file = tmp_path / "tokens.json"

        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": env_key}):
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                result = get_api_key()
                assert result == env_key

    def test_stored_token_second_priority(self, tmp_path):
        """Stored token should be used if no env var."""
        stored_token = "stored_token_12345"
        token_file = tmp_path / "tokens.json"
        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text(json.dumps({"access_token": stored_token}))

        with mock.patch.dict(os.environ, {}, clear=True):
            # Remove PARALLEL_API_KEY if it exists
            os.environ.pop("PARALLEL_API_KEY", None)

            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                result = get_api_key()
                assert result == stored_token

    def test_force_login_ignores_env_var(self, tmp_path):
        """force_login should skip env var and stored token."""
        env_key = "test_env_key"
        token_file = tmp_path / "tokens.json"

        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": env_key}):
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with mock.patch("parallel_web_tools.core.auth._do_oauth_flow") as mock_oauth:
                    mock_oauth.return_value = "new_oauth_token"

                    result = get_api_key(force_login=True)

                    assert result == "new_oauth_token"
                    mock_oauth.assert_called_once()


class TestAuthStatus:
    """Tests for get_auth_status function."""

    def test_status_with_env_var(self):
        """Status should show environment method when env var set."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "test_key"}):
            status = get_auth_status()
            assert status["authenticated"] is True
            assert status["method"] == "environment"

    def test_status_with_stored_token(self, tmp_path):
        """Status should show oauth method when token stored."""
        token_file = tmp_path / "tokens.json"
        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text(json.dumps({"access_token": "stored_token"}))

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)

            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                status = get_auth_status()
                assert status["authenticated"] is True
                assert status["method"] == "oauth"
                assert status["token_file"] == str(token_file)

    def test_status_not_authenticated(self, tmp_path):
        """Status should show not authenticated when nothing configured."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)

            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                status = get_auth_status()
                assert status["authenticated"] is False
                assert status["method"] is None


class TestLogout:
    """Tests for logout function."""

    def test_logout_removes_token(self, tmp_path):
        """Logout should remove stored token file."""
        token_file = tmp_path / "tokens.json"
        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text(json.dumps({"access_token": "test"}))

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            result = logout()
            assert result is True
            assert not token_file.exists()

    def test_logout_no_token(self, tmp_path):
        """Logout should return False if no token exists."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
            result = logout()
            assert result is False


class TestCreateClient:
    """Tests for create_client function."""

    def test_creates_client_with_explicit_key(self):
        """Should create Parallel client with explicit API key."""
        with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
            create_client(api_key="test-key-123", source="cli")

            mock_parallel.assert_called_once()
            call_kwargs = mock_parallel.call_args.kwargs
            assert call_kwargs["api_key"] == "test-key-123"
            assert "User-Agent" in call_kwargs["default_headers"]
            assert "(cli)" in call_kwargs["default_headers"]["User-Agent"]

    def test_creates_client_with_env_key(self):
        """Should resolve API key from environment when not explicit."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "env-key"}):
            with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
                create_client(source="duckdb")

                call_kwargs = mock_parallel.call_args.kwargs
                assert call_kwargs["api_key"] == "env-key"

    def test_raises_without_key(self, tmp_path):
        """Should raise ValueError when no API key is available."""
        token_file = tmp_path / "nonexistent.json"

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with pytest.raises(ValueError, match="Parallel API key required"):
                    create_client()

    def test_default_source_is_python(self):
        """Should default to python source."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "test-key"}):
            with mock.patch("parallel_web_tools.core.auth.Parallel") as mock_parallel:
                create_client()

                call_kwargs = mock_parallel.call_args.kwargs
                assert "(python)" in call_kwargs["default_headers"]["User-Agent"]


class TestResolveApiKeyInAuth:
    """Additional tests for resolve_api_key edge cases."""

    def test_empty_string_key_is_falsy(self):
        """Empty string api_key should fall through to env var."""
        with mock.patch.dict(os.environ, {"PARALLEL_API_KEY": "env-key"}):
            result = resolve_api_key(api_key="")
            assert result == "env-key"

    def test_stored_token_used_as_fallback(self, tmp_path):
        """Should use stored OAuth token when no env var."""
        token_file = tmp_path / "creds.json"
        token_file.write_text(json.dumps({"access_token": "stored-token"}))

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                result = resolve_api_key()
                assert result == "stored-token"


class TestIsHeadless:
    """Tests for headless environment detection."""

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
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("os.path.exists", return_value=False):
                assert _is_headless() is False


def _make_http_error(status, body):
    """Helper to create a urllib HTTPError with a JSON body."""
    import io
    from email.message import Message

    resp = io.BytesIO(json.dumps(body).encode())
    return urllib.error.HTTPError(
        url="https://example.com",
        code=status,
        msg="Bad Request",
        hdrs=Message(),
        fp=resp,
    )


SAMPLE_DEVICE_CODE_INFO = DeviceCodeInfo(
    device_code="a" * 48,
    user_code="BCDF-GHJK",
    verification_uri="https://platform.parallel.ai/getKeys/device",
    verification_uri_complete="https://platform.parallel.ai/getKeys/device?user_code=BCDF-GHJK",
    expires_in=600,
    interval=5,
)


def _mock_urlopen_sequence(responses):
    """Create a mock urlopen that returns a sequence of responses.

    Each response is either a dict (success) or an HTTPError (error).
    """
    import io
    from contextlib import contextmanager

    call_count = 0

    @contextmanager
    def mock_urlopen(req, timeout=None):
        nonlocal call_count
        idx = min(call_count, len(responses) - 1)
        resp = responses[idx]
        call_count += 1

        if isinstance(resp, urllib.error.HTTPError):
            raise resp

        body = json.dumps(resp).encode()
        fp = io.BytesIO(body)
        yield fp

    return mock_urlopen


class TestRequestDeviceCode:
    """Tests for the request_device_code public function."""

    DEVICE_RESPONSE = {
        "device_code": "a" * 48,
        "user_code": "BCDF-GHJK",
        "verification_uri": "https://platform.parallel.ai/getKeys/device",
        "verification_uri_complete": "https://platform.parallel.ai/getKeys/device?user_code=BCDF-GHJK",
        "expires_in": 600,
        "interval": 5,
    }

    def test_returns_device_code_info(self):
        """Should return a DeviceCodeInfo dataclass."""
        mock_urlopen = _mock_urlopen_sequence([self.DEVICE_RESPONSE])

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            info = request_device_code()

        assert isinstance(info, DeviceCodeInfo)
        assert info.device_code == "a" * 48
        assert info.user_code == "BCDF-GHJK"
        assert info.verification_uri == "https://platform.parallel.ai/getKeys/device"
        assert info.expires_in == 600
        assert info.interval == 5

    def test_raises_on_http_error(self):
        """Should raise on server error."""
        error = _make_http_error(500, {"error": "internal"})
        mock_urlopen = _mock_urlopen_sequence([error])

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            with pytest.raises(Exception, match="Device code request failed"):
                request_device_code()


class TestPollDeviceToken:
    """Tests for the poll_device_token public function."""

    TOKEN_RESPONSE = {
        "access_token": "test-api-key-from-device",
        "token_type": "bearer",
        "scope": "key:read",
    }

    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_returns_token_on_success(self, mock_sleep):
        """Should return access token when approved."""
        mock_urlopen = _mock_urlopen_sequence([self.TOKEN_RESPONSE])

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            token = poll_device_token(SAMPLE_DEVICE_CODE_INFO)

        assert token == "test-api-key-from-device"

    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_polls_through_pending(self, mock_sleep):
        """Should keep polling on authorization_pending."""
        mock_urlopen = _mock_urlopen_sequence(
            [
                _make_http_error(400, {"error": "authorization_pending"}),
                _make_http_error(400, {"error": "authorization_pending"}),
                self.TOKEN_RESPONSE,
            ]
        )

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            token = poll_device_token(SAMPLE_DEVICE_CODE_INFO)

        assert token == "test-api-key-from-device"
        assert mock_sleep.call_count == 3

    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_slow_down_increases_interval(self, mock_sleep):
        """slow_down should increase polling interval by 5 seconds."""
        mock_urlopen = _mock_urlopen_sequence(
            [
                _make_http_error(400, {"error": "slow_down"}),
                self.TOKEN_RESPONSE,
            ]
        )

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            poll_device_token(SAMPLE_DEVICE_CODE_INFO)

        assert mock_sleep.call_args_list[0] == mock.call(5)
        assert mock_sleep.call_args_list[1] == mock.call(10)

    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_raises_on_access_denied(self, mock_sleep):
        mock_urlopen = _mock_urlopen_sequence(
            [
                _make_http_error(400, {"error": "access_denied"}),
            ]
        )

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            with pytest.raises(Exception, match="Authorization denied"):
                poll_device_token(SAMPLE_DEVICE_CODE_INFO)

    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_raises_on_expired_token(self, mock_sleep):
        mock_urlopen = _mock_urlopen_sequence(
            [
                _make_http_error(400, {"error": "expired_token"}),
            ]
        )

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            with pytest.raises(Exception, match="expired"):
                poll_device_token(SAMPLE_DEVICE_CODE_INFO)


class TestDoDeviceFlow:
    """Tests for the _do_device_flow convenience wrapper."""

    DEVICE_RESPONSE = {
        "device_code": "a" * 48,
        "user_code": "BCDF-GHJK",
        "verification_uri": "https://platform.parallel.ai/getKeys/device",
        "verification_uri_complete": "https://platform.parallel.ai/getKeys/device?user_code=BCDF-GHJK",
        "expires_in": 600,
        "interval": 5,
    }

    TOKEN_RESPONSE = {
        "access_token": "test-api-key-from-device",
        "token_type": "bearer",
        "scope": "key:read",
    }

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_default_prints_to_stderr(self, mock_sleep, mock_browser_open):
        """Without callback, should print instructions to stderr."""
        mock_urlopen = _mock_urlopen_sequence(
            [
                self.DEVICE_RESPONSE,
                self.TOKEN_RESPONSE,
            ]
        )

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            token = _do_device_flow()

        assert token == "test-api-key-from-device"
        mock_browser_open.assert_called_once()

    @mock.patch("parallel_web_tools.core.auth.webbrowser.open")
    @mock.patch("parallel_web_tools.core.auth.time.sleep")
    def test_callback_receives_device_code_info(self, mock_sleep, mock_browser_open):
        """on_device_code callback should receive DeviceCodeInfo."""
        mock_urlopen = _mock_urlopen_sequence(
            [
                self.DEVICE_RESPONSE,
                self.TOKEN_RESPONSE,
            ]
        )

        captured = []

        with mock.patch("parallel_web_tools.core.auth.urllib.request.urlopen", side_effect=mock_urlopen):
            token = _do_device_flow(on_device_code=lambda info: captured.append(info))

        assert token == "test-api-key-from-device"
        assert len(captured) == 1
        assert isinstance(captured[0], DeviceCodeInfo)
        assert captured[0].user_code == "BCDF-GHJK"
        # Browser should NOT be opened when callback is provided
        mock_browser_open.assert_not_called()


class TestGetApiKeyDeviceFlow:
    """Tests for get_api_key with device flow integration."""

    def test_device_flag_uses_device_flow(self, tmp_path):
        """device=True should use device flow instead of browser OAuth."""
        token_file = tmp_path / "tokens.json"

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with mock.patch("parallel_web_tools.core.auth._do_device_flow") as mock_device:
                    mock_device.return_value = "device-token"

                    result = get_api_key(force_login=True, device=True)

                    assert result == "device-token"
                    mock_device.assert_called_once()

    def test_headless_auto_selects_device_flow(self, tmp_path):
        """Headless environment should auto-select device flow."""
        token_file = tmp_path / "tokens.json"

        with mock.patch.dict(os.environ, {"SSH_CLIENT": "1.2.3.4 54321 22"}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with mock.patch("parallel_web_tools.core.auth._do_device_flow") as mock_device:
                    mock_device.return_value = "ssh-device-token"

                    result = get_api_key(force_login=True)

                    assert result == "ssh-device-token"
                    mock_device.assert_called_once()

    def test_non_headless_uses_browser_flow(self, tmp_path):
        """Non-headless environment should use browser-based OAuth."""
        token_file = tmp_path / "tokens.json"
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("SSH_CLIENT", "SSH_TTY", "CI", "container", "PARALLEL_API_KEY")
        }

        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with mock.patch("os.path.exists", return_value=False):
                    with mock.patch("parallel_web_tools.core.auth._do_oauth_flow") as mock_oauth:
                        mock_oauth.return_value = "browser-token"

                        result = get_api_key(force_login=True)

                        assert result == "browser-token"
                        mock_oauth.assert_called_once()

    def test_on_device_code_callback_passed_through(self, tmp_path):
        """on_device_code callback should be passed to _do_device_flow."""
        token_file = tmp_path / "tokens.json"
        callback = mock.Mock()

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PARALLEL_API_KEY", None)
            with mock.patch("parallel_web_tools.core.auth.TOKEN_FILE", token_file):
                with mock.patch("parallel_web_tools.core.auth._do_device_flow") as mock_device:
                    mock_device.return_value = "callback-token"

                    result = get_api_key(force_login=True, device=True, on_device_code=callback)

                    assert result == "callback-token"
                    mock_device.assert_called_once_with(on_device_code=callback)
