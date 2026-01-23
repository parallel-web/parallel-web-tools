"""Tests for the auth module."""

import json
import os
from unittest import mock

from parallel_web_tools.core.auth import (
    _generate_code_challenge,
    _generate_code_verifier,
    _load_stored_token,
    _save_token,
    get_api_key,
    get_auth_status,
    logout,
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
