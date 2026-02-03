"""Tests for the CLI auto-updater module."""

import json
import time
from unittest import mock


class TestVersionComparison:
    """Tests for version comparison logic."""

    def test_newer_version_detected(self):
        """Should detect when latest version is newer."""
        from parallel_web_tools.cli.updater import _is_newer_version

        assert _is_newer_version("0.0.9", "0.0.8") is True
        assert _is_newer_version("0.1.0", "0.0.9") is True
        assert _is_newer_version("1.0.0", "0.9.9") is True

    def test_same_version_not_newer(self):
        """Should return False when versions are the same."""
        from parallel_web_tools.cli.updater import _is_newer_version

        assert _is_newer_version("0.0.8", "0.0.8") is False
        assert _is_newer_version("1.0.0", "1.0.0") is False

    def test_older_version_not_newer(self):
        """Should return False when latest is older."""
        from parallel_web_tools.cli.updater import _is_newer_version

        assert _is_newer_version("0.0.7", "0.0.8") is False
        assert _is_newer_version("0.9.0", "1.0.0") is False

    def test_prerelease_versions(self):
        """Should handle prerelease versions correctly."""
        from parallel_web_tools.cli.updater import _is_newer_version

        assert _is_newer_version("0.0.9", "0.0.9rc1") is True
        assert _is_newer_version("0.0.9rc2", "0.0.9rc1") is True
        assert _is_newer_version("0.0.9rc1", "0.0.9") is False

    def test_invalid_versions_fallback_safely(self):
        """Should return False for invalid versions (safe fallback)."""
        from parallel_web_tools.cli.updater import _is_newer_version

        # Invalid versions should return False (safe - don't prompt for update)
        assert _is_newer_version("invalid", "0.0.8") is False
        # Same strings should return False
        assert _is_newer_version("same", "same") is False


class TestConfigManagement:
    """Tests for config file management."""

    def test_load_json_file_returns_empty_dict_for_missing_file(self, tmp_path):
        """Should return empty dict when file doesn't exist."""
        from parallel_web_tools.cli.updater import _load_json_file

        result = _load_json_file(tmp_path / "nonexistent.json")
        assert result == {}

    def test_load_json_file_returns_empty_dict_for_invalid_json(self, tmp_path):
        """Should return empty dict when file contains invalid JSON."""
        from parallel_web_tools.cli.updater import _load_json_file

        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("not valid json {{{")
        result = _load_json_file(invalid_file)
        assert result == {}

    def test_load_json_file_returns_content(self, tmp_path):
        """Should return parsed JSON content."""
        from parallel_web_tools.cli.updater import _load_json_file

        valid_file = tmp_path / "valid.json"
        valid_file.write_text('{"key": "value", "num": 42}')
        result = _load_json_file(valid_file)
        assert result == {"key": "value", "num": 42}

    def test_save_json_file_creates_file(self, tmp_path):
        """Should save JSON content to file."""
        from parallel_web_tools.cli import updater

        test_file = tmp_path / "test.json"
        with mock.patch.object(updater, "CONFIG_DIR", tmp_path):
            updater._save_json_file(test_file, {"test": True, "num": 42})

        assert test_file.exists()
        content = json.loads(test_file.read_text())
        assert content == {"test": True, "num": 42}

    def test_auto_update_check_defaults_to_true(self, tmp_path):
        """Auto-update check should default to True when no config exists."""
        from parallel_web_tools.cli import updater

        with mock.patch.object(updater, "CONFIG_FILE", tmp_path / "config.json"):
            assert updater.is_auto_update_check_enabled() is True

    def test_set_and_get_auto_update_check(self, tmp_path):
        """Should be able to set and get auto_update_check setting."""
        from parallel_web_tools.cli import updater

        config_file = tmp_path / "config.json"
        with mock.patch.object(updater, "CONFIG_FILE", config_file):
            with mock.patch.object(updater, "CONFIG_DIR", tmp_path):
                # Initially True (default)
                assert updater.is_auto_update_check_enabled() is True

                # Set to False
                updater.set_auto_update_check(False)
                assert updater.is_auto_update_check_enabled() is False

                # Set back to True
                updater.set_auto_update_check(True)
                assert updater.is_auto_update_check_enabled() is True


class TestShouldCheckForUpdates:
    """Tests for the should_check_for_updates logic."""

    def test_returns_false_when_not_standalone(self):
        """Should return False when not running as standalone binary."""
        from parallel_web_tools.cli import updater

        # sys.frozen is not set in normal Python execution
        assert updater.should_check_for_updates() is False

    def test_returns_false_when_auto_update_disabled(self, tmp_path):
        """Should return False when auto-update check is disabled."""
        from parallel_web_tools.cli import updater

        with mock.patch.object(updater, "CONFIG_FILE", tmp_path / "config.json"):
            with mock.patch.object(updater, "CONFIG_DIR", tmp_path):
                with mock.patch("sys.frozen", True, create=True):
                    updater.set_auto_update_check(False)
                    assert updater.should_check_for_updates() is False

    def test_returns_false_when_checked_recently(self, tmp_path):
        """Should return False when last check was recent."""
        from parallel_web_tools.cli import updater

        state_file = tmp_path / "update-state.json"
        state_file.write_text(json.dumps({"last_check": time.time()}))

        with mock.patch.object(updater, "UPDATE_STATE_FILE", state_file):
            with mock.patch.object(updater, "CONFIG_FILE", tmp_path / "config.json"):
                with mock.patch("sys.frozen", True, create=True):
                    assert updater.should_check_for_updates() is False

    def test_returns_true_when_check_interval_passed(self, tmp_path):
        """Should return True when enough time has passed since last check."""
        from parallel_web_tools.cli import updater

        # Set last check to more than 24 hours ago
        old_time = time.time() - (updater.UPDATE_CHECK_INTERVAL + 100)
        state_file = tmp_path / "update-state.json"
        state_file.write_text(json.dumps({"last_check": old_time}))

        with mock.patch.object(updater, "UPDATE_STATE_FILE", state_file):
            with mock.patch.object(updater, "CONFIG_FILE", tmp_path / "config.json"):
                with mock.patch.object(updater, "CONFIG_DIR", tmp_path):
                    with mock.patch("sys.frozen", True, create=True):
                        assert updater.should_check_for_updates() is True


class TestGetPlatform:
    """Tests for platform detection."""

    def test_darwin_arm64(self):
        """Should detect macOS ARM64."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                assert get_platform() == "darwin-arm64"

    def test_darwin_x64(self):
        """Should detect macOS x64."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="x86_64"):
                assert get_platform() == "darwin-x64"

    def test_linux_x64(self):
        """Should detect Linux x64."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="x86_64"):
                assert get_platform() == "linux-x64"

    def test_linux_arm64(self):
        """Should detect Linux ARM64."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="aarch64"):
                assert get_platform() == "linux-arm64"

    def test_linux_arm64_alternate(self):
        """Should detect Linux ARM64 with arm64 machine string."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="arm64"):
                assert get_platform() == "linux-arm64"

    def test_windows_x64(self):
        """Should detect Windows x64."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="Windows"):
            with mock.patch("platform.machine", return_value="AMD64"):
                assert get_platform() == "windows-x64"

    def test_unsupported_platform_returns_none(self):
        """Should return None for unsupported platforms."""
        from parallel_web_tools.cli.updater import get_platform

        with mock.patch("platform.system", return_value="FreeBSD"):
            with mock.patch("platform.machine", return_value="amd64"):
                assert get_platform() is None


class TestCheckForUpdateNotification:
    """Tests for update notification checking."""

    def test_returns_notification_when_update_available(self):
        """Should return notification string when update is available."""
        from parallel_web_tools.cli import updater

        mock_release = {"tag_name": "v0.0.9", "assets": []}

        with mock.patch.object(updater, "_fetch_latest_release", return_value=mock_release):
            with mock.patch.object(updater, "_save_json_file"):
                result = updater.check_for_update_notification("0.0.8")
                assert result is not None
                assert "0.0.8" in result
                assert "0.0.9" in result
                assert "parallel-cli update" in result

    def test_returns_none_when_up_to_date(self):
        """Should return None when already at latest version."""
        from parallel_web_tools.cli import updater

        mock_release = {"tag_name": "v0.0.8", "assets": []}

        with mock.patch.object(updater, "_fetch_latest_release", return_value=mock_release):
            with mock.patch.object(updater, "_save_json_file"):
                result = updater.check_for_update_notification("0.0.8")
                assert result is None

    def test_returns_none_on_network_error(self):
        """Should return None when GitHub API call fails."""
        from parallel_web_tools.cli import updater

        with mock.patch.object(updater, "_fetch_latest_release", return_value=None):
            with mock.patch.object(updater, "_save_json_file"):
                result = updater.check_for_update_notification("0.0.8")
                assert result is None

    def test_saves_state_by_default(self):
        """Should save state (last_check timestamp) by default."""
        from parallel_web_tools.cli import updater

        mock_release = {"tag_name": "v0.0.8", "assets": []}

        with mock.patch.object(updater, "_fetch_latest_release", return_value=mock_release):
            with mock.patch.object(updater, "_save_json_file") as mock_save:
                updater.check_for_update_notification("0.0.8", save_state=True)
                mock_save.assert_called_once()

    def test_does_not_save_state_when_disabled(self):
        """Should not save state when save_state=False."""
        from parallel_web_tools.cli import updater

        mock_release = {"tag_name": "v0.0.8", "assets": []}

        with mock.patch.object(updater, "_fetch_latest_release", return_value=mock_release):
            with mock.patch.object(updater, "_save_json_file") as mock_save:
                updater.check_for_update_notification("0.0.8", save_state=False)
                mock_save.assert_not_called()


class TestDownloadAndInstallUpdate:
    """Tests for the download and install update functionality."""

    def test_sets_executable_permission_on_unix(self, tmp_path):
        """Should set executable permission on the binary after update on Unix."""
        import stat
        import zipfile

        from parallel_web_tools.cli import updater

        # Create a mock install directory with a non-executable "parallel-cli" file
        install_dir = tmp_path / "install"
        install_dir.mkdir()
        exe_file = install_dir / "parallel-cli"
        exe_file.write_text("#!/bin/bash\necho 'test'")
        # Remove executable permissions
        exe_file.chmod(0o644)

        # Verify it's not executable initially
        assert not (exe_file.stat().st_mode & stat.S_IXUSR)

        # Create a mock zip archive with a parallel-cli folder structure
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()
        cli_dir = archive_dir / "parallel-cli"
        cli_dir.mkdir()
        new_exe = cli_dir / "parallel-cli"
        new_exe.write_text("#!/bin/bash\necho 'updated'")
        # zipfile doesn't preserve permissions, so this will be non-executable
        new_exe.chmod(0o644)

        zip_path = tmp_path / "parallel-cli-darwin-arm64.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(new_exe, "parallel-cli/parallel-cli")

        # Create mock release data
        mock_release = {
            "tag_name": "v99.0.0",
            "assets": [{"name": "parallel-cli-darwin-arm64.zip", "browser_download_url": f"file://{zip_path}"}],
        }

        # Mock the console
        class MockConsole:
            def print(self, *args, **kwargs):
                pass

        # Mock httpx to return our local zip file
        class MockResponse:
            def __init__(self, path):
                self.path = path
                self._content = open(path, "rb").read()

            def raise_for_status(self):
                pass

            def iter_bytes(self):
                yield self._content

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        def mock_stream(method, url, **kwargs):
            return MockResponse(zip_path)

        with mock.patch.object(updater, "_fetch_latest_release", return_value=mock_release):
            with mock.patch.object(updater, "get_platform", return_value="darwin-arm64"):
                with mock.patch("sys.executable", str(exe_file)):
                    with mock.patch("platform.system", return_value="Darwin"):
                        with mock.patch("httpx.stream", mock_stream):
                            result = updater.download_and_install_update("0.0.1", MockConsole(), force=False)

        # The update should succeed
        assert result is True

        # The executable should now have the executable bit set
        mode = exe_file.stat().st_mode
        assert mode & stat.S_IXUSR, "User execute bit should be set"
        assert mode & stat.S_IXGRP, "Group execute bit should be set"
        assert mode & stat.S_IXOTH, "Other execute bit should be set"
