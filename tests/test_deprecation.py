"""Tests for API deprecation detection and the CLI's reaction to it."""

import json
from unittest import mock

import httpx
import pytest
from click.testing import CliRunner

from parallel_web_tools.cli import updater
from parallel_web_tools.core import deprecation
from parallel_web_tools.core.deprecation import DeprecationNotice

DEPRECATED_HEADERS = {
    "Deprecation": "@1795464000",
    "Link": '<https://docs.parallel.ai/search/search-migration-guide>; rel="deprecation", '
    '<https://api.parallel.ai/v1/search>; rel="successor-version"',
}


@pytest.fixture(autouse=True)
def _clear_notices():
    deprecation.clear_notices()
    yield
    deprecation.clear_notices()


def _response(headers, path="/v1beta/search"):
    request = httpx.Request("POST", f"https://api.parallel.ai{path}")
    return httpx.Response(200, headers=headers, json={}, request=request)


class TestHeaderDetection:
    def test_records_notice_with_links(self):
        deprecation.record_response(_response(DEPRECATED_HEADERS))

        assert deprecation.get_notices() == [
            DeprecationNotice(
                endpoint="POST /v1beta/search",
                link="https://docs.parallel.ai/search/search-migration-guide",
                successor="https://api.parallel.ai/v1/search",
            )
        ]

    def test_ignores_responses_without_deprecation_header(self):
        deprecation.record_response(_response({"Link": '<https://x>; rel="next"'}))
        assert deprecation.get_notices() == []

    def test_deduplicates_repeated_calls(self):
        deprecation.record_response(_response(DEPRECATED_HEADERS))
        deprecation.record_response(_response(DEPRECATED_HEADERS))
        assert len(deprecation.get_notices()) == 1

    def test_reads_sunset(self):
        deprecation.record_response(_response({"Deprecation": "@1", "Sunset": "Wed, 01 Jul 2026 00:00:00 GMT"}))
        assert deprecation.get_notices()[0].sunset == "Wed, 01 Jul 2026 00:00:00 GMT"

    @pytest.mark.parametrize("api_key", ["test-key", None], ids=["api-key", "oauth"])
    def test_cli_clients_install_response_hooks(self, api_key):
        from parallel_web_tools.core import auth

        with mock.patch.object(auth, "_resolve_client_auth", return_value=(api_key, "https://api.parallel.ai")):
            client = auth.create_client(source="cli")
            async_client = auth.get_async_client(source="cli")

        assert deprecation.record_response in client._client.event_hooks["response"]
        assert deprecation.record_response_async in async_client._client.event_hooks["response"]

    def test_non_cli_clients_do_not_record(self):
        from parallel_web_tools.core import auth

        assert auth._http_client("python") is None
        assert auth._http_client("cli") is not None


class TestBodyWarnings:
    def test_records_deprecation_warning_detail(self):
        deprecation.record_body_warnings(
            [
                {"type": "spec_validation_warning", "message": "ignored"},
                {
                    "type": "deprecation_warning",
                    "message": "This endpoint is deprecated.",
                    "detail": {
                        "replacement_endpoint": "/v1/search",
                        "documentation": "https://docs.parallel.ai/search/search-migration-guide",
                    },
                },
            ],
            endpoint="search",
        )

        assert deprecation.get_notices() == [
            DeprecationNotice(
                endpoint="search",
                message="This endpoint is deprecated.",
                link="https://docs.parallel.ai/search/search-migration-guide",
                successor="/v1/search",
            )
        ]

    def test_accepts_sdk_objects(self):
        warning = mock.Mock(type="deprecation_warning", message="Deprecated.", detail=None)
        deprecation.record_body_warnings([warning])
        assert deprecation.get_notices()[0].message == "Deprecated."


class TestInstallMethod:
    @pytest.mark.parametrize(
        ("exe", "frozen", "expected"),
        [
            ("/home/u/.local/share/parallel-cli/parallel-cli", True, "standalone"),
            ("/opt/homebrew/Caskroom/parallel-cli/0.9.4/parallel-cli/parallel-cli", True, "homebrew"),
            ("/usr/lib/node_modules/parallel-web-cli/bin/parallel-cli/parallel-cli", True, "npm"),
            ("/home/u/.local/share/uv/tools/parallel-web-tools/bin/python", False, "uv-tool"),
            ("/home/u/venv/bin/python", False, "pip"),
        ],
    )
    def test_detects_method(self, exe, frozen, expected):
        with mock.patch.object(updater.Path, "resolve", lambda self: self):
            assert updater.detect_install_method(exe, frozen=frozen) == expected


class _Console:
    def __init__(self):
        self.lines: list[str] = []

    def print(self, *args, **kwargs):
        self.lines.append(" ".join(str(a) for a in args))

    @property
    def text(self):
        return "\n".join(self.lines)


NOTICE = DeprecationNotice(endpoint="POST /v1beta/search", successor="https://api.parallel.ai/v1/search")


@pytest.fixture
def state_dir(tmp_path):
    with (
        mock.patch.object(updater, "CONFIG_DIR", tmp_path),
        mock.patch.object(updater, "CONFIG_FILE", tmp_path / "config.json"),
        mock.patch.object(updater, "UPDATE_STATE_FILE", tmp_path / "update-state.json"),
        mock.patch.object(updater, "_updated_this_process", False),
    ):
        yield tmp_path


class TestHandleDeprecationNotices:
    def test_no_notices_does_nothing(self, state_dir):
        with mock.patch.object(updater, "_fetch_latest_release") as fetch:
            assert updater.handle_deprecation_notices("0.9.4", [], _Console()) is None
        fetch.assert_not_called()

    def test_standalone_auto_updates_when_newer_release(self, state_dir):
        console = _Console()
        with (
            mock.patch.object(updater, "detect_install_method", return_value="standalone"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.5"}),
            mock.patch.object(updater, "download_and_install_update", return_value=True) as install,
        ):
            assert updater.handle_deprecation_notices("0.9.4", [NOTICE], console) == "updated"
        install.assert_called_once()
        assert "Replacement: https://api.parallel.ai/v1/search" in console.text

    def test_standalone_update_failure_tells_caller(self, state_dir):
        console = _Console()
        with (
            mock.patch.object(updater, "detect_install_method", return_value="standalone"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.5"}),
            mock.patch.object(updater, "download_and_install_update", return_value=False),
        ):
            assert updater.handle_deprecation_notices("0.9.4", [NOTICE], console) == "notified"
        assert "UPDATE REQUIRED" in console.text
        assert "parallel-cli update" in console.text

    def test_respects_disabled_auto_update(self, state_dir):
        updater.set_auto_update_check(False)
        with (
            mock.patch.object(updater, "detect_install_method", return_value="standalone"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.5"}),
            mock.patch.object(updater, "download_and_install_update") as install,
        ):
            assert updater.handle_deprecation_notices("0.9.4", [NOTICE], _Console()) == "notified"
        install.assert_not_called()

    @pytest.mark.parametrize(
        ("method", "command"),
        [
            ("pip", "pip install --upgrade parallel-web-tools"),
            ("uv-tool", "uv tool upgrade parallel-web-tools"),
            ("npm", "npm install -g parallel-web-cli@latest"),
            ("homebrew", "brew upgrade --cask parallel-cli"),
        ],
    )
    def test_package_manager_installs_get_update_command(self, state_dir, method, command):
        console = _Console()
        with (
            mock.patch.object(updater, "detect_install_method", return_value=method),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.5"}),
            mock.patch.object(updater, "download_and_install_update") as install,
        ):
            assert updater.handle_deprecation_notices("0.9.4", [NOTICE], console) == "notified"
        install.assert_not_called()
        assert f"v0.9.5 is available. Update with: {command}" in console.text

    def test_no_newer_release(self, state_dir):
        console = _Console()
        with (
            mock.patch.object(updater, "detect_install_method", return_value="standalone"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.4"}),
            mock.patch.object(updater, "download_and_install_update") as install,
        ):
            assert updater.handle_deprecation_notices("0.9.4", [NOTICE], console) == "notified"
        install.assert_not_called()
        assert "when a newer release is available" in console.text

    def test_release_check_is_rate_limited(self, state_dir):
        with (
            mock.patch.object(updater, "detect_install_method", return_value="pip"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v0.9.5"}) as fetch,
        ):
            updater.handle_deprecation_notices("0.9.4", [NOTICE], _Console())
            console = _Console()
            updater.handle_deprecation_notices("0.9.4", [NOTICE], console)
        assert fetch.call_count == 1
        assert "v0.9.5 is available" in console.text
        assert json.loads((state_dir / "update-state.json").read_text())["latest_version"] == "0.9.5"


class TestCliIntegration:
    def _search_result(self, warnings):
        result = mock.Mock()
        result.search_id = "search_1"
        result.session_id = None
        result.results = []
        result.usage = []
        result.warnings = warnings
        return result

    def test_search_json_keeps_stdout_clean_and_warns_on_stderr(self, state_dir):
        from parallel_web_tools.cli.commands import main

        warning = mock.Mock(
            type="deprecation_warning",
            message="This endpoint is deprecated.",
            detail={"replacement_endpoint": "/v1/search", "documentation": "https://docs.parallel.ai/x"},
        )
        client = mock.Mock()
        client.search.return_value = self._search_result([warning])

        runner = CliRunner()
        with (
            mock.patch("parallel_web_tools.core.auth.get_client", return_value=client),
            mock.patch.object(updater, "detect_install_method", return_value="pip"),
            mock.patch.object(updater, "_fetch_latest_release", return_value={"tag_name": "v99.0.0"}),
        ):
            result = runner.invoke(main, ["search", "q", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["warnings"][0]["type"] == "deprecation_warning"
        assert "UPDATE REQUIRED" in result.stderr
        assert "pip install --upgrade parallel-web-tools" in result.stderr

    def test_no_notice_when_not_deprecated(self, state_dir):
        from parallel_web_tools.cli.commands import main

        client = mock.Mock()
        client.search.return_value = self._search_result([])

        runner = CliRunner()
        with (
            mock.patch("parallel_web_tools.core.auth.get_client", return_value=client),
            mock.patch.object(updater, "_fetch_latest_release") as fetch,
        ):
            result = runner.invoke(main, ["search", "q", "--json"])

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.stderr
        fetch.assert_not_called()
