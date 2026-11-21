"""Smoke-test the installed CLI and its Search and Extract API requests."""

import argparse
import json
import os
import subprocess
import sysconfig
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import urlsplit

import click

URL = "https://example.com/"
QUERY = "IANA example domains"
FIXTURE_KEY = "cli-smoke-fixture"
FIXTURE_CONTENT = "Example Domain (CLI smoke fixture)"
SMOKE_COMMANDS = {
    "search": ("--query", QUERY, "--mode", "fast", "--max-results", "1"),
    "extract": (URL, "--full-content"),
}


class ApiFixture(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        command = self.path.removeprefix("/v1/")
        if self.path != f"/v1/{command}" or command not in SMOKE_COMMANDS:
            self.send_error(404)
            return
        if self.headers.get("x-api-key") != FIXTURE_KEY:
            self.send_error(401)
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        settings = request.get("advanced_settings", {})
        if command == "search":
            valid = request.get("search_queries") == [QUERY] and request.get("mode") == "fast"
            valid = valid and settings.get("max_results") == 1
            extra = {}
        else:
            valid = request.get("urls") == [URL] and settings.get("full_content") is True
            extra = {"full_content": FIXTURE_CONTENT}
        if not valid:
            self.send_error(400)
            return

        body = json.dumps(
            {
                f"{command}_id": f"{command}_smoke",
                "session_id": "session_smoke",
                "results": [{"url": URL, "title": "Example Domain", "excerpts": [FIXTURE_CONTENT], **extra}],
                "errors": [],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def local_api():
    with HTTPServer(("127.0.0.1", 0), ApiFixture) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            worker.join()


def invoke_cli(cli: Path, args: tuple[str, ...], env: dict[str, str]) -> str:
    label = " ".join(("parallel-cli", *args))
    with TemporaryDirectory(prefix="parallel-cli-smoke-") as tmp:
        try:
            result = subprocess.run(
                [str(cli), *args],
                env={**env, "HOME": tmp, "USERPROFILE": tmp},
                cwd=tmp,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            raise SystemExit(f"{label} timed out after 60s.") from None
    if result.returncode:
        raise SystemExit(f"{label} exited with status {result.returncode}.")
    return result.stdout


def command_paths(command: click.Command, path: tuple[str, ...] = ()) -> Iterator[tuple[str, ...]]:
    yield path
    if isinstance(command, click.Group):
        with click.Context(command) as ctx:
            for name in command.list_commands(ctx):
                child = command.get_command(ctx, name)
                if child is None:
                    raise SystemExit(f"Could not resolve registered command: {' '.join((*path, name))}")
                yield from command_paths(child, (*path, name))


def check_help(cli: Path, env: dict[str, str]) -> None:
    from parallel_web_tools.cli import main

    paths = list(command_paths(main))
    for path in paths:
        if "Usage:" not in invoke_cli(cli, (*path, "--help"), env):
            raise SystemExit(f"No usage text returned for {' '.join(('parallel-cli', *path, '--help'))}.")
    print(f"parallel-cli help passed ({len(paths)} command paths).", flush=True)


def check_api(cli: Path, env: dict[str, str], command: str, *, fixture: bool) -> None:
    stdout = invoke_cli(cli, (command, *SMOKE_COMMANDS[command], "--json"), env)
    try:
        payload = json.loads(stdout)
        [page] = payload["results"]
        request_id = payload[f"{command}_id"]
        valid = payload["status"] == "ok" and isinstance(request_id, str) and bool(request_id)
        if fixture:
            valid = valid and request_id == f"{command}_smoke"
        if command == "search":
            result_url = urlsplit(page["url"])
            valid = valid and result_url.scheme in {"http", "https"} and bool(result_url.hostname)
            if fixture:
                valid = valid and page["url"] == URL and page["excerpts"] == [FIXTURE_CONTENT]
        else:
            content = page["full_content"]
            valid = (
                valid
                and payload["errors"] == []
                and page["url"].rstrip("/") == URL.rstrip("/")
                and isinstance(content, str)
                and bool(content.strip())
                and (not fixture or content == FIXTURE_CONTENT)
            )
    except (AttributeError, KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        raise SystemExit(f"parallel-cli {command} did not return the expected successful JSON result.")
    print(f"parallel-cli {command} smoke passed ({'loopback HTTP' if fixture else 'live API'}).", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live", action="store_true", help="Run Search and Extract using PARALLEL_API_KEY (may bill usage)."
    )
    live = parser.parse_args().live
    env = os.environ.copy()
    cli = Path(sysconfig.get_path("scripts")) / ("parallel-cli.exe" if os.name == "nt" else "parallel-cli")
    if not cli.is_file():
        raise SystemExit("parallel-cli is not installed in this Python environment. Run with uv.")
    if live:
        if not env.get("PARALLEL_API_KEY", "").strip():
            parser.error("--live requires PARALLEL_API_KEY from a test account")
        for command in SMOKE_COMMANDS:
            check_api(cli, env, command, fixture=False)
    else:
        with local_api() as endpoint:
            env.update(
                PARALLEL_API_URL=endpoint,
                PARALLEL_PLATFORM_URL=endpoint,
                PARALLEL_SERVICE_API_URL=endpoint,
                PARALLEL_API_KEY=FIXTURE_KEY,
                NO_PROXY="127.0.0.1",
                no_proxy="127.0.0.1",
            )
            check_help(cli, env)
            for command in SMOKE_COMMANDS:
                check_api(cli, env, command, fixture=True)


if __name__ == "__main__":
    main()
