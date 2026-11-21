"""Exercise the installed parallel-cli against fake HTTP or the opted-in live API."""

import argparse
import json
import os
import subprocess
import sysconfig
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any
from urllib.parse import parse_qs, urlsplit

import click
from cli_smoke_cases import (
    EXCLUDED_COMMANDS,
    FIXTURE_KEY,
    HELP_ONLY_COMMANDS,
    RESEARCH_ARGS,
    WEB_ARGS,
    Case,
    Exchange,
    cases,
    research_poll_args,
)


def includes(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and includes(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(includes(item, fixture) for item, fixture in zip(actual, expected, strict=True))
        )
    return actual == expected


class LocalApi(HTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), ApiFixture)
        self.exchanges: deque[Exchange] = deque()
        self.errors: list[str] = []


class ApiFixture(BaseHTTPRequestHandler):
    server: LocalApi

    def handle_request(self) -> None:
        path = urlsplit(self.path)
        if path.netloc:
            self.reject("external HTTP is not allowed in offline CLI checks")
            return
        pending = self.server.exchanges
        expected = pending[0] if pending else None
        route = f"{self.command} {path.path}"
        if expected is None or route != f"{expected.method} {expected.path}":
            self.reject(
                f"unexpected {route}; next request: {expected.method + ' ' + expected.path if expected else 'none'}"
            )
            return
        if expected.auth and self.headers.get(expected.auth[0]) != expected.auth[1]:
            self.reject(f"missing or invalid test auth for {route}")
            return
        if not expected.auth and ("Authorization" in self.headers or "x-api-key" in self.headers):
            self.reject(f"unexpected auth for {route}")
            return
        if expected.query and not includes(parse_qs(path.query), expected.query):
            self.reject(f"incorrect query for {route}")
            return
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if expected.body is not None:
            try:
                payload = json.loads(raw) if "json" in self.headers.get("Content-Type", "") else parse_qs(raw.decode())
            except (UnicodeError, ValueError):
                self.reject(f"invalid request body for {route}")
                return
            if not includes(payload, expected.body):
                self.reject(f"incorrect request body for {route}")
                return

        pending.popleft()
        if isinstance(expected.reply, str):
            body = expected.reply.encode()
            content_type = "text/plain"
        else:
            body = b"" if expected.reply is None else json.dumps(expected.reply).encode()
            content_type = "application/json"
        self.send_response(expected.status)
        self.send_header("Content-Type", expected.content_type or content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PATCH = do_PUT = do_DELETE = handle_request

    def do_CONNECT(self) -> None:
        self.reject("external HTTPS is not allowed in offline CLI checks")

    def reject(self, error: str) -> None:
        self.server.errors.append(error)
        self.send_error(400, "Unexpected CLI smoke request")

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def local_api() -> Iterator[LocalApi]:
    with LocalApi() as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield server
        finally:
            server.shutdown()
            worker.join()


def invoke_cli(cli: Path, args: tuple[str, ...], env: dict[str, str], cwd: Path, *, timeout: int = 60) -> str:
    label = " ".join(("parallel-cli", *args))
    try:
        result = subprocess.run(
            [str(cli), *args],
            env={**env, "HOME": str(cwd), "USERPROFILE": str(cwd)},
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(f"{label} timed out after {timeout}s.") from None
    if result.returncode:
        detail = f"\n{result.stdout}{result.stderr}".rstrip() if env.get("PARALLEL_API_KEY") == FIXTURE_KEY else ""
        raise SystemExit(f"{label} exited with status {result.returncode}.{detail}")
    return result.stdout


def json_result(stdout: str, args: tuple[str, ...], *, lines: bool = False) -> Any:
    try:
        return [json.loads(line) for line in stdout.splitlines()] if lines else json.loads(stdout)
    except ValueError:
        raise SystemExit(f"Expected JSON from parallel-cli {' '.join(args)}.") from None


def run_case(cli: Path, case: Case, env: dict[str, str], server: LocalApi, cwd: Path) -> None:
    server.exchanges.extend(case.exchanges)
    try:
        stdout = invoke_cli(cli, case.args, env, cwd)
    finally:
        if server.errors:
            raise SystemExit(f"{case.name}: {server.errors[0]}") from None
    if server.exchanges:
        missing = server.exchanges[0]
        raise SystemExit(f"{case.name}: missing expected {missing.method} {missing.path}")
    if isinstance(case.expected, str):
        value = stdout
        valid = case.expected in stdout
    else:
        value = json_result(stdout, case.args, lines=case.json_lines)
        valid = includes(value, case.expected)
    if not valid:
        detail = f": {value!r}" if env.get("PARALLEL_API_KEY") == FIXTURE_KEY else ""
        raise SystemExit(f"{case.name}: unexpected fixture output{detail}")
    if case.output_file:
        output = cwd / case.output_file
        if not output.is_file() or output.stat().st_size == 0:
            raise SystemExit(f"{case.name}: missing or empty output file {case.output_file}")
        if case.name == "research poll" and json.loads(output.read_text(encoding="utf-8")) != value:
            raise SystemExit(f"{case.name}: saved JSON does not match stdout")


def command_paths(command: click.Command, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], bool]]:
    yield path, isinstance(command, click.Group)
    if isinstance(command, click.Group):
        with click.Context(command) as ctx:
            for name in command.list_commands(ctx):
                child = command.get_command(ctx, name)
                if child is None:
                    raise SystemExit(f"Could not resolve registered command: {' '.join((*path, name))}")
                yield from command_paths(child, (*path, name))


def check_help(cli: Path, env: dict[str, str], cwd: Path, scenarios: tuple[Case, ...]) -> None:
    from parallel_web_tools.cli import main

    registered = dict(command_paths(main))
    classified = {tuple(case.name.split()) for case in scenarios} | EXCLUDED_COMMANDS | HELP_ONLY_COMMANDS
    if unclassified := {path for path, group in registered.items() if not group} ^ classified:
        raise SystemExit(f"CLI behavior coverage does not match registered commands: {sorted(unclassified)!r}")
    paths = [path for path in registered if path not in EXCLUDED_COMMANDS]
    for path in paths:
        if "Usage:" not in invoke_cli(cli, (*path, "--help"), env, cwd):
            raise SystemExit(f"No usage text returned for {' '.join(('parallel-cli', *path, '--help'))}.")
    print(f"parallel-cli help passed ({len(paths)} command paths).", flush=True)


def live_web(cli: Path, env: dict[str, str], cwd: Path) -> None:
    for command in ("search", "extract"):
        args = (command, *WEB_ARGS[command], "--json")
        result = json_result(invoke_cli(cli, args, env, cwd), args)
        if not isinstance(result, dict):
            raise SystemExit(f"parallel-cli {command} did not return a JSON object.")
        try:
            [page] = result["results"]
            request_id = result[f"{command}_id"]
            valid = result["status"] == "ok" and isinstance(request_id, str) and bool(request_id)
            if command == "search":
                url = urlsplit(page["url"])
                valid = valid and url.scheme in {"http", "https"} and bool(url.hostname)
            else:
                valid = valid and not result["errors"] and bool(page["full_content"].strip())
        except (AttributeError, KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            raise SystemExit(f"parallel-cli {command} did not return the expected successful JSON result.")
        print(f"parallel-cli {command} passed (live API).", flush=True)


def live_research(cli: Path, env: dict[str, str], cwd: Path) -> None:
    def run(args: tuple[str, ...], *, timeout: int = 60) -> Any:
        return json_result(invoke_cli(cli, args, env, cwd, timeout=timeout), args)

    created = run(RESEARCH_ARGS)
    run_id = created.get("run_id") if isinstance(created, dict) else None
    if not isinstance(run_id, str) or not run_id.startswith("trun_"):
        raise SystemExit("Research did not return a task run ID.")
    status = run(("research", "status", run_id, "--json"))
    if (
        not isinstance(status, dict)
        or status.get("run_id") != run_id
        or status.get("status") not in {"pending", "running", "completed", "queued"}
    ):
        raise SystemExit("Research status did not return an active or completed task.")
    result = run(research_poll_args(run_id), timeout=150)
    if (
        not includes(result, {"run_id": run_id, "status": "completed"})
        or not isinstance(result, dict)
        or not isinstance(output := result.get("output"), dict)
    ):
        raise SystemExit("Research did not return a completed result.")
    try:
        saved = json.loads((cwd / "research-result.json").read_text(encoding="utf-8"))
        content = output.get("content")
        if output.get("content_file") == "research-result.md":
            content = (cwd / "research-result.md").read_text(encoding="utf-8").strip()
    except (OSError, ValueError):
        raise SystemExit("Research output was missing or invalid.") from None
    if saved != result or not content:
        raise SystemExit("Research did not save a nonempty result matching stdout.")
    print("parallel-cli research run, status and poll passed (live API).", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live", action="store_true", help="Run Search, Extract and one Research task (may bill usage)."
    )
    parser.add_argument("--case", action="append", help="Run a named offline case (repeatable).")
    args = parser.parse_args()
    env = os.environ.copy()
    cli = Path(sysconfig.get_path("scripts")) / ("parallel-cli.exe" if os.name == "nt" else "parallel-cli")
    if not cli.is_file():
        raise SystemExit("parallel-cli is not installed in this Python environment. Run with uv.")
    if args.live:
        if args.case:
            parser.error("--case is only supported offline")
        if not env.get("PARALLEL_API_KEY", "").strip():
            parser.error("--live requires PARALLEL_API_KEY from a test account")
        with TemporaryDirectory(prefix="parallel-cli-live-") as tmp:
            live_web(cli, env, Path(tmp))
            live_research(cli, env, Path(tmp))
    else:
        with local_api() as server, TemporaryDirectory(prefix="parallel-cli-offline-") as tmp:
            endpoint = f"http://127.0.0.1:{server.server_port}"
            env.update(
                PARALLEL_API_URL=endpoint,
                PARALLEL_PLATFORM_URL=endpoint,
                PARALLEL_SERVICE_API_URL=endpoint,
                PARALLEL_SKILLS_INDEX_URL=f"{endpoint}/skills/index.json",
                PARALLEL_API_KEY=FIXTURE_KEY,
                NO_PROXY="127.0.0.1",
                no_proxy="127.0.0.1",
                REQUEST_METHOD="",
            )
            for scheme in ("http", "https", "all"):
                env[f"{scheme}_proxy"] = env[f"{scheme.upper()}_PROXY"] = endpoint
            cwd = Path(tmp)
            scenarios = cases(endpoint)
            if args.case:
                unknown = set(args.case) - {case.name for case in scenarios}
                if unknown:
                    parser.error(f"unknown offline test case: {', '.join(sorted(unknown))}")
                scenarios = tuple(case for case in scenarios if case.name in args.case)
            else:
                check_help(cli, env, cwd, scenarios)
            for case in scenarios:
                case_dir = cwd / case.name.replace(" ", "-")
                case_dir.mkdir()
                for name, contents in case.files.items():
                    path = case_dir / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(contents)
                run_case(cli, case, env, server, case_dir)
            print(f"parallel-cli behavior passed ({len(scenarios)} loopback API cases).", flush=True)


if __name__ == "__main__":
    main()
