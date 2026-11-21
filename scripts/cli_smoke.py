"""Run the installed parallel-cli against a loopback fixture or the live Extract API."""

import argparse
import json
import os
import subprocess
import sysconfig
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

URL = "https://example.com/"
FIXTURE_KEY = "cli-smoke-fixture"
FIXTURE_CONTENT = "Example Domain (CLI smoke fixture)"


class ExtractFixture(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/v1/extract":
            self.send_error(404)
            return
        if self.headers.get("x-api-key") != FIXTURE_KEY:
            self.send_error(401)
            return
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if request.get("urls") != [URL] or request.get("advanced_settings", {}).get("full_content") is not True:
            self.send_error(400)
            return

        body = json.dumps(
            {
                "extract_id": "extract_smoke",
                "session_id": "session_smoke",
                "results": [{"url": URL, "title": "Example Domain", "excerpts": [], "full_content": FIXTURE_CONTENT}],
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
    with HTTPServer(("127.0.0.1", 0), ExtractFixture) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            worker.join()


def smoke(env: dict[str, str], *, fixture: bool) -> None:
    cli = Path(sysconfig.get_path("scripts")) / ("parallel-cli.exe" if os.name == "nt" else "parallel-cli")
    if not cli.is_file():
        raise SystemExit("parallel-cli is not installed in this Python environment. Run with uv.")
    with TemporaryDirectory(prefix="parallel-cli-smoke-") as tmp:
        try:
            result = subprocess.run(
                [str(cli), "extract", URL, "--full-content", "--json"],
                env={**env, "HOME": tmp, "USERPROFILE": tmp},
                cwd=tmp,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            raise SystemExit("parallel-cli extract timed out after 60s.") from None
    if result.returncode:
        raise SystemExit(f"parallel-cli extract exited with status {result.returncode}.")
    try:
        payload = json.loads(result.stdout)
        [page] = payload["results"]
        content = page["full_content"]
        valid = (
            payload["status"] == "ok"
            and isinstance(payload["extract_id"], str)
            and bool(payload["extract_id"])
            and payload["errors"] == []
            and page["url"].rstrip("/") == URL.rstrip("/")
            and isinstance(content, str)
            and bool(content.strip())
            and (not fixture or content == FIXTURE_CONTENT)
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        raise SystemExit("parallel-cli extract did not return a successful JSON result for the requested page.")
    print(f"parallel-cli extract smoke passed ({'loopback HTTP' if fixture else 'live API'}).")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live", action="store_true", help="Call the hosted API using PARALLEL_API_KEY (may bill usage)."
    )
    env = os.environ.copy()
    if parser.parse_args().live:
        if not env.get("PARALLEL_API_KEY", "").strip():
            parser.error("--live requires PARALLEL_API_KEY from a test account")
        smoke(env, fixture=False)
    else:
        with local_api() as endpoint:
            env.update(
                PARALLEL_API_URL=endpoint, PARALLEL_API_KEY=FIXTURE_KEY, NO_PROXY="127.0.0.1", no_proxy="127.0.0.1"
            )
            smoke(env, fixture=True)


if __name__ == "__main__":
    main()
