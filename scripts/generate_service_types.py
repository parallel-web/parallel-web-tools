#!/usr/bin/env python3
"""Generate Pydantic models from the service API OpenAPI spec.

Mirrors the npm workflow used elsewhere:

    openapi-typescript http://127.0.0.1:8090/openapi.json -o ./app/api/account-service-types.ts

Usage:

    uv run python scripts/generate_service_types.py            # prod (default)
    uv run python scripts/generate_service_types.py --env dev  # localhost:8090
    uv run python scripts/generate_service_types.py --url <custom>

Output is written to ``parallel_web_tools/core/service_types.py``.
Requires ``datamodel-code-generator`` (installed via the ``dev`` extra).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ENV_URLS = {
    "prod": "https://api.parallel.ai/account/service/openapi.json",
    "dev": "http://localhost:8090/service/openapi.json",
}

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "parallel_web_tools" / "core" / "service_types.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        choices=("prod", "dev"),
        default="prod",
        help="Which environment's OpenAPI spec to fetch (default: prod).",
    )
    parser.add_argument(
        "--url",
        help="Custom OpenAPI URL; overrides --env.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help=f"Output file (default: {OUTPUT_PATH.relative_to(Path.cwd()) if OUTPUT_PATH.is_relative_to(Path.cwd()) else OUTPUT_PATH}).",
    )
    args = parser.parse_args()

    url = args.url or ENV_URLS[args.env]
    print(f"Generating service types from {url} → {args.output}", file=sys.stderr)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "datamodel_code_generator",
        "--url",
        url,
        "--input-file-type",
        "openapi",
        "--output",
        str(args.output),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--target-python-version",
        "3.10",
        "--use-standard-collections",
        "--use-union-operator",
        "--use-annotated",
        "--snake-case-field",
        "--formatters",
        "ruff-format",
        "ruff-check",
    ]
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
