"""CLI commands for Parallel."""

import csv
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, NoReturn

import click
import httpx
from dotenv import load_dotenv
from rich.console import Console

from parallel_web_tools import __version__
from parallel_web_tools.cli.skills import create_skills_group
from parallel_web_tools.core import (
    AVAILABLE_PROCESSORS,
    FINDALL_GENERATORS,
    JSON_SCHEMA_TYPE_MAP,
    MONITOR_PROCESSORS,
    MONITOR_TYPES,
    RESEARCH_PROCESSORS,
    cancel_findall_run,
    cancel_monitor,
    create_findall_run,
    create_monitor,
    create_research_task,
    enrich_findall,
    extend_findall,
    get_api_key,
    get_auth_status,
    get_findall_result,
    get_findall_schema,
    get_findall_status,
    get_monitor,
    get_research_status,
    get_task_group_status,
    get_user_agent,
    ingest_findall,
    list_monitor_events,
    list_monitors,
    logout,
    poll_findall,
    poll_research,
    poll_task_group,
    run_enrichment_from_dict,
    run_findall,
    run_research,
    trigger_monitor,
    update_monitor,
)

# Standalone CLI (PyInstaller) has limited features to reduce bundle size
# YAML config and interactive planner require: pip install parallel-web-tools[cli]
_STANDALONE_MODE = getattr(sys, "frozen", False)

# CLI extras (yaml config, interactive planner) are optional
# Available with: pip install parallel-web-tools[cli]
_CLI_EXTRAS_AVAILABLE = False
if not _STANDALONE_MODE:
    try:
        from parallel_web_tools.cli.planner import create_config_interactive, save_config
        from parallel_web_tools.core import run_enrichment

        _CLI_EXTRAS_AVAILABLE = True
    except ImportError:
        # CLI extras not installed (pyyaml, questionary)
        pass

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress noisy HTTP request logging from httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
console = Console()

# Parallel wordmark — rendered from white-parallel-text-1080.png
# using half-block characters (▀▄█) for terminal display
_BANNER_LINES = [
    "[bold white]                                                        █████ █████                █████[/]",
    "[bold white]                                                        █████ █████                █████[/]",
    "[bold white]████████████▄  ▄███████████  ███████████▄ ████████████  █████ █████ ▄████████████▄ █████[/]",
    "[bold white]█████   █████  ▀▀▀▀▀   █████ █████  ▀▀▀▀▀ ▀▀▀▀   █████▄ █████ █████ █████    █████ █████[/]",
    "[bold white]█████   █████  ▄████████████ █████       ▄█████████████ █████ █████ ██████████████ █████[/]",
    "[bold white]█████   █████ █████    █████ █████       █████   ▀█████ █████ █████ █████    █████ █████[/]",
    "[bold white]████████████▀ ▀█████████████ █████       ▀█████████████ █████ █████ ▀████████████▀ █████[/]",
    "[bold white]█████[/]",
    "[bold white]█████[/]",
]


def _print_banner():
    """Print the Parallel CLI banner with logo."""
    if not console.is_terminal:
        return
    banner = "\n".join(line.format(version=__version__) for line in _BANNER_LINES)
    console.print(banner, highlight=False)
    console.print()
    console.print(
        f"  [bold #fb631b]parallel-cli[/bold #fb631b] v{__version__}  [dim]AI-powered web intelligence for your agent[/dim]",
        highlight=False,
    )
    console.print("  [dim]Get started at[/dim] [dim link=https://parallel.ai]parallel.ai[/dim link]", highlight=False)
    console.print()


class ParallelCLI(click.Group):
    """Custom Click group that shows the Parallel banner on help."""

    def format_help(self, ctx, formatter):
        _print_banner()
        super().format_help(ctx, formatter)


load_dotenv(".env.local")

# Source types available for enrich run/plan
# Standalone CLI only supports CSV to minimize bundle size
# DuckDB requires: pip install parallel-web-tools[duckdb]
# BigQuery requires: pip install parallel-web-tools[bigquery]
if _STANDALONE_MODE:
    AVAILABLE_SOURCE_TYPES = ["csv", "json"]
else:
    AVAILABLE_SOURCE_TYPES = ["csv", "json", "duckdb", "bigquery"]


# =============================================================================
# Exit Codes
# =============================================================================

EXIT_OK = 0
EXIT_BAD_INPUT = 2  # Invalid arguments or input data
EXIT_AUTH_ERROR = 3  # Authentication/authorization failure
EXIT_API_ERROR = 4  # API call failed
EXIT_TIMEOUT = 5  # Operation timed out
EXIT_INTERRUPTED = 130  # SIGINT / Ctrl-C (matches POSIX 128 + signal number)

# Default subdirectory for `research run` / `research poll` auto-saved results.
# Lives under the user's cwd so files don't leak into $HOME or wherever they
# happened to invoke the CLI.
DEFAULT_RESEARCH_OUTPUT_DIR = "parallel-research"


# =============================================================================
# Output Helpers
# =============================================================================


def _extract_api_message(error: Exception) -> str:
    """Extract a clean error message from API exceptions."""
    # SDK errors often embed a dict repr; try to extract the inner message
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        inner = body.get("error", {})
        if isinstance(inner, dict) and "message" in inner:
            return inner["message"]
    # httpx errors
    response = getattr(error, "response", None)
    if response is not None:
        try:
            data = response.json()
            inner = data.get("error", {})
            if isinstance(inner, dict) and "message" in inner:
                return inner["message"]
        except Exception:
            pass
    return str(error)


def _handle_error(
    error: Exception,
    output_json: bool = False,
    exit_code: int = EXIT_API_ERROR,
    prefix: str = "Error",
) -> NoReturn:
    """Handle an error with appropriate output format and exit code.

    In --json mode, outputs structured JSON to stdout. Otherwise, prints a
    Rich-formatted error message.
    """
    message = _extract_api_message(error)
    if output_json:
        error_data = {"error": {"message": message, "type": type(error).__name__}}
        print(json.dumps(error_data, indent=2))
    else:
        console.print(f"[bold red]{prefix}: {message}[/bold red]")
    sys.exit(exit_code)


def _exit_research_interrupted(run_id: str | None) -> NoReturn:
    """Print a helpful resume hint after Ctrl-C and exit."""
    if run_id:
        console.print("\n[bold yellow]Interrupted.[/bold yellow] The task is still running on the server.")
        console.print(f"[dim]Resume with: parallel-cli research poll {run_id}[/dim]")
    else:
        console.print("\n[bold yellow]Interrupted before task creation.[/bold yellow]")
    sys.exit(EXIT_INTERRUPTED)


def _exit_research_timeout(error: TimeoutError, output_json: bool, suggest_poll: bool = True) -> NoReturn:
    """Format a research timeout for human or JSON output and exit."""
    if output_json:
        print(json.dumps({"error": {"message": str(error), "type": "TimeoutError"}}, indent=2))
    else:
        console.print(f"[bold yellow]Timeout: {error}[/bold yellow]")
        if suggest_poll:
            console.print("[dim]The task is still running. Use 'parallel-cli research poll <run_id>' to resume.[/dim]")
    sys.exit(EXIT_TIMEOUT)


def parse_comma_separated(values: tuple[str, ...]) -> list[str]:
    """Parse a tuple of values that may contain comma-separated items.

    Supports both repeated flags and comma-separated values:
        --flag a,b --flag c  ->  ['a', 'b', 'c']
        --flag a --flag b    ->  ['a', 'b']
        --flag "a,b,c"       ->  ['a', 'b', 'c']
    """
    result = []
    for value in values:
        # Split by comma and strip whitespace
        parts = [p.strip() for p in value.split(",")]
        result.extend(p for p in parts if p)  # Skip empty strings
    return result


def write_json_output(data: dict[str, Any], output_file: str | None, output_json: bool) -> None:
    """Write output data to file and/or stdout as JSON.

    Args:
        data: The data dictionary to output.
        output_file: Optional file path to save JSON to.
        output_json: If True, print JSON to stdout.
    """
    if output_file:
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)
        console.print(f"[dim]Results saved to {output_file}[/dim]\n")

    if output_json:
        print(json.dumps(data, indent=2))


def parse_columns(columns_json: str | None) -> list[dict[str, str]] | None:
    """Parse columns from JSON string."""
    if not columns_json:
        return None
    try:
        columns = json.loads(columns_json)
        if not isinstance(columns, list):
            raise click.BadParameter("Columns must be a JSON array")
        for col in columns:
            if "name" not in col:
                raise click.BadParameter("Each column must have a 'name' field")
            if "description" not in col:
                raise click.BadParameter("Each column must have a 'description' field")
        return columns
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"Invalid JSON: {e}") from e


def validate_enrich_args(
    source_type: str | None,
    source: str | None,
    target: str | None,
    source_columns: str | None,
    enriched_columns: str | None,
    intent: str | None,
) -> None:
    """Validate enrichment CLI arguments.

    Raises click.UsageError for invalid argument combinations.
    """
    if enriched_columns and intent:
        raise click.UsageError("Use either --enriched-columns OR --intent, not both.")

    base_args = [source_type, source, target, source_columns]
    has_base = all(arg is not None for arg in base_args)
    has_output_spec = enriched_columns is not None or intent is not None

    if any(arg is not None for arg in base_args) or has_output_spec:
        if not has_base:
            missing = [
                n
                for n, v in [
                    ("--source-type", source_type),
                    ("--source", source),
                    ("--target", target),
                    ("--source-columns", source_columns),
                ]
                if not v
            ]
            raise click.UsageError(f"Missing required options: {', '.join(missing)}")
        if not has_output_spec:
            raise click.UsageError("Provide --enriched-columns OR --intent.")


def build_config_from_args(
    source_type: str,
    source: str,
    target: str,
    source_columns: list[dict[str, str]],
    enriched_columns: list[dict[str, str]],
    processor: str,
) -> dict[str, Any]:
    """Build configuration dict from CLI arguments."""
    return {
        "source_type": source_type,
        "source": source,
        "target": target,
        "source_columns": source_columns,
        "enriched_columns": enriched_columns,
        "processor": processor,
    }


def parse_inline_data(data_json: str) -> tuple[str, list[dict[str, str]]]:
    """Parse inline JSON data and write to a temporary CSV file.

    Args:
        data_json: JSON string containing array of objects

    Returns:
        Tuple of (temp_csv_path, inferred_source_columns)

    Raises:
        click.BadParameter: If JSON is invalid or not an array of objects
    """
    try:
        data = json.loads(data_json)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"Invalid JSON data: {e}") from e

    if not isinstance(data, list):
        raise click.BadParameter("Data must be a JSON array")

    if len(data) == 0:
        raise click.BadParameter("Data array cannot be empty")

    if not isinstance(data[0], dict):
        raise click.BadParameter("Data must be an array of objects")

    # Infer columns from the first row
    columns: list[str] = [str(k) for k in data[0].keys()]
    if not columns:
        raise click.BadParameter("Data objects must have at least one field")

    # Create source_columns with inferred descriptions
    source_columns: list[dict[str, str]] = [{"name": col, "description": f"The {col} field"} for col in columns]

    # Write to a temporary CSV file
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="")
    writer = csv.DictWriter(temp_file, fieldnames=columns)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    temp_file.close()

    return temp_file.name, source_columns


def suggest_from_intent(
    intent: str,
    source_columns: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Use Parallel Ingest API to suggest output columns and processor."""
    api_key = get_api_key()
    base_url = "https://api.parallel.ai"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "User-Agent": get_user_agent("cli"),
    }

    full_intent = intent
    if source_columns:
        col_descriptions = [f"- {col['name']}: {col.get('description', 'no description')}" for col in source_columns]
        full_intent = f"{intent}\n\nInput columns available:\n" + "\n".join(col_descriptions)

    suggest_body: dict[str, Any] = {"user_intent": full_intent}

    with httpx.Client(timeout=60) as client:
        response = client.post(f"{base_url}/v1beta/tasks/suggest", json=suggest_body, headers=headers)
        response.raise_for_status()
        data = response.json()

    output_schema = data.get("output_schema", {})
    properties = output_schema.get("properties", {})

    enriched_columns = []
    for name, prop in properties.items():
        col_type = prop.get("type", "string")
        mapped_type = JSON_SCHEMA_TYPE_MAP.get(col_type, "str")
        enriched_columns.append({"name": name, "description": prop.get("description", ""), "type": mapped_type})

    processor = "core-fast"
    try:
        input_schema = data.get("input_schema", {"type": "object", "properties": {}})
        task_spec = {"input_schema": input_schema, "output_schema": output_schema}

        with httpx.Client(timeout=60) as client:
            processor_response = client.post(
                f"{base_url}/v1beta/tasks/suggest-processor", json={"task_spec": task_spec}, headers=headers
            )
            if processor_response.status_code == 200:
                processor_data = processor_response.json()
                recommended = processor_data.get("recommended_processors", [])
                if recommended:
                    processor = recommended[0]
    except Exception as e:
        logger.debug("Processor suggestion failed, using default '%s': %s", processor, e)

    return {
        "enriched_columns": enriched_columns,
        "processor": processor,
        "title": data.get("title", ""),
        "warnings": data.get("warnings", []),
    }


# =============================================================================
# Main CLI Group
# =============================================================================


def _auto_update():
    """Check for updates and auto-install if available.

    Only runs in standalone mode, respects config, and rate-limits to once per day.
    """
    # Import here to avoid slowing down startup when not needed
    from parallel_web_tools.cli.updater import (
        check_for_update_notification,
        download_and_install_update,
        should_check_for_updates,
    )

    # should_check_for_updates() handles standalone mode check, config check, and rate limiting
    if not should_check_for_updates():
        return

    try:
        notification = check_for_update_notification(__version__, save_state=True)
        if notification:
            console.print()
            download_and_install_update(__version__, console)
    except Exception:
        # Silently ignore errors - don't disrupt user's workflow
        pass


@click.group(cls=ParallelCLI)
@click.version_option(version=__version__, prog_name="parallel-cli")
def main():
    """Parallel CLI - Search, research, enrich, and monitor the web."""
    pass


@main.result_callback()
def _after_command(*args, **kwargs):
    """Run after any command completes."""
    _auto_update()


# =============================================================================
# Auth Commands
# =============================================================================


@main.command()
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def auth(output_json: bool):
    """Check authentication status."""
    status = get_auth_status()

    if output_json:
        print(json.dumps(status, indent=2))
        return

    if status["authenticated"]:
        if status["method"] == "environment":
            console.print("[green]Authenticated via PARALLEL_API_KEY environment variable[/green]")
        else:
            console.print("[green]Authenticated via OAuth[/green]")
            console.print(f"  Credentials: {status['token_file']}")
    else:
        console.print("[yellow]Not authenticated[/yellow]")
        console.print("\n[cyan]To get started:[/cyan]")
        console.print("  1. Create an account at [link=https://parallel.ai]parallel.ai[/link]")
        console.print("  2. Run: parallel-cli login")
        console.print("  Or set PARALLEL_API_KEY environment variable")


@main.command()
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.option("--device", is_flag=True, help="Use device authorization flow (for SSH, containers, etc.)")
def login(output_json: bool, device: bool):
    """Authenticate with Parallel API."""
    if not output_json:
        if device:
            console.print("[bold cyan]Authenticating with Parallel (device flow)...[/bold cyan]\n")
        else:
            console.print("[bold cyan]Authenticating with Parallel...[/bold cyan]\n")

    def _on_device_code(info):
        if output_json:
            print(
                json.dumps(
                    {
                        "status": "waiting_for_authorization",
                        "verification_uri": info.verification_uri,
                        "verification_uri_complete": info.verification_uri_complete,
                        "user_code": info.user_code,
                        "expires_in": info.expires_in,
                    }
                ),
                flush=True,
            )
        else:
            console.print(f"Visit: [bold cyan]{info.verification_uri}[/bold cyan]")
            console.print(f"Enter code: [bold yellow]{info.user_code}[/bold yellow]\n")
            console.print(f"Or open: [link={info.verification_uri_complete}]{info.verification_uri_complete}[/link]\n")
            console.print("Waiting for authorization...")

    try:
        get_api_key(force_login=True, device=device, on_device_code=_on_device_code)
        if output_json:
            print(json.dumps({"status": "authenticated"}))
        else:
            console.print("\n[bold green]Authentication successful![/bold green]")
    except Exception as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_AUTH_ERROR, prefix="Authentication failed")


@main.command(name="logout")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def logout_cmd(output_json: bool):
    """Remove stored credentials."""
    removed = logout()
    if output_json:
        print(json.dumps({"status": "logged_out" if removed else "no_credentials"}, indent=2))
    elif removed:
        console.print("[green]Logged out successfully[/green]")
    else:
        console.print("[yellow]No stored credentials found[/yellow]")


@main.command(name="update")
@click.option("--check", is_flag=True, help="Check for updates without installing")
@click.option("--force", is_flag=True, help="Reinstall even if already at latest version")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def update_cmd(check: bool, force: bool, output_json: bool):
    """Update to the latest version (standalone CLI only)."""
    from parallel_web_tools.cli.updater import (
        check_for_update_notification,
        download_and_install_update,
    )

    if not _STANDALONE_MODE:
        if output_json:
            print(
                json.dumps(
                    {
                        "error": {
                            "message": "Update command is only available for standalone CLI.",
                            "type": "UnsupportedOperation",
                        }
                    },
                    indent=2,
                )
            )
        else:
            console.print("[yellow]Update command is only available for standalone CLI.[/yellow]")
            console.print("\nTo update via pip:")
            console.print("  [cyan]pip install --upgrade parallel-web-tools[/cyan]")
        return

    if check:
        # Don't save state for explicit --check (doesn't reset 24h timer)
        notification = check_for_update_notification(__version__, save_state=False)
        if output_json:
            print(
                json.dumps(
                    {
                        "current_version": __version__,
                        "update_available": notification is not None,
                        "message": notification,
                    },
                    indent=2,
                )
            )
        elif notification:
            console.print(f"[cyan]{notification}[/cyan]")
        else:
            console.print(f"[green]Already up to date (v{__version__})[/green]")
        return

    if not download_and_install_update(__version__, console, force=force):
        raise click.Abort()


@main.command(name="config")
@click.argument("key", required=False)
@click.argument("value", required=False)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def config_cmd(key: str | None, value: str | None, output_json: bool):
    """View or set CLI configuration (standalone CLI only).

    \b
    Examples:
      parallel-cli config                     # Show all settings
      parallel-cli config auto-update-check   # Show specific setting
      parallel-cli config auto-update-check on   # Enable auto-update check
      parallel-cli config auto-update-check off  # Disable auto-update check
    """
    from parallel_web_tools.cli.updater import (
        is_auto_update_check_enabled,
        set_auto_update_check,
    )

    if not _STANDALONE_MODE:
        if output_json:
            print(
                json.dumps(
                    {
                        "error": {
                            "message": "Config command is only available for standalone CLI.",
                            "type": "UnsupportedOperation",
                        }
                    },
                    indent=2,
                )
            )
        else:
            console.print("[yellow]Config command is only available for standalone CLI.[/yellow]")
        return

    valid_keys = ["auto-update-check"]

    def format_bool(v: bool) -> str:
        return "on" if v else "off"

    def parse_bool(v: str) -> bool:
        return v.lower() in ("on", "true", "1", "yes")

    # Show all settings
    if key is None:
        config_data = {"auto-update-check": is_auto_update_check_enabled()}
        if output_json:
            print(json.dumps(config_data, indent=2))
        else:
            console.print("[bold]Configuration:[/bold]")
            console.print(f"  auto-update-check: [cyan]{format_bool(is_auto_update_check_enabled())}[/cyan]")
        return

    if key not in valid_keys:
        raise click.UsageError(f"Unknown config key: {key}. Available keys: {', '.join(valid_keys)}")

    # Show or set the value
    if value is None:
        if output_json:
            print(json.dumps({key: is_auto_update_check_enabled()}, indent=2))
        else:
            console.print(f"{key}: [cyan]{format_bool(is_auto_update_check_enabled())}[/cyan]")
    else:
        set_auto_update_check(parse_bool(value))
        if output_json:
            print(json.dumps({key: is_auto_update_check_enabled()}, indent=2))
        else:
            console.print(f"[green]Set {key} = {format_bool(is_auto_update_check_enabled())}[/green]")


main.add_command(create_skills_group(console, _handle_error, EXIT_BAD_INPUT, EXIT_API_ERROR))


# =============================================================================
# Search Command
# =============================================================================

# Beta -> V1 mode mapping. Beta had three modes; V1 has two. We keep the old
# values as accepted CLI inputs and translate them so existing scripts work.
_SEARCH_MODE_MAP = {
    "fast": "basic",
    "one-shot": "basic",
    "agentic": "advanced",
    "basic": "basic",
    "advanced": "advanced",
}
_DEPRECATED_SEARCH_MODES = {"fast", "one-shot", "agentic"}


def _emit_deprecation(message: str) -> None:
    """Print a deprecation notice to stderr so it doesn't pollute --json output."""
    click.echo(f"[deprecated] {message}", err=True)


def build_search_v1_kwargs(
    *,
    objective: str | None,
    query: tuple[str, ...] | list[str],
    mode: str | None,
    max_results: int | None,
    source_policy: dict[str, Any] | None,
    excerpt_max_chars_per_result: int | None,
    excerpt_max_chars_total: int | None,
    fetch_policy: dict[str, Any] | None,
    location: str | None = None,
    session_id: str | None = None,
    client_model: str | None = None,
) -> dict[str, Any]:
    """Translate Beta-style search params to V1 client.search() kwargs.

    V1 requires search_queries; if the caller only provided an objective, we
    fall back to using it as the single query so older invocations keep working.
    """
    queries = list(query) if query else []
    if not queries and objective:
        queries = [objective]

    kwargs: dict[str, Any] = {"search_queries": queries}
    if objective:
        kwargs["objective"] = objective
    if mode:
        kwargs["mode"] = _SEARCH_MODE_MAP.get(mode, mode)
    if excerpt_max_chars_total is not None:
        kwargs["max_chars_total"] = excerpt_max_chars_total
    if session_id:
        kwargs["session_id"] = session_id
    if client_model:
        kwargs["client_model"] = client_model

    advanced: dict[str, Any] = {}
    if max_results is not None:
        advanced["max_results"] = max_results
    if source_policy:
        advanced["source_policy"] = source_policy
    if fetch_policy:
        advanced["fetch_policy"] = fetch_policy
    if excerpt_max_chars_per_result is not None:
        advanced["excerpt_settings"] = {"max_chars_per_result": excerpt_max_chars_per_result}
    if location:
        advanced["location"] = location
    if advanced:
        kwargs["advanced_settings"] = advanced

    return kwargs


@main.command()
@click.argument("objective", required=False)
@click.option("-q", "--query", multiple=True, help="Keyword search query (can be repeated)")
@click.option(
    "--mode",
    type=click.Choice(list(_SEARCH_MODE_MAP.keys())),
    default="basic",
    help="Search mode (one-shot/fast → basic, agentic → advanced)",
    show_default=True,
)
@click.option("--max-results", type=int, help="Maximum results (defaults to server-side default of 10)")
@click.option("--include-domains", multiple=True, help="Only search these domains (comma-separated or repeated)")
@click.option("--exclude-domains", multiple=True, help="Exclude these domains (comma-separated or repeated)")
@click.option("--after-date", help="Only results after this date (YYYY-MM-DD)")
@click.option("--excerpt-max-chars-per-result", type=int, help="Max characters per result for excerpts (min 1000)")
@click.option(
    "--excerpt-max-chars-total", type=int, default=60000, help="Max total characters for excerpts", show_default=True
)
@click.option("--max-age-seconds", type=int, help="Max age in seconds before fetching live content (min 600)")
@click.option("--timeout-seconds", type=float, help="Timeout in seconds for fetching live content")
@click.option("--disable-cache-fallback", is_flag=True, help="Return error instead of stale cached content")
@click.option("--location", help="ISO 3166-1 alpha-2 country code for geo-targeted results (e.g. us, gb, de)")
@click.option("--session-id", help="Session ID to group related search/extract calls")
@click.option(
    "--client-model",
    help="The model generating this request and consuming the results (e.g. claude-opus-4-7, gpt-5.4, gemini-3.1-pro)",
)
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to file (JSON)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def search(
    objective: str | None,
    query: tuple[str, ...],
    mode: str,
    max_results: int | None,
    include_domains: tuple[str, ...],
    exclude_domains: tuple[str, ...],
    after_date: str | None,
    excerpt_max_chars_per_result: int | None,
    excerpt_max_chars_total: int | None,
    max_age_seconds: int | None,
    timeout_seconds: float | None,
    disable_cache_fallback: bool,
    location: str | None,
    session_id: str | None,
    client_model: str | None,
    output_file: str | None,
    output_json: bool,
):
    """Search the web using Parallel's AI-powered search.

    OBJECTIVE is a natural language description of what you're looking for. You can
    also pass specific keyword queries with --query. At least one of OBJECTIVE or
    --query is required. Use "-" as OBJECTIVE to read from stdin.
    """
    # Read from stdin if "-" is passed
    if objective == "-":
        objective = click.get_text_stream("stdin").read().strip()

    if not objective and not query:
        raise click.UsageError("Provide an OBJECTIVE argument or at least one --query option.")

    if mode in _DEPRECATED_SEARCH_MODES:
        new_mode = _SEARCH_MODE_MAP[mode]
        _emit_deprecation(
            f"--mode {mode} is a Beta value and will stop working after the Beta API sunset (June 2026). "
            f"Use --mode {new_mode} instead."
        )

    source_policy: dict[str, Any] = {}
    if include_domains:
        source_policy["include_domains"] = parse_comma_separated(include_domains)
    if exclude_domains:
        source_policy["exclude_domains"] = parse_comma_separated(exclude_domains)
    domain_total = len(source_policy.get("include_domains", [])) + len(source_policy.get("exclude_domains", []))
    if domain_total > 200:
        raise click.UsageError(f"--include-domains and --exclude-domains combined must be <= 200 (got {domain_total}).")
    if after_date:
        source_policy["after_date"] = after_date

    try:
        from parallel import Parallel

        from parallel_web_tools.core import get_default_headers

        api_key = get_api_key()
        client = Parallel(api_key=api_key, default_headers=get_default_headers("cli"))

        fetch_policy: dict[str, Any] = {}
        if max_age_seconds is not None:
            fetch_policy["max_age_seconds"] = max_age_seconds
        if timeout_seconds is not None:
            fetch_policy["timeout_seconds"] = timeout_seconds
        if disable_cache_fallback:
            fetch_policy["disable_cache_fallback"] = True

        search_kwargs = build_search_v1_kwargs(
            objective=objective,
            query=query,
            mode=mode,
            max_results=max_results,
            source_policy=source_policy or None,
            excerpt_max_chars_per_result=excerpt_max_chars_per_result,
            excerpt_max_chars_total=excerpt_max_chars_total,
            fetch_policy=fetch_policy or None,
            location=location,
            session_id=session_id,
            client_model=client_model,
        )

        if not output_json:
            console.print("[dim]Searching...[/dim]\n")

        result = client.search(**search_kwargs)

        output_data = {
            "search_id": result.search_id,
            "session_id": getattr(result, "session_id", None),
            "status": "ok",
            "results": [
                {"url": r.url, "title": r.title, "publish_date": r.publish_date, "excerpts": r.excerpts}
                for r in result.results
            ],
            "usage": [{"name": u.name, "count": u.count} for u in (getattr(result, "usage", None) or [])],
            "warnings": [
                {"type": w.type, "message": w.message, "detail": getattr(w, "detail", None)} for w in result.warnings
            ]
            if hasattr(result, "warnings") and result.warnings
            else [],
        }

        write_json_output(output_data, output_file, output_json)

        if not output_json:
            console.print(f"[bold green]Found {len(result.results)} results[/bold green]\n")
            for i, r in enumerate(result.results, 1):
                console.print(f"[bold cyan]{i}. {r.title}[/bold cyan]")
                console.print(f"   [link={r.url}]{r.url}[/link]")
                if r.publish_date:
                    console.print(f"   [dim]Published: {r.publish_date}[/dim]")
                if r.excerpts:
                    excerpt = r.excerpts[0][:200] + "..." if len(r.excerpts[0]) > 200 else r.excerpts[0]
                    console.print(f"   [dim]{excerpt}[/dim]")
                console.print()

    except Exception as e:
        _handle_error(e, output_json=output_json)


# =============================================================================
# Extract Command
# =============================================================================


def build_extract_v1_kwargs(
    *,
    urls: tuple[str, ...] | list[str],
    objective: str | None,
    query: tuple[str, ...] | list[str],
    full_content: bool,
    full_content_max_chars: int | None,
    excerpt_max_chars_per_result: int | None,
    excerpt_max_chars_total: int | None,
    fetch_policy: dict[str, Any] | None,
    session_id: str | None = None,
    client_model: str | None = None,
) -> dict[str, Any]:
    """Translate Beta-style extract params to V1 client.extract() kwargs.

    Note: V1 always returns excerpts; the old `--no-excerpts` flag can no longer
    disable them server-side. The CLI handles that flag by filtering excerpts out
    of the output, not by passing it to the SDK.
    """
    kwargs: dict[str, Any] = {"urls": list(urls)}
    if objective:
        kwargs["objective"] = objective
    if query:
        kwargs["search_queries"] = list(query)
    if excerpt_max_chars_total is not None:
        kwargs["max_chars_total"] = excerpt_max_chars_total
    if session_id:
        kwargs["session_id"] = session_id
    if client_model:
        kwargs["client_model"] = client_model

    advanced: dict[str, Any] = {}
    if excerpt_max_chars_per_result is not None:
        advanced["excerpt_settings"] = {"max_chars_per_result": excerpt_max_chars_per_result}
    if full_content_max_chars is not None:
        advanced["full_content"] = {"max_chars_per_result": full_content_max_chars}
    elif full_content:
        advanced["full_content"] = True
    if fetch_policy:
        advanced["fetch_policy"] = fetch_policy
    if advanced:
        kwargs["advanced_settings"] = advanced

    return kwargs


@main.command()
@click.argument("urls", nargs=-1, required=True)
@click.option("--objective", help="Focus extraction on a specific goal")
@click.option("-q", "--query", multiple=True, help="Keywords to prioritize (can be repeated)")
@click.option("--full-content", is_flag=True, help="Include complete page content")
@click.option("--full-content-max-chars", type=int, help="Max characters per result for full content")
@click.option("--no-excerpts", is_flag=True, help="Strip excerpts from output (V1 always returns them server-side)")
@click.option("--excerpt-max-chars-per-result", type=int, help="Max characters per result for excerpts (min 1000)")
@click.option("--excerpt-max-chars-total", type=int, help="Max total characters for excerpts across all URLs")
@click.option("--max-age-seconds", type=int, help="Max age in seconds before fetching live content (min 600)")
@click.option("--timeout-seconds", type=float, help="Timeout in seconds for fetching live content")
@click.option("--disable-cache-fallback", is_flag=True, help="Return error instead of stale cached content")
@click.option("--session-id", help="Session ID to group related search/extract calls")
@click.option(
    "--client-model",
    help="The model generating this request and consuming the results (e.g. claude-opus-4-7, gpt-5.4, gemini-3.1-pro)",
)
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to file (JSON)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def extract(
    urls: tuple[str, ...],
    objective: str | None,
    query: tuple[str, ...],
    full_content: bool,
    full_content_max_chars: int | None,
    no_excerpts: bool,
    excerpt_max_chars_per_result: int | None,
    excerpt_max_chars_total: int | None,
    max_age_seconds: int | None,
    timeout_seconds: float | None,
    disable_cache_fallback: bool,
    session_id: str | None,
    client_model: str | None,
    output_file: str | None,
    output_json: bool,
):
    """Extract content from URLs as clean markdown."""
    if no_excerpts:
        _emit_deprecation(
            "--no-excerpts no longer disables excerpts server-side (V1 always returns them); "
            "the flag now just strips them from the CLI output."
        )

    if len(urls) > 20:
        raise click.UsageError(f"V1 extract accepts at most 20 URLs per request (got {len(urls)}).")
    if objective is not None and len(objective) > 5000:
        raise click.UsageError(f"--objective must be 5000 characters or fewer (got {len(objective)}).")

    try:
        from parallel import Parallel

        from parallel_web_tools.core import get_default_headers

        api_key = get_api_key()
        client = Parallel(api_key=api_key, default_headers=get_default_headers("cli"))

        fetch_policy: dict[str, Any] = {}
        if max_age_seconds is not None:
            fetch_policy["max_age_seconds"] = max_age_seconds
        if timeout_seconds is not None:
            fetch_policy["timeout_seconds"] = timeout_seconds
        if disable_cache_fallback:
            fetch_policy["disable_cache_fallback"] = True

        extract_kwargs = build_extract_v1_kwargs(
            urls=urls,
            objective=objective,
            query=query,
            full_content=full_content,
            full_content_max_chars=full_content_max_chars,
            excerpt_max_chars_per_result=excerpt_max_chars_per_result,
            excerpt_max_chars_total=excerpt_max_chars_total,
            fetch_policy=fetch_policy or None,
            session_id=session_id,
            client_model=client_model,
        )

        if not output_json:
            console.print(f"[dim]Extracting content from {len(urls)} URL(s)...[/dim]\n")

        result = client.extract(**extract_kwargs)

        results_list = []
        for r in result.results:
            result_dict: dict[str, Any] = {"url": r.url, "title": r.title, "publish_date": r.publish_date}
            if not no_excerpts and hasattr(r, "excerpts") and r.excerpts:
                result_dict["excerpts"] = r.excerpts
            if hasattr(r, "full_content") and r.full_content:
                result_dict["full_content"] = r.full_content
            results_list.append(result_dict)

        errors_list = []
        if hasattr(result, "errors") and result.errors:
            for e in result.errors:
                errors_list.append(
                    {
                        "url": getattr(e, "url", None),
                        "error_type": getattr(e, "error_type", None),
                        "http_status_code": getattr(e, "http_status_code", None),
                        "content": getattr(e, "content", None),
                    }
                )

        output_data = {
            "extract_id": result.extract_id,
            "session_id": getattr(result, "session_id", None),
            "status": "ok",
            "results": results_list,
            "errors": errors_list,
            "usage": [{"name": u.name, "count": u.count} for u in (getattr(result, "usage", None) or [])],
            "warnings": [
                {"type": w.type, "message": w.message, "detail": getattr(w, "detail", None)} for w in result.warnings
            ]
            if hasattr(result, "warnings") and result.warnings
            else [],
        }

        write_json_output(output_data, output_file, output_json)

        if not output_json:
            if result.errors:
                console.print(f"[yellow]Warning: {len(result.errors)} URL(s) failed[/yellow]\n")

            console.print(f"[bold green]Extracted {len(result.results)} page(s)[/bold green]\n")

            for r in result.results:
                console.print(f"[bold cyan]{r.title}[/bold cyan]")
                console.print(f"[link={r.url}]{r.url}[/link]\n")

                if not no_excerpts and hasattr(r, "excerpts") and r.excerpts:
                    console.print("[dim]Excerpts:[/dim]")
                    for excerpt in r.excerpts[:3]:
                        text = excerpt[:300] + "..." if len(excerpt) > 300 else excerpt
                        console.print(f"  {text}")
                    console.print()

                if hasattr(r, "full_content") and r.full_content:
                    console.print("[dim]Full content:[/dim]")
                    content = r.full_content[:1000] + "..." if len(r.full_content) > 1000 else r.full_content
                    console.print(content)
                    console.print()

    except Exception as e:
        _handle_error(e, output_json=output_json)


# Add fetch as an alias for extract
main.add_command(extract, name="fetch")


# =============================================================================
# Enrich Command Group
# =============================================================================


@main.group()
def enrich():
    """Data enrichment commands."""
    pass


@enrich.command(name="run")
@click.argument("config_file", required=False)
@click.option("--source-type", type=click.Choice(AVAILABLE_SOURCE_TYPES), help="Data source type")
@click.option("--source", help="Source file path or table name")
@click.option("--target", help="Target file path or table name")
@click.option("--source-columns", help="Source columns as JSON")
@click.option("--enriched-columns", help="Enriched columns as JSON")
@click.option("--intent", help="Natural language description (AI suggests columns)")
@click.option("--processor", type=click.Choice(AVAILABLE_PROCESSORS), help="Processor to use")
@click.option("--data", "inline_data", help="Inline JSON data array (alternative to --source)")
@click.option("--no-wait", is_flag=True, help="Return immediately after creating task group (don't poll)")
@click.option("--dry-run", is_flag=True, help="Show what would be executed without making API calls")
@click.option("--json", "output_json", is_flag=True, help="Output results as JSON to stdout")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option(
    "--previous-interaction-id",
    help="Interaction ID from a previous task to reuse as context",
)
def enrich_run(
    config_file: str | None,
    source_type: str | None,
    source: str | None,
    target: str | None,
    source_columns: str | None,
    enriched_columns: str | None,
    intent: str | None,
    processor: str | None,
    inline_data: str | None,
    no_wait: bool,
    dry_run: bool,
    output_json: bool,
    output_file: str | None,
    previous_interaction_id: str | None,
):
    """Run data enrichment from YAML config or CLI arguments.

    You can provide data in three ways:

    \b
    1. YAML config file:
       parallel-cli enrich run config.yaml

    \b
    2. CLI arguments with source file:
       parallel-cli enrich run --source-type csv --source data.csv ...

    \b
    3. Inline JSON data (no CSV file needed):
       parallel-cli enrich run --data '[{"company": "Google"}, {"company": "Apple"}]' \\
           --target output.csv --intent "Find the CEO"
    """
    temp_csv_path: str | None = None

    try:
        # Handle inline data - creates a temp CSV and infers source columns
        if inline_data:
            if source:
                raise click.UsageError("Use --data OR --source, not both.")
            if source_type and source_type != "csv":
                raise click.UsageError("--data only works with CSV output (--source-type csv).")

            temp_csv_path, inferred_cols = parse_inline_data(inline_data)
            source = temp_csv_path
            source_type = "csv"

            # Use inferred columns if not explicitly provided
            if not source_columns:
                source_columns = json.dumps(inferred_cols)
                if not output_json:
                    console.print(f"[dim]Inferred {len(inferred_cols)} source column(s) from data[/dim]")

        base_args = [source_type, source, target, source_columns]
        has_cli_args = any(arg is not None for arg in base_args) or enriched_columns or intent

        if config_file and has_cli_args:
            raise click.UsageError("Provide either a config file OR CLI arguments, not both.")

        if not config_file and not has_cli_args:
            raise click.UsageError("Provide a config file or CLI arguments.")

        # YAML config files require CLI extras (pyyaml)
        if config_file and not _CLI_EXTRAS_AVAILABLE:
            console.print("[bold red]Error: YAML config files require the CLI extras.[/bold red]")
            console.print("\nUse CLI arguments instead:")
            console.print("  parallel-cli enrich run --source-type csv --source data.csv ...")
            console.print("\nOr install CLI extras: [cyan]pip install parallel-web-tools\\[cli][/cyan]")
            raise click.Abort()

        if has_cli_args:
            validate_enrich_args(source_type, source, target, source_columns, enriched_columns, intent)

        if config_file:
            if dry_run:
                _handle_error(
                    click.UsageError("--dry-run is not supported with config files. Use CLI arguments instead."),
                    output_json=output_json,
                    exit_code=EXIT_BAD_INPUT,
                )
                return

            if not output_json:
                console.print(f"[bold cyan]Running enrichment from {config_file}...[/bold cyan]\n")
            result = run_enrichment(config_file, no_wait=no_wait, previous_interaction_id=previous_interaction_id)
        else:
            # After validation, these are guaranteed non-None
            assert source_type is not None
            assert source is not None
            assert target is not None

            src_cols = parse_columns(source_columns)
            assert src_cols is not None  # Validated above

            if intent:
                if dry_run:
                    # Skip suggest API call — show what we know without it
                    enr_cols: list[dict[str, str]] = []
                    final_processor = processor or "core-fast"
                else:
                    if not output_json:
                        console.print("[dim]Getting suggestions from Parallel API...[/dim]")
                    suggestion = suggest_from_intent(intent, src_cols)
                    enr_cols = suggestion["enriched_columns"]
                    final_processor = processor or suggestion["processor"]
                    if not output_json:
                        console.print(
                            f"[green]AI suggested {len(enr_cols)} columns, processor: {final_processor}[/green]\n"
                        )
            else:
                parsed_enr_cols = parse_columns(enriched_columns)
                assert parsed_enr_cols is not None  # Validated above
                enr_cols = parsed_enr_cols
                final_processor = processor or "core-fast"

            if dry_run:
                # Count rows in source file
                row_count = None
                source_display = source
                if inline_data:
                    source_display = "<inline data>"
                if source and os.path.exists(source):
                    with open(source) as f:
                        row_count = sum(1 for _ in f) - 1  # subtract header

                dry_run_data: dict[str, Any] = {
                    "dry_run": True,
                    "source_type": source_type,
                    "source": source_display,
                    "target": target,
                    "processor": final_processor,
                    "source_columns": src_cols,
                }
                if enr_cols:
                    dry_run_data["enriched_columns"] = enr_cols
                else:
                    dry_run_data["intent"] = intent
                    dry_run_data["note"] = (
                        "Columns will be suggested by AI at runtime (use without --dry-run to see suggestions)"
                    )
                if row_count is not None:
                    dry_run_data["row_count"] = row_count

                if output_json:
                    print(json.dumps(dry_run_data, indent=2))
                else:
                    console.print("[bold]Dry run — no API calls will be made[/bold]\n")
                    console.print(f"  [bold]Source:[/bold]      {source_display} ({source_type})")
                    if row_count is not None:
                        console.print(f"  [bold]Rows:[/bold]        {row_count}")
                    console.print(f"  [bold]Target:[/bold]      {target}")
                    console.print(f"  [bold]Processor:[/bold]   {final_processor}")
                    console.print(f"  [bold]Input cols:[/bold]  {len(src_cols)}")
                    for col in src_cols:
                        console.print(f"    [dim]- {col['name']}: {col.get('description', '')}[/dim]")
                    if enr_cols:
                        console.print(f"  [bold]Output cols:[/bold] {len(enr_cols)}")
                        for col in enr_cols:
                            console.print(f"    [dim]- {col['name']}: {col.get('description', '')}[/dim]")
                    else:
                        console.print(f"  [bold]Intent:[/bold]     {intent}")
                        console.print("  [dim]Output columns will be suggested by AI at runtime[/dim]")
                return

            config = build_config_from_args(
                source_type=source_type,
                source=source,
                target=target,
                source_columns=src_cols,
                enriched_columns=enr_cols,
                processor=final_processor,
            )

            if not output_json:
                console.print(f"[bold cyan]Running enrichment: {source} -> {target}[/bold cyan]\n")
            result = run_enrichment_from_dict(config, no_wait=no_wait, previous_interaction_id=previous_interaction_id)

        if no_wait and result:
            if output_json:
                print(json.dumps(result, indent=2))
            else:
                console.print(f"\n[bold green]Task group created: {result['taskgroup_id']}[/bold green]")
                console.print(f"Track progress: {result['url']}")
                console.print(f"[dim]Runs: {result['num_runs']}[/dim]")
                console.print("\n[dim]Use 'parallel-cli enrich status <id>' to check status[/dim]")
                console.print("[dim]Use 'parallel-cli enrich poll <id>' to wait for results[/dim]")

            if output_file:
                with open(output_file, "w") as f:
                    json.dump(result, f, indent=2)
                if not output_json:
                    console.print(f"[dim]Results saved to {output_file}[/dim]")
        else:
            # Wait mode completed - read back target file for JSON/output if requested
            if output_json or output_file:
                # Try to read enrichment results from the target file
                results_data: Any = None
                if target and os.path.exists(target):
                    with open(target) as f:
                        if target.endswith(".json"):
                            results_data = json.load(f)
                        else:
                            # CSV target - convert to list of dicts
                            reader = csv.DictReader(f)
                            results_data = list(reader)

                if results_data is not None:
                    if output_file:
                        with open(output_file, "w") as f:
                            json.dump(results_data, f, indent=2)
                        if not output_json:
                            console.print(f"[dim]Results saved to {output_file}[/dim]")

                    if output_json:
                        print(json.dumps(results_data, indent=2))

            if not output_json:
                console.print("\n[bold green]Enrichment complete![/bold green]")

    except FileNotFoundError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT)
    except (click.BadParameter, click.UsageError) as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid input")
    except Exception as e:
        _handle_error(e, output_json=output_json, prefix="Error during enrichment")
    finally:
        # Clean up temp file if we created one
        if temp_csv_path and os.path.exists(temp_csv_path):
            os.unlink(temp_csv_path)


# Plan command - only registered when not running as frozen executable (standalone CLI)
# Standalone CLI doesn't bundle planner dependencies (questionary, duckdb, pyyaml)
@click.command(name="plan")
@click.option("-o", "--output", default="config.yaml", help="Output YAML file path", show_default=True)
@click.option("--source-type", type=click.Choice(AVAILABLE_SOURCE_TYPES), help="Data source type")
@click.option("--source", help="Source file path or table name")
@click.option("--target", help="Target file path or table name")
@click.option("--source-columns", help="Source columns as JSON")
@click.option("--enriched-columns", help="Enriched columns as JSON")
@click.option("--intent", help="Natural language description (AI suggests columns)")
@click.option("--processor", type=click.Choice(AVAILABLE_PROCESSORS), help="Processor to use")
def enrich_plan(
    output: str,
    source_type: str | None,
    source: str | None,
    target: str | None,
    source_columns: str | None,
    enriched_columns: str | None,
    intent: str | None,
    processor: str | None,
):
    """Create an enrichment configuration file interactively or from CLI arguments."""
    base_args = [source_type, source, target, source_columns]
    has_cli_args = any(arg is not None for arg in base_args) or enriched_columns or intent

    if has_cli_args:
        validate_enrich_args(source_type, source, target, source_columns, enriched_columns, intent)
        # After validation, these are guaranteed non-None
        assert source_type is not None
        assert source is not None
        assert target is not None
        src_cols = parse_columns(source_columns)
        assert src_cols is not None  # Validated above

        if intent:
            console.print("[dim]Getting suggestions from Parallel API...[/dim]")
            suggestion = suggest_from_intent(intent, src_cols)
            enr_cols = suggestion["enriched_columns"]
            final_processor = processor or suggestion["processor"]
            console.print(f"[green]AI suggested {len(enr_cols)} columns, processor: {final_processor}[/green]")
        else:
            enr_cols = parse_columns(enriched_columns)
            assert enr_cols is not None  # Validated above
            final_processor = processor or "core-fast"

        config = build_config_from_args(
            source_type=source_type,
            source=source,
            target=target,
            source_columns=src_cols,
            enriched_columns=enr_cols,
            processor=final_processor,
        )

        save_config(config, output)
        console.print(f"[bold green]Configuration saved to {output}[/bold green]")
    else:
        try:
            config = create_config_interactive()
            save_config(config, output)
        except KeyboardInterrupt:
            console.print("\n[yellow]Configuration creation cancelled.[/yellow]")
            raise click.Abort() from None


# Only register plan command when CLI extras are available
# Requires: pip install parallel-web-tools[cli]
if _CLI_EXTRAS_AVAILABLE:
    enrich.add_command(enrich_plan)


@enrich.command(name="suggest")
@click.argument("intent")
@click.option("--source-columns", help="Source columns as JSON")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def enrich_suggest(intent: str, source_columns: str | None, output_json: bool):
    """Use AI to suggest output columns and processor."""
    try:
        src_cols = parse_columns(source_columns) if source_columns else None

        if not output_json:
            console.print("[dim]Getting suggestions from Parallel API...[/dim]\n")

        result = suggest_from_intent(intent, src_cols)

        if output_json:
            print(json.dumps(result, indent=2))
        else:
            if result.get("title"):
                console.print(f"[bold]Task: {result['title']}[/bold]\n")

            console.print(f"[bold green]Recommended Processor:[/bold green] {result['processor']}\n")

            console.print("[bold green]Suggested Output Columns:[/bold green]")
            for col in result["enriched_columns"]:
                console.print(f"  [cyan]{col['name']}[/cyan] ({col['type']}): {col['description']}")

            if result.get("warnings"):
                console.print("\n[yellow]Warnings:[/yellow]")
                for warning in result["warnings"]:
                    console.print(f"  {warning}")

            console.print("\n[dim]JSON (for --enriched-columns):[/dim]")
            console.print(json.dumps(result["enriched_columns"]))

    except Exception as e:
        _handle_error(e, output_json=output_json)


@enrich.command(name="status")
@click.argument("taskgroup_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def enrich_status(taskgroup_id: str, output_json: bool):
    """Check the status of an enrichment task group.

    TASKGROUP_ID is the task group identifier (e.g., tgrp_xxx).
    """
    try:
        result = get_task_group_status(taskgroup_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2))
        else:
            is_active = result["is_active"]
            status_counts = result["status_counts"]
            completed = status_counts.get("completed", 0)
            failed = status_counts.get("failed", 0)
            total = result["num_runs"]

            if not is_active and completed + failed >= total:
                status_color = "green"
                status_label = "completed"
            elif is_active:
                status_color = "cyan"
                status_label = "running"
            else:
                status_color = "yellow"
                status_label = "pending"

            console.print(f"[bold]Task Group:[/bold] {taskgroup_id}")
            console.print(f"[bold]Status:[/bold] [{status_color}]{status_label}[/{status_color}]")
            console.print(f"[bold]Progress:[/bold] {completed} completed, {failed} failed / {total} total")
            console.print(f"[bold]URL:[/bold] {result['url']}")

            if status_label == "completed":
                console.print("\n[dim]Use 'parallel-cli enrich poll <id>' to retrieve results[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@enrich.command(name="poll")
@click.argument("taskgroup_id")
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=5, show_default=True, help="Seconds between status checks")
@click.option("--json", "output_json", is_flag=True, help="Output results as JSON to stdout")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
def enrich_poll(
    taskgroup_id: str,
    timeout: int,
    poll_interval: int,
    output_json: bool,
    output_file: str | None,
):
    """Poll an enrichment task group until completion.

    TASKGROUP_ID is the task group identifier (e.g., tgrp_xxx).
    """
    try:
        if not output_json:
            console.print(f"[bold cyan]Polling task group: {taskgroup_id}[/bold cyan]")
            console.print(
                f"[dim]Track progress: https://platform.parallel.ai/view/task-run-group/{taskgroup_id}[/dim]\n"
            )

        start_time = time.time()

        def on_progress(completed: int, failed: int, total: int):
            if output_json:
                return
            elapsed = time.time() - start_time
            mins, secs = divmod(int(elapsed), 60)
            elapsed_str = f"{mins}m{secs:02d}s" if mins else f"{secs}s"
            rate_str = f", {completed / elapsed:.1f}/s" if elapsed > 0 and completed > 0 else ""
            console.print(
                f"[dim]Progress: {completed}/{total} completed, {failed} failed ({elapsed_str}{rate_str})[/dim]"
            )

        results = poll_task_group(
            taskgroup_id,
            timeout=timeout,
            poll_interval=poll_interval,
            on_progress=on_progress,
            source="cli",
        )

        completed = sum(1 for r in results if "output" in r)
        failed = sum(1 for r in results if "error" in r)

        if output_file:
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            console.print(f"[dim]Results saved to {output_file}[/dim]\n")

        if output_json:
            print(json.dumps(results, indent=2))

        if not output_json:
            console.print("\n[bold green]Task group complete![/bold green]")
            console.print(f"{completed} completed, {failed} failed out of {len(results)} total runs")

            if not output_file:
                console.print("[dim]Use --json to output full results, or --output to save to a file[/dim]")

    except TimeoutError as e:
        if output_json:
            error_data = {"error": {"message": str(e), "type": "TimeoutError"}}
            print(json.dumps(error_data, indent=2))
        else:
            console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
            console.print("[dim]The task group is still running. Use 'parallel-cli enrich poll <id>' to resume.[/dim]")
        sys.exit(EXIT_TIMEOUT)
    except Exception as e:
        _handle_error(e, output_json=output_json)


# Deploy command - only registered when not running as frozen executable (standalone CLI)
# Standalone CLI users should use: pip install parallel-web-tools[snowflake|bigquery]
@click.command(name="deploy")
@click.option(
    "--system", type=click.Choice(["bigquery", "snowflake"]), required=True, help="Target system to deploy to"
)
@click.option("--project", "-p", help="Cloud project ID (required for bigquery)")
@click.option("--region", "-r", default="us-central1", show_default=True, help="Cloud region (BigQuery)")
@click.option("--api-key", "-k", help="Parallel API key (or use PARALLEL_API_KEY env var)")
@click.option("--dataset", default="parallel_functions", show_default=True, help="Dataset name (BigQuery)")
@click.option("--account", help="Snowflake account identifier (e.g., abc12345.us-east-1)")
@click.option("--user", "-u", help="Snowflake username")
@click.option("--password", help="Snowflake password (or use SSO with --authenticator)")
@click.option("--warehouse", "-w", default="COMPUTE_WH", show_default=True, help="Snowflake warehouse")
@click.option("--authenticator", default="externalbrowser", show_default=True, help="Snowflake auth method")
@click.option("--passcode", help="MFA passcode from authenticator app (use with --authenticator username_password_mfa)")
@click.option("--role", default="ACCOUNTADMIN", show_default=True, help="Snowflake role for deployment")
def enrich_deploy(
    system: str,
    project: str | None,
    region: str,
    api_key: str | None,
    dataset: str,
    account: str | None,
    user: str | None,
    password: str | None,
    warehouse: str,
    authenticator: str,
    passcode: str | None,
    role: str,
):
    """Deploy Parallel enrichment to a cloud system."""
    from parallel_web_tools.core.auth import get_api_key

    # Validate required parameters FIRST (before triggering OAuth)
    if system == "bigquery" and not project:
        raise click.UsageError("--project is required for BigQuery deployment.")
    if system == "snowflake":
        if not account:
            raise click.UsageError("--account is required for Snowflake deployment.")
        if not user:
            raise click.UsageError("--user is required for Snowflake deployment.")

    # Now resolve API key (may trigger OAuth flow if needed)
    if not api_key:
        api_key = get_api_key()

    if system == "bigquery":
        assert project is not None  # Validated above
        try:
            from parallel_web_tools.integrations.bigquery import deploy_bigquery_integration
        except ImportError:
            console.print("[bold red]Error: BigQuery deployment is not available in the standalone CLI.[/bold red]")
            console.print("\nInstall via pip: [cyan]pip install parallel-web-tools[/cyan]")
            console.print("Also requires: gcloud CLI installed and authenticated")
            raise click.Abort() from None

        console.print(f"[bold cyan]Deploying to BigQuery in {project}...[/bold cyan]\n")

        try:
            result = deploy_bigquery_integration(
                project_id=project,
                api_key=api_key,
                region=region,
                dataset_id=dataset,
            )
            console.print("\n[bold green]Deployment complete![/bold green]")
            console.print(f"\nFunction URL: {result['function_url']}")
            console.print("\n[cyan]Example query:[/cyan]")
            console.print(result["example_query"])
        except Exception as e:
            _handle_error(e, prefix="Deployment failed")

    elif system == "snowflake":
        assert account is not None and user is not None  # Validated above
        try:
            from parallel_web_tools.integrations.snowflake import deploy_parallel_functions
        except ImportError:
            console.print("[bold red]Error: Snowflake deployment is not available in the standalone CLI.[/bold red]")
            console.print("\nInstall via pip: [cyan]pip install parallel-web-tools[snowflake][/cyan]")
            raise click.Abort() from None

        console.print(f"[bold cyan]Deploying to Snowflake account {account}...[/bold cyan]\n")

        try:
            deploy_parallel_functions(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                role=role,
                parallel_api_key=api_key,
                authenticator=authenticator if not password else None,
                passcode=passcode,
            )
            console.print("\n[bold green]Deployment complete![/bold green]")
            console.print("\n[cyan]Example query:[/cyan]")
            console.print("""
WITH companies AS (
    SELECT * FROM (VALUES
        ('Google', 'google.com'),
        ('Anthropic', 'anthropic.com'),
        ('Apple', 'apple.com')
    ) AS t(company_name, website)
)
SELECT
    e.input:company_name::STRING AS company_name,
    e.input:website::STRING AS website,
    e.enriched:ceo_name::STRING AS ceo_name,
    e.enriched:founding_year::STRING AS founding_year
FROM companies t,
     TABLE(PARALLEL_INTEGRATION.ENRICHMENT.parallel_enrich(
         TO_JSON(OBJECT_CONSTRUCT('company_name', t.company_name, 'website', t.website)),
         ARRAY_CONSTRUCT('CEO name', 'Founding year')
     ) OVER (PARTITION BY 1)) e;
""")
        except Exception as e:
            _handle_error(e, prefix="Deployment failed")


# Only register deploy command when not running as frozen executable (PyInstaller)
# Standalone CLI doesn't bundle deploy dependencies - use pip install instead
if not getattr(sys, "frozen", False):
    enrich.add_command(enrich_deploy)


# =============================================================================
# Research Command Group
# =============================================================================


@main.group()
def research():
    """Deep research commands for open-ended questions."""
    pass


@research.command(name="run")
@click.argument("query", required=False)
@click.option("--input-file", "-f", type=click.Path(exists=True), help="Read query from file")
@click.option(
    "--processor",
    "-p",
    type=click.Choice(list(RESEARCH_PROCESSORS.keys())),
    default="pro-fast",
    show_default=True,
    help="Processor tier (higher = more thorough but slower)",
)
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=45, show_default=True, help="Seconds between status checks")
@click.option("--no-wait", is_flag=True, help="Return immediately after creating task (don't save or poll)")
@click.option("--dry-run", is_flag=True, help="Show what would be executed without making API calls")
@click.option(
    "--text",
    "use_text",
    is_flag=True,
    help="Return a markdown report (text schema) instead of the default structured JSON.",
)
@click.option(
    "--text-description",
    default=None,
    help="Steering description for --text reports (e.g. 'Keep under 1000 words, focus on M&A')",
)
@click.option(
    "-o",
    "--output",
    "output_base",
    type=click.Path(),
    default=None,
    help=(
        "Output base path; writes {base}.json (and {base}.md with --text). "
        f"Default: ./{DEFAULT_RESEARCH_OUTPUT_DIR}/<run_id>. Any .json/.md suffix is stripped."
    ),
)
@click.option("--force", is_flag=True, help="Overwrite existing output files")
@click.option("--json", "output_json", is_flag=True, help="Also print the result as JSON to stdout")
@click.option(
    "--previous-interaction-id",
    help="Interaction ID from a previous task to reuse as context",
)
def research_run(
    query: str | None,
    input_file: str | None,
    processor: str,
    timeout: int,
    poll_interval: int,
    no_wait: bool,
    dry_run: bool,
    use_text: bool,
    text_description: str | None,
    output_base: str | None,
    force: bool,
    output_json: bool,
    previous_interaction_id: str | None,
):
    """Run deep research on a question or topic.

    QUERY is the research question (max 15,000 chars). Alternatively, use --input-file
    or pass "-" as QUERY to read from stdin.

    \b
    Output (when --no-wait is not set):
      Results are always saved to disk so a long-running task is never lost.
      Default base path: ./parallel-research/<run_id>. Override with -o NAME
      (writes NAME.json, plus NAME.md with --text). Existing files are not
      overwritten unless --force is passed.

    \b
    Schemas:
      Default: auto schema (API-chosen structured JSON; deep-research outputs
      on `pro` tiers and above).
      --text:  text schema (markdown report with inline citations). Use
      --text-description to steer length or focus.

    Use --previous-interaction-id to continue research from a prior task.

    \b
    Examples:
      parallel-cli research run "What are the latest developments in quantum computing?"
      parallel-cli research run --text "Market analysis of HVAC industry" -o report
      parallel-cli research run -f question.txt --processor ultra --text -o report
      echo "My research question" | parallel-cli research run - --json
      parallel-cli research run "What are the implications?" \\
          --previous-interaction-id trun_abc123
    """
    output_schema = "text" if use_text else "auto"

    if text_description and not use_text:
        raise click.UsageError("--text-description requires --text.")

    # Read from stdin if "-" is passed
    if query == "-":
        query = click.get_text_stream("stdin").read().strip()

    # Get query from argument or file
    if input_file:
        with open(input_file) as f:
            query = f.read().strip()
    elif not query:
        raise click.UsageError("Provide a QUERY argument or use --input-file.")

    if len(query) > 15000:
        console.print(f"[yellow]Warning: Query truncated from {len(query)} to 15,000 characters[/yellow]")
        query = query[:15000]

    if dry_run:
        # Show where files will go using a placeholder run_id so users can see the layout.
        planned_base = _resolve_research_base_path(output_base, "<run_id>")
        planned_paths = [f"{planned_base}.json"]
        if use_text:
            planned_paths.append(f"{planned_base}.md")
        dry_run_data = {
            "dry_run": True,
            "query": query[:200] + "..." if len(query) > 200 else query,
            "query_length": len(query),
            "processor": processor,
            "output_schema": output_schema,
            "expected_latency": RESEARCH_PROCESSORS[processor],
            "output_paths": planned_paths,
            "force": force,
        }
        if output_json:
            print(json.dumps(dry_run_data, indent=2))
        else:
            console.print("[bold]Dry run — no API calls will be made[/bold]\n")
            console.print(f"  [bold]Query:[/bold]     {dry_run_data['query']}")
            console.print(f"  [bold]Length:[/bold]    {len(query)} chars")
            console.print(f"  [bold]Processor:[/bold] {processor}")
            console.print(f"  [bold]Schema:[/bold]    {output_schema}")
            console.print(f"  [bold]Latency:[/bold]   {RESEARCH_PROCESSORS[processor]}")
            console.print(f"  [bold]Output:[/bold]    {', '.join(planned_paths)}")
        return

    # Single-element list captures the run_id from the on_status callback so a
    # Ctrl-C during the long poll can suggest `parallel-cli research poll
    # <run_id>`. List-as-box keeps the closure simple — no `nonlocal` needed.
    run_id_box: list[str] = []

    try:
        if no_wait:
            if not output_json:
                console.print(f"[dim]Creating research task with processor: {processor}...[/dim]")
            result = create_research_task(
                query,
                processor=processor,
                source="cli",
                previous_interaction_id=previous_interaction_id,
                output_schema=output_schema,
                text_description=text_description,
            )
            run_id_box.append(result["run_id"])

            if not output_json:
                console.print(f"\n[bold green]Task created: {result['run_id']}[/bold green]")
                if result.get("interaction_id"):
                    console.print(f"Interaction ID: {result['interaction_id']}")
                console.print(f"Track progress: {result['result_url']}")
                console.print("\n[dim]Use 'parallel-cli research status <run_id>' to check status[/dim]")
                console.print("[dim]Use 'parallel-cli research poll <run_id>' to fetch and save results[/dim]")
                console.print("[dim]Use '--previous-interaction-id' on a new run to continue this research[/dim]")

            if output_json:
                print(json.dumps(result, indent=2))
        else:
            if not output_json:
                console.print(f"[bold cyan]Starting deep research with processor: {processor}[/bold cyan]")
                console.print(f"[dim]This may take {RESEARCH_PROCESSORS[processor]}[/dim]")
                if output_base:
                    planned_base = _resolve_research_base_path(output_base, "")
                    console.print(f"[dim]Will save to: {planned_base}.json[/dim]\n")
                else:
                    console.print(f"[dim]Will save to: ./{DEFAULT_RESEARCH_OUTPUT_DIR}/<run_id>.json[/dim]\n")

            start_time = time.time()

            def on_status(status: str, run_id: str):
                if not run_id_box:
                    run_id_box.append(run_id)
                if output_json:
                    return
                elapsed = time.time() - start_time
                mins, secs = divmod(int(elapsed), 60)
                elapsed_str = f"{mins}m{secs:02d}s" if mins else f"{secs}s"
                if status == "created":
                    console.print(f"[green]Task created: {run_id}[/green]")
                    console.print(
                        f"[dim]Track progress: https://platform.parallel.ai/play/deep-research/{run_id}[/dim]\n"
                    )
                else:
                    console.print(f"[dim]Status: {status} ({elapsed_str})[/dim]")

            result = run_research(
                query,
                processor=processor,
                timeout=timeout,
                poll_interval=poll_interval,
                on_status=on_status,
                source="cli",
                previous_interaction_id=previous_interaction_id,
                output_schema=output_schema,
                text_description=text_description,
            )

            _save_and_display_research(result, output_base, output_json, force=force)

    except KeyboardInterrupt:
        _exit_research_interrupted(run_id_box[0] if run_id_box else None)
    except TimeoutError as e:
        _exit_research_timeout(e, output_json)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
    except click.ClickException:
        raise
    except Exception as e:
        _handle_error(e, output_json=output_json)


@research.command(name="status")
@click.argument("run_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def research_status(run_id: str, output_json: bool):
    """Check the status of a research task.

    RUN_ID is the task identifier (e.g., trun_xxx).
    """
    try:
        result = get_research_status(run_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2))
        else:
            status = result["status"]
            status_color = {
                "completed": "green",
                "running": "cyan",
                "pending": "yellow",
                "failed": "red",
                "cancelled": "red",
            }.get(status, "white")

            console.print(f"[bold]Task:[/bold] {run_id}")
            console.print(f"[bold]Interaction ID:[/bold] {result.get('interaction_id', run_id)}")
            console.print(f"[bold]Status:[/bold] [{status_color}]{status}[/{status_color}]")
            console.print(f"[bold]URL:[/bold] {result['result_url']}")

            if status == "completed":
                console.print("\n[dim]Use 'parallel-cli research poll <run_id>' to retrieve results[/dim]")
                console.print("[dim]Use '--previous-interaction-id' on a new run to continue this research[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@research.command(name="poll")
@click.argument("run_id")
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=45, show_default=True, help="Seconds between status checks")
@click.option(
    "-o",
    "--output",
    "output_base",
    type=click.Path(),
    default=None,
    help=(
        "Output base path; writes {base}.json (and {base}.md if the task used text schema). "
        f"Default: ./{DEFAULT_RESEARCH_OUTPUT_DIR}/<run_id>. Any .json/.md suffix is stripped."
    ),
)
@click.option("--force", is_flag=True, help="Overwrite existing output files")
@click.option("--json", "output_json", is_flag=True, help="Also print the result as JSON to stdout")
def research_poll(
    run_id: str,
    timeout: int,
    poll_interval: int,
    output_base: str | None,
    force: bool,
    output_json: bool,
):
    """Poll an existing research task until completion and save the result.

    RUN_ID is the task identifier (e.g., trun_xxx).

    \b
    Output:
      Same as `research run`. Default base path: ./parallel-research/<run_id>.
      Override with -o NAME (writes NAME.json, plus NAME.md if the task was
      created with text schema). Existing files are not overwritten unless
      --force is passed.
    """
    try:
        if not output_json:
            console.print(f"[bold cyan]Polling task: {run_id}[/bold cyan]")
            console.print(f"[dim]Track progress: https://platform.parallel.ai/play/deep-research/{run_id}[/dim]")
            planned_base = _resolve_research_base_path(output_base, run_id)
            console.print(f"[dim]Will save to: {planned_base}.json (+.md for text schema)[/dim]\n")

        start_time = time.time()

        def on_status(status: str, run_id: str):
            if output_json:
                return
            elapsed = time.time() - start_time
            mins, secs = divmod(int(elapsed), 60)
            elapsed_str = f"{mins}m{secs:02d}s" if mins else f"{secs}s"
            console.print(f"[dim]Status: {status} ({elapsed_str})[/dim]")

        result = poll_research(
            run_id,
            timeout=timeout,
            poll_interval=poll_interval,
            on_status=on_status,
            source="cli",
        )

        _save_and_display_research(result, output_base, output_json, force=force)

    except KeyboardInterrupt:
        _exit_research_interrupted(run_id)
    except TimeoutError as e:
        _exit_research_timeout(e, output_json, suggest_poll=False)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
    except click.ClickException:
        raise
    except Exception as e:
        _handle_error(e, output_json=output_json)


@research.command(name="processors")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def research_processors(output_json: bool):
    """List available research processors and their characteristics."""
    if output_json:
        processors = [{"name": name, "description": desc} for name, desc in RESEARCH_PROCESSORS.items()]
        print(json.dumps({"processors": processors}, indent=2))
        return

    console.print("[bold]Available Research Processors:[/bold]\n")
    for proc, desc in RESEARCH_PROCESSORS.items():
        console.print(f"  [cyan]{proc:15}[/cyan] {desc}")
    console.print("\n[dim]Use --processor/-p to select a processor[/dim]")


def _extract_executive_summary(content: Any) -> str | None:
    """Extract the executive summary from research content.

    For markdown strings, the executive summary is the text before the first
    ## heading. For dicts, checks for a 'summary' or 'executive_summary' key.
    Returns None if no summary can be extracted.
    """
    if isinstance(content, str):
        # Find text before the first ## heading
        text = content.strip()
        if not text:
            return None

        # Split on first ## heading (but not # which is the title)
        import re

        match = re.search(r"^##\s", text, re.MULTILINE)
        if match:
            summary = text[: match.start()].strip()
        else:
            summary = text

        # Strip a leading # title line if present
        lines = summary.split("\n")
        if lines and lines[0].startswith("# "):
            lines = lines[1:]
            summary = "\n".join(lines).strip()

        # Return if we have meaningful content (not just a title)
        if summary and len(summary) > 20:
            return summary

    if isinstance(content, dict):
        # Check for {text: "..."} structure
        if "text" in content and len(content) == 1:
            return _extract_executive_summary(content["text"])

        for key in ("summary", "executive_summary"):
            if key in content:
                val = content[key]
                return str(val) if val else None

    return None


def _content_to_markdown(content: Any, level: int = 1) -> str:
    """Convert structured content to markdown.

    Handles:
    - Strings: returned as-is
    - Dicts with 'text' key: extracts the text
    - Dicts with other keys: converts to headings and nested content
    - Lists: converts to bullet points or numbered lists
    """
    if content is None:
        return ""

    if isinstance(content, str):
        return content

    if isinstance(content, dict):
        # Check for {text: "..."} structure
        if "text" in content and len(content) == 1:
            return content["text"]

        # Convert dict to markdown sections
        lines = []
        for key, value in content.items():
            # Convert key to title (e.g., "quantum_computing_summary" -> "Quantum Computing Summary")
            title = key.replace("_", " ").title()
            heading = "#" * min(level, 6)
            lines.append(f"{heading} {title}\n")

            # Recursively convert value
            if isinstance(value, str):
                lines.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # For complex items, render as sub-content
                        lines.append(_content_to_markdown(item, level + 1))
                    else:
                        lines.append(f"- {item}")
            elif isinstance(value, dict):
                lines.append(_content_to_markdown(value, level + 1))
            else:
                lines.append(str(value))

            lines.append("")  # Blank line after section

        return "\n".join(lines)

    if isinstance(content, list):
        lines = []
        for item in content:
            if isinstance(item, dict):
                lines.append(_content_to_markdown(item, level))
            else:
                lines.append(f"- {item}")
        return "\n".join(lines)

    return str(content)


def _resolve_research_base_path(output_base: str | None, run_id: str) -> Path:
    """Resolve the base path for research output files.

    Returns a Path with no .json/.md suffix. If `output_base` is None, defaults
    to ./parallel-research/{run_id} so results don't pollute cwd.

    If `output_base` looks like a directory (trailing slash, or an existing
    directory), append <run_id> inside it so `-o outputs/` does the obvious
    thing instead of writing `outputs.json`. Otherwise treat it as a base
    filename, only stripping a trailing `.json`/`.md` so `-o report` and
    `-o report.json` produce the same result. Other suffixes (e.g. `.v2`,
    `.bak`) are preserved as part of the name.
    """
    if not output_base:
        return Path(DEFAULT_RESEARCH_OUTPUT_DIR) / run_id

    looks_like_dir = output_base.endswith(("/", os.sep)) or Path(output_base).is_dir()
    if looks_like_dir:
        return Path(output_base) / run_id

    base_path = Path(output_base)
    if base_path.suffix.lower() in {".json", ".md"}:
        base_path = base_path.with_suffix("")
    return base_path


def _save_and_display_research(
    result: dict,
    output_base: str | None,
    output_json: bool,
    force: bool = False,
):
    """Save the research result to disk and display a summary.

    Always writes {base}.json. Writes {base}.md as well when the task used
    text schema (a markdown report). Auto-schema results stay JSON-only.

    Without --force, refuses to overwrite existing files. On write failure
    (e.g. permission denied), falls back to /tmp/{run_id}.{ext} so the result
    is never lost.
    """
    output = result.get("output", {})
    run_id = result.get("run_id", "research")

    base_path = _resolve_research_base_path(output_base, run_id)
    # Append rather than `.with_suffix(".json")` so unconventional bases like
    # `report.v2` are preserved as `report.v2.json` (with_suffix would replace).
    json_path = base_path.parent / f"{base_path.name}.json"

    # The SDK's response carries a `type` discriminator ("text" or "json").
    # Fall back to the requested `output_schema` we threaded through, then to
    # a content-shape heuristic for older mocks/poll flows.
    content = output.get("content") if isinstance(output, dict) else None
    response_type = output.get("type") if isinstance(output, dict) else None
    is_text_response = (
        response_type == "text"
        or result.get("output_schema") == "text"
        or (response_type is None and isinstance(content, str) and content != "")
    )
    md_path = base_path.parent / f"{base_path.name}.md" if is_text_response else None

    output_payload = output.copy() if isinstance(output, dict) else output
    if md_path is not None and isinstance(content, str):
        # Move the markdown body to the .md sibling and reference it from JSON.
        output_payload["content_file"] = md_path.name
        output_payload.pop("content", None)

    output_data = {
        "run_id": run_id,
        "interaction_id": result.get("interaction_id"),
        "result_url": result.get("result_url"),
        "status": result.get("status"),
        "output": output_payload,
    }

    targets = [(json_path, "json")] + ([(md_path, "md")] if md_path else [])
    if not force:
        existing = [p for p, _ in targets if p.exists()]
        if existing:
            lines = []
            for p, _ in targets:
                lines.append(f"  {p} {'(exists)' if p.exists() else '(new)'}")
            raise click.ClickException(
                "Refusing to overwrite existing output:\n" + "\n".join(lines) + "\nPass --force to overwrite."
            )

    def _write_outputs(json_target: Path, md_target: Path | None) -> None:
        if md_target is not None and isinstance(content, str):
            md_target.parent.mkdir(parents=True, exist_ok=True)
            md_target.write_text(content)
            if not output_json:
                console.print(f"[green]Content saved to:[/green] {md_target}")

        json_target.parent.mkdir(parents=True, exist_ok=True)
        with open(json_target, "w") as f:
            json.dump(output_data, f, indent=2, default=str)
        if not output_json:
            console.print(f"[green]Metadata saved to:[/green] {json_target}")

    try:
        _write_outputs(json_path, md_path)
    except OSError as e:
        # Fall back to /tmp so a successful (and billed) API call is never lost.
        tmp_dir = Path(tempfile.gettempdir())
        fallback_json = tmp_dir / f"{run_id}.json"
        fallback_md = tmp_dir / f"{run_id}.md" if md_path else None
        if md_path is not None:
            output_data["output"]["content_file"] = fallback_md.name if fallback_md else None
        if not output_json:
            console.print(f"[yellow]Failed to write to {json_path.parent}: {e}. Falling back to {tmp_dir}.[/yellow]")
        _write_outputs(fallback_json, fallback_md)

    if output_json:
        print(json.dumps(output_data, indent=2, default=str))
        return

    console.print("\n[bold green]Research Complete![/bold green]")
    console.print(f"[dim]Task: {run_id}[/dim]")
    console.print(f"[dim]Interaction ID: {result.get('interaction_id')}[/dim]")
    console.print(f"[dim]URL: {result.get('result_url')}[/dim]\n")

    summary = _extract_executive_summary(content) if content else None
    if summary:
        from rich.markdown import Markdown
        from rich.panel import Panel

        console.print(Panel(Markdown(summary), title="Executive Summary", border_style="cyan"))
        console.print()

    interaction_id = result.get("interaction_id")
    if interaction_id:
        console.print(f"[dim]Use '--previous-interaction-id {interaction_id}' to continue this research[/dim]")


# =============================================================================
# FindAll Commands
# =============================================================================


@main.group()
def findall():
    """FindAll: discover entities from the web using natural language."""
    pass


@findall.command(name="run")
@click.argument("objective")
@click.option(
    "--generator",
    "-g",
    type=click.Choice(list(FINDALL_GENERATORS.keys())),
    default="core",
    show_default=True,
    help="Generator tier (higher = more thorough but slower/costlier)",
)
@click.option(
    "--match-limit",
    "-n",
    type=int,
    default=10,
    show_default=True,
    help="Maximum number of matched candidates (5-1000)",
)
@click.option("--exclude", "exclude_json", help="Entities to exclude as JSON array of {name, url} objects")
@click.option("--metadata", "metadata_json", help="Metadata as JSON string")
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=30, show_default=True, help="Seconds between status checks")
@click.option("--no-wait", is_flag=True, help="Return immediately after creating run (don't poll)")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Ingest schema via API to preview entity type and conditions, but don't create the run",
)
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def findall_run(
    objective: str,
    generator: str,
    match_limit: int,
    exclude_json: str | None,
    metadata_json: str | None,
    timeout: int,
    poll_interval: int,
    no_wait: bool,
    dry_run: bool,
    output_file: str | None,
    output_json: bool,
):
    """Run a FindAll query to discover and match entities from the web.

    OBJECTIVE is a natural language description of what to find.

    Examples:

        parallel-cli findall run "Find all AI companies that raised Series A in 2026"

        parallel-cli findall run "Find roofing companies in Charlotte NC" -g base -n 25

        parallel-cli findall run "Find YC companies in developer tools" --no-wait --json

        parallel-cli findall run "Find AI startups" --exclude '[{"name": "OpenAI", "url": "openai.com"}]'
    """
    try:
        exclude_list = json.loads(exclude_json) if exclude_json else None
        metadata = json.loads(metadata_json) if metadata_json else None
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        if dry_run:
            # Ingest schema only — no run created
            if not output_json:
                console.print("[dim]Ingesting objective...[/dim]\n")

            schema = ingest_findall(objective, source="cli")
            dry_run_data = {
                "dry_run": True,
                "objective": objective,
                "generator": generator,
                "generator_description": FINDALL_GENERATORS[generator],
                "match_limit": match_limit,
                "entity_type": schema.get("entity_type", "unknown"),
                "match_conditions": schema.get("match_conditions", []),
                "enrichments": schema.get("enrichments", []),
            }

            if output_json:
                print(json.dumps(dry_run_data, indent=2, default=str))
            else:
                console.print("[bold]Dry run — schema ingested, no run created[/bold]\n")
                console.print(f"  [bold]Entity type:[/bold]  {dry_run_data['entity_type']}")
                console.print(f"  [bold]Generator:[/bold]    {generator} ({FINDALL_GENERATORS[generator]})")
                console.print(f"  [bold]Match limit:[/bold]  {match_limit}")
                conditions = dry_run_data["match_conditions"]
                console.print(f"  [bold]Conditions:[/bold]   {len(conditions)}")
                for mc in conditions:
                    console.print(f"    [dim]- {mc.get('name', '')}: {mc.get('description', '')}[/dim]")
                enrichments = dry_run_data["enrichments"]
                if enrichments:
                    console.print(f"  [bold]Enrichments:[/bold]  {len(enrichments)}")
                    for e in enrichments:
                        console.print(f"    [dim]- {e.get('name', '')}: {e.get('description', '')}[/dim]")
            return

        if no_wait:
            # Ingest + create, then return immediately
            if not output_json:
                console.print("[dim]Ingesting objective...[/dim]")

            schema = ingest_findall(objective, source="cli")

            if not output_json:
                entity_type = schema.get("entity_type", "entities")
                conditions = schema.get("match_conditions", [])
                console.print(f"[green]Entity type:[/green] {entity_type}")
                console.print(f"[green]Match conditions:[/green] {len(conditions)}")
                for mc in conditions:
                    name = mc.get("name", "")
                    desc = mc.get("description", "")
                    console.print(f"  [dim]- {name}: {desc}[/dim]")
                console.print(f"\n[dim]Creating run with generator={generator}, match_limit={match_limit}...[/dim]")

            result = create_findall_run(
                objective=objective,
                entity_type=schema.get("entity_type", "entities"),
                match_conditions=schema.get("match_conditions", []),
                generator=generator,
                match_limit=match_limit,
                exclude_list=exclude_list,
                metadata=metadata,
                source="cli",
            )

            if output_json:
                print(json.dumps(result, indent=2, default=str))
            else:
                console.print(f"\n[bold green]Run created: {result['findall_id']}[/bold green]")
                console.print(
                    f"\n[dim]Use 'parallel-cli findall status {result['findall_id']}' to check progress[/dim]"
                )
                console.print(f"[dim]Use 'parallel-cli findall poll {result['findall_id']}' to wait for results[/dim]")

            if output_file:
                with open(output_file, "w") as f:
                    json.dump(result, f, indent=2, default=str)
                if not output_json:
                    console.print(f"[dim]Results saved to {output_file}[/dim]")
        else:
            # Full flow: ingest, create, poll
            if not output_json:
                console.print(
                    f"[bold cyan]Starting FindAll with generator={generator}, match_limit={match_limit}[/bold cyan]"
                )
                console.print(f"[dim]Generator: {FINDALL_GENERATORS[generator]}[/dim]\n")

            start_time = time.time()

            def on_status(status: str, findall_id: str, metrics: dict):
                if output_json:
                    return
                elapsed = time.time() - start_time
                mins, secs = divmod(int(elapsed), 60)
                elapsed_str = f"{mins}m{secs:02d}s" if mins else f"{secs}s"

                if status == "ingested":
                    entity_type = metrics.get("entity_type", "")
                    console.print(f"[green]Schema ready - entity type: {entity_type}[/green]")
                elif status == "created":
                    console.print(f"[green]Run created: {findall_id}[/green]\n")
                else:
                    generated = metrics.get("generated_candidates_count", 0)
                    matched = metrics.get("matched_candidates_count", 0)
                    console.print(
                        f"[dim]Status: {status} | generated: {generated}, matched: {matched} ({elapsed_str})[/dim]"
                    )

            result = run_findall(
                objective,
                generator=generator,
                match_limit=match_limit,
                exclude_list=exclude_list,
                metadata=metadata,
                timeout=timeout,
                poll_interval=poll_interval,
                on_status=on_status,
                source="cli",
            )

            _output_findall_result(result, output_file, output_json)

    except TimeoutError as e:
        if output_json:
            print(json.dumps({"error": {"message": str(e), "type": "TimeoutError"}}, indent=2))
        else:
            console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
            console.print("[dim]The run is still active. Use 'parallel-cli findall poll <findall_id>' to resume.[/dim]")
        sys.exit(EXIT_TIMEOUT)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="ingest")
@click.argument("objective")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def findall_ingest(objective: str, output_json: bool):
    """Convert a natural language objective into a FindAll schema.

    Use this to preview the schema (entity type, match conditions) before
    creating a run. You can then use the output to refine your query.

    OBJECTIVE is a natural language description of what to find.

    Examples:

        parallel-cli findall ingest "Find all AI companies that raised Series A in 2026"
    """
    try:
        if not output_json:
            console.print("[dim]Ingesting objective...[/dim]\n")

        schema = ingest_findall(objective, source="cli")

        if output_json:
            print(json.dumps(schema, indent=2, default=str))
        else:
            console.print(f"[bold]Entity type:[/bold] {schema.get('entity_type', 'unknown')}")
            console.print(f"[bold]Generator:[/bold]   {schema.get('generator', 'core')}")
            conditions = schema.get("match_conditions", [])
            console.print(f"\n[bold]Match conditions ({len(conditions)}):[/bold]")
            for mc in conditions:
                name = mc.get("name", "")
                desc = mc.get("description", "")
                console.print(f"  [cyan]{name}[/cyan]: {desc}")

            enrichments = schema.get("enrichments") or []
            if enrichments:
                console.print(f"\n[bold]Suggested enrichments ({len(enrichments)}):[/bold]")
                for e in enrichments:
                    name = e.get("name", "")
                    desc = e.get("description", "")
                    console.print(f"  [cyan]{name}[/cyan]: {desc}")

            console.print("\n[dim]Use 'parallel-cli findall run' to start a run with this schema[/dim]")
            console.print("[dim]Use --json to get machine-readable output for programmatic use[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="status")
@click.argument("findall_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def findall_status(findall_id: str, output_json: bool):
    """Check the status of a FindAll run.

    FINDALL_ID is the run identifier (e.g., findall_xxx).
    """
    try:
        result = get_findall_status(findall_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            status = result["status"]
            status_color = {
                "completed": "green",
                "running": "cyan",
                "queued": "yellow",
                "failed": "red",
                "cancelled": "red",
            }.get(status, "white")

            metrics = result.get("metrics", {})
            generated = metrics.get("generated_candidates_count", 0)
            matched = metrics.get("matched_candidates_count", 0)

            console.print(f"[bold]Run:[/bold]       {findall_id}")
            console.print(f"[bold]Status:[/bold]    [{status_color}]{status}[/{status_color}]")
            console.print(f"[bold]Generator:[/bold] {result.get('generator', 'unknown')}")
            console.print(f"[bold]Generated:[/bold] {generated}")
            console.print(f"[bold]Matched:[/bold]   {matched}")

            if result.get("is_active"):
                console.print(f"\n[dim]Use 'parallel-cli findall poll {findall_id}' to wait for results[/dim]")
            elif status == "completed":
                console.print(f"\n[dim]Use 'parallel-cli findall result {findall_id}' to get results[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="poll")
@click.argument("findall_id")
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=30, show_default=True, help="Seconds between status checks")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def findall_poll(
    findall_id: str,
    timeout: int,
    poll_interval: int,
    output_file: str | None,
    output_json: bool,
):
    """Poll a FindAll run until completion and show results.

    FINDALL_ID is the run identifier (e.g., findall_xxx).
    """
    try:
        if not output_json:
            console.print(f"[bold cyan]Polling FindAll run: {findall_id}[/bold cyan]\n")

        start_time = time.time()

        def on_status(status: str, fid: str, metrics: dict):
            if output_json:
                return
            elapsed = time.time() - start_time
            mins, secs = divmod(int(elapsed), 60)
            elapsed_str = f"{mins}m{secs:02d}s" if mins else f"{secs}s"
            generated = metrics.get("generated_candidates_count", 0)
            matched = metrics.get("matched_candidates_count", 0)
            console.print(f"[dim]Status: {status} | generated: {generated}, matched: {matched} ({elapsed_str})[/dim]")

        result = poll_findall(
            findall_id,
            timeout=timeout,
            poll_interval=poll_interval,
            on_status=on_status,
            source="cli",
        )

        _output_findall_result(result, output_file, output_json)

    except TimeoutError as e:
        if output_json:
            print(json.dumps({"error": {"message": str(e), "type": "TimeoutError"}}, indent=2))
        else:
            console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
        sys.exit(EXIT_TIMEOUT)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="result")
@click.argument("findall_id")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def findall_result(findall_id: str, output_file: str | None, output_json: bool):
    """Fetch results of a completed FindAll run.

    FINDALL_ID is the run identifier (e.g., findall_xxx).
    """
    try:
        result = get_findall_result(findall_id, source="cli")
        _output_findall_result(result, output_file, output_json)
    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="cancel")
@click.argument("findall_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def findall_cancel(findall_id: str, output_json: bool):
    """Cancel a running FindAll run.

    FINDALL_ID is the run identifier (e.g., findall_xxx).
    """
    try:
        result = cancel_findall_run(findall_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2))
        else:
            console.print(f"[bold green]Cancelled:[/bold green] {findall_id}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="enrich")
@click.argument("findall_id")
@click.argument("output_schema_json")
@click.option(
    "--processor",
    "-p",
    type=click.Choice(AVAILABLE_PROCESSORS),
    default="core",
    show_default=True,
    help="Processor to use for enrichment",
)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def findall_enrich(findall_id: str, output_schema_json: str, processor: str, output_json: bool):
    """Add enrichments to a completed FindAll run.

    FINDALL_ID is the run identifier (e.g., findall_xxx).

    OUTPUT_SCHEMA_JSON is a JSON object describing the enrichment fields.

    Examples:

        parallel-cli findall enrich findall_xxx '{"properties": {"ceo": {"type": "string"}}}'

        parallel-cli findall enrich findall_xxx '{"properties": {"stock_price": {"type": "number"}}}' -p base
    """
    try:
        output_schema = json.loads(output_schema_json)
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        result = enrich_findall(
            findall_id,
            output_schema=output_schema,
            processor=processor,
            source="cli",
        )

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Enrichment started for:[/bold green] {findall_id}")
            console.print(f"[dim]Processor: {processor}[/dim]")
            console.print(f"\n[dim]Use 'parallel-cli findall poll {findall_id}' to wait for enrichment results[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="extend")
@click.argument("findall_id")
@click.argument("additional_match_limit", type=int)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def findall_extend(findall_id: str, additional_match_limit: int, output_json: bool):
    """Extend a FindAll run to get more matches.

    FINDALL_ID is the run identifier (e.g., findall_xxx).

    ADDITIONAL_MATCH_LIMIT is the number of additional matches to find.

    Note: Preview runs cannot be extended.

    Examples:

        parallel-cli findall extend findall_xxx 20

        parallel-cli findall extend findall_xxx 50 --json
    """
    try:
        result = extend_findall(
            findall_id,
            additional_match_limit=additional_match_limit,
            source="cli",
        )

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Extended:[/bold green] {findall_id}")
            console.print(f"[dim]Additional matches requested: {additional_match_limit}[/dim]")
            console.print(f"\n[dim]Use 'parallel-cli findall poll {findall_id}' to wait for results[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@findall.command(name="schema")
@click.argument("findall_id")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save schema to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def findall_schema(findall_id: str, output_file: str | None, output_json: bool):
    """Retrieve the schema of a FindAll run for refresh/rerun.

    FINDALL_ID is the run identifier (e.g., findall_xxx).

    The schema can be used to re-run with modifications (e.g., exclude_list).

    Examples:

        parallel-cli findall schema findall_xxx --json

        parallel-cli findall schema findall_xxx -o schema.json
    """
    try:
        result = get_findall_schema(findall_id, source="cli")

        write_json_output(result, output_file, output_json)

        if not output_json:
            console.print(f"[bold]Schema for:[/bold] {findall_id}")
            console.print(f"\n{json.dumps(result, indent=2, default=str)}")
            if not output_file:
                console.print("\n[dim]Use -o to save schema to a file for reuse[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


def _output_findall_result(
    result: dict,
    output_file: str | None,
    output_json: bool,
):
    """Format and output FindAll results to console and/or files."""
    candidates = result.get("candidates", [])
    matched = [c for c in candidates if c.get("match_status") == "matched"]
    metrics = result.get("metrics", {})

    output_data = {
        "findall_id": result.get("findall_id"),
        "status": result.get("status"),
        "metrics": metrics,
        "candidates": candidates,
    }

    if output_file:
        from pathlib import Path

        out_path = Path(output_file)
        if not out_path.suffix:
            out_path = out_path.with_suffix(".json")

        with open(out_path, "w") as f:
            json.dump(output_data, f, indent=2, default=str)
        if not output_json:
            console.print(f"[green]Results saved to:[/green] {out_path}")

    if output_json:
        print(json.dumps(output_data, indent=2, default=str))
    else:
        console.print("\n[bold green]FindAll Complete![/bold green]")
        console.print(f"[dim]Run: {result.get('findall_id')}[/dim]")
        console.print(
            f"[dim]Generated: {metrics.get('generated_candidates_count', 0)} | "
            f"Matched: {metrics.get('matched_candidates_count', 0)}[/dim]\n"
        )

        if matched:
            from rich.table import Table

            table = Table(title=f"Matched Candidates ({len(matched)})")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("URL", style="dim")
            table.add_column("Description", max_width=50)

            for c in matched:
                table.add_row(
                    c.get("name", ""),
                    c.get("url", ""),
                    (c.get("description", "") or "")[:50],
                )

            console.print(table)
            console.print()
        else:
            console.print("[yellow]No matched candidates found.[/yellow]\n")

        if not output_file:
            console.print("[dim]Use --output to save full results, or --json for machine-readable output[/dim]")


# =============================================================================
# Monitor Commands
# =============================================================================


@main.group()
def monitor():
    """Monitor: continuously track the web for changes."""
    pass


@monitor.command(name="create")
@click.argument("query", required=False)
@click.option(
    "--frequency",
    "-f",
    default="1d",
    show_default=True,
    help=(
        "How often to run the monitor. SDK format '<n><unit>' with unit h/d/w "
        "(e.g. 1h, 6h, 1d, 2w). Aliases also accepted: hourly, daily, weekly, every_two_weeks."
    ),
)
@click.option(
    "--type",
    "monitor_type",
    type=click.Choice(list(MONITOR_TYPES)),
    default="event_stream",
    show_default=True,
    help="Monitor type: 'event_stream' tracks a search query; 'snapshot' tracks a Task Run output.",
)
@click.option("--task-run-id", help="Required for type=snapshot: the Task Run whose output to track.")
@click.option(
    "--processor",
    type=click.Choice(list(MONITOR_PROCESSORS)),
    help="Monitor processor (default: lite). 'base' is more thorough at higher cost.",
)
@click.option("--webhook", help="Webhook URL for event delivery")
@click.option("--metadata", "metadata_json", help="Metadata as JSON string")
@click.option("--output-schema", "output_schema_json", help="Output schema as JSON string (event_stream only)")
@click.option(
    "--include-backfill",
    is_flag=True,
    help="event_stream only: include a sample of historical events on first run.",
)
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save result to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_create(
    query: str | None,
    frequency: str,
    monitor_type: str,
    task_run_id: str | None,
    processor: str | None,
    webhook: str | None,
    metadata_json: str | None,
    output_schema_json: str | None,
    include_backfill: bool,
    output_file: str | None,
    output_json: bool,
):
    """Create a new monitor to track the web for changes.

    QUERY is the search query for type=event_stream (the default). For type=snapshot,
    omit QUERY and pass --task-run-id instead.

    Examples:

        parallel-cli monitor create "Track price changes for iPhone 16"

        parallel-cli monitor create "New AI funding announcements" --frequency 1h

        parallel-cli monitor create "SEC filings from Tesla" --webhook https://example.com/hook

        parallel-cli monitor create --type snapshot --task-run-id trun_abc --frequency 1d
    """
    if monitor_type == "event_stream" and not query:
        _handle_error(
            click.UsageError("QUERY is required when --type=event_stream"),
            output_json=output_json,
            exit_code=EXIT_BAD_INPUT,
        )
        return
    if monitor_type == "snapshot" and not task_run_id:
        _handle_error(
            click.UsageError("--task-run-id is required when --type=snapshot"),
            output_json=output_json,
            exit_code=EXIT_BAD_INPUT,
        )
        return

    try:
        metadata = json.loads(metadata_json) if metadata_json else None
        output_schema = json.loads(output_schema_json) if output_schema_json else None
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        if not output_json:
            console.print(f"[dim]Creating {monitor_type} monitor (frequency={frequency})...[/dim]")

        result = create_monitor(
            query=query,
            frequency=frequency,
            type=monitor_type,
            task_run_id=task_run_id,
            webhook=webhook,
            metadata=metadata,
            output_schema=output_schema,
            include_backfill=include_backfill or None,
            processor=processor,
            source="cli",
        )

        write_json_output(result, output_file, output_json)

        if not output_json:
            monitor_id = result.get("monitor_id", "unknown")
            console.print(f"\n[bold green]Monitor created: {monitor_id}[/bold green]")
            console.print(f"[dim]Type: {result.get('type', monitor_type)}[/dim]")
            console.print(f"[dim]Frequency: {result.get('frequency', frequency)}[/dim]")
            if query:
                console.print(f"[dim]Query: {query}[/dim]")
            if task_run_id:
                console.print(f"[dim]Task run: {task_run_id}[/dim]")
            if webhook:
                console.print(f"[dim]Webhook: {webhook}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="list")
@click.option("--limit", "-n", type=int, help="Maximum number of monitors to return (1-10000)")
@click.option("--cursor", help="Pagination token from a previous response")
@click.option(
    "--status",
    type=click.Choice(["active", "cancelled"]),
    multiple=True,
    help="Filter by status (repeatable). Defaults to active only.",
)
@click.option(
    "--type",
    "monitor_type",
    type=click.Choice(list(MONITOR_TYPES)),
    multiple=True,
    help="Filter by monitor type (repeatable).",
)
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_list(
    limit: int | None,
    cursor: str | None,
    status: tuple[str, ...],
    monitor_type: tuple[str, ...],
    output_json: bool,
):
    """List monitors (newest first).

    Examples:

        parallel-cli monitor list

        parallel-cli monitor list --limit 10 --json

        parallel-cli monitor list --status active --status cancelled
    """
    try:
        result = list_monitors(
            cursor=cursor,
            limit=limit,
            status=list(status) if status else None,
            type=list(monitor_type) if monitor_type else None,
            source="cli",
        )
        monitors = result.get("monitors", []) if isinstance(result, dict) else []

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            if not monitors:
                console.print("[yellow]No monitors found.[/yellow]")
                return

            from rich.table import Table

            table = Table(title=f"Monitors ({len(monitors)})")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Type", style="magenta")
            table.add_column("Query / Task Run", max_width=50)
            table.add_column("Frequency", style="green")
            table.add_column("Status", style="yellow")

            for m in monitors:
                settings = m.get("settings", {}) or {}
                tracked = settings.get("query") or settings.get("task_run_id") or ""
                table.add_row(
                    m.get("monitor_id", ""),
                    m.get("type", ""),
                    str(tracked)[:50],
                    m.get("frequency", ""),
                    m.get("status", ""),
                )

            console.print(table)
            if next_cursor := result.get("next_cursor"):
                console.print(f"[dim]Next cursor: {next_cursor}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="get")
@click.argument("monitor_id")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_get(monitor_id: str, output_json: bool):
    """Get details of a specific monitor.

    MONITOR_ID is the monitor identifier.
    """
    try:
        result = get_monitor(monitor_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            settings = result.get("settings", {}) or {}
            console.print(f"[bold]Monitor:[/bold]    {result.get('monitor_id', monitor_id)}")
            console.print(f"[bold]Type:[/bold]       {result.get('type', '')}")
            console.print(f"[bold]Frequency:[/bold]  {result.get('frequency', '')}")
            console.print(f"[bold]Status:[/bold]     {result.get('status', '')}")
            console.print(f"[bold]Processor:[/bold]  {result.get('processor', '')}")
            if query := settings.get("query"):
                console.print(f"[bold]Query:[/bold]      {query}")
            if task_run_id := settings.get("task_run_id"):
                console.print(f"[bold]Task run:[/bold]   {task_run_id}")
            if webhook := result.get("webhook"):
                console.print(
                    f"[bold]Webhook:[/bold]    {webhook.get('url') if isinstance(webhook, dict) else webhook}"
                )
            if created_at := result.get("created_at"):
                console.print(f"[bold]Created:[/bold]    {created_at}")
            if last_run := result.get("last_run_at"):
                console.print(f"[bold]Last run:[/bold]   {last_run}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="update")
@click.argument("monitor_id")
@click.option(
    "--frequency",
    "-f",
    help="Updated frequency (e.g. 1h, 6h, 1d, 2w; or aliases hourly/daily/weekly/every_two_weeks).",
)
@click.option("--webhook", help="Updated webhook URL")
@click.option("--metadata", "metadata_json", help="Updated metadata as JSON string")
@click.option(
    "--advanced-settings",
    "advanced_settings_json",
    help="event_stream advanced_settings as JSON string (sets type=event_stream automatically).",
)
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_update(
    monitor_id: str,
    frequency: str | None,
    webhook: str | None,
    metadata_json: str | None,
    advanced_settings_json: str | None,
    output_json: bool,
):
    """Update an existing monitor.

    MONITOR_ID is the monitor identifier. Note: query and task_run_id are
    immutable — create a new monitor to change them.

    Examples:

        parallel-cli monitor update mon_abc --frequency 1h

        parallel-cli monitor update mon_abc --webhook https://example.com/hook
    """
    if not any([frequency, webhook, metadata_json, advanced_settings_json]):
        _handle_error(
            click.UsageError(
                "Provide at least one field to update (--frequency, --webhook, --metadata, --advanced-settings)"
            ),
            output_json=output_json,
            exit_code=EXIT_BAD_INPUT,
        )
        return

    try:
        metadata = json.loads(metadata_json) if metadata_json else None
        advanced_settings = json.loads(advanced_settings_json) if advanced_settings_json else None
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        result = update_monitor(
            monitor_id,
            frequency=frequency,
            webhook=webhook,
            metadata=metadata,
            advanced_settings=advanced_settings,
            source="cli",
        )

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Monitor updated: {monitor_id}[/bold green]")
            if frequency:
                console.print(f"[dim]Frequency: {frequency}[/dim]")
            if webhook:
                console.print(f"[dim]Webhook: {webhook}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="cancel")
@click.argument("monitor_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def monitor_cancel(monitor_id: str, output_json: bool):
    """Cancel a monitor (irreversible).

    MONITOR_ID is the monitor identifier. Cancellation permanently stops the
    monitor from running. Create a new monitor to resume monitoring.
    """
    try:
        result = cancel_monitor(monitor_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Cancelled:[/bold green] {monitor_id}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="events")
@click.argument("monitor_id")
@click.option("--cursor", help="Pagination token from a previous response")
@click.option("--event-group-id", help="Restrict results to a single execution.")
@click.option(
    "--include-completions",
    is_flag=True,
    help="Include no-change completion events for audit history.",
)
@click.option("--limit", type=int, help="Maximum number of events to return (1-100, default 20)")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_events(
    monitor_id: str,
    cursor: str | None,
    event_group_id: str | None,
    include_completions: bool,
    limit: int | None,
    output_file: str | None,
    output_json: bool,
):
    """List events for a monitor (newest first).

    MONITOR_ID is the monitor identifier.

    Examples:

        parallel-cli monitor events mon_abc

        parallel-cli monitor events mon_abc --include-completions --limit 50

        parallel-cli monitor events mon_abc --event-group-id egrp_xyz
    """
    try:
        result = list_monitor_events(
            monitor_id,
            cursor=cursor,
            event_group_id=event_group_id,
            include_completions=include_completions,
            limit=limit,
            source="cli",
        )

        write_json_output(result, output_file, output_json)

        if not output_json:
            events = result.get("events", [])
            if not events:
                console.print(f"[yellow]No events found for {monitor_id}.[/yellow]")
                return

            from rich.table import Table

            table = Table(title=f"Events for {monitor_id} ({len(events)})")
            table.add_column("Type", style="green")
            table.add_column("Event Group / ID", style="cyan", no_wrap=True)
            table.add_column("Date", style="dim")
            table.add_column("Summary", max_width=50)

            for ev in events:
                ev_type = ev.get("event_type", "") or ev.get("type", "")
                if ev_type == "event_stream":
                    ev_id = ev.get("event_group_id", "") or ev.get("event_id", "")
                    date = ev.get("event_date", "") or ""
                    output = ev.get("output", {}) or {}
                    if isinstance(output, dict):
                        summary = (output.get("content", "") or str(output))[:50]
                    else:
                        summary = str(output)[:50]
                elif ev_type == "snapshot":
                    ev_id = ev.get("event_group_id", "") or ev.get("event_id", "")
                    date = ev.get("event_date", "") or ""
                    changed = ev.get("changed_output", {}) or {}
                    if isinstance(changed, dict):
                        summary = (changed.get("content", "") or "fields changed")[:50]
                    else:
                        summary = "fields changed"
                elif ev_type == "completion":
                    ev_id = ""
                    date = ev.get("timestamp", "") or ""
                    summary = "Run completed (no change)"
                elif ev_type == "error":
                    ev_id = ""
                    date = ev.get("timestamp", "") or ""
                    summary = (ev.get("error_message", "") or "")[:50]
                else:
                    ev_id = ""
                    date = ""
                    summary = ""
                table.add_row(ev_type, ev_id, date, summary)

            console.print(table)
            if next_cursor := result.get("next_cursor"):
                console.print(f"[dim]Next cursor: {next_cursor}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="trigger")
@click.argument("monitor_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def monitor_trigger(monitor_id: str, output_json: bool):
    """Trigger an immediate one-off run of a monitor.

    The monitor's regular schedule is unaffected. An event is only emitted if a
    material change is detected. Cancelled monitors cannot be triggered.

    MONITOR_ID is the monitor identifier.

    Examples:

        parallel-cli monitor trigger mon_abc
    """
    try:
        trigger_monitor(monitor_id, source="cli")

        if output_json:
            print(json.dumps({"monitor_id": monitor_id, "triggered": True}, indent=2))
        else:
            console.print(f"[bold green]Triggered:[/bold green] {monitor_id}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


# =============================================================================
# Shell Completion
# =============================================================================

SUPPORTED_SHELLS = ["bash", "zsh", "fish"]

# Completion script templates using Click's built-in shell completion.
# These eval the _PARALLEL_CLI_COMPLETE env var at shell startup.
_COMPLETION_SCRIPTS = {
    "bash": 'eval "$(_PARALLEL_CLI_COMPLETE=bash_source parallel-cli)"',
    "zsh": 'eval "$(_PARALLEL_CLI_COMPLETE=zsh_source parallel-cli)"',
    "fish": "_PARALLEL_CLI_COMPLETE=fish_source parallel-cli | source",
}

# Default shell config file paths
_SHELL_CONFIG_FILES = {
    "bash": "~/.bashrc",
    "zsh": "~/.zshrc",
    "fish": "~/.config/fish/config.fish",
}

# Guard comment to prevent duplicate installs
_GUARD_COMMENT = "# parallel-cli shell completion"


def _detect_shell() -> str | None:
    """Detect the current shell from the SHELL environment variable."""
    shell_path = os.environ.get("SHELL", "")
    for shell in SUPPORTED_SHELLS:
        if shell in shell_path:
            return shell
    return None


@main.group()
def completion():
    """Shell completion for bash, zsh, and fish."""
    pass


@completion.command(name="show")
@click.option(
    "--shell",
    "shell_name",
    type=click.Choice(SUPPORTED_SHELLS),
    default=None,
    help="Shell type (auto-detected from $SHELL if not specified)",
)
def completion_show(shell_name: str | None):
    """Print the shell completion script to stdout.

    The output can be saved to a file or eval'd directly:

    \b
        # Add to your shell config
        parallel-cli completion show --shell zsh >> ~/.zshrc

        # Or eval directly
        eval "$(parallel-cli completion show --shell bash)"
    """
    if shell_name is None:
        shell_name = _detect_shell()
        if shell_name is None:
            console.print("[red]Could not detect shell.[/red] Use --shell to specify: bash, zsh, or fish")
            sys.exit(EXIT_BAD_INPUT)

    click.echo(_COMPLETION_SCRIPTS[shell_name])


@completion.command(name="install")
@click.option(
    "--shell",
    "shell_name",
    type=click.Choice(SUPPORTED_SHELLS),
    default=None,
    help="Shell type (auto-detected from $SHELL if not specified)",
)
def completion_install(shell_name: str | None):
    """Install shell completions for the current shell.

    Appends the completion script to your shell config file.
    Safe to run multiple times (won't add duplicates).

    \b
    Examples:
        parallel-cli completion install
        parallel-cli completion install --shell zsh
    """
    if _STANDALONE_MODE:
        console.print(
            "[yellow]Shell completions are not supported in standalone binary mode.[/yellow]\n"
            "Install via pip to use completions: "
            "[cyan]pip install parallel-web-tools[/cyan]"
        )
        sys.exit(EXIT_BAD_INPUT)

    if shell_name is None:
        shell_name = _detect_shell()
        if shell_name is None:
            console.print("[red]Could not detect shell.[/red] Use --shell to specify: bash, zsh, or fish")
            sys.exit(EXIT_BAD_INPUT)

    config_path = os.path.expanduser(_SHELL_CONFIG_FILES[shell_name])
    script_line = _COMPLETION_SCRIPTS[shell_name]
    install_line = f"{_GUARD_COMMENT}\n{script_line}\n"

    # Check for existing installation
    if os.path.exists(config_path):
        with open(config_path) as f:
            content = f.read()
        if _GUARD_COMMENT in content:
            console.print(f"[green]Shell completions already installed[/green] in {config_path}")
            return

    # Append to config file
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "a") as f:
        f.write(f"\n{install_line}")

    console.print("[bold green]Shell completions installed![/bold green]")
    console.print(f"[dim]Added to {config_path}[/dim]")
    console.print(f"\nRestart your shell or run: [cyan]source {config_path}[/cyan]")


if __name__ == "__main__":
    main()
