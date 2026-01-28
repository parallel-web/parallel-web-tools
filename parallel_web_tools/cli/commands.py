"""CLI commands for Parallel."""

import csv
import json
import logging
import os
import sys
import tempfile
from typing import Any

import click
import httpx
from dotenv import load_dotenv
from rich.console import Console

from parallel_web_tools import __version__
from parallel_web_tools.core import (
    AVAILABLE_PROCESSORS,
    JSON_SCHEMA_TYPE_MAP,
    RESEARCH_PROCESSORS,
    create_research_task,
    get_api_key,
    get_auth_status,
    get_research_status,
    get_user_agent,
    logout,
    poll_research,
    run_enrichment_from_dict,
    run_research,
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

load_dotenv(".env.local")

# Source types available for enrich run/plan
# Standalone CLI only supports CSV to minimize bundle size
# DuckDB requires: pip install parallel-web-tools[duckdb]
# BigQuery requires: pip install parallel-web-tools[bigquery]
if _STANDALONE_MODE:
    AVAILABLE_SOURCE_TYPES = ["csv"]
else:
    AVAILABLE_SOURCE_TYPES = ["csv", "duckdb", "bigquery"]


# =============================================================================
# Output Helpers
# =============================================================================


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

    Raises click.Abort with appropriate error messages for invalid combinations.
    """
    if enriched_columns and intent:
        console.print("[bold red]Error: Use either --enriched-columns OR --intent, not both.[/bold red]")
        raise click.Abort()

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
            console.print(f"[bold red]Error: Missing required options: {', '.join(missing)}[/bold red]")
            raise click.Abort()
        if not has_output_spec:
            console.print("[bold red]Error: Provide --enriched-columns OR --intent.[/bold red]")
            raise click.Abort()


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
    columns = list(data[0].keys())
    if not columns:
        raise click.BadParameter("Data objects must have at least one field")

    # Create source_columns with inferred descriptions
    source_columns = [{"name": col, "description": f"The {col} field"} for col in columns]

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
    except Exception:
        pass

    return {
        "enriched_columns": enriched_columns,
        "processor": processor,
        "title": data.get("title", ""),
        "warnings": data.get("warnings", []),
    }


# =============================================================================
# Main CLI Group
# =============================================================================


def _check_for_update_notification():
    """Check for updates and print notification if available.

    Only runs in standalone mode, respects config, and rate-limits to once per day.
    """
    # Import here to avoid slowing down startup when not needed
    from parallel_web_tools.cli.updater import (
        check_for_update_notification,
        should_check_for_updates,
    )

    # should_check_for_updates() handles standalone mode check, config check, and rate limiting
    if not should_check_for_updates():
        return

    try:
        notification = check_for_update_notification(__version__, save_state=True)
        if notification:
            console.print(f"\n[dim]{notification}[/dim]")
    except Exception:
        # Silently ignore errors - don't disrupt user's workflow
        pass


@click.group()
@click.version_option(version=__version__, prog_name="parallel-cli")
def main():
    """Parallel CLI - AI-powered data enrichment and search."""
    pass


@main.result_callback()
def _after_command(*args, **kwargs):
    """Run after any command completes."""
    _check_for_update_notification()


# =============================================================================
# Auth Commands
# =============================================================================


@main.command()
def auth():
    """Check authentication status."""
    status = get_auth_status()

    if status["authenticated"]:
        if status["method"] == "environment":
            console.print("[green]Authenticated via PARALLEL_API_KEY environment variable[/green]")
        else:
            console.print("[green]Authenticated via OAuth[/green]")
            console.print(f"  Credentials: {status['token_file']}")
    else:
        console.print("[yellow]Not authenticated[/yellow]")
        console.print("\n[cyan]To authenticate:[/cyan]")
        console.print("  Run: parallel-cli login")
        console.print("  Or set PARALLEL_API_KEY environment variable")


@main.command()
def login():
    """Authenticate with Parallel API."""
    console.print("[bold cyan]Authenticating with Parallel...[/bold cyan]\n")

    try:
        get_api_key(force_login=True)
        console.print("\n[bold green]Authentication successful![/bold green]")
    except Exception as e:
        console.print(f"[bold red]Authentication failed: {e}[/bold red]")
        raise click.Abort() from None


@main.command(name="logout")
def logout_cmd():
    """Remove stored credentials."""
    if logout():
        console.print("[green]Logged out successfully[/green]")
    else:
        console.print("[yellow]No stored credentials found[/yellow]")


@main.command(name="update")
@click.option("--check", is_flag=True, help="Check for updates without installing")
@click.option("--force", is_flag=True, help="Reinstall even if already at latest version")
def update_cmd(check: bool, force: bool):
    """Update to the latest version (standalone CLI only)."""
    from parallel_web_tools.cli.updater import (
        check_for_update_notification,
        download_and_install_update,
    )

    if not _STANDALONE_MODE:
        console.print("[yellow]Update command is only available for standalone CLI.[/yellow]")
        console.print("\nTo update via pip:")
        console.print("  [cyan]pip install --upgrade parallel-web-tools[/cyan]")
        return

    if check:
        # Don't save state for explicit --check (doesn't reset 24h timer)
        notification = check_for_update_notification(__version__, save_state=False)
        if notification:
            console.print(f"[cyan]{notification}[/cyan]")
        else:
            console.print(f"[green]Already up to date (v{__version__})[/green]")
        return

    if not download_and_install_update(__version__, console, force=force):
        raise click.Abort()


@main.command(name="config")
@click.argument("key", required=False)
@click.argument("value", required=False)
def config_cmd(key: str | None, value: str | None):
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
        console.print("[yellow]Config command is only available for standalone CLI.[/yellow]")
        return

    valid_keys = ["auto-update-check"]

    def format_bool(v: bool) -> str:
        return "on" if v else "off"

    def parse_bool(v: str) -> bool:
        return v.lower() in ("on", "true", "1", "yes")

    # Show all settings
    if key is None:
        console.print("[bold]Configuration:[/bold]")
        console.print(f"  auto-update-check: [cyan]{format_bool(is_auto_update_check_enabled())}[/cyan]")
        return

    if key not in valid_keys:
        console.print(f"[red]Unknown config key: {key}[/red]")
        console.print(f"\nAvailable keys: {', '.join(valid_keys)}")
        raise click.Abort()

    # Show or set the value
    if value is None:
        console.print(f"{key}: [cyan]{format_bool(is_auto_update_check_enabled())}[/cyan]")
    else:
        set_auto_update_check(parse_bool(value))
        console.print(f"[green]Set {key} = {format_bool(is_auto_update_check_enabled())}[/green]")


# =============================================================================
# Search Command
# =============================================================================


@main.command()
@click.argument("objective", required=False)
@click.option("-q", "--query", multiple=True, help="Keyword search query (can be repeated)")
@click.option(
    "--mode", type=click.Choice(["one-shot", "agentic"]), default="agentic", help="Search mode", show_default=True
)
@click.option("--max-results", type=int, default=10, help="Maximum results", show_default=True)
@click.option("--include-domains", multiple=True, help="Only search these domains (comma-separated or repeated)")
@click.option("--exclude-domains", multiple=True, help="Exclude these domains (comma-separated or repeated)")
@click.option("--after-date", help="Only results after this date (YYYY-MM-DD)")
@click.option("--excerpt-max-chars-per-result", type=int, help="Max characters per result for excerpts")
@click.option(
    "--excerpt-max-chars-total", type=int, default=60000, help="Max total characters for excerpts", show_default=True
)
@click.option("--max-age-seconds", type=int, help="Max age in seconds before fetching live content (min 600)")
@click.option("--timeout-seconds", type=float, help="Timeout in seconds for fetching live content")
@click.option("--disable-cache-fallback", is_flag=True, help="Return error instead of stale cached content")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to file (JSON)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def search(
    objective: str | None,
    query: tuple[str, ...],
    mode: str,
    max_results: int,
    include_domains: tuple[str, ...],
    exclude_domains: tuple[str, ...],
    after_date: str | None,
    excerpt_max_chars_per_result: int | None,
    excerpt_max_chars_total: int | None,
    max_age_seconds: int | None,
    timeout_seconds: float | None,
    disable_cache_fallback: bool,
    output_file: str | None,
    output_json: bool,
):
    """Search the web using Parallel's AI-powered search."""
    if not objective and not query:
        console.print("[bold red]Error: Provide an objective or at least one --query.[/bold red]")
        raise click.Abort()

    try:
        from parallel import Parallel

        from parallel_web_tools.core import get_default_headers

        api_key = get_api_key()
        client = Parallel(api_key=api_key, default_headers=get_default_headers("cli"))

        search_kwargs: dict[str, Any] = {"mode": mode, "max_results": max_results}
        if objective:
            search_kwargs["objective"] = objective
        if query:
            search_kwargs["search_queries"] = list(query)

        source_policy: dict[str, Any] = {}
        if include_domains:
            source_policy["include_domains"] = parse_comma_separated(include_domains)
        if exclude_domains:
            source_policy["exclude_domains"] = parse_comma_separated(exclude_domains)
        if after_date:
            source_policy["after_date"] = after_date
        if source_policy:
            search_kwargs["source_policy"] = source_policy

        # Excerpt settings (max_chars_total has a default, so always set)
        excerpts_settings: dict[str, Any] = {"max_chars_total": excerpt_max_chars_total}
        if excerpt_max_chars_per_result is not None:
            excerpts_settings["max_chars_per_result"] = excerpt_max_chars_per_result
        search_kwargs["excerpts"] = excerpts_settings

        # Fetch policy
        fetch_policy: dict[str, Any] = {}
        if max_age_seconds is not None:
            fetch_policy["max_age_seconds"] = max_age_seconds
        if timeout_seconds is not None:
            fetch_policy["timeout_seconds"] = timeout_seconds
        if disable_cache_fallback:
            fetch_policy["disable_cache_fallback"] = True
        if fetch_policy:
            search_kwargs["fetch_policy"] = fetch_policy

        if not output_json:
            console.print("[dim]Searching...[/dim]\n")

        result = client.beta.search(**search_kwargs)

        output_data = {
            "search_id": result.search_id,
            "results": [
                {"url": r.url, "title": r.title, "publish_date": r.publish_date, "excerpts": r.excerpts}
                for r in result.results
            ],
            "warnings": result.warnings if hasattr(result, "warnings") else [],
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
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


# =============================================================================
# Extract Command
# =============================================================================


@main.command()
@click.argument("urls", nargs=-1, required=True)
@click.option("--objective", help="Focus extraction on a specific goal")
@click.option("-q", "--query", multiple=True, help="Keywords to prioritize (can be repeated)")
@click.option("--full-content", is_flag=True, help="Include complete page content")
@click.option("--full-content-max-chars", type=int, help="Max characters per result for full content")
@click.option("--no-excerpts", is_flag=True, help="Exclude excerpts from output")
@click.option("--excerpt-max-chars-per-result", type=int, help="Max characters per result for excerpts (min 1000)")
@click.option("--excerpt-max-chars-total", type=int, help="Max total characters for excerpts across all URLs")
@click.option("--max-age-seconds", type=int, help="Max age in seconds before fetching live content (min 600)")
@click.option("--timeout-seconds", type=float, help="Timeout in seconds for fetching live content")
@click.option("--disable-cache-fallback", is_flag=True, help="Return error instead of stale cached content")
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
    output_file: str | None,
    output_json: bool,
):
    """Extract content from URLs as clean markdown."""
    try:
        from parallel import Parallel

        from parallel_web_tools.core import get_default_headers

        api_key = get_api_key()
        client = Parallel(api_key=api_key, default_headers=get_default_headers("cli"))

        extract_kwargs: dict[str, Any] = {
            "urls": list(urls),
            "betas": ["search-extract-2025-10-10"],
        }

        # Excerpt settings - can be bool or object with settings
        if no_excerpts:
            extract_kwargs["excerpts"] = False
        elif excerpt_max_chars_per_result is not None or excerpt_max_chars_total is not None:
            excerpts_settings: dict[str, Any] = {}
            if excerpt_max_chars_per_result is not None:
                excerpts_settings["max_chars_per_result"] = excerpt_max_chars_per_result
            if excerpt_max_chars_total is not None:
                excerpts_settings["max_chars_total"] = excerpt_max_chars_total
            extract_kwargs["excerpts"] = excerpts_settings
        else:
            extract_kwargs["excerpts"] = True

        # Full content settings - can be bool or object with settings
        if full_content_max_chars is not None:
            extract_kwargs["full_content"] = {"max_chars_per_result": full_content_max_chars}
        else:
            extract_kwargs["full_content"] = full_content

        # Fetch policy
        fetch_policy: dict[str, Any] = {}
        if max_age_seconds is not None:
            fetch_policy["max_age_seconds"] = max_age_seconds
        if timeout_seconds is not None:
            fetch_policy["timeout_seconds"] = timeout_seconds
        if disable_cache_fallback:
            fetch_policy["disable_cache_fallback"] = True
        if fetch_policy:
            extract_kwargs["fetch_policy"] = fetch_policy

        if objective:
            extract_kwargs["objective"] = objective
        if query:
            extract_kwargs["search_queries"] = list(query)

        if not output_json:
            console.print(f"[dim]Extracting content from {len(urls)} URL(s)...[/dim]\n")

        result = client.beta.extract(**extract_kwargs)

        results_list = []
        for r in result.results:
            result_dict: dict[str, Any] = {"url": r.url, "title": r.title}
            if hasattr(r, "excerpts") and r.excerpts:
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
                        "error": str(getattr(e, "error", "")),
                        "status_code": getattr(e, "status_code", None),
                    }
                )

        output_data = {"extract_id": result.extract_id, "results": results_list, "errors": errors_list}

        write_json_output(output_data, output_file, output_json)

        if not output_json:
            if result.errors:
                console.print(f"[yellow]Warning: {len(result.errors)} URL(s) failed[/yellow]\n")

            console.print(f"[bold green]Extracted {len(result.results)} page(s)[/bold green]\n")

            for r in result.results:
                console.print(f"[bold cyan]{r.title}[/bold cyan]")
                console.print(f"[link={r.url}]{r.url}[/link]\n")

                if hasattr(r, "excerpts") and r.excerpts:
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
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


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
                console.print("[bold red]Error: Use --data OR --source, not both.[/bold red]")
                raise click.Abort()
            if source_type and source_type != "csv":
                console.print("[bold red]Error: --data only works with CSV output (--source-type csv).[/bold red]")
                raise click.Abort()

            temp_csv_path, inferred_cols = parse_inline_data(inline_data)
            source = temp_csv_path
            source_type = "csv"

            # Use inferred columns if not explicitly provided
            if not source_columns:
                source_columns = json.dumps(inferred_cols)
                console.print(f"[dim]Inferred {len(inferred_cols)} source column(s) from data[/dim]")

        base_args = [source_type, source, target, source_columns]
        has_cli_args = any(arg is not None for arg in base_args) or enriched_columns or intent

        if config_file and has_cli_args:
            console.print("[bold red]Error: Provide either a config file OR CLI arguments, not both.[/bold red]")
            raise click.Abort()

        if not config_file and not has_cli_args:
            console.print("[bold red]Error: Provide a config file or CLI arguments.[/bold red]")
            raise click.Abort()

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
            console.print(f"[bold cyan]Running enrichment from {config_file}...[/bold cyan]\n")
            run_enrichment(config_file)  # type: ignore[name-defined]
        else:
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
                console.print(f"[green]AI suggested {len(enr_cols)} columns, processor: {final_processor}[/green]\n")
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

            console.print(f"[bold cyan]Running enrichment: {source} -> {target}[/bold cyan]\n")
            run_enrichment_from_dict(config)

        console.print("\n[bold green]Enrichment complete![/bold green]")

    except FileNotFoundError as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None
    except Exception as e:
        console.print(f"[bold red]Error during enrichment: {e}[/bold red]")
        raise
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

        save_config(config, output)  # type: ignore[name-defined]
        console.print(f"[bold green]Configuration saved to {output}[/bold green]")
    else:
        try:
            config = create_config_interactive()  # type: ignore[name-defined]
            save_config(config, output)  # type: ignore[name-defined]
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
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


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
        console.print("[bold red]Error: --project is required for BigQuery deployment.[/bold red]")
        raise click.Abort()
    if system == "snowflake":
        if not account:
            console.print("[bold red]Error: --account is required for Snowflake deployment.[/bold red]")
            raise click.Abort()
        if not user:
            console.print("[bold red]Error: --user is required for Snowflake deployment.[/bold red]")
            raise click.Abort()

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
            console.print(f"[bold red]Deployment failed: {e}[/bold red]")
            raise click.Abort() from None

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
            console.print(f"[bold red]Deployment failed: {e}[/bold red]")
            raise click.Abort() from None


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
@click.option("--no-wait", is_flag=True, help="Return immediately after creating task (don't poll)")
@click.option(
    "-o", "--output", "output_file", type=click.Path(), help="Save results (creates {name}.json and {name}.md)"
)
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def research_run(
    query: str | None,
    input_file: str | None,
    processor: str,
    timeout: int,
    poll_interval: int,
    no_wait: bool,
    output_file: str | None,
    output_json: bool,
):
    """Run deep research on a question or topic.

    QUERY is the research question (max 15,000 chars). Alternatively, use --input-file.

    Examples:

        parallel-cli research run "What are the latest developments in quantum computing?"

        parallel-cli research run -f question.txt --processor ultra -o report
    """
    # Get query from argument or file
    if input_file:
        with open(input_file) as f:
            query = f.read().strip()
    elif not query:
        console.print("[bold red]Error: Provide a query or use --input-file[/bold red]")
        raise click.Abort()

    if len(query) > 15000:
        console.print(f"[yellow]Warning: Query truncated from {len(query)} to 15,000 characters[/yellow]")
        query = query[:15000]

    try:
        if no_wait:
            # Create task and return immediately
            console.print(f"[dim]Creating research task with processor: {processor}...[/dim]")
            result = create_research_task(query, processor=processor, source="cli")

            console.print(f"\n[bold green]Task created: {result['run_id']}[/bold green]")
            console.print(f"Track progress: {result['result_url']}")
            console.print("\n[dim]Use 'parallel-cli research status <run_id>' to check status[/dim]")
            console.print("[dim]Use 'parallel-cli research poll <run_id>' to wait for results[/dim]")

            if output_json:
                print(json.dumps(result, indent=2))
        else:
            # Run and wait for results
            console.print(f"[bold cyan]Starting deep research with processor: {processor}[/bold cyan]")
            console.print(f"[dim]This may take {RESEARCH_PROCESSORS[processor]}[/dim]\n")

            def on_status(status: str, run_id: str):
                if status == "created":
                    console.print(f"[green]Task created: {run_id}[/green]")
                    console.print(
                        f"[dim]Track progress: https://platform.parallel.ai/play/deep-research/{run_id}[/dim]\n"
                    )
                else:
                    console.print(f"[dim]Status: {status}[/dim]")

            result = run_research(
                query,
                processor=processor,
                timeout=timeout,
                poll_interval=poll_interval,
                on_status=on_status,
                source="cli",
            )

            _output_research_result(result, output_file, output_json)

    except TimeoutError as e:
        console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
        console.print("[dim]The task is still running. Use 'parallel-cli research poll <run_id>' to resume.[/dim]")
        raise click.Abort() from None
    except RuntimeError as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


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
            console.print(f"[bold]Status:[/bold] [{status_color}]{status}[/{status_color}]")
            console.print(f"[bold]URL:[/bold] {result['result_url']}")

            if status == "completed":
                console.print("\n[dim]Use 'parallel-cli research poll <run_id>' to retrieve results[/dim]")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


@research.command(name="poll")
@click.argument("run_id")
@click.option("--timeout", type=int, default=3600, show_default=True, help="Max wait time in seconds")
@click.option("--poll-interval", type=int, default=45, show_default=True, help="Seconds between status checks")
@click.option(
    "-o", "--output", "output_file", type=click.Path(), help="Save results (creates {name}.json and {name}.md)"
)
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def research_poll(
    run_id: str,
    timeout: int,
    poll_interval: int,
    output_file: str | None,
    output_json: bool,
):
    """Poll an existing research task until completion.

    RUN_ID is the task identifier (e.g., trun_xxx).
    """
    try:
        console.print(f"[bold cyan]Polling task: {run_id}[/bold cyan]")
        console.print(f"[dim]Track progress: https://platform.parallel.ai/play/deep-research/{run_id}[/dim]\n")

        def on_status(status: str, run_id: str):
            console.print(f"[dim]Status: {status}[/dim]")

        result = poll_research(
            run_id,
            timeout=timeout,
            poll_interval=poll_interval,
            on_status=on_status,
            source="cli",
        )

        _output_research_result(result, output_file, output_json)

    except TimeoutError as e:
        console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
        raise click.Abort() from None
    except RuntimeError as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise click.Abort() from None


@research.command(name="processors")
def research_processors():
    """List available research processors and their characteristics."""
    console.print("[bold]Available Research Processors:[/bold]\n")
    for proc, desc in RESEARCH_PROCESSORS.items():
        console.print(f"  [cyan]{proc:15}[/cyan] {desc}")
    console.print("\n[dim]Use --processor/-p to select a processor[/dim]")


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


def _output_research_result(
    result: dict,
    output_file: str | None,
    output_json: bool,
):
    """Output research result to console and/or files.

    When saving to a file, creates two files from the base name:
    - {name}.json: metadata and citations
    - {name}.md: research content as markdown
    """
    output = result.get("output", {})
    output_data = {
        "run_id": result.get("run_id"),
        "result_url": result.get("result_url"),
        "status": result.get("status"),
        "output": output.copy() if isinstance(output, dict) else output,
    }

    # Save to files if requested
    if output_file:
        from pathlib import Path

        # Strip any extension to get base name
        base_path = Path(output_file)
        if base_path.suffix:
            base_path = base_path.with_suffix("")

        json_path = base_path.with_suffix(".json")
        md_path = base_path.with_suffix(".md")

        # Extract content to markdown file
        if isinstance(output, dict) and "content" in output:
            content = output["content"]
            content_text = _content_to_markdown(content)

            if content_text:
                with open(md_path, "w") as f:
                    f.write(content_text)
                console.print(f"[green]Content saved to:[/green] {md_path}")

                # Replace content in JSON with reference to markdown file
                output_data["output"] = output_data["output"].copy()
                output_data["output"]["content_file"] = md_path.name
                del output_data["output"]["content"]

        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2, default=str)
        console.print(f"[green]Metadata saved to:[/green] {json_path}")

    # Output to console
    if output_json:
        print(json.dumps(output_data, indent=2, default=str))
    else:
        console.print("\n[bold green]Research Complete![/bold green]")
        console.print(f"[dim]Task: {result.get('run_id')}[/dim]")
        console.print(f"[dim]URL: {result.get('result_url')}[/dim]\n")

        # Show summary of output
        output = result.get("output", {})
        if isinstance(output, dict):
            console.print(f"[dim]Output contains {len(output)} fields[/dim]")
            if not output_file:
                console.print("[dim]Use --output to save full JSON to a file, or --json to print to stdout[/dim]")


if __name__ == "__main__":
    main()
