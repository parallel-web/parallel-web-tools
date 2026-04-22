"""CLI commands for Parallel."""

import csv
import json
import logging
import os
import sys
import tempfile
import time
from typing import Any

import click
import httpx
from dotenv import load_dotenv
from rich.console import Console

from parallel_web_tools import __version__
from parallel_web_tools.core import (
    AVAILABLE_PROCESSORS,
    FINDALL_GENERATORS,
    JSON_SCHEMA_TYPE_MAP,
    MONITOR_CADENCES,
    MONITOR_EVENT_TYPES,
    RESEARCH_PROCESSORS,
    ReauthenticationRequired,
    cancel_findall_run,
    create_findall_run,
    create_monitor,
    create_research_task,
    delete_monitor,
    enrich_findall,
    extend_findall,
    get_api_key,
    get_auth_status,
    get_control_api_access_token,
    get_findall_result,
    get_findall_schema,
    get_findall_status,
    get_monitor,
    get_monitor_event_group,
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
    simulate_monitor_event,
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
) -> None:
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


def _build_login_hint(login_method: str | None, email: str | None) -> str | None:
    """Format a platform-compatible ``login_hint`` query value.

    Scheme — the hint always names the method only; any email travels as a
    separate top-level query param (see :func:`_login_extra_params`):

    - ``"email"`` → ``login=email`` (requires an email; passed as ``&email=…``)
    - ``"google"`` → ``login=google``
    - ``"sso"`` → ``login=sso`` (requires an email; passed as ``&email=…``)

    Returns ``None`` when ``login_method`` is ``None`` so the caller can
    skip the query param entirely.
    """
    if login_method is None:
        return None
    if login_method in ("email", "sso"):
        if not email:
            raise ValueError(f"login_method={login_method!r} requires an email")
        return f"login={login_method}"
    if login_method == "google":
        return "login=google"
    raise ValueError(f"Unknown login_method: {login_method!r}")


def _login_extra_params(login_method: str | None, email: str | None) -> dict[str, str] | None:
    """Extra query params to append alongside ``login_hint``.

    Returns ``{"email": <email>}`` for identity-bearing methods (``email``
    and ``sso``) so the platform's login page receives the address as a
    top-level param, e.g. ``...&login_hint=login=sso&email=you@example.com``.
    Returns ``None`` for methods that carry no identity (``google``, or
    none at all).
    """
    if login_method in ("email", "sso") and email:
        return {"email": email}
    return None


def _run_login(output_json: bool, email: str | None, login_method: str | None) -> None:
    """Shared body for all ``parallel-cli login`` variants.

    ``login_method`` selects the identity-provider hint and UX flavor:

    - ``None``            → plain device flow: print URL + code, open browser.
    - ``"email"``         → email magic-link: POST ``/api/auth/send-magic-link``,
                            tell the user to check their inbox, do NOT open
                            the browser. Falls back to manual display on
                            magic-link failure.
    - ``"google"``        → append ``login_hint=login=google`` to the URL
                            and open the browser.
    - ``"sso"``           → append ``login_hint=login=sso,e=<email>`` to the
                            URL and open the browser.
    """
    import webbrowser

    from parallel_web_tools.core.auth import (
        _build_verification_uri,
        _ensure_client_id,
        _is_headless,
        send_magic_link,
    )

    login_hint = _build_login_hint(login_method, email)
    extra_params = _login_extra_params(login_method, email)

    if not output_json:
        console.print("[bold cyan]Authenticating with Parallel...[/bold cyan]\n")

    def _on_device_code(info):
        magic_link_sent = False
        magic_link_error: str | None = None
        if login_method == "email" and email:
            try:
                send_magic_link(client_id=_ensure_client_id(), email=email, user_code=info.user_code)
                magic_link_sent = True
            except Exception as e:
                magic_link_error = str(e)

        enriched_uri = _build_verification_uri(info.verification_uri_complete, login_hint, extra_params=extra_params)

        if output_json:
            payload = {
                "status": "waiting_for_authorization",
                "verification_uri": info.verification_uri,
                "verification_uri_complete": enriched_uri,
                "user_code": info.user_code,
                "expires_in": info.expires_in,
            }
            if login_method == "email":
                payload["magic_link_sent"] = magic_link_sent
                if magic_link_error:
                    payload["magic_link_error"] = magic_link_error
            print(json.dumps(payload), flush=True)
            return

        if magic_link_sent:
            # Email login succeeded: tell the user to check their inbox.
            # Still print the URL + code as a fallback in case the mail is
            # slow or lands in spam. Do NOT open the browser.
            console.print(f"[green]Magic link sent to {email}.[/green] Check your inbox to authorize.")
            console.print(
                f"\nOr visit [bold cyan]{info.verification_uri}[/bold cyan] "
                f"and enter code [bold yellow]{info.user_code}[/bold yellow]."
            )
            console.print("Waiting for authorization...")
            return

        if magic_link_error:
            console.print(
                f"[yellow]Could not send magic link ({magic_link_error}); "
                "falling back to manual authorization.[/yellow]\n"
            )

        console.print(f"Visit: [bold cyan]{info.verification_uri}[/bold cyan]")
        console.print(f"Enter code: [bold yellow]{info.user_code}[/bold yellow]\n")
        console.print(f"Or open: [link={enriched_uri}]{enriched_uri}[/link]\n")
        console.print("Confirm the code matches what your browser shows, then authorize.")
        console.print("Waiting for authorization...")

        # Providing an on_device_code callback suppresses auth.py's default
        # browser-launch branch, so open it here for interactive CLI use.
        if not _is_headless():
            try:
                webbrowser.open(enriched_uri)
            except Exception:
                pass

    try:
        get_api_key(force_login=True, on_device_code=_on_device_code, login_hint=login_hint)
        if output_json:
            print(json.dumps({"status": "authenticated"}))
        else:
            console.print("\n[bold green]Authentication successful![/bold green]")
    except Exception as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_AUTH_ERROR, prefix="Authentication failed")


@main.group(invoke_without_command=True)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def login(ctx: click.Context, output_json: bool):
    """Authenticate with Parallel API (device authorization flow).

    \b
    Examples:
      parallel-cli login                         # opens browser for SSO
      parallel-cli login email you@example.com   # sends a magic-link email
      parallel-cli login google                  # opens browser, hints Google SSO
      parallel-cli login sso you@example.com     # opens browser, hints SSO + email
    """
    ctx.ensure_object(dict)
    ctx.obj["output_json"] = output_json
    if ctx.invoked_subcommand is None:
        _run_login(output_json=output_json, email=None, login_method=None)


@login.command("email")
@click.argument("user_email")
@click.pass_context
def login_email(ctx: click.Context, user_email: str):
    """Send a magic-link email to USER_EMAIL that auto-confirms the CLI's device code.

    No browser is opened — the link in the email handles authorization. If the
    email can't be sent, the CLI falls back to printing the URL and code for
    manual entry.
    """
    output_json = ctx.obj.get("output_json", False) if ctx.obj else False
    _run_login(output_json=output_json, email=user_email, login_method="email")


@login.command("google")
@click.pass_context
def login_google(ctx: click.Context):
    """Authenticate via Google SSO.

    Opens the browser on a verification URL that hints ``login=google`` so the
    landing page auto-routes to Google's SSO (and auto-submits where it can
    if the user is already signed in).
    """
    output_json = ctx.obj.get("output_json", False) if ctx.obj else False
    _run_login(output_json=output_json, email=None, login_method="google")


@login.command("sso")
@click.argument("user_email")
@click.pass_context
def login_sso(ctx: click.Context, user_email: str):
    """Authenticate via enterprise SSO for USER_EMAIL.

    Opens the browser on a verification URL that hints ``login=sso,e=<email>``
    so the landing page resolves the right SSO tenant for the email domain
    and pre-fills the address.
    """
    output_json = ctx.obj.get("output_json", False) if ctx.obj else False
    _run_login(output_json=output_json, email=user_email, login_method="sso")


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


def _format_cents(cents: int | float) -> str:
    """Render a cents amount as ``$X.YZ (N¢)``."""
    return f"${cents / 100:.2f} ({int(cents)}¢)"


def _derive_idempotency_key(amount_cents: int) -> str:
    """Build a deterministic idempotency key for ``balance add``.

    Format: ``{client_id}-{amount_cents}-{five_min_bucket}``, where
    ``five_min_bucket`` is the current unix time rounded down to the nearest
    300 seconds. Identical repeat requests inside the same 5-minute window
    reuse the same key, so Stripe's idempotency dedupes them server-side.
    """
    from parallel_web_tools.core.auth import _ensure_client_id

    client_id = _ensure_client_id()
    five_min_bucket = int(time.time() // 300) * 300
    return f"{client_id}-{amount_cents}-{five_min_bucket}"


def _render_balance(resp, output_json: bool, *, prefix_lines: list[str] | None = None) -> None:
    """Render a :class:`BalanceResponse` in JSON or Rich-console form."""
    if output_json:
        print(json.dumps(resp.model_dump(), indent=2))
        return

    for line in prefix_lines or []:
        console.print(line)
    console.print(f"Organization: [cyan]{resp.org_id}[/cyan]")
    console.print(f"Credit balance: [bold green]{_format_cents(resp.credit_balance_cents)}[/bold green]")
    pending = resp.pending_debit_balance_cents or 0
    if pending:
        console.print(f"Pending debit:  [yellow]{_format_cents(pending)}[/yellow]")
    if resp.will_invoice:
        console.print("[dim]Billed by invoice (postpaid)[/dim]")


@main.group(name="balance")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.pass_context
def balance(ctx: click.Context, output_json: bool):
    """Inspect or top up the org's prepaid credit balance."""
    ctx.ensure_object(dict)
    ctx.obj["output_json"] = output_json


@balance.command("get")
@click.pass_context
def balance_get(ctx: click.Context):
    """Show the current credit balance."""
    from parallel_web_tools.core import service

    output_json = ctx.obj.get("output_json", False) if ctx.obj else False
    try:
        token = get_control_api_access_token()
        resp = service.get_balance(token)
    except ReauthenticationRequired as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_AUTH_ERROR, prefix="Authentication required")
        return
    except Exception as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_API_ERROR, prefix="Balance API error")
        return

    _render_balance(resp, output_json)


@balance.command("add")
@click.argument("amount_cents", type=int)
@click.option(
    "--idempotency-key",
    "idempotency_key_override",
    default=None,
    help="Override the auto-derived idempotency key. Defaults to "
    "{client_id}-{amount_cents}-{5min_bucket} so repeat attempts inside "
    "the same 5-minute window dedupe server-side.",
)
@click.pass_context
def balance_add(ctx: click.Context, amount_cents: int, idempotency_key_override: str | None):
    """Charge and top up the prepaid balance by AMOUNT_CENTS."""
    from parallel_web_tools.core import service

    output_json = ctx.obj.get("output_json", False) if ctx.obj else False
    idempotency_key = idempotency_key_override or _derive_idempotency_key(amount_cents)
    try:
        token = get_control_api_access_token()
        resp = service.add_balance(token, amount_cents, idempotency_key)
    except ReauthenticationRequired as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_AUTH_ERROR, prefix="Authentication required")
        return
    except Exception as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_API_ERROR, prefix="Balance API error")
        return

    _render_balance(
        resp,
        output_json,
        prefix_lines=[f"[green]Added {_format_cents(amount_cents)} to balance.[/green]"],
    )


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


# =============================================================================
# Search Command
# =============================================================================


@main.command()
@click.argument("objective", required=False)
@click.option("-q", "--query", multiple=True, help="Keyword search query (can be repeated)")
@click.option(
    "--mode",
    type=click.Choice(["one-shot", "agentic", "fast"]),
    default="fast",
    help="Search mode",
    show_default=True,
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

    try:
        from parallel_web_tools.core.auth import get_client

        client = get_client(source="cli")

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
            "status": "ok",
            "results": [
                {"url": r.url, "title": r.title, "publish_date": r.publish_date, "excerpts": r.excerpts}
                for r in result.results
            ],
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
        from parallel_web_tools.core.auth import get_client

        client = get_client(source="cli")

        extract_kwargs: dict[str, Any] = {
            "urls": list(urls),
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
            result_dict: dict[str, Any] = {"url": r.url, "title": r.title, "publish_date": r.publish_date}
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
                        "error_type": getattr(e, "error_type", None),
                        "http_status_code": getattr(e, "http_status_code", None),
                        "content": getattr(e, "content", None),
                    }
                )

        output_data = {
            "extract_id": result.extract_id,
            "status": "ok",
            "results": results_list,
            "errors": errors_list,
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
@click.option("--no-wait", is_flag=True, help="Return immediately after creating task (don't poll)")
@click.option("--dry-run", is_flag=True, help="Show what would be executed without making API calls")
@click.option(
    "-o", "--output", "output_file", type=click.Path(), help="Save results (creates {name}.json and {name}.md)"
)
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
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
    output_file: str | None,
    output_json: bool,
    previous_interaction_id: str | None,
):
    """Run deep research on a question or topic.

    QUERY is the research question (max 15,000 chars). Alternatively, use --input-file
    or pass "-" as QUERY to read from stdin.

    Use --previous-interaction-id to continue research from a prior task's context.
    The interaction ID is shown in the output of every research run.

    Examples:

        parallel-cli research run "What are the latest developments in quantum computing?"

        parallel-cli research run -f question.txt --processor ultra -o report

        echo "My research question" | parallel-cli research run - --json

        # Follow-up research using context from a previous task:
        parallel-cli research run "What are the implications?" --previous-interaction-id trun_abc123
    """
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
        dry_run_data = {
            "dry_run": True,
            "query": query[:200] + "..." if len(query) > 200 else query,
            "query_length": len(query),
            "processor": processor,
            "expected_latency": RESEARCH_PROCESSORS[processor],
        }
        if output_json:
            print(json.dumps(dry_run_data, indent=2))
        else:
            console.print("[bold]Dry run — no API calls will be made[/bold]\n")
            console.print(f"  [bold]Query:[/bold]     {dry_run_data['query']}")
            console.print(f"  [bold]Length:[/bold]    {len(query)} chars")
            console.print(f"  [bold]Processor:[/bold] {processor}")
            console.print(f"  [bold]Latency:[/bold]   {RESEARCH_PROCESSORS[processor]}")
        return

    try:
        if no_wait:
            # Create task and return immediately
            if not output_json:
                console.print(f"[dim]Creating research task with processor: {processor}...[/dim]")
            result = create_research_task(
                query, processor=processor, source="cli", previous_interaction_id=previous_interaction_id
            )

            if not output_json:
                console.print(f"\n[bold green]Task created: {result['run_id']}[/bold green]")
                if result.get("interaction_id"):
                    console.print(f"Interaction ID: {result['interaction_id']}")
                console.print(f"Track progress: {result['result_url']}")
                console.print("\n[dim]Use 'parallel-cli research status <run_id>' to check status[/dim]")
                console.print("[dim]Use 'parallel-cli research poll <run_id>' to wait for results[/dim]")
                console.print("[dim]Use '--previous-interaction-id' on a new run to continue this research[/dim]")

            if output_json:
                print(json.dumps(result, indent=2))
        else:
            # Run and wait for results
            if not output_json:
                console.print(f"[bold cyan]Starting deep research with processor: {processor}[/bold cyan]")
                console.print(f"[dim]This may take {RESEARCH_PROCESSORS[processor]}[/dim]\n")

            start_time = time.time()

            def on_status(status: str, run_id: str):
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
            )

            _output_research_result(result, output_file, output_json)

    except TimeoutError as e:
        if output_json:
            error_data = {"error": {"message": str(e), "type": "TimeoutError"}}
            print(json.dumps(error_data, indent=2))
        else:
            console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
            console.print("[dim]The task is still running. Use 'parallel-cli research poll <run_id>' to resume.[/dim]")
        sys.exit(EXIT_TIMEOUT)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
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
        if not output_json:
            console.print(f"[bold cyan]Polling task: {run_id}[/bold cyan]")
            console.print(f"[dim]Track progress: https://platform.parallel.ai/play/deep-research/{run_id}[/dim]\n")

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

        _output_research_result(result, output_file, output_json)

    except TimeoutError as e:
        if output_json:
            error_data = {"error": {"message": str(e), "type": "TimeoutError"}}
            print(json.dumps(error_data, indent=2))
        else:
            console.print(f"[bold yellow]Timeout: {e}[/bold yellow]")
        sys.exit(EXIT_TIMEOUT)
    except RuntimeError as e:
        _handle_error(e, output_json=output_json)
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
        "interaction_id": result.get("interaction_id"),
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
        console.print(f"[dim]Interaction ID: {result.get('interaction_id')}[/dim]")
        console.print(f"[dim]URL: {result.get('result_url')}[/dim]\n")

        # Show executive summary if available
        output = result.get("output", {})
        content = output.get("content") if isinstance(output, dict) else None
        summary = _extract_executive_summary(content) if content else None

        if summary:
            from rich.markdown import Markdown
            from rich.panel import Panel

            console.print(Panel(Markdown(summary), title="Executive Summary", border_style="cyan"))
            console.print()

        if not output_file:
            console.print("[dim]Use --output to save full results to a file, or --json to print to stdout[/dim]")
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
@click.argument("query")
@click.option(
    "--cadence",
    "-c",
    type=click.Choice(list(MONITOR_CADENCES.keys())),
    default="daily",
    show_default=True,
    help="How often to check for changes",
)
@click.option("--webhook", help="Webhook URL for event delivery")
@click.option("--metadata", "metadata_json", help="Metadata as JSON string")
@click.option("--output-schema", "output_schema_json", help="Output schema as JSON string")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save result to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_create(
    query: str,
    cadence: str,
    webhook: str | None,
    metadata_json: str | None,
    output_schema_json: str | None,
    output_file: str | None,
    output_json: bool,
):
    """Create a new monitor to track the web for changes.

    QUERY is a natural language description of what to track.

    Examples:

        parallel-cli monitor create "Track price changes for iPhone 16"

        parallel-cli monitor create "New AI funding announcements" --cadence hourly

        parallel-cli monitor create "SEC filings from Tesla" --webhook https://example.com/hook
    """
    try:
        metadata = json.loads(metadata_json) if metadata_json else None
        output_schema = json.loads(output_schema_json) if output_schema_json else None
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        if not output_json:
            console.print(f"[dim]Creating monitor with cadence={cadence}...[/dim]")

        result = create_monitor(
            query=query,
            cadence=cadence,
            webhook=webhook,
            metadata=metadata,
            output_schema=output_schema,
            source="cli",
        )

        write_json_output(result, output_file, output_json)

        if not output_json:
            monitor_id = result.get("monitor_id", "unknown")
            console.print(f"\n[bold green]Monitor created: {monitor_id}[/bold green]")
            console.print(f"[dim]Query: {query}[/dim]")
            console.print(f"[dim]Cadence: {cadence} ({MONITOR_CADENCES[cadence]})[/dim]")
            if webhook:
                console.print(f"[dim]Webhook: {webhook}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="list")
@click.option("--limit", "-n", type=int, help="Maximum number of monitors to return")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_list(limit: int | None, output_json: bool):
    """List all monitors.

    Examples:

        parallel-cli monitor list

        parallel-cli monitor list --limit 10 --json
    """
    try:
        result = list_monitors(limit=limit, source="cli")

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            if not result:
                console.print("[yellow]No monitors found.[/yellow]")
                return

            from rich.table import Table

            table = Table(title=f"Monitors ({len(result)})")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Query", max_width=50)
            table.add_column("Cadence", style="green")
            table.add_column("Status", style="yellow")

            for m in result:
                table.add_row(
                    m.get("monitor_id", ""),
                    (m.get("query", "") or "")[:50],
                    m.get("cadence", ""),
                    m.get("status", ""),
                )

            console.print(table)

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
            console.print(f"[bold]Monitor:[/bold]  {result.get('monitor_id', monitor_id)}")
            console.print(f"[bold]Query:[/bold]    {result.get('query', '')}")
            console.print(f"[bold]Cadence:[/bold]  {result.get('cadence', '')}")
            console.print(f"[bold]Status:[/bold]   {result.get('status', '')}")
            if result.get("webhook"):
                console.print(f"[bold]Webhook:[/bold]  {result['webhook']}")
            if result.get("created_at"):
                console.print(f"[bold]Created:[/bold]  {result['created_at']}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="update")
@click.argument("monitor_id")
@click.option("--query", "-q", help="Updated query text")
@click.option("--cadence", "-c", type=click.Choice(list(MONITOR_CADENCES.keys())), help="Updated cadence")
@click.option("--webhook", help="Updated webhook URL")
@click.option("--metadata", "metadata_json", help="Updated metadata as JSON string")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_update(
    monitor_id: str,
    query: str | None,
    cadence: str | None,
    webhook: str | None,
    metadata_json: str | None,
    output_json: bool,
):
    """Update an existing monitor.

    MONITOR_ID is the monitor identifier.

    Examples:

        parallel-cli monitor update mon_abc --cadence hourly

        parallel-cli monitor update mon_abc --query "Updated tracking query"
    """
    if not any([query, cadence, webhook, metadata_json]):
        _handle_error(
            click.UsageError("Provide at least one field to update (--query, --cadence, --webhook, --metadata)"),
            output_json=output_json,
            exit_code=EXIT_BAD_INPUT,
        )
        return

    try:
        metadata = json.loads(metadata_json) if metadata_json else None
    except json.JSONDecodeError as e:
        _handle_error(e, output_json=output_json, exit_code=EXIT_BAD_INPUT, prefix="Invalid JSON")
        return

    try:
        result = update_monitor(
            monitor_id=monitor_id,
            query=query,
            cadence=cadence,
            webhook=webhook,
            metadata=metadata,
            source="cli",
        )

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Monitor updated: {monitor_id}[/bold green]")
            if query:
                console.print(f"[dim]Query: {query}[/dim]")
            if cadence:
                console.print(f"[dim]Cadence: {cadence}[/dim]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="delete")
@click.argument("monitor_id")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def monitor_delete(monitor_id: str, output_json: bool):
    """Delete a monitor.

    MONITOR_ID is the monitor identifier.
    """
    try:
        result = delete_monitor(monitor_id, source="cli")

        if output_json:
            print(json.dumps(result, indent=2, default=str))
        else:
            console.print(f"[bold green]Deleted:[/bold green] {monitor_id}")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="events")
@click.argument("monitor_id")
@click.option(
    "--lookback", default="10d", show_default=True, help="Lookback period using d (days) or w (weeks), e.g., 10d, 1w"
)
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save results to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_events(monitor_id: str, lookback: str, output_file: str | None, output_json: bool):
    """List events for a monitor.

    MONITOR_ID is the monitor identifier.

    Examples:

        parallel-cli monitor events mon_abc

        parallel-cli monitor events mon_abc --lookback 3d --json
    """
    try:
        result = list_monitor_events(monitor_id, lookback_period=lookback, source="cli")

        write_json_output(result, output_file, output_json)

        if not output_json:
            events = result.get("events", [])
            if not events:
                console.print(f"[yellow]No events found for {monitor_id} in the last {lookback}.[/yellow]")
                return

            from rich.table import Table

            table = Table(title=f"Events for {monitor_id} ({len(events)})")
            table.add_column("Type", style="green")
            table.add_column("Event Group / ID", style="cyan", no_wrap=True)
            table.add_column("Date", style="dim")
            table.add_column("Summary", max_width=50)

            for ev in events:
                ev_type = ev.get("type", "")
                if ev_type == "event":
                    ev_id = ev.get("event_group_id", "")
                    date = ev.get("event_date", "")
                    summary = (ev.get("output", "") or "")[:50]
                elif ev_type == "completion":
                    ev_id = ev.get("monitor_ts", "")
                    date = ""
                    summary = "Run completed"
                elif ev_type == "error":
                    ev_id = ev.get("id", "")
                    date = ev.get("date", "")
                    summary = (ev.get("error", "") or "")[:50]
                else:
                    ev_id = ""
                    date = ""
                    summary = ""
                table.add_row(ev_type, ev_id, date, summary)

            console.print(table)

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="event-group")
@click.argument("monitor_id")
@click.argument("event_group_id")
@click.option("-o", "--output", "output_file", type=click.Path(), help="Save result to JSON file")
@click.option("--json", "output_json", is_flag=True, help="Output JSON to stdout")
def monitor_event_group(monitor_id: str, event_group_id: str, output_file: str | None, output_json: bool):
    """Get details of an event group.

    MONITOR_ID is the monitor identifier.
    EVENT_GROUP_ID is the event group identifier.
    """
    try:
        result = get_monitor_event_group(monitor_id, event_group_id, source="cli")

        write_json_output(result, output_file, output_json)

        if not output_json:
            console.print(f"[bold]Monitor:[/bold]     {monitor_id}")
            console.print(f"[bold]Event Group:[/bold] {event_group_id}")
            events = result.get("events", [])
            if events:
                console.print(f"\n[bold]Events ({len(events)}):[/bold]")
                for ev in events:
                    date = ev.get("event_date", "")
                    output = ev.get("output", "")
                    urls = ev.get("source_urls", [])
                    console.print(f"  [dim]{date}[/dim] {output}")
                    for u in urls:
                        console.print(f"    [cyan]{u}[/cyan]")
            else:
                console.print("[yellow]No events in this group.[/yellow]")

    except Exception as e:
        _handle_error(e, output_json=output_json)


@monitor.command(name="simulate")
@click.argument("monitor_id")
@click.option(
    "--event-type",
    type=click.Choice(MONITOR_EVENT_TYPES),
    help="Event type to simulate (default: monitor.event.detected)",
)
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def monitor_simulate(monitor_id: str, event_type: str | None, output_json: bool):
    """Simulate an event for webhook testing.

    Requires a webhook to be configured on the monitor.

    MONITOR_ID is the monitor identifier.

    Examples:

        parallel-cli monitor simulate mon_abc

        parallel-cli monitor simulate mon_abc --event-type monitor.execution.completed
    """
    try:
        simulate_monitor_event(monitor_id, event_type=event_type, source="cli")

        if output_json:
            print(json.dumps({"monitor_id": monitor_id, "simulated": True}, indent=2))
        else:
            console.print(f"[bold green]Event simulated for:[/bold green] {monitor_id}")
            if event_type:
                console.print(f"[dim]Event type: {event_type}[/dim]")

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
