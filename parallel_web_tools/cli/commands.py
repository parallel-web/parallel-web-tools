"""CLI commands for Parallel."""

import json
import logging
import os
from typing import Any

import click
import httpx
from dotenv import load_dotenv
from rich.console import Console

from parallel_web_tools import __version__
from parallel_web_tools.cli.planner import create_config_interactive, save_config
from parallel_web_tools.core import (
    AVAILABLE_PROCESSORS,
    JSON_SCHEMA_TYPE_MAP,
    get_api_key,
    get_auth_status,
    logout,
    run_enrichment,
    run_enrichment_from_dict,
)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
console = Console()

load_dotenv(".env.local")


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


def suggest_from_intent(
    intent: str,
    source_columns: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Use Parallel Ingest API to suggest output columns and processor."""
    api_key = get_api_key()
    base_url = "https://api.parallel.ai"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}

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


@click.group()
@click.version_option(version=__version__, prog_name="parallel-cli")
def main():
    """Parallel CLI - AI-powered data enrichment and search."""
    pass


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


# =============================================================================
# Search Command
# =============================================================================


@main.command()
@click.argument("objective", required=False)
@click.option("-q", "--query", multiple=True, help="Keyword search query (can be repeated)")
@click.option(
    "--mode", type=click.Choice(["one-shot", "agentic"]), default="one-shot", help="Search mode", show_default=True
)
@click.option("--max-results", type=int, default=10, help="Maximum results", show_default=True)
@click.option("--include-domains", multiple=True, help="Only search these domains")
@click.option("--exclude-domains", multiple=True, help="Exclude these domains")
@click.option("--after-date", help="Only results after this date (YYYY-MM-DD)")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def search(
    objective: str | None,
    query: tuple[str, ...],
    mode: str,
    max_results: int,
    include_domains: tuple[str, ...],
    exclude_domains: tuple[str, ...],
    after_date: str | None,
    output_json: bool,
):
    """Search the web using Parallel's AI-powered search."""
    if not objective and not query:
        console.print("[bold red]Error: Provide an objective or at least one --query.[/bold red]")
        raise click.Abort()

    try:
        from parallel import Parallel

        api_key = get_api_key()
        client = Parallel(api_key=api_key)

        search_kwargs: dict[str, Any] = {"mode": mode, "max_results": max_results}
        if objective:
            search_kwargs["objective"] = objective
        if query:
            search_kwargs["search_queries"] = list(query)

        source_policy: dict[str, Any] = {}
        if include_domains:
            source_policy["include_domains"] = list(include_domains)
        if exclude_domains:
            source_policy["exclude_domains"] = list(exclude_domains)
        if after_date:
            source_policy["after_date"] = after_date
        if source_policy:
            search_kwargs["source_policy"] = source_policy

        if not output_json:
            console.print("[dim]Searching...[/dim]\n")

        result = client.beta.search(**search_kwargs)

        if output_json:
            output = {
                "search_id": result.search_id,
                "results": [
                    {"url": r.url, "title": r.title, "publish_date": r.publish_date, "excerpts": r.excerpts}
                    for r in result.results
                ],
                "warnings": result.warnings if hasattr(result, "warnings") else [],
            }
            print(json.dumps(output, indent=2))
        else:
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
@click.option("--no-excerpts", is_flag=True, help="Exclude excerpts from output")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def extract(
    urls: tuple[str, ...],
    objective: str | None,
    query: tuple[str, ...],
    full_content: bool,
    no_excerpts: bool,
    output_json: bool,
):
    """Extract content from URLs as clean markdown."""
    try:
        from parallel import Parallel

        api_key = get_api_key()
        client = Parallel(api_key=api_key)

        extract_kwargs: dict[str, Any] = {
            "urls": list(urls),
            "betas": ["search-extract-2025-10-10"],
            "excerpts": not no_excerpts,
            "full_content": full_content,
        }

        if objective:
            extract_kwargs["objective"] = objective
        if query:
            extract_kwargs["search_queries"] = list(query)

        if not output_json:
            console.print(f"[dim]Extracting content from {len(urls)} URL(s)...[/dim]\n")

        result = client.beta.extract(**extract_kwargs)

        if output_json:
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

            output = {"extract_id": result.extract_id, "results": results_list, "errors": errors_list}
            print(json.dumps(output, indent=2))
        else:
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


# =============================================================================
# Enrich Command Group
# =============================================================================


@main.group()
def enrich():
    """Data enrichment commands."""
    pass


@enrich.command(name="run")
@click.argument("config_file", required=False)
@click.option("--source-type", type=click.Choice(["csv", "duckdb", "bigquery"]), help="Data source type")
@click.option("--source", help="Source file path or table name")
@click.option("--target", help="Target file path or table name")
@click.option("--source-columns", help="Source columns as JSON")
@click.option("--enriched-columns", help="Enriched columns as JSON")
@click.option("--intent", help="Natural language description (AI suggests columns)")
@click.option("--processor", type=click.Choice(AVAILABLE_PROCESSORS), help="Processor to use")
def enrich_run(
    config_file: str | None,
    source_type: str | None,
    source: str | None,
    target: str | None,
    source_columns: str | None,
    enriched_columns: str | None,
    intent: str | None,
    processor: str | None,
):
    """Run data enrichment from YAML config or CLI arguments."""
    base_args = [source_type, source, target, source_columns]
    has_cli_args = any(arg is not None for arg in base_args) or enriched_columns or intent

    if config_file and has_cli_args:
        console.print("[bold red]Error: Provide either a config file OR CLI arguments, not both.[/bold red]")
        raise click.Abort()

    if not config_file and not has_cli_args:
        console.print("[bold red]Error: Provide a config file or CLI arguments.[/bold red]")
        raise click.Abort()

    if has_cli_args:
        validate_enrich_args(source_type, source, target, source_columns, enriched_columns, intent)

    try:
        if config_file:
            console.print(f"[bold cyan]Running enrichment from {config_file}...[/bold cyan]\n")
            run_enrichment(config_file)
        else:
            src_cols = parse_columns(source_columns)

            if intent:
                console.print("[dim]Getting suggestions from Parallel API...[/dim]")
                suggestion = suggest_from_intent(intent, src_cols)
                enr_cols = suggestion["enriched_columns"]
                final_processor = processor or suggestion["processor"]
                console.print(f"[green]AI suggested {len(enr_cols)} columns, processor: {final_processor}[/green]\n")
            else:
                enr_cols = parse_columns(enriched_columns)
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


@enrich.command(name="plan")
@click.option("-o", "--output", default="config.yaml", help="Output YAML file path", show_default=True)
@click.option("--source-type", type=click.Choice(["csv", "duckdb", "bigquery"]), help="Data source type")
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
    """Create an enrichment configuration file."""
    base_args = [source_type, source, target, source_columns]
    has_cli_args = any(arg is not None for arg in base_args) or enriched_columns or intent

    if has_cli_args:
        validate_enrich_args(source_type, source, target, source_columns, enriched_columns, intent)
        src_cols = parse_columns(source_columns)

        if intent:
            console.print("[dim]Getting suggestions from Parallel API...[/dim]")
            suggestion = suggest_from_intent(intent, src_cols)
            enr_cols = suggestion["enriched_columns"]
            final_processor = processor or suggestion["processor"]
            console.print(f"[green]AI suggested {len(enr_cols)} columns, processor: {final_processor}[/green]")
        else:
            enr_cols = parse_columns(enriched_columns)
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


@enrich.command(name="deploy")
@click.option("--system", type=click.Choice(["bigquery"]), required=True, help="Target system to deploy to")
@click.option("--project", "-p", help="Cloud project ID (required for bigquery)")
@click.option("--region", "-r", default="us-central1", show_default=True, help="Cloud region")
@click.option("--api-key", "-k", help="Parallel API key (or use PARALLEL_API_KEY env var)")
@click.option("--dataset", default="parallel_functions", show_default=True, help="Dataset name (BigQuery)")
def enrich_deploy(system: str, project: str | None, region: str, api_key: str | None, dataset: str):
    """Deploy Parallel enrichment to a cloud system."""
    if system == "bigquery":
        if not project:
            console.print("[bold red]Error: --project is required for BigQuery deployment.[/bold red]")
            raise click.Abort()

        from parallel_web_tools.integrations.bigquery import deploy_bigquery_integration

        if not api_key:
            api_key = os.environ.get("PARALLEL_API_KEY")
        if not api_key:
            try:
                api_key = get_api_key()
            except Exception:
                pass
        if not api_key:
            console.print("[bold red]Error: Parallel API key required[/bold red]")
            console.print("  Use --api-key, PARALLEL_API_KEY env var, or run 'parallel-cli login'")
            raise click.Abort()

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


if __name__ == "__main__":
    main()
