"""Interactive planner for creating YAML configuration files."""

import csv
import os
from pathlib import Path
from typing import Any

import duckdb
import questionary
import yaml
from questionary import Style
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from parallel_web_tools.core import get_client
from parallel_web_tools.core.schema import JSON_SCHEMA_TYPE_MAP, get_available_types

# Custom style for questionary
custom_style = Style(
    [
        ("qmark", "fg:#673ab7 bold"),
        ("question", "bold"),
        ("answer", "fg:#f44336 bold"),
        ("pointer", "fg:#673ab7 bold"),
        ("highlighted", "fg:#673ab7 bold"),
        ("selected", "fg:#cc5454"),
        ("separator", "fg:#cc5454"),
        ("instruction", ""),
        ("text", ""),
        ("disabled", "fg:#858585 italic"),
    ]
)

console = Console()


def get_available_processors() -> list[str]:
    """Get available processors."""
    from parallel_web_tools.core.schema import AVAILABLE_PROCESSORS

    return AVAILABLE_PROCESSORS


def suggest_output_columns(source_columns: list[dict[str, str]], user_intent: str) -> list[dict[str, str]] | None:
    """Use the Parallel Ingest API to suggest output columns."""
    try:
        client = get_client()

        input_properties = {}
        for col in source_columns:
            input_properties[col["name"]] = {
                "type": "string",
                "description": col.get("description", ""),
            }

        previous_task = {
            "input_schema": {
                "type": "object",
                "properties": input_properties,
                "required": list(input_properties.keys()),
            }
        }

        response = client.post(
            path="/v1beta/tasks/suggest",
            body={"user_intent": user_intent, "previous_task": previous_task},
            cast_to=dict,
        )

        output_schema = response.get("output_schema", {})
        properties = output_schema.get("properties", {})

        suggested_columns = []
        for name, prop in properties.items():
            col_type = prop.get("type", "string")
            mapped_type = JSON_SCHEMA_TYPE_MAP.get(col_type, "str")
            suggested_columns.append(
                {
                    "name": name,
                    "description": prop.get("description", ""),
                    "type": mapped_type,
                }
            )

        return suggested_columns

    except Exception as e:
        console.print(f"[yellow]Warning: Could not get suggestions: {e}[/yellow]")
        return None


def print_header():
    """Print a beautiful header."""
    header = Text()
    header.append("Parallel Data Enrichment Planner", style="bold magenta")
    console.print(Panel(header, border_style="magenta"))
    console.print()


def get_source_type() -> str:
    """Prompt user to select source type."""
    return questionary.select(
        "What type of data source are you using?",
        choices=[
            questionary.Choice("CSV File", value="csv"),
            questionary.Choice("DuckDB Database", value="duckdb"),
            questionary.Choice("Google BigQuery", value="bigquery"),
        ],
        style=custom_style,
    ).ask()


def get_csv_columns(file_path: str) -> list[str]:
    """Get column names from CSV file."""
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            return list(reader.fieldnames or [])
    except Exception as e:
        console.print(f"[yellow]Warning: Could not read CSV file: {e}[/yellow]")
        return []


def get_duckdb_columns(db_path: str, table_name: str) -> list[str]:
    """Get column names from DuckDB table."""
    try:
        with duckdb.connect(db_path) as con:
            result = con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            ).fetchall()
            return [row[0] for row in result]
    except Exception as e:
        console.print(f"[yellow]Warning: Could not read DuckDB table: {e}[/yellow]")
        return []


def get_bigquery_columns(project: str, table_name: str) -> list[str]:
    """Get column names from BigQuery table."""
    try:
        from sqlalchemy import create_engine, inspect

        engine = create_engine(f"bigquery://{project}")
        insp = inspect(engine)

        parts = table_name.split(".")
        if len(parts) >= 2:
            schema = parts[-2]
            table = parts[-1]
            columns = insp.get_columns(table, schema=schema)
            return [col["name"] for col in columns]
    except Exception as e:
        console.print(f"[yellow]Warning: Could not read BigQuery table: {e}[/yellow]")
    return []


def prompt_for_columns(
    column_names: list[str],
    prompt_text: str,
    allow_new: bool = True,
    prompt_for_type: bool = False,
) -> list[dict[str, str]]:
    """Prompt user to select and describe columns."""
    columns = []
    type_choices = get_available_types()

    if column_names:
        console.print(f"\n[bold cyan]{prompt_text}[/bold cyan]")
        selected = questionary.checkbox(
            "Select columns (use space to select, enter to confirm):",
            choices=column_names,
            style=custom_style,
        ).ask()

        if not selected:
            selected = []
    else:
        selected = []

    for col in selected:
        description = questionary.text(f"Description for '{col}':", style=custom_style).ask()
        col_dict = {"name": col, "description": description}

        if prompt_for_type:
            col_type = questionary.select(
                f"Type for '{col}':",
                choices=type_choices,
                default="str",
                style=custom_style,
            ).ask()
            col_dict["type"] = col_type

        columns.append(col_dict)

    if allow_new:
        while True:
            if columns:
                add_more = questionary.confirm("Add another column?", default=False, style=custom_style).ask()
                if not add_more:
                    break

            col_name = questionary.text("Column name:", style=custom_style).ask()
            col_desc = questionary.text(f"Description for '{col_name}':", style=custom_style).ask()
            col_dict = {"name": col_name, "description": col_desc}

            if prompt_for_type:
                col_type = questionary.select(
                    f"Type for '{col_name}':",
                    choices=type_choices,
                    default="str",
                    style=custom_style,
                ).ask()
                col_dict["type"] = col_type

            columns.append(col_dict)

    return columns


def display_summary(config: dict[str, Any]):
    """Display a summary of the configuration."""
    console.print("\n[bold green]Configuration Summary[/bold green]\n")

    info_table = Table(show_header=False, box=None, padding=(0, 2))
    info_table.add_column(style="cyan bold")
    info_table.add_column()

    info_table.add_row("Source Type:", config["source_type"].upper())
    info_table.add_row("Source:", config["source"])
    info_table.add_row("Target:", config["target"])
    info_table.add_row("Processor:", config.get("processor", "core-fast"))

    console.print(info_table)
    console.print()

    if config["source_columns"]:
        console.print("[bold]Source Columns:[/bold]")
        src_table = Table(show_header=True, header_style="bold magenta")
        src_table.add_column("Column Name", style="cyan")
        src_table.add_column("Description")

        for col in config["source_columns"]:
            src_table.add_row(col["name"], col["description"])

        console.print(src_table)
        console.print()

    if config["enriched_columns"]:
        console.print("[bold]Enriched Columns:[/bold]")
        enr_table = Table(show_header=True, header_style="bold green")
        enr_table.add_column("Column Name", style="green")
        enr_table.add_column("Type", style="yellow")
        enr_table.add_column("Description")

        for col in config["enriched_columns"]:
            col_type = col.get("type", "str")
            enr_table.add_row(col["name"], col_type, col["description"])

        console.print(enr_table)
        console.print()


def create_config_interactive() -> dict[str, Any]:
    """Interactive configuration creation."""
    print_header()

    config: dict[str, Any] = {}

    source_type = get_source_type()
    config["source_type"] = source_type

    if source_type == "csv":
        source_path = questionary.path("Path to source CSV file:", style=custom_style).ask()
        config["source"] = source_path
        target_path = questionary.text(
            "Path to target CSV file:",
            default=source_path.replace(".csv", "_enriched.csv"),
            style=custom_style,
        ).ask()
        config["target"] = target_path
        detected_columns = get_csv_columns(source_path)

    elif source_type == "duckdb":
        db_path = questionary.text(
            "Path to DuckDB database:",
            default=os.getenv("DUCKDB_FILE", "data/file.db"),
            style=custom_style,
        ).ask()
        source_table = questionary.text("Source table name:", style=custom_style).ask()
        config["source"] = source_table
        target_table = questionary.text(
            "Target table name:",
            default=f"{source_table}_enriched",
            style=custom_style,
        ).ask()
        config["target"] = target_table
        detected_columns = get_duckdb_columns(db_path, source_table)

    elif source_type == "bigquery":
        project = questionary.text(
            "Google Cloud Project ID:",
            default=os.getenv("BIGQUERY_PROJECT", ""),
            style=custom_style,
        ).ask()
        source_table = questionary.text(
            "Source table (format: dataset.table or project.dataset.table):",
            style=custom_style,
        ).ask()
        config["source"] = source_table
        target_table = questionary.text(
            "Target table:",
            default=f"{source_table}_enriched",
            style=custom_style,
        ).ask()
        config["target"] = target_table
        detected_columns = get_bigquery_columns(project, source_table)
    else:
        raise NotImplementedError(f"{source_type} not a supported source type")

    if detected_columns:
        console.print(f"\n[bold green]Detected {len(detected_columns)} columns[/bold green]")
        console.print(", ".join(detected_columns[:10]))
        if len(detected_columns) > 10:
            console.print(f"... and {len(detected_columns) - 10} more")

    console.print()
    source_columns = []
    while len(source_columns) == 0:
        source_columns = prompt_for_columns(
            detected_columns,
            "Select source columns to use as input for enrichment:",
            allow_new=True,
        )
        if len(source_columns) == 0:
            console.print("[bold red]Error: At least one source column is required![/bold red]\n")

    config["source_columns"] = source_columns

    console.print()
    console.print("[bold yellow]Now define the new columns you want to enrich your data with.[/bold yellow]")

    use_suggestions = questionary.confirm(
        "Would you like AI to suggest output columns based on your intent?",
        default=True,
        style=custom_style,
    ).ask()

    enriched_columns = []

    if use_suggestions:
        user_intent = questionary.text(
            "Describe what you want to enrich (e.g., 'Find the CEO and company valuation'):",
            style=custom_style,
        ).ask()

        if user_intent:
            console.print("\n[dim]Getting suggestions from Parallel API...[/dim]")
            suggested = suggest_output_columns(source_columns, user_intent)

            if suggested:
                console.print(f"\n[bold green]AI suggested {len(suggested)} output columns:[/bold green]\n")

                suggest_table = Table(show_header=True, header_style="bold green")
                suggest_table.add_column("Column Name", style="green")
                suggest_table.add_column("Type", style="yellow")
                suggest_table.add_column("Description")

                for col in suggested:
                    suggest_table.add_row(col["name"], col.get("type", "str"), col["description"])

                console.print(suggest_table)
                console.print()

                accept_suggestions = questionary.select(
                    "How would you like to proceed?",
                    choices=[
                        questionary.Choice("Accept all suggestions", value="accept"),
                        questionary.Choice("Select which to keep", value="select"),
                        questionary.Choice("Start fresh (ignore suggestions)", value="ignore"),
                    ],
                    style=custom_style,
                ).ask()

                if accept_suggestions == "accept":
                    enriched_columns = suggested
                elif accept_suggestions == "select":
                    selected_names = questionary.checkbox(
                        "Select columns to keep:",
                        choices=[col["name"] for col in suggested],
                        style=custom_style,
                    ).ask()
                    enriched_columns = [col for col in suggested if col["name"] in selected_names]

    if len(enriched_columns) == 0:
        console.print("[dim](At least one enriched column is required)[/dim]\n")

    while len(enriched_columns) == 0:
        enriched_columns = prompt_for_columns([], "Define enriched columns:", allow_new=True, prompt_for_type=True)
        if len(enriched_columns) == 0:
            console.print("[bold red]Error: At least one enriched column is required![/bold red]\n")

    if enriched_columns and use_suggestions:
        add_more = questionary.confirm("Add additional columns?", default=False, style=custom_style).ask()
        if add_more:
            additional = prompt_for_columns([], "Add more columns:", allow_new=True, prompt_for_type=True)
            enriched_columns.extend(additional)

    config["enriched_columns"] = enriched_columns

    console.print()
    console.print("[bold cyan]Select the Parallel API processor to use:[/bold cyan]")
    console.print("[dim](See https://parallel.ai/pricing for details)[/dim]\n")

    processor_choices = get_available_processors()
    processor = questionary.select(
        "Which processor would you like to use?",
        choices=processor_choices,
        default="core-fast",
        style=custom_style,
    ).ask()
    config["processor"] = processor

    display_summary(config)
    return config


def save_config(config: dict[str, Any], output_path: str):
    """Save configuration to YAML file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[bold green]Configuration saved to {output_path}[/bold green]")
    console.print("\n[cyan]Run your enrichment with:[/cyan]")
    console.print(f"[bold]  parallel-cli enrich run {output_path}[/bold]\n")
