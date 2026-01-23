"""BigQuery processor for data enrichment."""

import logging
import os
from typing import Any

import polars as pl
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.base import Connection

from parallel_web_tools.core import InputSchema, parse_input_and_output_models, run_tasks

logger = logging.getLogger(__name__)


def split_bq_name(fqtn: str) -> tuple[str | None, str | None, str]:
    """Split BigQuery fully qualified table name.

    Accepts:
      table                -> (None, default_dataset?, 'table')   # schema None here
      dataset.table        -> (None, 'dataset', 'table')
      project.dataset.table-> ('project', 'dataset', 'table')

    We only return (project, dataset, table). Use project in engine URL.
    """
    parts = fqtn.split(".")
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(f"Bad BigQuery name: {fqtn}")


def fetch_all(conn: Connection, table: str) -> list[dict[str, Any]]:
    """Fetch all rows from BigQuery table."""
    query = text(f"SELECT * FROM {table}")
    rows = conn.execute(query).mappings().all()
    return [dict(row) for row in rows]


def process_bigquery(schema: InputSchema):
    """Process BigQuery table and enrich data."""
    InputModel, OutputModel = parse_input_and_output_models(schema)

    ENGINE_URL = f"bigquery://{os.getenv('BIGQUERY_PROJECT')}"
    engine = create_engine(ENGINE_URL)

    with engine.begin() as conn:
        data = fetch_all(conn, schema.source)

    output_rows = run_tasks(data, InputModel, OutputModel, schema.processor)
    df = pl.DataFrame(output_rows)

    _project, dataset, table = split_bq_name(schema.target)
    insp = inspect(engine)
    exists = insp.has_table(table_name=table, schema=dataset)

    logger.info(f"Table exists? {exists}")

    df.write_database(
        table_name=schema.target,
        connection=engine,
        engine="sqlalchemy",
        if_table_exists="append",
        engine_options={
            "chunksize": 10_000,  # tune
        },
    )
