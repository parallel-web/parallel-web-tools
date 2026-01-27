"""
Parallel Spark Structured Streaming Integration

This module provides functions for enriching streaming data in Spark Structured Streaming
using the Parallel Web Systems Task API. It supports micro-batch processing with
automatic batching for efficient enrichment of streaming records.

Example Usage:
    from pyspark.sql import SparkSession
    from parallel_web_tools.integrations.spark import enrich_streaming_batch

    spark = SparkSession.builder.appName("StreamingEnrichment").getOrCreate()

    # Read from streaming source
    stream_df = spark.readStream.format("rate").load()

    # Define enrichment function for foreachBatch
    def process_batch(batch_df, batch_id):
        enriched_df = enrich_streaming_batch(
            batch_df,
            input_columns={"company_name": "company", "website": "url"},
            output_columns=["CEO name", "founding year", "brief description"],
            processor="lite-fast"
        )
        enriched_df.write.format("parquet").mode("append").save("/path/to/output")

    # Write enriched stream
    query = stream_df.writeStream.foreachBatch(process_batch).start()
    query.awaitTermination()
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import udf as spark_udf
from pyspark.sql.types import StringType

from parallel_web_tools.core import build_output_schema
from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.user_agent import get_default_headers, get_user_agent


def _stream_task_run_events(
    client,
    run_id: str,
    on_progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    """
    Stream SSE events for a task run in a background thread.

    Args:
        client: Parallel API client
        run_id: Task run ID to stream events for
        on_progress: Optional callback for progress events
    """
    import threading

    def _stream_events():
        event_type = None
        try:
            import httpx

            base_url = getattr(client, "_base_url", "https://api.parallel.ai")
            api_key = getattr(client, "_api_key", None)

            if not api_key:
                return

            url = f"{base_url}/v1beta/tasks/runs/{run_id}/events"
            headers = {
                "x-api-key": api_key,
                "parallel-beta": "events-sse-2025-07-24",
                "User-Agent": get_user_agent("spark"),
            }

            with httpx.Client(timeout=600.0) as http_client:
                with http_client.stream("GET", url, headers=headers) as response:
                    for line in response.iter_lines():
                        if not line or not line.strip():
                            continue

                        # Parse SSE format: "event: type\ndata: json\n"
                        if line.startswith("event:"):
                            event_type = line.split(":", 1)[1].strip()
                        elif line.startswith("data:"):
                            try:
                                event_data = json.loads(line.split(":", 1)[1].strip())
                                if on_progress and event_type:
                                    on_progress(event_type, event_data)
                                    event_type = None  # Reset after processing
                            except (json.JSONDecodeError, IndexError):
                                pass

        except Exception:
            # Silently fail - SSE is optional
            pass

    # Start streaming in background thread
    thread = threading.Thread(target=_stream_events, daemon=True)
    thread.start()


def enrich_streaming_batch(
    batch_df: DataFrame,
    input_columns: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    output_column_name: str = "enriched_data",
    enable_events: bool = False,
    on_progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> DataFrame:
    """
    Enrich a micro-batch DataFrame from Spark Structured Streaming.

    This function is designed to be used with foreachBatch in Structured Streaming.
    It collects the batch data, enriches it using the Parallel Task Group API,
    and returns a DataFrame with the enriched results.

    Args:
        batch_df: The micro-batch DataFrame to enrich.
        input_columns: Mapping from Parallel input names to DataFrame column names.
            Example: {"company_name": "company", "website": "url"}
            - Keys are the names/descriptions to pass to Parallel API
            - Values are the column names in the DataFrame
        output_columns: List of descriptions for columns to enrich.
            Example: ["CEO name", "brief company description", "founding year"]
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is "lite-fast".
        timeout: Timeout in seconds for the batch enrichment. Default is 600 (10 min).
        output_column_name: Name for the column containing enriched JSON data.
            Default is "enriched_data".
        enable_events: Enable Server-Sent Events (SSE) for real-time progress updates.
            Only works with pro tier processors and above. Default is False.
        on_progress: Optional callback function for progress events. Called with
            (event_type, event_data) for each SSE event received. Only used when
            enable_events=True.

    Returns:
        DataFrame with original columns plus the enriched data column.
        The enriched data is a JSON string that can be parsed with from_json().

    Example:
        >>> def process_batch(batch_df, batch_id):
        ...     enriched = enrich_streaming_batch(
        ...         batch_df,
        ...         input_columns={"company_name": "company"},
        ...         output_columns=["CEO name", "founding year"],
        ...     )
        ...     # Parse JSON results
        ...     schema = StructType([
        ...         StructField("ceo_name", StringType()),
        ...         StructField("founding_year", StringType()),
        ...     ])
        ...     enriched.select(
        ...         "*",
        ...         from_json("enriched_data", schema).alias("enriched")
        ...     ).write.mode("append").parquet("/output")
        >>>
        >>> query = stream_df.writeStream.foreachBatch(process_batch).start()
    """
    from parallel.types import JsonSchemaParam, TaskSpecParam
    from parallel.types.beta import BetaRunInputParam

    # Collect batch data (this is safe in micro-batches, which are already small)
    rows = batch_df.collect()

    if not rows:
        # Empty batch - return original DataFrame with empty enrichment column
        return batch_df.withColumn(output_column_name, spark_udf(lambda: None, StringType())())

    try:
        from parallel import Parallel

        client = Parallel(
            api_key=resolve_api_key(api_key),
            default_headers=get_default_headers("spark"),
        )

        # Build output schema
        output_schema = build_output_schema(output_columns)

        task_spec = TaskSpecParam(
            output_schema=JsonSchemaParam(
                type="json",
                json_schema=output_schema,
            )
        )

        # Create task group
        task_group = client.beta.task_group.create()
        taskgroup_id = task_group.task_group_id

        # Build inputs from batch rows
        run_inputs: list[BetaRunInputParam] = []
        for row in rows:
            # Convert Row to dict for reliable field access
            row_dict = row.asDict()
            input_data: dict[str, object] = {}
            for parallel_name, col_name in input_columns.items():
                # Get value from row, handle None gracefully
                value = row_dict.get(col_name)
                if value is not None:
                    input_data[parallel_name] = str(value)

            run_input: BetaRunInputParam = {
                "input": input_data,
                "processor": processor,
            }

            # Enable SSE if requested
            if enable_events:
                run_input["enable_events"] = True

            run_inputs.append(run_input)

        # Add all runs to the group
        response = client.beta.task_group.add_runs(
            taskgroup_id,
            default_task_spec=task_spec,
            inputs=run_inputs,
        )
        run_ids = response.run_ids

        # Start SSE streams for each run if events are enabled
        if enable_events and on_progress:
            for run_id in run_ids:
                _stream_task_run_events(client, run_id, on_progress)

        # Poll for completion with initial delay
        time.sleep(3)  # Initial delay before first poll
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = client.beta.task_group.retrieve(taskgroup_id)
            status_counts = status.status.task_run_status_counts or {}
            completed = status_counts.get("completed", 0)
            failed = status_counts.get("failed", 0)
            total = status.status.num_task_runs

            if completed + failed >= total or not status.status.is_active:
                break
            time.sleep(10)  # Poll every 10 seconds

        # Collect results
        results_by_id: dict[str, str] = {}
        runs_stream = client.beta.task_group.get_runs(
            taskgroup_id,
            include_input=True,
            include_output=True,
        )

        for event in runs_stream:
            if event.type == "task_run.state":
                run_id = event.run.run_id
                if event.output and hasattr(event.output, "content"):
                    content = event.output.content
                    if isinstance(content, dict):
                        results_by_id[run_id] = json.dumps(content)
                    else:
                        results_by_id[run_id] = str(content)
                elif event.run.error:
                    results_by_id[run_id] = json.dumps({"error": str(event.run.error)})

        # Map results back to rows (in order)
        enriched_results = []
        for run_id in run_ids:
            result = results_by_id.get(run_id, json.dumps({"error": "No result"}))
            enriched_results.append(result)

        # Create a UDF to add enriched data to each row
        # We'll create a dictionary mapping row index to enriched data
        enrichment_lookup = {i: result for i, result in enumerate(enriched_results)}

        def get_enrichment(idx: int) -> str:
            return enrichment_lookup.get(idx, json.dumps({"error": "Index not found"}))

        # Add monotonically increasing ID to track row order
        from pyspark.sql.functions import monotonically_increasing_id

        df_with_id = batch_df.withColumn("__row_id", monotonically_increasing_id())

        # Register the enrichment UDF
        enrichment_udf = spark_udf(get_enrichment, StringType())

        # Add enriched column and drop temporary ID
        result_df = df_with_id.withColumn(output_column_name, enrichment_udf(col("__row_id"))).drop("__row_id")

        return result_df

    except Exception as e:
        # On error, return original DataFrame with error in enrichment column
        error_json = json.dumps({"error": str(e)})
        error_udf = spark_udf(lambda: error_json, StringType())
        return batch_df.withColumn(output_column_name, error_udf())


def create_streaming_enrichment_function(
    input_columns: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    output_column_name: str = "enriched_data",
    enable_events: bool = False,
    on_progress: Callable[[str, dict[str, Any]], None] | None = None,
):
    """
    Create a foreachBatch function for streaming enrichment.

    This factory function creates a foreachBatch-compatible function with
    pre-configured enrichment settings. Use this for cleaner streaming code.

    Args:
        input_columns: Mapping from Parallel input names to DataFrame column names.
        output_columns: List of descriptions for columns to enrich.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is "lite-fast".
        timeout: Timeout in seconds for batch enrichment. Default is 600 (10 min).
        output_column_name: Name for the enriched data column. Default is "enriched_data".
        enable_events: Enable Server-Sent Events (SSE) for real-time progress updates.
            Only works with pro tier processors and above. Default is False.
        on_progress: Optional callback function for progress events. Called with
            (event_type, event_data) for each SSE event received.

    Returns:
        A function compatible with writeStream.foreachBatch() that enriches each batch.

    Example:
        >>> from parallel_web_tools.integrations.spark import create_streaming_enrichment_function
        >>>
        >>> # Create the enrichment function
        >>> enrich_func = create_streaming_enrichment_function(
        ...     input_columns={"company_name": "company", "website": "url"},
        ...     output_columns=["CEO name", "founding year"],
        ...     processor="lite-fast"
        ... )
        >>>
        >>> # Use in streaming query with custom write logic
        >>> def process_batch(batch_df, batch_id):
        ...     enriched_df = enrich_func(batch_df, batch_id)
        ...     enriched_df.write.mode("append").parquet("/output")
        >>>
        >>> query = stream_df.writeStream.foreachBatch(process_batch).start()
    """

    def enrich_batch(batch_df: DataFrame, batch_id: int):
        """
        Enrichment function for foreachBatch.

        Args:
            batch_df: The micro-batch DataFrame.
            batch_id: The batch ID (provided by Spark).
        """
        return enrich_streaming_batch(
            batch_df=batch_df,
            input_columns=input_columns,
            output_columns=output_columns,
            api_key=api_key,
            processor=processor,
            timeout=timeout,
            output_column_name=output_column_name,
            enable_events=enable_events,
            on_progress=on_progress,
        )

    return enrich_batch


def enrich_streaming_with_watermark(
    stream_df: DataFrame,
    timestamp_column: str,
    watermark_delay: str = "10 seconds",
) -> DataFrame:
    """
    Apply watermarking to a streaming DataFrame for late data handling.

    This is a convenience function that applies a watermark to handle late-arriving
    data in streaming sources with event-time semantics.

    Args:
        stream_df: The streaming DataFrame to apply watermark to.
        timestamp_column: Name of the timestamp column for watermarking.
        watermark_delay: Watermark delay threshold (e.g., "10 seconds", "1 minute").
            Data arriving later than this will be dropped.

    Returns:
        A streaming DataFrame with watermark applied.

    Example:
        >>> # Apply watermark then enrich
        >>> watermarked_df = enrich_streaming_with_watermark(
        ...     stream_df, "event_time", "10 seconds"
        ... )
        >>>
        >>> def process_batch(batch_df, batch_id):
        ...     enriched = enrich_streaming_batch(
        ...         batch_df,
        ...         input_columns={"company_name": "company"},
        ...         output_columns=["CEO name"],
        ...     )
        ...     enriched.write.mode("append").parquet("/output")
        >>>
        >>> query = watermarked_df.writeStream.foreachBatch(process_batch).start()
    """
    return stream_df.withWatermark(timestamp_column, watermark_delay)
