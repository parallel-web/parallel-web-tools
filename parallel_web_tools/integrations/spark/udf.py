"""
Parallel Spark UDF Implementation

This module provides SQL-native User Defined Functions (UDFs) for Apache Spark
that integrate with the Parallel Web Systems Task API for data enrichment.

The main function `parallel_enrich` allows you to enrich data directly in SQL:

    SELECT parallel_enrich(
        map('company_name', name, 'website', url),
        array('CEO name', 'company description', 'founding year')
    ) as enriched
    FROM companies

This implementation uses pandas_udf with the Task Group API to process rows
in batches, chunked to a maximum of 1000 rows per task group.
"""

from __future__ import annotations

import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from parallel_web_tools.core.auth import resolve_api_key
from parallel_web_tools.core.batch import enrich_batch

_MAX_CHUNK_SIZE = 1000


def _parallel_enrich_partition(
    input_data_series: pd.Series,
    output_columns: list[str],
    api_key: str,
    processor: str = "lite-fast",
    timeout: int = 300,
    include_basis: bool = False,
) -> pd.Series:
    """
    Enrich an entire partition of data using the Task Group API.

    Args:
        input_data_series: Pandas Series of input dictionaries.
        output_columns: List of descriptions for columns to enrich.
        api_key: Parallel API key.
        processor: Parallel processor to use.
        timeout: Timeout in seconds for each API call.
        include_basis: Whether to include basis/citations in the response.

    Returns:
        Pandas Series of JSON strings containing enrichment results.
    """
    items = input_data_series.tolist()

    # Handle empty partitions
    if not items:
        return pd.Series([], dtype=str)

    # Filter out None values, tracking their positions
    valid_items = []
    valid_indices = []
    for i, item in enumerate(items):
        if item is not None:
            valid_items.append(item)
            valid_indices.append(i)

    if not valid_items:
        return pd.Series([None] * len(items))

    # Process valid items in chunks of _MAX_CHUNK_SIZE via enrich_batch
    all_results: list[dict] = []
    for chunk_start in range(0, len(valid_items), _MAX_CHUNK_SIZE):
        chunk = valid_items[chunk_start : chunk_start + _MAX_CHUNK_SIZE]
        chunk_results = enrich_batch(
            inputs=chunk,
            output_columns=output_columns,
            api_key=api_key,
            processor=processor,
            timeout=timeout,
            include_basis=include_basis,
            source="spark",
        )
        all_results.extend(chunk_results)

    # Convert dicts to JSON strings, renaming "basis" -> "_basis"
    json_results: list[str] = []
    for result in all_results:
        if isinstance(result, dict):
            if "basis" in result:
                result["_basis"] = result.pop("basis")
            json_results.append(json.dumps(result))
        else:
            json_results.append(json.dumps({"result": str(result)}))

    # Map results back to original positions
    output: list[str | None] = [None] * len(items)
    for i, result in zip(valid_indices, json_results, strict=True):
        output[i] = result

    return pd.Series(output)


def create_parallel_enrich_udf(
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
    include_basis: bool = False,
):
    """
    Create a Spark pandas_udf for parallel_enrich with pre-configured parameters.

    This factory function creates a pandas UDF with the API key and other settings
    baked in, so they don't need to be passed in SQL. The UDF processes rows
    in batches using the Task Group API.

    Args:
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is 'lite-fast'.
        timeout: Timeout in seconds for each API call. Default is 300 (5 min).
        include_basis: Whether to include basis/citations in the response. Default is False.

    Returns:
        A Spark pandas_udf function that can be registered with spark.udf.register().
    """
    # Resolve and capture the API key at registration time
    # This is critical because Spark executors may not have the env var
    key = resolve_api_key(api_key)

    @pandas_udf(StringType())
    def _enrich(input_data: pd.Series, output_columns: pd.Series) -> pd.Series:
        """
        Pandas UDF that processes all rows in the partition using Task Group API.

        Args:
            input_data: Series of input dictionaries (map type in Spark).
            output_columns: Series of output column arrays (same value for all rows).

        Returns:
            Series of JSON strings with enrichment results.
        """
        # output_columns is the same for all rows, get from first row
        cols = output_columns.iloc[0] if len(output_columns) > 0 else []

        return _parallel_enrich_partition(
            input_data_series=input_data,
            output_columns=list(cols) if cols is not None else [],
            api_key=key,
            processor=processor,
            timeout=timeout,
            include_basis=include_basis,
        )

    return _enrich


def register_parallel_udfs(
    spark: SparkSession,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
    include_basis: bool = False,
    udf_name: str = "parallel_enrich",
) -> None:
    """
    Register Parallel enrichment UDFs with a Spark session.

    This is the main entry point for using Parallel enrichment in Spark SQL.
    After calling this function, you can use the UDF in SQL queries:

        spark.sql('''
            SELECT parallel_enrich(
                map('company_name', 'Acme Corp', 'website', 'https://acme.com'),
                array('CEO name', 'company description', 'founding year')
            ) as enriched
        ''')

    The UDF uses pandas_udf with the Task Group API to process rows in batches,
    chunked to a maximum of 1000 rows per task group.

    Args:
        spark: The SparkSession to register UDFs with.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided,
            or stored OAuth credentials from 'parallel-cli login'.
        processor: Parallel processor to use. Default is 'lite-fast'.
            Options: lite, lite-fast, base, base-fast, core, core-fast,
            pro, pro-fast, ultra, ultra-fast, etc.
        timeout: Timeout in seconds for each API call. Default is 300 (5 min).
        include_basis: Whether to include basis/citations in the response. Default is False.
            When True, each result will include a '_basis' field with citations.
        udf_name: Name to register the UDF under. Default is 'parallel_enrich'.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from parallel_web_tools.integrations.spark import register_parallel_udfs
        >>>
        >>> spark = SparkSession.builder.appName("test").getOrCreate()
        >>> register_parallel_udfs(spark, api_key="your-key")
        >>>
        >>> # Now use in SQL
        >>> df = spark.sql('''
        ...     SELECT parallel_enrich(
        ...         map('company', 'Google'),
        ...         array('CEO', 'headquarters')
        ...     ) as info
        ... ''')
        >>>
        >>> # With basis/citations
        >>> register_parallel_udfs(spark, include_basis=True)
    """
    # Resolve and capture the API key at registration time
    # This is critical because Spark executors may not have the env var
    key = resolve_api_key(api_key)

    # Create the pandas UDF with captured configuration
    enrich_udf = create_parallel_enrich_udf(
        api_key=key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
    )

    # Register with Spark
    spark.udf.register(udf_name, enrich_udf)

    # Also register a version that allows processor override per call
    @pandas_udf(StringType())
    def _enrich_with_processor(input_data: pd.Series, output_columns: pd.Series, proc: pd.Series) -> pd.Series:
        """Pandas UDF that allows processor override per partition."""
        # Get processor from first row (same for all rows in partition)
        proc_val = proc.iloc[0] if len(proc) > 0 and proc.iloc[0] else processor
        cols = output_columns.iloc[0] if len(output_columns) > 0 else []

        return _parallel_enrich_partition(
            input_data_series=input_data,
            output_columns=list(cols) if cols is not None else [],
            api_key=key,
            processor=proc_val,
            timeout=timeout,
            include_basis=include_basis,
        )

    spark.udf.register(f"{udf_name}_with_processor", _enrich_with_processor)
