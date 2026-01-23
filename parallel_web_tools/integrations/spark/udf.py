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
"""

from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from parallel_web_tools.core import (
    enrich_batch,
    enrich_single,
)
from parallel_web_tools.core.auth import resolve_api_key


def _parallel_enrich(
    input_data: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
) -> str:
    """
    Enrich a single row of data using the Parallel Task Group API.

    Args:
        input_data: Dictionary mapping column names/descriptions to values.
        output_columns: List of descriptions for columns to enrich.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is 'lite-fast'.
        timeout: Timeout in seconds for the API call. Default is 300 (5 min).

    Returns:
        JSON string containing the enrichment results with the requested columns.
    """
    result = enrich_single(
        input_data=input_data,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=True,
    )
    return json.dumps(result)


def _parallel_enrich_batch(
    input_data_list: list[dict[str, str]],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
) -> list[str]:
    """
    Enrich multiple rows of data using the Parallel Task Group API.

    Args:
        input_data_list: List of dictionaries, each mapping column names to values.
        output_columns: List of descriptions for columns to enrich.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is 'lite-fast'.
        timeout: Total timeout in seconds for the batch. Default is 600 (10 min).

    Returns:
        List of JSON strings containing the enrichment results (same order as inputs).
    """
    results = enrich_batch(
        inputs=input_data_list,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=True,
    )
    return [json.dumps(r) for r in results]


def create_parallel_enrich_udf(
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
):
    """
    Create a Spark UDF for parallel_enrich with pre-configured parameters.

    This factory function creates a UDF with the API key and other settings
    baked in, so they don't need to be passed in SQL.

    Args:
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is 'lite-fast'.
        timeout: Timeout in seconds for the API call. Default is 300 (5 min).

    Returns:
        A Spark UDF function that can be registered with spark.udf.register().
    """
    # Resolve and capture the API key at registration time
    # This is critical because Spark executors may not have the env var
    key = resolve_api_key(api_key)

    def _enrich(input_data, output_columns):
        """Inner UDF function with captured configuration."""
        if input_data is None or output_columns is None:
            return None
        return _parallel_enrich(
            input_data=input_data,
            output_columns=output_columns,
            api_key=key,
            processor=processor,
            timeout=timeout,
        )

    return udf(_enrich, StringType())


def register_parallel_udfs(
    spark: SparkSession,
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 300,
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

    Args:
        spark: The SparkSession to register UDFs with.
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided,
            or stored OAuth credentials from 'parallel-cli login'.
        processor: Parallel processor to use. Default is 'lite-fast'.
            Options: lite, lite-fast, base, base-fast, core, core-fast,
            pro, pro-fast, ultra, ultra-fast, etc.
        timeout: Timeout in seconds for API calls. Default is 300 (5 min).
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
    """
    # Resolve and capture the API key at registration time
    # This is critical because Spark executors may not have the env var
    key = resolve_api_key(api_key)

    # Create the UDF with captured configuration
    enrich_udf = create_parallel_enrich_udf(
        api_key=key,
        processor=processor,
        timeout=timeout,
    )

    # Register with Spark
    spark.udf.register(udf_name, enrich_udf)

    # Also register a version that allows processor override per call
    def _enrich_with_processor(input_data, output_columns, proc=None):
        """UDF that allows processor override."""
        if input_data is None or output_columns is None:
            return None
        return _parallel_enrich(
            input_data=input_data,
            output_columns=output_columns,
            api_key=key,
            processor=proc or processor,
            timeout=timeout,
        )

    spark.udf.register(
        f"{udf_name}_with_processor",
        udf(_enrich_with_processor, StringType()),
    )
