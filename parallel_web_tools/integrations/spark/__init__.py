"""
Parallel Spark Integration

SQL-native data enrichment for Apache Spark using the Parallel Web Systems Task API.
Supports both batch and streaming data enrichment.

Installation:
    pip install parallel-web-tools[spark]

Batch Example:
    from parallel_web_tools.integrations.spark import register_parallel_udfs

    # Register UDFs with your Spark session
    register_parallel_udfs(spark, api_key="your-api-key")

    # Use in SQL
    spark.sql('''
        SELECT parallel_enrich(
            map('company_name', name, 'website', url),
            array('CEO name', 'company description', 'founding year')
        ) as enriched
        FROM companies
    ''')

Streaming Example:
    from parallel_web_tools.integrations.spark import enrich_streaming_batch

    def process_batch(batch_df, batch_id):
        enriched_df = enrich_streaming_batch(
            batch_df,
            input_columns={"company_name": "company", "website": "url"},
            output_columns=["CEO name", "founding year"],
        )
        enriched_df.write.mode("append").parquet("/output")

    query = stream_df.writeStream.foreachBatch(process_batch).start()
"""

from parallel_web_tools.integrations.spark.streaming import (
    create_streaming_enrichment_function,
    enrich_streaming_batch,
    enrich_streaming_with_watermark,
)
from parallel_web_tools.integrations.spark.udf import (
    create_parallel_enrich_udf,
    register_parallel_udfs,
)

__all__ = [
    # Batch enrichment
    "register_parallel_udfs",
    "create_parallel_enrich_udf",
    # Streaming enrichment
    "enrich_streaming_batch",
    "create_streaming_enrichment_function",
    "enrich_streaming_with_watermark",
]
