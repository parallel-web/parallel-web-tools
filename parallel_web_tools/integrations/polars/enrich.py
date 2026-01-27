"""
Parallel Polars Integration

Provides DataFrame-native data enrichment for Polars using the Parallel API.

Example:
    import polars as pl
    from parallel_web_tools.integrations.polars import parallel_enrich

    df = pl.DataFrame({
        "company": ["Google", "Microsoft", "Apple"],
        "website": ["google.com", "microsoft.com", "apple.com"],
    })

    result = parallel_enrich(
        df,
        input_columns={"company_name": "company", "website": "website"},
        output_columns=["CEO name", "Founding year", "Headquarters"],
    )

    print(result.result)
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import polars as pl

from parallel_web_tools.core import EnrichmentResult, build_output_schema, enrich_batch

if TYPE_CHECKING:
    PolarsEnrichmentResult = EnrichmentResult[pl.DataFrame]


def parallel_enrich(
    df: pl.DataFrame,
    input_columns: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    include_basis: bool = False,
) -> EnrichmentResult:
    """
    Enrich a Polars DataFrame using the Parallel API.

    This function takes a DataFrame, extracts input data from specified columns,
    sends it to the Parallel API for enrichment, and returns a new DataFrame
    with the enriched columns added.

    Args:
        df: The Polars DataFrame to enrich.
        input_columns: Mapping from Parallel input descriptions to DataFrame column names.
            Example: {"company_name": "company", "website": "url"}
            - Keys are the descriptions/names passed to Parallel API
            - Values are the column names in the DataFrame
        output_columns: List of descriptions for columns to generate.
            Example: ["CEO name", "Founding year", "Brief description"]
        api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        processor: Parallel processor to use. Default is "lite-fast".
            Options: lite, lite-fast, base, base-fast, core, core-fast, pro, pro-fast
        timeout: Timeout in seconds for the enrichment. Default is 600 (10 min).
        include_basis: Whether to include basis/citations in results. Default is False.

    Returns:
        EnrichmentResult containing:
        - result: The enriched DataFrame with new columns
        - success_count: Number of successful enrichments
        - error_count: Number of failed enrichments
        - errors: List of error details
        - elapsed_time: Processing time in seconds

    Example:
        >>> import polars as pl
        >>> from parallel_web_tools.integrations.polars import parallel_enrich
        >>>
        >>> df = pl.DataFrame({
        ...     "company": ["Google", "Apple"],
        ...     "website": ["google.com", "apple.com"],
        ... })
        >>>
        >>> result = parallel_enrich(
        ...     df,
        ...     input_columns={"company_name": "company", "website": "website"},
        ...     output_columns=["CEO name", "Founding year"],
        ... )
        >>>
        >>> print(result.result)
        shape: (2, 4)
        ┌─────────┬────────────┬─────────────────┬──────────────┐
        │ company ┆ website    ┆ ceo_name        ┆ founding_year│
        │ ---     ┆ ---        ┆ ---             ┆ ---          │
        │ str     ┆ str        ┆ str             ┆ str          │
        ╞═════════╪════════════╪═════════════════╪══════════════╡
        │ Google  ┆ google.com ┆ Sundar Pichai   ┆ 1998         │
        │ Apple   ┆ apple.com  ┆ Tim Cook        ┆ 1976         │
        └─────────┴────────────┴─────────────────┴──────────────┘
    """
    start_time = time.time()

    # Validate input columns exist
    missing_cols = [col for col in input_columns.values() if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    if df.is_empty():
        return EnrichmentResult(
            result=df,
            success_count=0,
            error_count=0,
            errors=[],
            elapsed_time=time.time() - start_time,
        )

    # Convert DataFrame rows to list of input dicts
    inputs = []
    for row in df.iter_rows(named=True):
        input_data = {}
        for desc, col_name in input_columns.items():
            value = row[col_name]
            if value is not None:
                input_data[desc] = str(value)
        inputs.append(input_data)

    # Call the shared enrichment function
    results = enrich_batch(
        inputs=inputs,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
        source="polars",
    )

    # Build output schema to get property names
    schema = build_output_schema(output_columns)
    prop_names = list(schema["properties"].keys())

    # Extract columns from results
    new_columns: dict[str, list[Any]] = {name: [] for name in prop_names}
    if include_basis:
        new_columns["_basis"] = []

    errors = []
    success_count = 0
    error_count = 0

    for i, result in enumerate(results):
        if "error" in result:
            error_count += 1
            errors.append({"row": i, "error": result["error"]})
            for name in prop_names:
                new_columns[name].append(None)
            if include_basis:
                new_columns["_basis"].append(None)
        else:
            success_count += 1
            for name in prop_names:
                new_columns[name].append(result.get(name))
            if include_basis:
                new_columns["_basis"].append(result.get("basis"))

    # Add new columns to DataFrame
    enriched_df = df.clone()
    for col_name, values in new_columns.items():
        enriched_df = enriched_df.with_columns(pl.Series(name=col_name, values=values))

    elapsed = time.time() - start_time

    return EnrichmentResult(
        result=enriched_df,
        success_count=success_count,
        error_count=error_count,
        errors=errors,
        elapsed_time=elapsed,
    )


def parallel_enrich_lazy(
    lf: pl.LazyFrame,
    input_columns: dict[str, str],
    output_columns: list[str],
    api_key: str | None = None,
    processor: str = "lite-fast",
    timeout: int = 600,
    include_basis: bool = False,
) -> EnrichmentResult:
    """
    Enrich a Polars LazyFrame using the Parallel API.

    This is a convenience function that collects the LazyFrame, enriches it,
    and returns the result. For large datasets, consider using `parallel_enrich`
    with batched collection.

    Args:
        lf: The Polars LazyFrame to enrich.
        input_columns: Mapping from Parallel input descriptions to column names.
        output_columns: List of descriptions for columns to generate.
        api_key: Parallel API key.
        processor: Parallel processor to use.
        timeout: Timeout in seconds.
        include_basis: Whether to include basis/citations.

    Returns:
        EnrichmentResult with enriched DataFrame.
    """
    df = lf.collect()
    return parallel_enrich(
        df=df,
        input_columns=input_columns,
        output_columns=output_columns,
        api_key=api_key,
        processor=processor,
        timeout=timeout,
        include_basis=include_basis,
    )
