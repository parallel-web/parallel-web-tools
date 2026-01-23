"""
Parallel Polars Integration

DataFrame-native data enrichment for Polars using the Parallel API.

Installation:
    pip install parallel-web-tools[polars]

Example:
    import polars as pl
    from parallel_web_tools.integrations.polars import parallel_enrich

    df = pl.DataFrame({
        "company": ["Google", "Microsoft"],
        "website": ["google.com", "microsoft.com"],
    })

    result = parallel_enrich(
        df,
        input_columns={"company_name": "company", "website": "website"},
        output_columns=["CEO name", "Founding year"],
    )

    print(result.result)
"""

from parallel_web_tools.core.result import EnrichmentResult
from parallel_web_tools.integrations.polars.enrich import (
    parallel_enrich,
    parallel_enrich_lazy,
)

__all__ = [
    "parallel_enrich",
    "parallel_enrich_lazy",
    "EnrichmentResult",
]
