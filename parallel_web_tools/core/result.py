"""Common result classes for enrichment operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class EnrichmentResult[T]:
    """Result of an enrichment operation.

    Generic over the result type (e.g., DuckDB relation, Polars DataFrame).
    """

    result: T
    """The enriched data in the appropriate format."""

    success_count: int
    """Number of rows successfully enriched."""

    error_count: int
    """Number of rows that failed enrichment."""

    errors: list[dict[str, Any]] = field(default_factory=list)
    """List of error details for failed rows."""

    elapsed_time: float = 0.0
    """Total processing time in seconds."""
