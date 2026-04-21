from __future__ import annotations

import pandas as pd


class DuckDBResultRelation:
    """Thin wrapper that preserves `None` values in `fetchdf()` output."""

    def __init__(self, relation):
        self._relation = relation

    def fetchdf(self):
        df = self._relation.fetchdf()
        return df.astype(object).where(pd.notna(df), None)

    def __getattr__(self, name):
        return getattr(self._relation, name)


def wrap_relation(relation):
    """Wrap a DuckDB relation so pandas materialization keeps nulls as `None`."""
    return DuckDBResultRelation(relation)
