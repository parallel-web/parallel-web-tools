"""SQL utility functions for safe query construction."""

import re


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier (table/column name) to prevent SQL injection.

    Handles simple names, schema-qualified names (schema.table),
    and fully-qualified names (catalog.schema.table).

    Each component is double-quoted with any embedded double quotes escaped
    by doubling them (standard SQL quoting).

    Args:
        name: The identifier to quote (e.g., "my_table" or "schema.table").

    Returns:
        A safely quoted identifier string.

    Raises:
        ValueError: If the name is empty or contains invalid characters.
    """
    if not name or not name.strip():
        raise ValueError("Identifier name cannot be empty")

    parts = name.split(".")
    quoted_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            raise ValueError(f"Invalid identifier: {name!r}")
        # Escape any double quotes inside the identifier by doubling them
        escaped = part.replace('"', '""')
        quoted_parts.append(f'"{escaped}"')

    return ".".join(quoted_parts)


def validate_table_name(name: str) -> str:
    """Validate that a string is a safe table name (alphanumeric, underscores, dots).

    This is a stricter check than quote_identifier - it rejects names that
    contain anything other than alphanumeric chars, underscores, and dots.
    Use this when you want to reject suspicious input rather than quote it.

    Args:
        name: The table name to validate.

    Returns:
        The validated table name.

    Raises:
        ValueError: If the name contains invalid characters.
    """
    if not name or not name.strip():
        raise ValueError("Table name cannot be empty")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", name):
        raise ValueError(
            f"Invalid table name: {name!r}. "
            "Table names must start with a letter or underscore and contain only "
            "alphanumeric characters, underscores, and dots."
        )

    return name
