"""Schema definitions and parsing logic."""

from dataclasses import dataclass, fields
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, create_model


class ParseError(Exception):
    """Raised when schema parsing fails."""

    pass


@dataclass
class Column:
    """Represents a column with name and description."""

    name: str
    description: str
    type: str = "str"


# Type mapping from string names to Python types
TYPE_MAP: dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "datetime": datetime,
    "date": date,
    "str | None": str | None,
    "int | None": int | None,
    "float | None": float | None,
    "bool | None": bool | None,
    "datetime | None": datetime | None,
    "date | None": date | None,
    "list[str]": list[str],
    "list[int]": list[int],
    "list[float]": list[float],
    "list[bool]": list[bool],
}

# Mapping from JSON schema types to our type names
JSON_SCHEMA_TYPE_MAP: dict[str, str] = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "array": "list[str]",
}


def get_available_types() -> list[str]:
    """Get list of available type names."""
    return list(TYPE_MAP.keys())


# Available processors from https://parallel.ai/pricing
AVAILABLE_PROCESSORS = [
    "lite",
    "lite-fast",
    "base",
    "base-fast",
    "core",
    "core-fast",
    "pro",
    "pro-fast",
    "ultra",
    "ultra-fast",
    "ultra2x",
    "ultra2x-fast",
    "ultra4x",
    "ultra4x-fast",
    "ultra8x",
    "ultra8x-fast",
]

ProcessorType = Literal[
    "lite",
    "lite-fast",
    "base",
    "base-fast",
    "core",
    "core-fast",
    "pro",
    "pro-fast",
    "ultra",
    "ultra-fast",
    "ultra2x",
    "ultra2x-fast",
    "ultra4x",
    "ultra4x-fast",
    "ultra8x",
    "ultra8x-fast",
]


class SourceType(Enum):
    """Supported data sources."""

    CSV = "csv"
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"


@dataclass
class InputSchema:
    """Schema for input data configuration."""

    source: str
    target: str
    source_type: SourceType
    source_columns: list[Column]
    enriched_columns: list[Column]
    processor: ProcessorType = "core-fast"

    def __post_init__(self):
        if not isinstance(self.source_type, SourceType):
            raise ValueError(f"'{self.source_type}' is not a valid SourceType.")


def load_schema(filename: str) -> dict[str, Any]:
    """Load schema from YAML file.

    Requires pyyaml: pip install parallel-web-tools[cli]
    """
    try:
        import yaml
    except ImportError as e:
        raise ImportError("YAML support requires pyyaml. Install with: pip install parallel-web-tools[cli]") from e

    with open(filename) as f:
        return yaml.safe_load(f)


def dict_to_column_list(input_dict: list[dict[str, str]]) -> list[Column]:
    """Convert dictionary list to Column list."""
    return [Column(col["name"], col["description"], col.get("type", "str")) for col in input_dict]


def parse_schema(schema: dict[str, Any]) -> InputSchema:
    """Parse schema dictionary into InputSchema object."""
    try:
        return InputSchema(
            source=schema["source"],
            target=schema["target"],
            source_type=SourceType[schema["source_type"].upper()],
            source_columns=dict_to_column_list(schema["source_columns"]),
            enriched_columns=dict_to_column_list(schema["enriched_columns"]),
            processor=schema.get("processor", "core-fast"),
        )
    except Exception as e:
        raise ParseError(f"Failed to parse schema. Schema must match {fields(InputSchema)}", e) from e


def parse_input_and_output_models(
    schema: InputSchema,
) -> tuple[type[BaseModel], type[BaseModel]]:
    """Create Pydantic models from schema."""
    # Build field definitions with proper typing for create_model
    input_fields: dict[str, Any] = {
        col.name: (str, Field(description=col.description)) for col in schema.source_columns
    }
    output_fields: dict[str, Any] = {
        col.name: (TYPE_MAP.get(col.type, str), Field(description=col.description)) for col in schema.enriched_columns
    }

    InputModel = create_model("InputModel", **input_fields)
    OutputModel = create_model("OutputModel", **output_fields)

    return InputModel, OutputModel
