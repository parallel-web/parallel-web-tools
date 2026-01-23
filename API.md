# Parallel Data API Reference

## Core Functions

### `run_enrichment(config_file: str | Path) -> None`

Run data enrichment using a YAML configuration file.

**Parameters:**
- `config_file`: Path to YAML configuration file

**Raises:**
- `FileNotFoundError`: If config file doesn't exist
- `ValueError`: If config is invalid
- `NotImplementedError`: If source type is not supported

**Example:**
```python
from parallel_web_tools import run_enrichment

run_enrichment("configs/my_enrichment.yaml")
```

---

### `run_enrichment_from_dict(config: dict) -> None`

Run data enrichment using a configuration dictionary.

**Parameters:**
- `config`: Configuration dictionary matching YAML schema

**Raises:**
- `ValueError`: If config is invalid
- `NotImplementedError`: If source type is not supported

**Example:**
```python
from parallel_web_tools import run_enrichment_from_dict

config = {
    "source": "data.csv",
    "target": "enriched.csv",
    "source_type": "csv",
    "source_columns": [
        {"name": "company", "description": "Company name"}
    ],
    "enriched_columns": [
        {"name": "revenue", "description": "Annual revenue"}
    ]
}

run_enrichment_from_dict(config)
```

---

## Schema Classes

### `SourceType(Enum)`

Enumeration of supported data source types.

**Values:**
- `SourceType.CSV`: CSV file source
- `SourceType.DUCKDB`: DuckDB database source
- `SourceType.BIGQUERY`: Google BigQuery source

**Example:**
```python
from parallel_web_tools import SourceType

source_type = SourceType.CSV
```

---

### `Column(dataclass)`

Represents a column with name and description.

**Attributes:**
- `name` (str): Column name
- `description` (str): Column description

**Example:**
```python
from parallel_web_tools import Column

col = Column("revenue", "Annual revenue in USD")
```

---

### `InputSchema(dataclass)`

Schema for input data configuration.

**Attributes:**
- `source` (str): Source location (file path, table name)
- `target` (str): Target location
- `source_type` (SourceType): Type of data source
- `source_columns` (list[Column]): Input columns
- `enriched_columns` (list[Column]): Columns to enrich

**Example:**
```python
from parallel_web_tools import InputSchema, Column, SourceType

schema = InputSchema(
    source="data.csv",
    target="enriched.csv",
    source_type=SourceType.CSV,
    source_columns=[Column("company", "Company name")],
    enriched_columns=[Column("revenue", "Annual revenue")]
)
```

---

## Utility Functions

### `load_schema(filename: str) -> dict`

Load schema from YAML file.

**Parameters:**
- `filename`: Path to YAML file

**Returns:**
- Dictionary containing schema configuration

**Example:**
```python
from parallel_web_tools import load_schema

schema_dict = load_schema("config.yaml")
```

---

### `parse_schema(schema: dict) -> InputSchema`

Parse schema dictionary into InputSchema object.

**Parameters:**
- `schema`: Schema dictionary

**Returns:**
- `InputSchema` object

**Raises:**
- `ParseError`: If schema is invalid

**Example:**
```python
from parallel_web_tools import parse_schema

schema_dict = {
    "source": "data.csv",
    "target": "enriched.csv",
    "source_type": "csv",
    "source_columns": [{"name": "company", "description": "Company name"}],
    "enriched_columns": [{"name": "revenue", "description": "Annual revenue"}]
}

schema = parse_schema(schema_dict)
```

---

## Processor Functions (Advanced)

For direct access to processors (advanced usage):

### `process_csv(schema: InputSchema) -> None`

Process CSV file and enrich data.

### `process_duckdb(schema: InputSchema) -> None`

Process DuckDB table and enrich data.

### `process_bigquery(schema: InputSchema) -> None`

Process BigQuery table and enrich data.

**Example:**
```python
from parallel_web_tools import InputSchema, Column, SourceType
from parallel_web_tools.processors import process_csv

schema = InputSchema(
    source="data.csv",
    target="enriched.csv",
    source_type=SourceType.CSV,
    source_columns=[Column("company", "Company name")],
    enriched_columns=[Column("revenue", "Annual revenue")]
)

process_csv(schema)
```

---

## Configuration Schema

### YAML Configuration Format

```yaml
source: path/to/source  # File path or table name
target: path/to/target  # Output location
source_type: csv  # One of: csv, duckdb, bigquery

source_columns:
  - name: column_name
    description: Column description

enriched_columns:
  - name: new_column_name
    description: Description of enriched column
```

### Dictionary Configuration Format

```python
{
    "source": "path/to/source",
    "target": "path/to/target",
    "source_type": "csv",  # or "duckdb", "bigquery"
    "source_columns": [
        {"name": "column_name", "description": "Column description"}
    ],
    "enriched_columns": [
        {"name": "new_column", "description": "Description"}
    ]
}
```

---

## Environment Variables

Required environment variables:

- `PARALLEL_API_KEY`: Your Parallel API key (required)
- `DUCKDB_FILE`: Path to DuckDB file (optional, default: `data/file.db`)
- `BIGQUERY_PROJECT`: Google Cloud Project ID for BigQuery (optional)

Load from `.env.local`:
```python
from dotenv import load_dotenv
load_dotenv(".env.local")
```

---

## Error Handling

All functions may raise standard Python exceptions:

- `FileNotFoundError`: Config or data file not found
- `ValueError`: Invalid configuration
- `NotImplementedError`: Unsupported source type
- `ParseError`: Schema parsing failed

**Example with error handling:**
```python
from parallel_web_tools import run_enrichment, ParseError

try:
    run_enrichment("config.yaml")
except FileNotFoundError:
    print("Config file not found")
except ParseError as e:
    print(f"Invalid configuration: {e}")
except Exception as e:
    print(f"Enrichment failed: {e}")
```
