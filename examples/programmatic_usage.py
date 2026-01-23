"""Example of using parallel_web_tools programmatically (non-CLI)."""

from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.local")

# Method 1: Run enrichment from a YAML file
from parallel_web_tools import run_enrichment

print("=" * 60)
print("Method 1: Running from YAML file")
print("=" * 60)

try:
    run_enrichment("examples/example_csv_schema.yaml")
    print("✓ Enrichment completed successfully!")
except Exception as e:
    print(f"✗ Error: {e}")


# Method 2: Run enrichment from a dictionary
from parallel_web_tools import run_enrichment_from_dict

print("\n" + "=" * 60)
print("Method 2: Running from configuration dictionary")
print("=" * 60)

config = {
    "source": "examples/example_file.csv",
    "target": "data/output_programmatic.csv",
    "source_type": "csv",
    "source_columns": [
        {"name": "business_name", "description": "The name of a business"},
        {"name": "web_site", "description": "The business's web site"},
    ],
    "enriched_columns": [
        {
            "name": "estimated_ad_spend_last_quarter",
            "description": "The estimated spend in the last quarter by this business in USD. Write as an int",
        }
    ],
}

try:
    run_enrichment_from_dict(config)
    print("✓ Enrichment completed successfully!")
except Exception as e:
    print(f"✗ Error: {e}")


# Method 3: Using schema utilities directly
from parallel_web_tools import Column, SourceType

print("\n" + "=" * 60)
print("Method 3: Building schema programmatically")
print("=" * 60)

# You can also build schemas programmatically
from parallel_web_tools.schema import InputSchema

schema = InputSchema(
    source="examples/example_file.csv",
    target="data/output_custom.csv",
    source_type=SourceType.CSV,
    source_columns=[
        Column("business_name", "The name of a business"),
        Column("web_site", "The business's web site"),
    ],
    enriched_columns=[
        Column("industry", "The primary industry of the business"),
        Column("employee_count", "Estimated number of employees"),
    ],
)

print(f"Created schema: {schema.source} -> {schema.target}")
print(f"Source columns: {[c.name for c in schema.source_columns]}")
print(f"Enriched columns: {[c.name for c in schema.enriched_columns]}")


# Method 4: Integration example - process multiple configs
print("\n" + "=" * 60)
print("Method 4: Batch processing multiple configs")
print("=" * 60)

configs = [
    "examples/example_csv_schema.yaml",
    # "examples/example_duckdb_schema.yaml",
    # Add more as needed
]

for config_file in configs:
    print(f"\nProcessing: {config_file}")
    try:
        run_enrichment(config_file)
        print("  ✓ Completed")
    except Exception as e:
        print(f"  ✗ Failed: {e}")


print("\n" + "=" * 60)
print("All examples completed!")
print("=" * 60)
