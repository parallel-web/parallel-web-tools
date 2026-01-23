"""
Parallel BigQuery Integration

SQL-native data enrichment for Google BigQuery using Remote Functions.
The integration deploys a Cloud Function that interfaces with the Parallel API.

Installation:
    pip install parallel-web-tools[bigquery]

Quick Start:
    # Deploy to GCP
    parallel-cli enrich deploy --system bigquery --project=PROJECT_ID --api-key=KEY

    # Then use in BigQuery SQL
    SELECT parallel_enrich(
        JSON_OBJECT('company_name', name, 'website', url),
        JSON_ARRAY('CEO name', 'founding year')
    ) as enriched
    FROM companies;

For detailed setup instructions, see docs/bigquery-setup.md
"""

from parallel_web_tools.integrations.bigquery.deploy import (
    cleanup_bigquery_integration,
    deploy_bigquery_integration,
    get_deployment_status,
)

__all__ = [
    "deploy_bigquery_integration",
    "get_deployment_status",
    "cleanup_bigquery_integration",
]
