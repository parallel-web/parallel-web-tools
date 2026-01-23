"""
BigQuery Integration Deployment Helper

Provides programmatic deployment of the Parallel BigQuery integration.
For most users, the CLI command `parallel-cli enrich deploy --system bigquery` is simpler.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from parallel_web_tools.integrations.utils import confirm_overwrite


def _run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        # Include both stdout and stderr as some tools output errors to stdout
        error_output = result.stderr or result.stdout or "(no output)"
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{error_output}")
    return result


def _get_cloud_function_dir() -> Path:
    """Get the path to the cloud_function directory."""
    return Path(__file__).parent / "cloud_function"


def _get_sql_template() -> str:
    """Get the SQL template for creating remote functions."""
    sql_path = Path(__file__).parent / "sql" / "create_functions.sql"
    return sql_path.read_text()


def _check_existing_resources(
    project_id: str,
    region: str,
    function_name: str,
    connection_id: str,
    dataset_id: str,
) -> list[str]:
    """Check which resources already exist and would be overwritten."""
    existing = []

    # Check secret
    result = _run_command(
        ["gcloud", "secrets", "describe", "parallel-api-key", "--project", project_id],
        check=False,
    )
    if result.returncode == 0:
        existing.append("Secret: parallel-api-key")

    # Check Cloud Function
    result = _run_command(
        ["gcloud", "functions", "describe", function_name, "--gen2", "--region", region, "--project", project_id],
        check=False,
    )
    if result.returncode == 0:
        existing.append(f"Cloud Function: {function_name}")

    # Check BigQuery connection
    result = _run_command(
        ["bq", "show", "--connection", f"{project_id}.{region}.{connection_id}"],
        check=False,
    )
    if result.returncode == 0:
        existing.append(f"BigQuery Connection: {connection_id}")

    # Check BigQuery dataset
    result = _run_command(
        ["bq", "show", f"{project_id}:{dataset_id}"],
        check=False,
    )
    if result.returncode == 0:
        existing.append(f"BigQuery Dataset: {dataset_id}")

    return existing


def deploy_bigquery_integration(
    project_id: str,
    api_key: str,
    region: str = "us-central1",
    dataset_id: str = "parallel_functions",
    connection_id: str = "parallel-connection",
    function_name: str = "parallel-enrich",
    force: bool = False,
) -> dict[str, Any]:
    """
    Deploy the Parallel BigQuery integration to Google Cloud.

    This function:
    1. Creates a secret for the API key
    2. Deploys the Cloud Function
    3. Creates a BigQuery connection
    4. Creates the remote functions

    Args:
        project_id: Google Cloud project ID
        api_key: Parallel API key
        region: GCP region (default: us-central1)
        dataset_id: BigQuery dataset for functions (default: parallel_functions)
        connection_id: BigQuery connection name (default: parallel-connection)
        function_name: Cloud Function name (default: parallel-enrich)
        force: Skip confirmation prompt when overwriting existing resources

    Returns:
        Dict with deployment details including function_url and example queries.

    Raises:
        RuntimeError: If any deployment step fails or user declines confirmation.
    """
    # Check for required CLI tools
    import shutil

    missing_tools = []
    if not shutil.which("gcloud"):
        missing_tools.append("gcloud")
    if not shutil.which("bq"):
        missing_tools.append("bq")

    if missing_tools:
        raise RuntimeError(
            f"Required CLI tools not found: {', '.join(missing_tools)}\n"
            "Please install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
        )

    print(f"Checking for existing resources in {project_id}...")

    # Check for existing resources
    existing = _check_existing_resources(
        project_id=project_id,
        region=region,
        function_name=function_name,
        connection_id=connection_id,
        dataset_id=dataset_id,
    )

    if existing and not force:
        if not confirm_overwrite(existing):
            raise RuntimeError("Deployment cancelled by user.")

    print(f"\nDeploying Parallel BigQuery integration to {project_id}...")

    # Step 1: Enable required APIs
    print("Enabling required APIs...")
    apis = [
        "bigquery.googleapis.com",
        "bigqueryconnection.googleapis.com",
        "cloudfunctions.googleapis.com",
        "cloudbuild.googleapis.com",
        "secretmanager.googleapis.com",
        "run.googleapis.com",
    ]
    for api in apis:
        _run_command(["gcloud", "services", "enable", api, "--project", project_id])

    # Step 2: Create/update secret for API key
    print("Creating secret for API key...")
    secret_name = "parallel-api-key"

    # Check if secret exists
    result = _run_command(["gcloud", "secrets", "describe", secret_name, "--project", project_id], check=False)

    if result.returncode == 0:
        # Update existing secret - use Popen to provide api_key via stdin
        process = subprocess.Popen(
            ["gcloud", "secrets", "versions", "add", secret_name, "--data-file=-", "--project", project_id],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = process.communicate(input=api_key)
        if process.returncode != 0:
            raise RuntimeError(f"Failed to update secret: {stderr}")
    else:
        # Create new secret
        process = subprocess.Popen(
            [
                "gcloud",
                "secrets",
                "create",
                secret_name,
                "--data-file=-",
                "--replication-policy=automatic",
                "--project",
                project_id,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = process.communicate(input=api_key)
        if process.returncode != 0:
            raise RuntimeError(f"Failed to create secret: {stderr}")

    secret_resource = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Step 3: Deploy Cloud Function
    print("Deploying Cloud Function...")
    function_dir = _get_cloud_function_dir()

    _run_command(
        [
            "gcloud",
            "functions",
            "deploy",
            function_name,
            "--gen2",
            "--runtime=python312",
            "--region",
            region,
            "--source",
            str(function_dir),
            "--entry-point=parallel_enrich",
            "--trigger-http",
            "--no-allow-unauthenticated",
            "--timeout=300s",
            "--memory=512MB",
            f"--set-env-vars=PARALLEL_API_KEY_SECRET={secret_resource}",
            "--project",
            project_id,
        ]
    )

    # Get function URL
    result = _run_command(
        [
            "gcloud",
            "functions",
            "describe",
            function_name,
            "--gen2",
            "--region",
            region,
            "--format=value(serviceConfig.uri)",
            "--project",
            project_id,
        ]
    )
    function_url = result.stdout.strip()
    print(f"Function deployed: {function_url}")

    # Grant function access to secret
    result = _run_command(
        [
            "gcloud",
            "functions",
            "describe",
            function_name,
            "--gen2",
            "--region",
            region,
            "--format=value(serviceConfig.serviceAccountEmail)",
            "--project",
            project_id,
        ]
    )
    function_sa = result.stdout.strip()

    _run_command(
        [
            "gcloud",
            "secrets",
            "add-iam-policy-binding",
            secret_name,
            f"--member=serviceAccount:{function_sa}",
            "--role=roles/secretmanager.secretAccessor",
            "--project",
            project_id,
        ]
    )

    # Step 4: Create BigQuery connection
    print("Creating BigQuery connection...")
    result = _run_command(["bq", "show", "--connection", f"{project_id}.{region}.{connection_id}"], check=False)

    if result.returncode != 0:
        _run_command(
            [
                "bq",
                "mk",
                "--connection",
                "--connection_type=CLOUD_RESOURCE",
                f"--project_id={project_id}",
                f"--location={region}",
                connection_id,
            ]
        )

    # Get connection service account
    result = _run_command(
        [
            "bq",
            "show",
            "--connection",
            "--format=json",
            f"{project_id}.{region}.{connection_id}",
        ]
    )
    connection_info = json.loads(result.stdout)
    connection_sa = connection_info["cloudResource"]["serviceAccountId"]

    # Grant connection permission to invoke function
    _run_command(
        [
            "gcloud",
            "functions",
            "add-iam-policy-binding",
            function_name,
            "--gen2",
            "--region",
            region,
            f"--member=serviceAccount:{connection_sa}",
            "--role=roles/cloudfunctions.invoker",
            "--project",
            project_id,
        ]
    )
    _run_command(
        [
            "gcloud",
            "run",
            "services",
            "add-iam-policy-binding",
            function_name,
            "--region",
            region,
            f"--member=serviceAccount:{connection_sa}",
            "--role=roles/run.invoker",
            "--project",
            project_id,
        ]
    )

    # Step 5: Create BigQuery dataset and functions
    print("Creating BigQuery remote functions...")

    # Create dataset
    _run_command(
        ["bq", "mk", "--dataset", f"--location={region}", f"{project_id}:{dataset_id}"],
        check=False,  # May already exist
    )

    # Create remote functions
    sql_template = _get_sql_template()
    sql = sql_template.format(
        project_id=project_id,
        dataset_id=dataset_id,
        location=region,
        connection_id=connection_id,
        function_url=function_url,
    )

    # Execute each CREATE FUNCTION statement
    # Strip SQL comments (lines starting with --) before checking for CREATE
    def strip_sql_comments(sql_text: str) -> str:
        lines = [line for line in sql_text.split("\n") if not line.strip().startswith("--")]
        return "\n".join(lines).strip()

    statements = []
    for chunk in sql.split(";"):
        clean = strip_sql_comments(chunk)
        if clean.startswith("CREATE"):
            statements.append(clean)

    if not statements:
        print("  Warning: No CREATE statements found in SQL template")
    for statement in statements:
        # Extract function name for logging
        func_name = statement.split("`")[1] if "`" in statement else "unknown"
        print(f"  Creating {func_name}...")
        _run_command(["bq", "query", "--use_legacy_sql=false", f"--project_id={project_id}", statement + ";"])

    print("\nDeployment complete!")

    return {
        "project_id": project_id,
        "region": region,
        "dataset_id": dataset_id,
        "function_url": function_url,
        "example_query": f"""
-- Basic usage (returns JSON with enriched fields and basis/citations)
SELECT `{project_id}.{dataset_id}.parallel_enrich`(
    JSON_OBJECT('company_name', 'Google', 'website', 'google.com'),
    JSON_ARRAY('CEO name', 'Founding year', 'Brief description')
) as enriched_data;

-- Parsing the JSON result into columns
SELECT
    JSON_VALUE(enriched_data, '$.ceo_name') as ceo_name,
    JSON_VALUE(enriched_data, '$.founding_year') as founding_year,
    JSON_VALUE(enriched_data, '$.brief_description') as description,
    JSON_QUERY(enriched_data, '$.basis') as citations
FROM (
    SELECT `{project_id}.{dataset_id}.parallel_enrich`(
        JSON_OBJECT('company_name', 'Google', 'website', 'google.com'),
        JSON_ARRAY('CEO name', 'Founding year', 'Brief description')
    ) as enriched_data
);
""".strip(),
    }


def get_deployment_status(
    project_id: str,
    region: str = "us-central1",
    function_name: str = "parallel-enrich",
) -> dict[str, Any]:
    """
    Check the status of an existing deployment.

    Returns:
        Dict with status information or error details.
    """
    status = {
        "project_id": project_id,
        "region": region,
        "function_deployed": False,
        "function_url": None,
    }

    # Check if function exists
    result = _run_command(
        ["gcloud", "functions", "describe", function_name, "--gen2", "--region", region, "--project", project_id],
        check=False,
    )

    if result.returncode == 0:
        status["function_deployed"] = True
        result = _run_command(
            [
                "gcloud",
                "functions",
                "describe",
                function_name,
                "--gen2",
                "--region",
                region,
                "--format=value(serviceConfig.uri)",
                "--project",
                project_id,
            ]
        )
        status["function_url"] = result.stdout.strip()

    return status


def cleanup_bigquery_integration(
    project_id: str,
    region: str = "us-central1",
    dataset_id: str = "parallel_functions",
    connection_id: str = "parallel-connection",
    function_name: str = "parallel-enrich",
    delete_secret: bool = False,
) -> None:
    """
    Remove deployed BigQuery integration resources.

    Args:
        project_id: Google Cloud project ID
        region: GCP region
        dataset_id: BigQuery dataset to remove
        connection_id: BigQuery connection to remove
        function_name: Cloud Function to remove
        delete_secret: Whether to also delete the API key secret
    """
    print(f"Cleaning up Parallel BigQuery integration from {project_id}...")

    # Delete Cloud Function
    print("Deleting Cloud Function...")
    _run_command(
        [
            "gcloud",
            "functions",
            "delete",
            function_name,
            "--gen2",
            "--region",
            region,
            "--project",
            project_id,
            "--quiet",
        ],
        check=False,
    )

    # Delete BigQuery connection
    print("Deleting BigQuery connection...")
    _run_command(["bq", "rm", "--connection", "--force", f"{project_id}.{region}.{connection_id}"], check=False)

    # Delete BigQuery dataset
    print("Deleting BigQuery dataset...")
    _run_command(["bq", "rm", "-r", "-f", f"{project_id}:{dataset_id}"], check=False)

    # Optionally delete secret
    if delete_secret:
        print("Deleting API key secret...")
        _run_command(
            ["gcloud", "secrets", "delete", "parallel-api-key", "--project", project_id, "--quiet"], check=False
        )

    print("Cleanup complete!")
