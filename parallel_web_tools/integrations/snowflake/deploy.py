"""
Snowflake Deployment Helper

Provides utilities for deploying and managing the Parallel Snowflake integration.

Example:
    from parallel_web_tools.integrations.snowflake import deploy_parallel_functions

    # Deploy to Snowflake
    deploy_parallel_functions(
        account="your-account",
        user="your-user",
        password="your-password",
        warehouse="COMPUTE_WH",
        parallel_api_key="your-api-key",
    )
"""

from __future__ import annotations

import os
from pathlib import Path

from parallel_web_tools.integrations.utils import confirm_overwrite


def get_sql_template(name: str) -> str:
    """
    Get the contents of a SQL template file.

    Args:
        name: Name of the SQL file (without .sql extension).
            Options: "01_setup", "02_create_udf", "03_cleanup"

    Returns:
        Contents of the SQL template file.

    Raises:
        FileNotFoundError: If the template file doesn't exist.
    """
    sql_dir = Path(__file__).parent / "sql"
    sql_file = sql_dir / f"{name}.sql"

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL template not found: {sql_file}")

    return sql_file.read_text()


def get_setup_sql(api_key: str | None = None) -> str:
    """
    Get the setup SQL with optional API key substitution.

    Args:
        api_key: Parallel API key to embed in the SQL.
            If not provided, uses PARALLEL_API_KEY env var.

    Returns:
        Setup SQL with API key substituted.
    """
    sql = get_sql_template("01_setup")

    key = api_key or os.environ.get("PARALLEL_API_KEY")
    if key:
        sql = sql.replace("YOUR_PARALLEL_API_KEY", key)

    return sql


def get_udf_sql() -> str:
    """
    Get the UDF creation SQL.

    Returns:
        SQL to create the parallel_enrich() UDF.
    """
    return get_sql_template("02_create_udf")


def get_cleanup_sql() -> str:
    """
    Get the cleanup SQL.

    Returns:
        SQL to remove all Parallel integration objects.
    """
    return get_sql_template("03_cleanup")


def _check_existing_resources(cursor, database: str, schema: str) -> list[str]:
    """Check which Snowflake resources already exist and would be overwritten."""
    existing = []

    # Check database
    try:
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if cursor.fetchone():
            existing.append(f"Database: {database}")
    except Exception:
        pass

    # Check schema (only if database exists)
    if existing:
        try:
            cursor.execute(f"SHOW SCHEMAS LIKE '{schema}' IN DATABASE {database}")
            if cursor.fetchone():
                existing.append(f"Schema: {database}.{schema}")
        except Exception:
            pass

    # Check external access integration
    try:
        cursor.execute("SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'parallel_api_access_integration'")
        if cursor.fetchone():
            existing.append("External Access Integration: parallel_api_access_integration")
    except Exception:
        pass

    # Check secret
    try:
        cursor.execute(f"SHOW SECRETS LIKE 'parallel_api_key' IN DATABASE {database}")
        if cursor.fetchone():
            existing.append(f"Secret: {database}.{schema}.parallel_api_key")
    except Exception:
        pass

    # Check network rule
    try:
        cursor.execute(f"SHOW NETWORK RULES LIKE 'parallel_api_network_rule' IN DATABASE {database}")
        if cursor.fetchone():
            existing.append(f"Network Rule: {database}.{schema}.parallel_api_network_rule")
    except Exception:
        pass

    return existing


def deploy_parallel_functions(
    account: str,
    user: str,
    password: str | None = None,
    warehouse: str = "COMPUTE_WH",
    database: str = "PARALLEL_INTEGRATION",
    schema: str = "ENRICHMENT",
    role: str = "ACCOUNTADMIN",
    parallel_api_key: str | None = None,
    authenticator: str | None = None,
    force: bool = False,
) -> None:
    """
    Deploy Parallel enrichment functions to Snowflake.

    This function:
    1. Creates the network rule, secret, and external access integration
    2. Creates the parallel_enrich() UDF
    3. Grants permissions to PARALLEL_USER and PARALLEL_DEVELOPER roles

    Args:
        account: Snowflake account identifier (e.g., "abc12345.us-east-1").
        user: Snowflake username.
        password: Snowflake password. If None, uses authenticator.
        warehouse: Snowflake warehouse to use. Default is "COMPUTE_WH".
        database: Database to create integration in. Default is "PARALLEL_INTEGRATION".
        schema: Schema to create integration in. Default is "ENRICHMENT".
        role: Role to use for deployment. Default is "ACCOUNTADMIN".
        parallel_api_key: Parallel API key. Uses PARALLEL_API_KEY env var if not provided.
        authenticator: Authentication method (e.g., "externalbrowser" for SSO).
        force: Skip confirmation prompt when overwriting existing resources.

    Raises:
        ImportError: If snowflake-connector-python is not installed.
        ValueError: If no API key is provided.
        RuntimeError: If user declines confirmation.
        snowflake.connector.Error: If Snowflake connection or execution fails.

    Example:
        >>> from parallel_web_tools.integrations.snowflake import deploy_parallel_functions
        >>>
        >>> deploy_parallel_functions(
        ...     account="your-account",
        ...     user="your-user",
        ...     password="your-password",
        ...     parallel_api_key="your-api-key",
        ... )
    """
    try:
        import snowflake.connector
    except ImportError as e:
        raise ImportError(
            "snowflake-connector-python is required for deployment. "
            "Install it with: pip install parallel-web-tools[snowflake]"
        ) from e

    # Get API key
    api_key = parallel_api_key or os.environ.get("PARALLEL_API_KEY")
    if not api_key:
        raise ValueError(
            "Parallel API key required. Provide via parallel_api_key parameter "
            "or PARALLEL_API_KEY environment variable."
        )

    # Build connection parameters
    conn_params = {
        "account": account,
        "user": user,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "role": role,
    }

    if password:
        conn_params["password"] = password
    elif authenticator:
        conn_params["authenticator"] = authenticator
    else:
        conn_params["authenticator"] = "externalbrowser"

    # Connect to Snowflake
    print(f"Connecting to Snowflake account: {account}")
    conn = snowflake.connector.connect(**conn_params)

    try:
        cursor = conn.cursor()

        # Check for existing resources
        print("Checking for existing resources...")
        existing = _check_existing_resources(cursor, database, schema)

        if existing and not force:
            if not confirm_overwrite(existing):
                raise RuntimeError("Deployment cancelled by user.")

        # Run setup SQL
        print("Running setup SQL (network rule, secret, integration)...")
        setup_sql = get_setup_sql(api_key)
        for statement in setup_sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    # Skip errors for verification queries
                    if "SHOW" not in statement and "SELECT" not in statement:
                        print(f"Warning: {e}")

        # Run UDF creation SQL
        print("Creating parallel_enrich() UDF...")
        udf_sql = get_udf_sql()
        for statement in udf_sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    # Skip errors for verification queries
                    if "SELECT" not in statement:
                        print(f"Warning: {e}")

        print("Deployment complete!")
        print()
        print("Test the integration with:")
        print("  SELECT parallel_enrich(")
        print("      OBJECT_CONSTRUCT('company_name', 'Google'),")
        print("      ARRAY_CONSTRUCT('CEO name', 'Founding year')")
        print("  ) AS enriched_data;")

    finally:
        conn.close()


def cleanup_parallel_functions(
    account: str,
    user: str,
    password: str | None = None,
    warehouse: str = "COMPUTE_WH",
    role: str = "ACCOUNTADMIN",
    authenticator: str | None = None,
) -> None:
    """
    Remove Parallel enrichment functions and related objects from Snowflake.

    This function removes:
    - parallel_enrich() UDFs
    - External access integration
    - API key secret
    - Network rule
    - Roles (PARALLEL_USER, PARALLEL_DEVELOPER)

    Args:
        account: Snowflake account identifier.
        user: Snowflake username.
        password: Snowflake password. If None, uses authenticator.
        warehouse: Snowflake warehouse to use. Default is "COMPUTE_WH".
        role: Role to use for cleanup. Default is "ACCOUNTADMIN".
        authenticator: Authentication method (e.g., "externalbrowser" for SSO).

    Raises:
        ImportError: If snowflake-connector-python is not installed.
        snowflake.connector.Error: If Snowflake connection or execution fails.
    """
    try:
        import snowflake.connector
    except ImportError as e:
        raise ImportError(
            "snowflake-connector-python is required. Install it with: pip install parallel-web-tools[snowflake]"
        ) from e

    # Build connection parameters
    conn_params = {
        "account": account,
        "user": user,
        "warehouse": warehouse,
        "database": "PARALLEL_INTEGRATION",
        "schema": "ENRICHMENT",
        "role": role,
    }

    if password:
        conn_params["password"] = password
    elif authenticator:
        conn_params["authenticator"] = authenticator
    else:
        conn_params["authenticator"] = "externalbrowser"

    # Connect to Snowflake
    print(f"Connecting to Snowflake account: {account}")
    conn = snowflake.connector.connect(**conn_params)

    try:
        cursor = conn.cursor()

        # Run cleanup SQL
        print("Running cleanup SQL...")
        cleanup_sql = get_cleanup_sql()
        for statement in cleanup_sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"Warning: {e}")

        print("Cleanup complete!")

    finally:
        conn.close()
