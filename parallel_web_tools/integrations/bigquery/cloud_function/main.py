"""
BigQuery Remote Function for Parallel API Enrichment

This Cloud Function serves as the HTTP endpoint for BigQuery remote functions,
enabling SQL-native data enrichment using Parallel's web intelligence API.

Usage from BigQuery:
    SELECT parallel_enrich(
        JSON_OBJECT('company_name', name, 'website', url),
        JSON_ARRAY('CEO name', 'Company description', 'Founding year')
    ) as enriched_data
    FROM companies;
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import functions_framework
from flask import Request, jsonify
from google.cloud import secretmanager

# Import shared enrichment utilities
from parallel_web_tools.core import enrich_batch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache for API key to avoid repeated Secret Manager calls
_api_key_cache: str | None = None


def get_api_key() -> str:
    """Retrieve the Parallel API key from environment or Secret Manager."""
    global _api_key_cache

    if _api_key_cache is not None:
        return _api_key_cache

    # Try environment variable first (for local testing)
    api_key = os.environ.get("PARALLEL_API_KEY")
    if api_key:
        _api_key_cache = api_key
        return api_key

    # Try Google Secret Manager
    secret_name = os.environ.get("PARALLEL_API_KEY_SECRET")
    if secret_name:
        try:
            client = secretmanager.SecretManagerServiceClient()
            response = client.access_secret_version(name=secret_name)
            api_key = response.payload.data.decode("UTF-8")
            return api_key
        except Exception as e:
            logger.error(f"Failed to retrieve secret: {e}")
            raise ValueError(f"Failed to retrieve API key from Secret Manager: {e}") from e

    raise ValueError("No Parallel API key configured. Set PARALLEL_API_KEY or PARALLEL_API_KEY_SECRET.")


def _process_batch(
    inputs: list[dict[str, Any]],
    output_columns: list[str],
    processor: str = "lite-fast",
    timeout: int = 600,
) -> list[dict[str, Any]]:
    """Process multiple enrichment requests using shared enrichment module."""
    if not inputs:
        return []

    logger.info(f"Processing batch of {len(inputs)} inputs with processor: {processor}")

    return enrich_batch(
        inputs=inputs,
        output_columns=output_columns,
        api_key=get_api_key(),
        processor=processor,
        timeout=timeout,
        include_basis=True,
        source="bigquery",
    )


@functions_framework.http
def parallel_enrich(request: Request):
    """
    HTTP Cloud Function endpoint for BigQuery remote function.

    BigQuery sends POST requests with batched calls in format:
    {
        "calls": [
            [input_data, output_columns],
            ...
        ],
        "userDefinedContext": {"processor": "lite-fast"}
    }

    Returns: {"replies": [result1, result2, ...]}
    """
    # Handle CORS preflight
    if request.method == "OPTIONS":
        return (
            "",
            204,
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST",
                "Access-Control-Allow-Headers": "Content-Type",
            },
        )

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return jsonify({"errorMessage": "Invalid request: no JSON body"}), 400

        # Extract processor from user-defined context
        user_context = request_json.get("userDefinedContext", {})
        processor = user_context.get("processor", "lite-fast")

        calls = request_json.get("calls", [])
        if not calls:
            return jsonify({"replies": []})

        logger.info(f"Processing {len(calls)} requests with processor: {processor}")

        # Parse all calls
        parsed_calls = []
        for i, call in enumerate(calls):
            if len(call) < 2:
                parsed_calls.append({"index": i, "error": "Invalid arguments"})
                continue

            input_data = call[0]
            output_columns = call[1]

            # Parse JSON strings
            if isinstance(input_data, str):
                try:
                    input_data = json.loads(input_data)
                except json.JSONDecodeError:
                    parsed_calls.append({"index": i, "error": "Invalid input JSON"})
                    continue

            if isinstance(output_columns, str):
                try:
                    output_columns = json.loads(output_columns)
                except json.JSONDecodeError:
                    output_columns = [output_columns]

            if not isinstance(output_columns, list):
                output_columns = [str(output_columns)]

            parsed_calls.append(
                {
                    "index": i,
                    "input_data": input_data,
                    "output_columns": output_columns,
                }
            )

        valid_calls = [c for c in parsed_calls if "error" not in c]
        error_calls = [c for c in parsed_calls if "error" in c]

        # Use batch processing if all calls have same output columns
        if len(valid_calls) > 1:
            output_cols_set = {tuple(c["output_columns"]) for c in valid_calls}
            if len(output_cols_set) == 1:
                common_output_cols = valid_calls[0]["output_columns"]
                inputs = [c["input_data"] for c in valid_calls]

                logger.info(f"Using batch processing for {len(inputs)} inputs")
                batch_results = _process_batch(inputs, common_output_cols, processor)

                replies: list[dict[str, Any] | None] = [None] * len(calls)
                for error_call in error_calls:
                    replies[error_call["index"]] = {"error": error_call["error"]}
                for j, valid_call in enumerate(valid_calls):
                    replies[valid_call["index"]] = batch_results[j]

                return jsonify({"replies": replies})

        # Fallback: process individually
        replies: list[dict[str, Any] | None] = [None] * len(calls)
        for error_call in error_calls:
            replies[error_call["index"]] = {"error": error_call["error"]}

        for valid_call in valid_calls:
            result = _process_batch([valid_call["input_data"]], valid_call["output_columns"], processor)
            replies[valid_call["index"]] = result[0] if result else {"error": "No result"}

        return jsonify({"replies": replies})

    except Exception as e:
        logger.error(f"Error in parallel_enrich: {e}")
        return jsonify({"errorMessage": str(e)}), 500
