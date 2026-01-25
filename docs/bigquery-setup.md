# BigQuery Setup Guide

This guide covers how to deploy and use the Parallel BigQuery integration for SQL-native data enrichment.

## Architecture

The integration uses BigQuery Remote Functions to call a Cloud Function, which interfaces with the Parallel API:

```
BigQuery Query
    │
    ▼
BigQuery Remote Function (parallel_enrich)
    │
    ▼
Cloud Function (HTTP endpoint)
    │
    ▼
Parallel Task API
```

## Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Parallel API Key** from [platform.parallel.ai](https://platform.parallel.ai)
3. **Google Cloud SDK** installed and authenticated:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
4. **BigQuery CLI** (`bq` command)

## Installation

```bash
pip install parallel-web-tools[bigquery]
```

> **Note:** The standalone `parallel-cli` binary does not include deployment commands. You must install via pip with the `[bigquery]` extra to use `parallel-cli enrich deploy --system bigquery`.

## Quick Start Deployment

### Option 1: CLI (Recommended)

```bash
parallel-cli enrich deploy --system bigquery \
    --project=your-gcp-project \
    --region=us-central1 \
    --api-key=your-parallel-api-key
```

### Option 2: Python API

```python
from parallel_web_tools.integrations.bigquery import deploy_bigquery_integration

result = deploy_bigquery_integration(
    project_id="your-gcp-project",
    api_key="your-parallel-api-key",
    region="us-central1",
)

print(result["example_query"])
```

## What Gets Deployed

The deployment creates:

1. **Secret** in Secret Manager for the API key
2. **Cloud Function** (Gen2) that handles enrichment requests
3. **BigQuery Connection** for remote function calls
4. **BigQuery Dataset** (`parallel_functions` by default)
5. **Remote Functions**:
   - `parallel_enrich(input_data, output_columns)` - Main enrichment function
   - `parallel_enrich_company(name, website, fields)` - Convenience function

## Usage Examples

### Basic Enrichment

```sql
SELECT
    name,
    `your-project.parallel_functions.parallel_enrich`(
        JSON_OBJECT('company_name', name, 'website', website),
        JSON_ARRAY('CEO name', 'Founding year', 'Brief description')
    ) as enriched_data
FROM your_dataset.companies
LIMIT 10;
```

### Parse JSON Results

```sql
WITH enriched AS (
    SELECT
        name,
        `your-project.parallel_functions.parallel_enrich`(
            JSON_OBJECT('company_name', name),
            JSON_ARRAY('CEO name', 'Industry', 'Headquarters')
        ) as info
    FROM your_dataset.companies
)
SELECT
    name,
    JSON_EXTRACT_SCALAR(info, '$.ceo_name') as ceo,
    JSON_EXTRACT_SCALAR(info, '$.industry') as industry,
    JSON_EXTRACT_SCALAR(info, '$.headquarters') as hq
FROM enriched;
```

### Company Convenience Function

```sql
SELECT
    `your-project.parallel_functions.parallel_enrich_company`(
        'Google',
        'google.com',
        JSON_ARRAY('CEO name', 'Employee count', 'Stock ticker')
    ) as company_info;
```

---

## Manual Deployment

If you prefer manual control, follow these steps:

### Step 1: Enable APIs

```bash
export PROJECT_ID=your-project
export REGION=us-central1

gcloud services enable bigquery.googleapis.com --project=$PROJECT_ID
gcloud services enable bigqueryconnection.googleapis.com --project=$PROJECT_ID
gcloud services enable cloudfunctions.googleapis.com --project=$PROJECT_ID
gcloud services enable secretmanager.googleapis.com --project=$PROJECT_ID
gcloud services enable run.googleapis.com --project=$PROJECT_ID
```

### Step 2: Create Secret for API Key

```bash
echo -n "your-parallel-api-key" | gcloud secrets create parallel-api-key \
    --data-file=- \
    --replication-policy=automatic \
    --project=$PROJECT_ID
```

### Step 3: Deploy Cloud Function

```bash
# Get the cloud_function directory path
FUNCTION_DIR=$(python -c "from parallel_web_tools.integrations.bigquery import deploy; print(deploy._get_cloud_function_dir())")

gcloud functions deploy parallel-enrich \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=$FUNCTION_DIR \
    --entry-point=parallel_enrich \
    --trigger-http \
    --no-allow-unauthenticated \
    --timeout=300s \
    --memory=512MB \
    --set-env-vars="PARALLEL_API_KEY_SECRET=projects/$PROJECT_ID/secrets/parallel-api-key/versions/latest" \
    --project=$PROJECT_ID
```

### Step 4: Create BigQuery Connection

```bash
bq mk --connection \
    --connection_type=CLOUD_RESOURCE \
    --project_id=$PROJECT_ID \
    --location=$REGION \
    parallel-connection
```

### Step 5: Grant Permissions

```bash
# Get service accounts
FUNCTION_SA=$(gcloud functions describe parallel-enrich --gen2 --region=$REGION --format='value(serviceConfig.serviceAccountEmail)' --project=$PROJECT_ID)
CONNECTION_SA=$(bq show --connection $PROJECT_ID.$REGION.parallel-connection --format=json | jq -r '.cloudResource.serviceAccountId')

# Grant function access to secret
gcloud secrets add-iam-policy-binding parallel-api-key \
    --member="serviceAccount:$FUNCTION_SA" \
    --role="roles/secretmanager.secretAccessor" \
    --project=$PROJECT_ID

# Grant connection permission to invoke function
gcloud functions add-iam-policy-binding parallel-enrich \
    --gen2 --region=$REGION \
    --member="serviceAccount:$CONNECTION_SA" \
    --role="roles/cloudfunctions.invoker" \
    --project=$PROJECT_ID

gcloud run services add-iam-policy-binding parallel-enrich \
    --region=$REGION \
    --member="serviceAccount:$CONNECTION_SA" \
    --role="roles/run.invoker" \
    --project=$PROJECT_ID
```

### Step 6: Create Remote Functions

```bash
FUNCTION_URL=$(gcloud functions describe parallel-enrich --gen2 --region=$REGION --format='value(serviceConfig.uri)' --project=$PROJECT_ID)

bq mk --dataset --location=$REGION $PROJECT_ID:parallel_functions

bq query --use_legacy_sql=false "
CREATE OR REPLACE FUNCTION \`$PROJECT_ID.parallel_functions.parallel_enrich\`(
    input_data STRING,
    output_columns STRING
)
RETURNS STRING
REMOTE WITH CONNECTION \`$PROJECT_ID.$REGION.parallel-connection\`
OPTIONS (
    endpoint = '$FUNCTION_URL',
    user_defined_context = [(\"processor\", \"lite-fast\")]
);
"
```

---

## Configuration

### Processor Selection

The default processor is `lite-fast`. To use a different processor, create a custom function:

```sql
CREATE OR REPLACE FUNCTION `your-project.parallel_functions.parallel_enrich_pro`(
    input_data STRING,
    output_columns STRING
)
RETURNS STRING
REMOTE WITH CONNECTION `your-project.us-central1.parallel-connection`
OPTIONS (
    endpoint = 'YOUR_FUNCTION_URL',
    user_defined_context = [("processor", "pro-fast")]
);
```

Available processors:
- `lite-fast` / `lite` - Basic metadata, lowest cost (~$0.005/query)
- `base-fast` / `base` - Standard enrichments (~$0.01/query)
- `core-fast` / `core` - Complex outputs (~$0.025/query)
- `pro-fast` / `pro` - Deep web research (~$0.10/query)

### Timeout Settings

The Cloud Function has a 300-second timeout. For large batches, consider:
- Increasing the function timeout (max 3600s for Gen2)
- Using `lite-fast` processor for faster results
- Processing in smaller batches

---

## Cleanup

Remove all deployed resources:

### Python

```python
from parallel_web_tools.integrations.bigquery import cleanup_bigquery_integration

cleanup_bigquery_integration(
    project_id="your-gcp-project",
    region="us-central1",
    delete_secret=True,  # Also remove the API key secret
)
```

### Manual

```bash
gcloud functions delete parallel-enrich --gen2 --region=$REGION --project=$PROJECT_ID --quiet
bq rm --connection --force $PROJECT_ID.$REGION.parallel-connection
bq rm -r -f $PROJECT_ID:parallel_functions
gcloud secrets delete parallel-api-key --project=$PROJECT_ID --quiet
```

---

## Troubleshooting

### "Permission denied" when calling function

1. Verify the BigQuery connection's service account has Cloud Functions Invoker role
2. Check Cloud Run Invoker role is also granted (required for Gen2 functions)

```bash
# Re-grant permissions
CONNECTION_SA=$(bq show --connection $PROJECT_ID.$REGION.parallel-connection --format=json | jq -r '.cloudResource.serviceAccountId')

gcloud run services add-iam-policy-binding parallel-enrich \
    --region=$REGION \
    --member="serviceAccount:$CONNECTION_SA" \
    --role="roles/run.invoker" \
    --project=$PROJECT_ID
```

### "Secret not found" error

```bash
# Check secret exists
gcloud secrets list --project=$PROJECT_ID

# Check function has access
gcloud secrets get-iam-policy parallel-api-key --project=$PROJECT_ID
```

### Function timeout

- Use `lite-fast` processor for faster results
- Increase function timeout:
  ```bash
  gcloud functions deploy parallel-enrich --gen2 --timeout=600s ...
  ```

### View logs

```bash
gcloud functions logs read parallel-enrich --gen2 --region=$REGION --project=$PROJECT_ID
```

---

## Cost Estimation

| Component | Cost |
|-----------|------|
| Cloud Functions | ~$0.40/million invocations + compute |
| BigQuery | Query processing costs |
| Parallel API | $0.005-$0.10 per enrichment |
| Secret Manager | ~$0.06/10,000 accesses |

For 1,000 company enrichments using `lite-fast`:
- Parallel API: ~$5
- GCP infrastructure: <$1
