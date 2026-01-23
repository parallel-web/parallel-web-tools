# Spark Setup Guide

This guide covers how to configure `parallel-web-tools` for different Spark environments.

## Installation

```bash
pip install parallel-web-tools[spark]
```

## Authentication

The Spark integration uses the `PARALLEL_API_KEY` environment variable or an explicit `api_key` parameter. Choose the method that fits your environment.

---

## Local Development

Set the environment variable before starting Spark:

```bash
export PARALLEL_API_KEY="your-api-key"
pyspark
```

Or in Python:

```python
import os
os.environ["PARALLEL_API_KEY"] = "your-api-key"

from parallel_web_tools.integrations.spark import register_parallel_udfs
register_parallel_udfs(spark)
```

---

## Databricks

### Option 1: Databricks Secrets (Recommended)

1. Create a secret scope and store your API key:
   ```bash
   databricks secrets create-scope --scope parallel
   databricks secrets put --scope parallel --key api-key
   ```

2. Access the secret in your notebook:
   ```python
   from parallel_web_tools.integrations.spark import register_parallel_udfs

   api_key = dbutils.secrets.get("parallel", "api-key")
   register_parallel_udfs(spark, api_key=api_key)
   ```

### Option 2: Cluster Environment Variable

1. Go to **Compute** → Select your cluster → **Edit**
2. Expand **Advanced Options** → **Spark** tab
3. Under **Environment Variables**, add:
   ```
   PARALLEL_API_KEY=your-api-key
   ```
4. Restart the cluster

Then in your notebook:
```python
from parallel_web_tools.integrations.spark import register_parallel_udfs
register_parallel_udfs(spark)
```

---

## AWS EMR

### Option 1: AWS Secrets Manager (Recommended)

1. Store your API key in Secrets Manager:
   ```bash
   aws secretsmanager create-secret \
     --name parallel/api-key \
     --secret-string "your-api-key"
   ```

2. Add IAM permissions for EMR to access the secret

3. Retrieve in your Spark job:
   ```python
   import boto3
   from parallel_web_tools.integrations.spark import register_parallel_udfs

   secrets = boto3.client("secretsmanager")
   api_key = secrets.get_secret_value(SecretId="parallel/api-key")["SecretString"]
   register_parallel_udfs(spark, api_key=api_key)
   ```

### Option 2: Bootstrap Action

Create a bootstrap script that sets the environment variable:

```bash
#!/bin/bash
echo "export PARALLEL_API_KEY=your-api-key" >> /etc/spark/conf/spark-env.sh
```

Add it when creating the cluster:
```bash
aws emr create-cluster \
  --bootstrap-actions Path=s3://your-bucket/set-parallel-key.sh
```

### Option 3: EMR Step Configuration

Pass as a Spark configuration:
```bash
aws emr add-steps --cluster-id j-XXXXX --steps '[{
  "Name": "Spark Job",
  "ActionOnFailure": "CONTINUE",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--conf", "spark.executorEnv.PARALLEL_API_KEY=your-api-key",
      "--conf", "spark.yarn.appMasterEnv.PARALLEL_API_KEY=your-api-key",
      "s3://your-bucket/your-script.py"
    ]
  }
}]'
```

---

## Google Cloud Dataproc

### Option 1: Secret Manager (Recommended)

1. Store your API key:
   ```bash
   echo -n "your-api-key" | gcloud secrets create parallel-api-key --data-file=-
   ```

2. Grant access to the Dataproc service account

3. Retrieve in your Spark job:
   ```python
   from google.cloud import secretmanager
   from parallel_web_tools.integrations.spark import register_parallel_udfs

   client = secretmanager.SecretManagerServiceClient()
   name = "projects/your-project/secrets/parallel-api-key/versions/latest"
   api_key = client.access_secret_version(name=name).payload.data.decode()
   register_parallel_udfs(spark, api_key=api_key)
   ```

### Option 2: Cluster Properties

Set when creating the cluster:
```bash
gcloud dataproc clusters create my-cluster \
  --properties "spark-env:PARALLEL_API_KEY=your-api-key"
```

### Option 3: Job Properties

Pass when submitting a job:
```bash
gcloud dataproc jobs submit pyspark my-script.py \
  --cluster=my-cluster \
  --properties="spark.executorEnv.PARALLEL_API_KEY=your-api-key,spark.yarn.appMasterEnv.PARALLEL_API_KEY=your-api-key"
```

---

## Azure Synapse / HDInsight

### Option 1: Azure Key Vault (Recommended)

1. Store your API key in Key Vault

2. Link Key Vault to your Synapse workspace

3. Access in your notebook:
   ```python
   from notebookutils import mssparkutils
   from parallel_web_tools.integrations.spark import register_parallel_udfs

   api_key = mssparkutils.credentials.getSecret("your-keyvault", "parallel-api-key")
   register_parallel_udfs(spark, api_key=api_key)
   ```

### Option 2: Spark Configuration

In Synapse, add to your Spark pool configuration:
```
spark.executorEnv.PARALLEL_API_KEY=your-api-key
spark.yarn.appMasterEnv.PARALLEL_API_KEY=your-api-key
```

---

## Standalone Spark Cluster

### Option 1: spark-env.sh

Add to `$SPARK_HOME/conf/spark-env.sh` on all nodes:
```bash
export PARALLEL_API_KEY="your-api-key"
```

### Option 2: spark-submit

Pass when submitting:
```bash
spark-submit \
  --conf "spark.executorEnv.PARALLEL_API_KEY=your-api-key" \
  --conf "spark.driverEnv.PARALLEL_API_KEY=your-api-key" \
  your-script.py
```

### Option 3: SparkConf in Code

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.executorEnv.PARALLEL_API_KEY", "your-api-key") \
    .getOrCreate()
```

---

## Kubernetes (Spark on K8s)

### Option 1: Kubernetes Secrets

1. Create the secret:
   ```bash
   kubectl create secret generic parallel-api-key \
     --from-literal=api-key=your-api-key
   ```

2. Reference in spark-submit:
   ```bash
   spark-submit \
     --conf "spark.kubernetes.driver.secretKeyRef.PARALLEL_API_KEY=parallel-api-key:api-key" \
     --conf "spark.kubernetes.executor.secretKeyRef.PARALLEL_API_KEY=parallel-api-key:api-key" \
     your-script.py
   ```

---

## Security Best Practices

1. **Never hardcode API keys** in notebooks or scripts that are version controlled
2. **Use secret managers** when available (Databricks Secrets, AWS Secrets Manager, etc.)
3. **Rotate keys regularly** and update your secret manager
4. **Limit access** to secrets using IAM policies or RBAC
5. **Audit access** to secrets in production environments
