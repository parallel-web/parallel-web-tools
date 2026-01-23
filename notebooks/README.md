# Notebooks

Interactive Jupyter notebooks demonstrating parallel-web-tools features.

## Available Notebooks

### [spark_enrichment_demo.ipynb](spark_enrichment_demo.ipynb)

Demonstrates Apache Spark integration with SQL-native UDFs:

- Register `parallel_enrich()` UDF with Spark
- Enrich data using SQL queries
- Parse JSON results into structured columns
- Batch enrichment with Python API
- Streaming enrichment with `foreachBatch`
- Processor selection guide

**Prerequisites:**
```bash
pip install parallel-web-tools[spark] jupyter
```

## Running the Notebooks

1. Install dependencies:
   ```bash
   pip install parallel-web-tools[spark] jupyter
   ```

2. Authenticate:
   ```bash
   parallel-cli login
   ```

3. Start Jupyter:
   ```bash
   jupyter notebook
   ```

4. Open the desired notebook and run the cells.

## Notes

- Notebooks require Java Runtime Environment for Spark
- API calls may take a few seconds per row depending on processor
- Use `lite-fast` processor for quick demos, `pro` for deeper research
