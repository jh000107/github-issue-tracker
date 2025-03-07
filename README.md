# github-issue-tracker

## Data Ingestion

Approach 1: Ingest compressed .gz files directly from GH Archive through Azure Data Factory
- 1 Hour of github events data takes up approximately 70 Mib

Approach 2: Use AzCopy to directly store the data into the storage instead of processing through ADF
- Pros: Cost Reduction

## Data Analysis
