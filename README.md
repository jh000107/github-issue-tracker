# github-issue-tracker

## Data Ingestion

### Historical Data
**Approach 1**: Ingest compressed .gz files directly from [GH Archive](https://www.gharchive.org/) through Azure Data Factory
- 1 Hour of github events data takes up approximately 70 Mib
- Time range: 2024-03-01-0 ~ 2025-02-28-23 (1 year)

Pipeline:
1. Generate timestamps using `generate_timestamps.py`.
2. Upload it to our blob storage.
3. Look up `timestamps.json`.
4. Loop through `timestamps.json` to ingest data hourly.

Approach 2: Use AzCopy to directly store the data into the storage instead of processing through ADF
- Pros: Cost Reduction

Approach 1 was taken. However, in order to reduce the size, we chose to convert compressed json files into parquet after removing all unnecessary columns.

### Real-Time (Streaming) Data
Ingest real-time events from (GitHub Firehose)[https://github-firehose.libraries.io/].


## Data Processing
