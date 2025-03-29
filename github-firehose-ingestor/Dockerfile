FROM python:3.10-slim

WORKDIR /app

COPY github_firehose_ingest.py .

RUN pip install requests python-dotenv sseclient-py azure-eventhub azure-storage-blob

CMD ["python", "github_firehose_ingest.py"]