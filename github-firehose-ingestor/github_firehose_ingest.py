# GitHub Firehose Ingestor
# Goal: Listen to GitHub Firehose, filter `IssuesEvent`, send to Event Hub, store to ADLS

import requests
import sseclient
import json
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  # Load from .env file

# --- Configuration from environment variables ---
EVENT_HUB_CONN_STR = os.environ.get("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")
ADLS_CONNECTION_STRING = os.environ.get("ADLS_CONNECTION_STRING")
ADLS_CONTAINER_NAME = os.environ.get("ADLS_CONTAINER_NAME", "github-realtime-issue")

# --- Initialize Azure Clients ---
eventhub_client = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONN_STR,
    eventhub_name=EVENT_HUB_NAME
)

blob_service_client = BlobServiceClient.from_connection_string(ADLS_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(ADLS_CONTAINER_NAME)
# container_client.create_container()  # Only creates if doesn't exist

# --- Connect to GitHub Firehose ---
print("Connecting to GitHub Firehose...")
response = requests.get("http://github-firehose.libraries.io/events", stream=True)
client = sseclient.SSEClient(response)

# --- Process incoming events ---
print("Listening for IssuesEvent...")

for event in client.events():
    if event.event == "event":
        try:
            data = json.loads(event.data)
            if data.get("type") == "IssuesEvent":
                print("\n✅ IssuesEvent received")

                # --- Send to Event Hub ---
                event_data = EventData(json.dumps(data))
                batch = eventhub_client.create_batch()
                batch.add(event_data)
                eventhub_client.send_batch(batch)
                print("   → Sent to Event Hub")

                # --- Store to ADLS ---
                timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
                blob_name = f"bronze/firehose/issue-{timestamp}.json"
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(json.dumps(data, indent=2))
                print(f"   → Stored to ADLS as {blob_name}")

        except Exception as e:
            print("⚠️ Error processing event:", str(e))