from azure.storage.blob import BlobServiceClient
import os

from dotenv import load_dotenv
load_dotenv()  # Load from .env file

account_name = "team13adls"
account_key = os.getenv("STORAGE_ACCOUNT_KEY")  # Set this in your environment variables
container_name = "github-realtime-issue"
target_path = "models/issue_classifier/"
local_model_folder = "./trained_model"

blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

container_client = blob_service_client.get_container_client(container_name)

# === Upload all .pkl files ===
for filename in os.listdir(local_model_folder):
    if filename.endswith(".pkl"):
        local_path = os.path.join(local_model_folder, filename)
        blob_path = target_path + filename

        print(f"Uploading {filename} to {blob_path}...")
        with open(local_path, "rb") as data:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(data, overwrite=True)

print("All model artifacts uploaded to ADLS.")