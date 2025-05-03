from azure.storage.blob import BlobServiceClient

import os
import pandas as pd
import glob

from dotenv import load_dotenv
load_dotenv()  # Load from .env file


account_name = "team13adls"
account_key = os.getenv("STORAGE_ACCOUNT_KEY")  # Set this in your environment variables
container_name = "github-realtime-issue"
folder_path_in_blob = "gold/training_data/"
local_download_folder = "./data/"

blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

container_client = blob_service_client.get_container_client(container_name)

blobs_list = container_client.list_blobs(name_starts_with=folder_path_in_blob)

for blob in blobs_list:
    blob_name = blob.name
    print(f"Downloading: {blob_name}")

    # Create local file path
    relative_path = os.path.relpath(blob_name, folder_path_in_blob)  # remove the prefix
    local_file_path = os.path.join(local_download_folder, relative_path)

    # Make sure local directory exists
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)


    # Download the blob
    blob_client = container_client.get_blob_client(blob)
    with open(local_file_path, "wb") as file:
        data = blob_client.download_blob()
        file.write(data.readall())

print("✅ Folder download complete!")

csv_files = glob.glob("./data/*.csv")
df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
df.to_csv("training_data_combined.csv", index=False)

print("✅ Combined CSV saved to training_data_combined.csv")