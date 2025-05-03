import azure.functions as func
import logging
import os
import re
import json
import joblib
from azure.storage.blob import BlobServiceClient
from datetime import datetime


app = func.FunctionApp()

# Global cache
model, tfidf, label_encoder = None, None, None

def clean_text(text):
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def load_model():
    global model, tfidf, label_encoder
    if model and tfidf and label_encoder:
        return
    
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["STORAGE_ACCOUNT_KEY"]
    container_name = "github-realtime-issue"
    model_path = "models/issue_classifier/"

    blob_service = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )

    container = blob_service.get_container_client(container_name)

    def download_blob(filename):
        blob = container.get_blob_client(model_path + filename)
        local_path = f"/tmp/{filename}"
        with open(local_path, "wb") as f:
            f.write(blob.download_blob().readall())
        return local_path
    
    model = joblib.load(download_blob("classifier.pkl"))
    tfidf = joblib.load(download_blob("tfidf.pkl"))
    label_encoder = joblib.load(download_blob("label_encoder.pkl"))

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="firehose",
                               connection="EventHubConnection") 
def PredictIssue(azeventhub: func.EventHubEvent):
    logging.info('🔔 Event received.')

    load_model()

    try:
        data = json.loads(azeventhub.get_body().decode("utf-8"))
        title = data.get("issue_title", "")
        body = data.get("issue_body", "")
        full_text = clean_text(f"{title} {body}")

        X = tfidf.transform([full_text])
        pred_idx = model.predict(X)[0]
        pred_label = label_encoder.inverse_transform([pred_idx])[0]

        result = {
            "issue_title": title,
            "issue_body": body,
            "predicted_label": pred_label,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Save prediction to blob
        blob_service = BlobServiceClient(
            account_url=f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.blob.core.windows.net",
            credential=os.environ["STORAGE_ACCOUNT_KEY"]
        )
        container = blob_service.get_container_client("github-realtime-issue")
        blob_name = f"predictions/{datetime.utcnow().isoformat()}.json"
        container.upload_blob(blob_name, data=json.dumps(result), overwrite=True)

        logging.info(f"Prediction saved: {result}")

    except Exception as e:
        logging.error(f"Prediction failed: {e}")


