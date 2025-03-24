import logging
import requests
import json
import sseclient
from azure.eventhub import EventHubProducerClient, EventData
import azure.functions as func
import datetime
import pytz
import os

app = func.FunctionApp()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Azure Event Hub configuration
connection_str = os.getenv("EVENT_HUB_CONNECTION_STRING")
eventhub_name = 'githubfirehose'

firehose_url = "http://github-firehose.libraries.io/events"

# Function to stream from GitHub Firehose and send each event to Event Hub
def stream_and_send_firehose_events(max_events=10):
    try:
        response = requests.get(firehose_url, stream=True)
        client = sseclient.SSEClient(response)

        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_str,
            eventhub_name=eventhub_name
        )
        event_batch = producer.create_batch()
        count = 0

        for event in client.events():
            if event.event == 'event':
                logging.info(f"Received GitHub Event: {event.data[:200]}...")

                event_batch.add(EventData(event.data))
                count += 1

                if count >= max_events:
                    break

        producer.send_batch(event_batch)
        producer.close()
        logging.info(f"{count} GitHub events sent to Event Hub.")

    except Exception as e:
        logging.error(f"Error streaming GitHub Firehose: {e}")
        raise

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def main(mytimer: func.TimerRequest) -> None:
    try:
        if mytimer.past_due:
            logging.info('The timer is past due!')
        # Get the current time in UTC and convert to EST
        utc_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        est_timezone = pytz.timezone('America/New_York')
        est_time = utc_time.astimezone(est_timezone).isoformat()

        logging.info("Starting function execution...")

        logging.info(f"GitHub Firehose function triggered at {est_time}")
        stream_and_send_firehose_events()

    except Exception as e:
        logging.error(f"Function execution failed: {e}")
