import os
import json
from datetime import datetime
from google.cloud import pubsub_v1

# Set your GCP project ID and subscription name
project_id = "data-engineering-ij-indiv"
subscription_id = "vehicle-breadcrumbs-sub"

# Initialize subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Output file for today
today = datetime.utcnow().date().isoformat()
output_file = f"breadcrumbs_{today}.json"

# Ensure file exists
if not os.path.exists(output_file):
    with open(output_file, "w") as f:
        json.dump([], f)

# Load existing content
with open(output_file, "r") as f:
    try:
        breadcrumbs = json.load(f)
    except json.JSONDecodeError:
        breadcrumbs = []

def callback(message):
    global breadcrumbs
    try:
        breadcrumb = json.loads(message.data.decode("utf-8"))
        breadcrumbs.append(breadcrumb)
        print(f"Received message for vehicle: {breadcrumb.get('VehicleID')}")
        message.ack()

        # Write to file
        with open(output_file, "w") as f:
            json.dump(breadcrumbs, f, indent=2)
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

# Listen indefinitely
print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()

