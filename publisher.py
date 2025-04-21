import pandas as pd
import requests
import json
from google.cloud import pubsub_v1

# Set your GCP project ID and topic name
project_id = "data-engineering-ij-indiv"
topic_id = "vehicle-breadcrumbs"

# Initialize the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Load the Excel file
df = pd.read_excel("vehicle_ids.xlsx")

# Extract the vehicle IDs from the "Titan" column
vehicle_ids = df['Titan'].dropna().astype(str)

# Loop through and fetch data
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()

            # Loop through each breadcrumb and publish to Pub/Sub
            for breadcrumb in data:
                message_data = json.dumps(breadcrumb).encode("utf-8")
                future = publisher.publish(topic_path, data=message_data)
                print(f"Published breadcrumb for vehicle {vehicle_id}")

        except Exception as e:
            print(f"Failed to process JSON for vehicle {vehicle_id}: {e}")
    else:
        print(f"Failed to fetch for {vehicle_id} (Status: {response.status_code})")

