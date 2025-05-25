

import requests
import json
from google.cloud import pubsub_v1

# === Configuration ===
project_id = "data-engineering-ij-indiv"  # Replace with your actual project ID
topic_id = "lab-topic"
vehicle_ids = [3003,3007]
api_url_base = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

# === Step 1: Fetch breadcrumb data for both vehicle_ids ===
all_data = []

for vid in vehicle_ids:
    response = requests.get(f"{api_url_base}{vid}")
    if response.status_code == 200:
        vehicle_data = response.json()
        all_data.extend(vehicle_data)
        print(f"Fetched {len(vehicle_data)} records for vehicle {vid}")
    else:
        print(f"Failed to fetch data for vehicle {vid}, status code: {response.status_code}")

# === Step 2: Save the data to bcsample.json ===
with open("bcsample.json", "w") as f:
    json.dump(all_data, f, indent=2)
print(f"Saved {len(all_data)} records to bcsample.json")

# === Step 3: Publish to Pub/Sub ===
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for crumb in all_data:
    data = json.dumps(crumb).encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

print(f"Published {len(all_data)} messages to {topic_path}.")

