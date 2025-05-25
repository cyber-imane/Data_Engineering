


import requests
import json
from google.cloud import pubsub_v1
from concurrent import futures

def future_callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"An error occurred: {e}")

# Set your GCP project ID and topic name
project_id = "data-engineering-ij-indiv"
topic_id = "lab-topic"

# Initialize the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Step 1: Fetch all breadcrumb data from general endpoint
breadcrumb_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?"
response = requests.get(breadcrumb_url)

if response.status_code != 200:
    print(f"Failed to fetch breadcrumbs (Status: {response.status_code})")
    exit()

try:
    all_breadcrumbs = response.json()
except Exception as e:
    print(f"Failed to parse breadcrumb JSON: {e}")
    exit()

# Step 2: Extract unique Titan vehicle IDs (first 100)
titan_vehicle_ids = []
seen_ids = set()

for breadcrumb in all_breadcrumbs:
    vehicle_id = str(breadcrumb.get("vehicle_id"))
    group = breadcrumb.get("group")

    if group == "Titan" and vehicle_id not in seen_ids:
        titan_vehicle_ids.append(vehicle_id)
        seen_ids.add(vehicle_id)

    if len(titan_vehicle_ids) == 100:
        break

if not titan_vehicle_ids:
    print("No Titan vehicles found in breadcrumb data.")
    exit()

# Step 3: Fetch breadcrumbs for each Titan vehicle and publish
future_list = []

for vehicle_id in titan_vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()

            for breadcrumb in data:
                message_data = json.dumps(breadcrumb).encode("utf-8")
                future = publisher.publish(topic_path, data=message_data)
                future.add_done_callback(future_callback)
                future_list.append(future)
                print(f"Published breadcrumb for Titan vehicle {vehicle_id}")

        except Exception as e:
            print(f"Failed to process JSON for vehicle {vehicle_id}: {e}")
    else:
        print(f"Failed to fetch for {vehicle_id} (Status: {response.status_code})")

# Step 4: Wait for all futures to complete
for future in futures.as_completed(future_list):
    continue


















