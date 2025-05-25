



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
topic_id = "vehicle-breadcrumbs"

# Initialize the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# ✅ Correct vehicle list source
vehicle_list_url = "https://busdata.cs.pdx.edu/api/vehicles"
response = requests.get(vehicle_list_url)

if response.status_code != 200:
    print(f"Failed to fetch vehicle list (Status: {response.status_code})")
    exit()

try:
    vehicles = response.json()

    # Filter Titan vehicles
    titan_vehicle_ids = [
        str(v["vehicle_id"]) for v in vehicles
        if v.get("group") == "Titan"
    ][:100]

    if not titan_vehicle_ids:
        print("No Titan vehicles found.")
        exit()

except Exception as e:
    print(f"Failed to parse vehicle list: {e}")
    exit()

future_list = []

# Fetch and publish breadcrumbs
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

# Wait for publishing to complete
for future in futures.as_completed(future_list):
    continue







