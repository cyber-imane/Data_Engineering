
from google.cloud import pubsub_v1
import time
import threading

# === Configuration ===
project_id = "data-engineering-ij-indiv"  # Replace with your actual project ID
subscription_id = "my-sub"      # Replace with your actual subscription ID

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Counter for received messages
received_count = 0
lock = threading.Lock()

def callback(message):
    global received_count
    with lock:
        received_count += 1
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()

# Listen for messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Run for a fixed time then stop
timeout = 300  # seconds

def stop_listener():
    streaming_pull_future.cancel()

timer = threading.Timer(timeout, stop_listener)
timer.start()

try:
    streaming_pull_future.result()
except:  # Cancelled
    pass

print(f"\n✅ Total messages received: {received_count}")


















