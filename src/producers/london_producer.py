import os
import json
import time
import requests
from google.cloud import pubsub_v1
from datetime import datetime

# Configuration
PROJECT_ID = "transit-stream-global"
TOPIC_ID = "mta-subway-stream" # Using the existing topic
TFL_LINES = ["victoria", "jubilee", "northern", "central", "piccadilly"] # Focus on major lines
TFL_BASE_URL = "https://api.tfl.gov.uk/Line/{line}/Arrivals"

# Initialize Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_and_normalize_tfl():
    total_count = 0
    for line in TFL_LINES:
        try:
            print(f"Fetching from TfL Line: {line}...")
            url = TFL_BASE_URL.format(line=line)
            response = requests.get(url, timeout=10)
            
            if response.status_code != 200:
                print(f"Error fetching {line}: {response.status_code}")
                continue
                
            predictions = response.json()
            
            line_count = 0
            for p in predictions:
                # Map TfL JSON to our internal schema
                # Internal Schema: event_type, trip_id, route_id, stop_id, status, timestamp, city
                
                # Convert TfL ISO timestamp to Unix timestamp
                # Example: "2026-06-17T14:23:58.9309222Z"
                try:
                    ts_str = p.get("timestamp").split('.')[0].replace('Z', '')
                    dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
                    unix_ts = int(dt.timestamp())
                except:
                    unix_ts = int(time.time())

                message_data = {
                    "event_type": "trip_update", # TfL data is essentially predictions
                    "trip_id": p.get("vehicleId", "N/A"),
                    "route_id": p.get("lineId", "N/A"),
                    "stop_id": p.get("naptanId", "N/A"),
                    "status": p.get("currentLocation", "N/A"),
                    "timestamp": unix_ts,
                    "city": "London"
                }

                # Publish
                data_str = json.dumps(message_data)
                publisher.publish(topic_path, data_str.encode("utf-8"))
                line_count += 1
            
            print(f"Published {line_count} messages for {line}.")
            total_count += line_count
            
        except Exception as e:
            print(f"Error processing {line}: {e}")
            
    print(f"Total London messages published: {total_count}")

if __name__ == "__main__":
    while True:
        try:
            fetch_and_normalize_tfl()
            # TfL data usually refreshes every 30s, but we'll wait 60s to be polite 
            # and avoid hitting rate limits across multiple lines.
            time.sleep(60)
        except Exception as e:
            print(f"Global Loop Error: {e}")
            time.sleep(10)
