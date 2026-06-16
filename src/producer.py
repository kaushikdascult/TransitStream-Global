import os
import json
import time
import requests
from google.cloud import pubsub_v1
from google.transit import gtfs_realtime_pb2

# Configuration
PROJECT_ID = "transit-stream-global"
TOPIC_ID = "mta-subway-stream"
MTA_FEED_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

# Initialize Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def get_status_name(status_int):
    # Convert Enum integer to string name
    try:
        return gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(status_int)
    except:
        return "UNKNOWN"

def flatten_and_publish():
    print(f"Fetching from MTA...")
    response = requests.get(MTA_FEED_URL)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    count = 0 
    for entity in feed.entity:
        message_data = None
        current_ts = int(time.time())

        # Handle Vehicle Positions
        if entity.HasField('vehicle'):
            v = entity.vehicle
            message_data = {
                "event_type": "vehicle_position",
                "trip_id": v.trip.trip_id,
                "route_id": v.trip.route_id,
                "stop_id": str(v.stop_id),
                "status": get_status_name(v.current_status),
                "timestamp": v.timestamp if v.timestamp else current_ts,
                "city": "NYC"
            }

        # Handle Trip Updates (Predictions)
        elif entity.HasField('trip_update'):
            tu = entity.trip_update
            # We take the first stop time update as a sample for flattening
            stop_time = tu.stop_time_update[0] if tu.stop_time_update else None 
            message_data = {
                "event_type": "trip_update",
                "trip_id": tu.trip.trip_id,
                "route_id": tu.trip.route_id,
                "stop_id": str(stop_time.stop_id) if stop_time else "N/A",
                "timestamp": tu.timestamp if tu.timestamp else current_ts,
                "city": "NYC"
            }

        if message_data:
            # Convert to JSON string and then to bytes
            data_str = json.dumps(message_data)
            publisher.publish(topic_path, data_str.encode("utf-8"))
            count += 1
    print(f"Published {count} flattened messages to {TOPIC_ID}.")

if __name__ == "__main__":
    while True:
        try:
            flatten_and_publish()
            time.sleep(30) # MTA feeds refresh every ~30 seconds
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)
