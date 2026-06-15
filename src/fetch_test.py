import requests
from google.transit import gtfs_realtime_pb2

# Subway Feed 1 (Lines 1, 2, 3, 4, 5, 6, and 5)
MTA_FEED_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"



def fetch_transit_data():
	print(f"Fetching data from: {MTA_FEED_URL}...")

	try:
		# 1. Get the binary response
		response = requests.get(MTA_FEED_URL)
		response.raise_for_status() # Raise error if download fails

		# 2. Initialize the protobuf message
		feed = gtfs_realtime_pb2.FeedMessage()

		# 3. Parse the binary content
		feed.ParseFromString(response.content)

		# 4. Print a sample of the data
		print(f"Successfully decoded {len(feed.entity)} transit entities.\n")

		# Let's look at the first vehicle position found
		for entity in feed.entity:
			if entity.HasField('vehicle'):
				print("--- Sample Vehicle Position ---")
				print(entity.vehicle)
				break # Just print one for the test

	
	except Exception as e:
		print(f"Error fetching data: {e}")
	
if __name__ == "__main__":
	fetch_transit_data()