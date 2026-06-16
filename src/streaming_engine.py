import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import time
from datetime import datetime

# Custom DoFn to write to BigTable
class WriteToBigTableFn(beam.DoFn):
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

    def process(self, element):
        from google.cloud import bigtable
        
        client = bigtable.Client(project=self.project_id, admin=True)
        instance = client.instance(self.instance_id)
        table = instance.table(self.table_id)
        
        # Row Key: CITY#ROUTE#TRIP_ID
        # Ensure all components are strings
        city = str(element.get('city', 'N/A'))
        route_id = str(element.get('route_id', 'N/A'))
        trip_id = str(element.get('trip_id', 'N/A'))
        
        row_key = f"{city}#{route_id}#{trip_id}".encode()
        row = table.direct_row(row_key)
        
        # Column Family: status_updates
        family_id = "status_updates"
        
        # Fallback timestamp
        ts = element.get('timestamp', int(time.time()))
        dt = datetime.fromtimestamp(ts)
        
        row.set_cell(
            family_id,
            "stop_id".encode(),
            str(element.get('stop_id', 'N/A')).encode(),
            timestamp=dt
        )
        row.set_cell(
            family_id,
            "status".encode(),
            str(element.get('status', 'N/A')).encode(),
            timestamp=dt
        )
        row.set_cell(
            family_id,
            "timestamp".encode(),
            str(ts).encode(),
            timestamp=dt
        )
        
        row.commit()
        yield element

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        required=True,
        help="GCP Project ID"
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Pub/Sub topic to read from"
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="BigQuery table to write to (project:dataset.table)"
    )
    parser.add_argument(
        "--bt_instance",
        required=True,
        help="BigTable instance ID"
    )
    parser.add_argument(
        "--bt_table",
        required=True,
        help="BigTable table ID"
    )

    args, beam_args = parser.parse_known_args()
    
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True
    
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project_id
    # Setting job name for Dataflow visibility
    google_cloud_options.job_name = f"transit-stream-engine-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    with beam.Pipeline(options=options) as p:
        # 1. READ from Pub/Sub
        raw_stream = (
            p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=args.topic)
              | "ParseJSON" >> beam.Map(json.loads)
        )

        # 2. WRITE to BigQuery (Long-term Analytics)
        (
            raw_stream 
            | "FormatForBQ" >> beam.Map(lambda x: {
                "event_type": str(x.get("event_type", "N/A")),
                "trip_id": str(x.get("trip_id", "N/A")),
                "route_id": str(x.get("route_id", "N/A")),
                "stop_id": str(x.get("stop_id", "N/A")),
                "status": str(x.get("status", "N/A")),
                "event_timestamp": datetime.fromtimestamp(x.get("timestamp", int(time.time()))).isoformat(),
                "city": str(x.get("city", "N/A"))
            })
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                args.bq_table,
                schema="event_type:STRING,trip_id:STRING,route_id:STRING,stop_id:STRING,status:STRING,event_timestamp:TIMESTAMP,city:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 3. WRITE to BigTable (Real-time Status)
        (
            raw_stream
            | "WriteToBigTable" >> beam.ParDo(WriteToBigTableFn(args.project_id, args.bt_instance, args.bt_table))
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
