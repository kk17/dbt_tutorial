from tkinter import PROJECTING
from google.cloud import bigquery
import os
from os import path

# Construct a BigQuery client object.
client = bigquery.Client()

BASE_DIR = "./flights_data"
PROJECT_NAME = os.environ["PROJECT_NAME"]
TABLE_ID_PREFIX = PROJECT_NAME + ".landing_zone_flights."

def load_one_csv(table_name, schema):
    print(f"loading table {table_name}")
    file_path = path.join(BASE_DIR, table_name + ".csv")
    table_id = TABLE_ID_PREFIX + table_name
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    with open(file_path, "rb") as file:
        load_job = client.load_table_from_file(
            file, table_id, job_config=job_config)  # Make an API request.

        load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


table_map = {
    # "airports": [
    #     bigquery.SchemaField("Code", "INTEGER"),
    #     bigquery.SchemaField("Description", "STRING"),
    # ],
    # "carriers": [
    #     bigquery.SchemaField("Code", "STRING"),
    #     bigquery.SchemaField("Description", "STRING"),
    # ],
    "flights": [
        bigquery.SchemaField("FL_DATE", "DATE"),
        bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING"),
        bigquery.SchemaField("ORIGIN_AIRPORT_ID", "INTEGER"),
        bigquery.SchemaField("ORIGIN_AIRPORT_SEQ_ID", "INTEGER"),
        bigquery.SchemaField("ORIGIN_CITY_MARKET_ID", "INTEGER"),
        bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING"),
        bigquery.SchemaField("DEST_AIRPORT_ID", "INTEGER"),
        bigquery.SchemaField("DEST_AIRPORT_SEQ_ID", "INTEGER"),
        bigquery.SchemaField("DEST_CITY_MARKET_ID", "INTEGER"),
        bigquery.SchemaField("DEST_CITY_NAME", "STRING"),
        bigquery.SchemaField("DEP_DELAY_NEW", "FLOAT"),
        bigquery.SchemaField("ARR_DELAY_NEW", "FLOAT"),
        bigquery.SchemaField("CANCELLED", "FLOAT"),
        bigquery.SchemaField("DIVERTED", "FLOAT"),
        bigquery.SchemaField("ACTUAL_ELAPSED_TIME", "FLOAT"),
        bigquery.SchemaField("DISTANCE", "FLOAT"),
        bigquery.SchemaField("REMARK", "STRING", mode='NULLABLE'),
    ],
}

for t, s in table_map.items():
    load_one_csv(t, s)
