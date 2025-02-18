import json
import os

from datetime import datetime
from google.cloud import bigquery

from big_query.bq_schemas import bq_schemas

# loading in the credentials dictionary
file_path = os.path.join("credentials", "bq_credentials.json")
with open(file_path, "r") as file:
    creds = json.load(file)


def upload_data_to_bq(df, dataset_name, table_name, mode="append") -> None:
    """
    df (dataframe):     pandas dataframe containing the data to be uploaded
    dataset_name (str): name of bq dataset containing the data table
    table_name (str):   name of bq table and identifier for BQ schema
    mode (str):         "append" for adding data to existing table (default), "truncate" for uploading new table, 
    """
    # validating the mode parameter
    allowed_modes = {"truncate", "append"}
    if mode not in allowed_modes:
        raise ValueError(f"Invalid mode '{mode}'. Allowed values are {allowed_modes}")

    # initialize BigQuery client
    project_id = creds["project_id"]
    client = bigquery.Client(project=project_id)

    # append time_added to dataframe
    current_datetime = datetime.now()
    rounded_datetime = current_datetime.replace(microsecond=0)
    df["time_added"] = rounded_datetime

    # set your dataset and table name
    dataset_id = f"{project_id}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"

    # define load job configuration with clustering only
    bq_schema = bq_schemas[table_name]

    if mode == "truncate":
        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite table if exists
        )
    else:
        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # append data to table
        )

    # Upload the DataFrame
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    # Wait for the job to finish
    job.result()

    # Confirm upload
    n_rows = len(df)
    if mode == "truncate":
        print(f"Created table {table_id} with {n_rows} rows.")
    else:
        print(f"Appended {n_rows} rows to table {table_id}.")


def load_data_to_df(dataset_name, table_name):
    """
    dataset_name (str): name of bq dataset containing the data table
    table_name (str):   name of bq table and identifier for BQ schema 
    """
    # Initialize BigQuery client
    client = bigquery.Client()

    # Find table_id based on project_id, dataset_name, and table_name
    project_id = creds["project_id"]
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Load the table into a DataFrame
    df = client.list_rows(table_id).to_dataframe()

    return df