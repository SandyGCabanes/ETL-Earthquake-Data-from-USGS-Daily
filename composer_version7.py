# sandy.g.cabanes
# Title: composer_version7.py
# Date:August 12, 2025
# ------------------------------------------------------------
#
# This DAG represents a significant modernization of the original multi-script,
# local pipeline. It orchestrates the entire earthquake data pipeline in a
# unified, cloud-native architecture.
#
# Overall Architecture:
# 1.  Unified & Orchestrated: All three separate scripts are now combined into
#     a single, cohesive DAG file, where Airflow/Composer manages the
#     execution order, retries, and logging.
# 2.  Cloud-Native: The pipeline is re-architected from a local-file/PostgreSQL
#     model to a GCS Data Lake + BigQuery Data Warehouse model. This is more
#     scalable, resilient, and manageable in the cloud.
# 3.  Idempotent: Tasks are designed to be re-runnable without causing
#     duplicate data or errors. This is achieved through date-based file
#     naming and using `WRITE_TRUNCATE` dispositions in BigQuery.
# 4.  Declarative & Secure: Uses high-level GCP operators and avoids hardcoded
#     credentials or configurations by leveraging Airflow Connections and Variables.
#
# Pipeline Steps:
# 1. Fetches the latest earthquake data from the USGS API for the execution day.
# 2. Uploads the raw CSV data to a Google Cloud Storage (GCS) bucket.
# 3. Loads the CSV data from GCS into a daily raw table in BigQuery.
# 4. Transforms the raw data into a cleaner, partitioned final stage table in BigQuery.
#
# -----------------------------------------------------------

import os
from datetime import datetime, timedelta
import io

import pandas as pd
import requests
from airflow.decorators import dag, task
# The BigQueryInsertJobOperator is the modern replacement for the deprecated operator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from google.cloud import storage

# ---------------------------------
# --- AIRFLOW VARIABLES SECTION ---
# ---------------------------------
# These configurations have been moved from hardcoded Python variables to
# Airflow Variables for better security, maintainability, and reusability.
#
# **ACTION REQUIRED:** Set these in the Airflow UI -> Admin -> Variables
#
# KEY: gcp_project_id
# VAL: composite-keel-455007-b9
#
# KEY: gcs_bucket
# VAL: earthquake-etl-bucket-sgc
#
# KEY: bq_dataset
# VAL: earthquake_data
#
# The DAG code below will read these values at runtime using Jinja templating
# (e.g., `{{ var.value.gcp_project_id }}`).
# ---------------------------------

# Rest of the variables that can be defined here and not in the Variables section of Admin in Airflow UI
# These values are less likely to change and can remain as constants
BQ_RAW_TABLE = "earthquakes_raw"
BQ_STG_TABLE = "earthquakes_stage"
GCP_CONN_ID = "google_cloud_default" # Default connection created by Composer

# --------------------------------------
# Use Airflow's macros for dynamic dates
# --------------------------------------

# Explicitly formatting data_interval_start and data_interval_end
# to the strict ISO-8601 format (YYYY-MM-DDTHH:MM:SSZ) required by USGS API.
API_START_TIME = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%SZ') }}"
API_END_TIME = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%SZ') }}"

# -----------------
# default arguments
# -----------------
default_args = {
    "owner": "Sandy",
    "start_date": datetime(2025, 8, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}


# ---------------------------------
# use of dag from airflow.decorators
# ---------------------------------
@dag(
    dag_id="earthquake_data_pipeline_sandy_v2",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["gcp", "bigquery", "api", "etl"],
    description="Fetches USGS earthquake data and processes it in BigQuery using modern operators.",
)


def earthquake_etl_dag_v2():
    """
    ### Earthquake Data ETL Pipeline (v2)

    This DAG uses the modern `BigQueryInsertJobOperator` for transformations.
    1.  **Fetching Data**: Getting data from the USGS API.
    2.  **Uploading to GCS**: Saving the data as a CSV in a GCS bucket.
    3.  **Loading to BigQuery (Raw)**: Loading the CSV into a daily raw table in BigQuery.
    4.  **Transforming in BigQuery (Stage)**: Running a SQL query to clean the data and saving it to a final, partitioned staging table.
    """

# This task replaces the logic from `process_earthquake_data_complete_sandy.py`.
# It fetches data from the API and uploads it directly to GCS instead of a local file.
# @task placed here so fetch_and_upload_to_gcs is modified later as a special object in Airflow
    # Explicitly setting the task_id for the decorated function to ensure correct naming in Airflow UI.
    @task(task_id="fetch_and_upload_to_gcs")
    def fetch_and_upload_to_gcs(start_date: str, end_date: str, bucket_name: str) -> str:
        """Fetching data from USGS API and uploading it as a CSV to GCS."""
        print(f"Fetching data for range: {start_date} to {end_date}")
        url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        features = data.get("features", [])
        if not features:
            print("No earthquake features found for the given time range.")
            # If no data, returning an empty string will cause the subsequent GCS->BQ operator to skip.
            return ""

        # Using the date part of the data_interval_start for filename consistency
        # Removed reliance on slicing 'T' because start_date is now precisely formatted with 'Z' at the end.
        execution_date_str = start_date.split("T")[0].replace("-", "")
        filename = f"{execution_date_str}_earthquakedata.csv"
        gcs_path = f"data/earthquakes/{filename}"

        earthquakes = []
        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            time_ms = properties.get("time")
            ts = datetime.utcfromtimestamp(time_ms / 1000).isoformat() if time_ms else None

            earthquake = {
                "ts": ts,
                "place": properties.get("place"),
                "magnitude": properties.get("mag"),
                "longitude": geometry["coordinates"][0],
                "latitude": geometry["coordinates"][1],
                "depth": geometry["coordinates"][2],
                "file_name": gcs_path,
            }
            earthquakes.append(earthquake)

        df = pd.DataFrame(earthquakes)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        # Reverting to upload_from_string for robustness against byte count mismatch errors.
        # This converts the StringIO content to a string and uploads directly.
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        print(f"File {filename} successfully uploaded to gs://{bucket_name}/{gcs_path}")
        # The GCS path is returned via XComs for the next task to use.
        return gcs_path

# --------------------
# --- EXTRACT TASK ---
# --------------------
    gcs_file_path = fetch_and_upload_to_gcs(
        start_date=API_START_TIME,
        end_date=API_END_TIME,
        # The bucket name is read from Airflow Variables at runtime.
        bucket_name="{{ var.value.gcs_bucket }}",
    )

# -----------------
# --- LOAD TASK ---
# -----------------
# This task replaces the entire `load_csv_complete_sandy.py` script.
# The destination is BigQuery, not PostgreSQL.
# It uses a standard, optimized GCP operator instead of custom psycopg2 code.
# We are creating an instance of the GCSToBigQueryOperator so no need for @task decorator.

    load_gcs_to_bigquery_raw = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery_raw",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=["{{ ti.xcom_pull(task_ids='fetch_and_upload_to_gcs') }}"],
        # ** REMARK: Changed to a single Jinja template string to ensure all parts are rendered by Airflow.
        # ** REMARK: Embeds BQ_RAW_TABLE directly into the Jinja template.
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.earthquakes_raw_{{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE", # Replacing the `delete_old_records` logic
        gcp_conn_id=GCP_CONN_ID,
        # The 'allow_empty_source' parameter was removed here as it is not a valid argument for GCSToBigQueryOperator in this Airflow version.
    )

# ----------------------
# --- TRANSFORM TASK ---
# ----------------------
# This task replaces the entire `transform_earthquake_sandy.py` script.
# It uses a single, powerful operator to run a transformation query in BigQuery.
# We are creating an instance of the BigQueryInsertJobOperator so no need for @task decorator.

# ** Summary of Operator Change **
# The `BigQueryExecuteQueryOperator` was deprecated and is replaced by `BigQueryInsertJobOperator`.
# Key changes:
#   1.  Configuration Parameter: Using a single `configuration` dictionary that mirrors the BigQuery Job API.
#   2.  SQL Simplification: The SQL is now a pure `SELECT` statement. Table creation, replacement, and
#       partitioning logic are all handled in the operator's configuration dictionary.
#   3.  Clear Separation of Concerns: The SQL query focuses on the transformation logic, while the
#       configuration handles the destination table's properties (name, partitioning, write behavior).
#
    transform_and_load_to_stage = BigQueryInsertJobOperator(
        task_id="transform_and_load_to_stage",
        gcp_conn_id=GCP_CONN_ID,
        # The configuration dictionary defines the entire BigQuery job.
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        ts AS ts,
                        DATE(ts) AS dt,
                        -- Replacing PostgreSQL's SUBSTRING with a more robust REGEXP_EXTRACT in GoogleSQL.
                        COALESCE(REGEXP_EXTRACT(place, r' of (.*)'), place) as place,
                        magnitude,
                        latitude,
                        longitude
                    FROM
                        `{{{{ var.value.gcp_project_id }}}}.{{{{ var.value.bq_dataset }}}}.{BQ_RAW_TABLE}_{{{{ ds_nodash }}}}`
                    WHERE ts IS NOT NULL
                """,
                "destinationTable": {
                    "projectId": "{{ var.value.gcp_project_id }}",
                    "datasetId": "{{ var.value.bq_dataset }}",
                    "tableId": BQ_STG_TABLE,
                },
                # This replaces the table content with the query results, ensuring idempotency.
                "writeDisposition": "WRITE_TRUNCATE",
                # This explicitly defines how the destination table should be partitioned.
                "timePartitioning": {
                    "type": "DAY",
                    "field": "dt",
                },
                "useLegacySql": False,
            }
        },
    )

    # Defining task dependencies for the entire pipeline
    gcs_file_path >> load_gcs_to_bigquery_raw >> transform_and_load_to_stage

# Instantiating the DAG
earthquake_etl_dag_instance = earthquake_etl_dag_v2()
