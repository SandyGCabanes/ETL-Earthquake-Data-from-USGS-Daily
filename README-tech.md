# Earthquake Data Pipeline (Composer Version 7)

## Overview

This Airflow DAG (`composer_version7.py`) modernizes a legacy multi-script earthquake data pipeline into a unified, cloud-native architecture. It orchestrates daily ETL tasks using Google Cloud services, ensuring reproducibility, auditability, and scalability.

## Architecture Highlights

- **Unified DAG**: Combines three previously separate scripts into a single Airflow DAG.
- **Cloud-Native**: Migrates from local file/PostgreSQL to GCS + BigQuery.
- **Idempotent Execution**: Uses date-based file naming and `WRITE_TRUNCATE` to ensure safe re-runs.
- **Secure Configuration**: Leverages Airflow Variables and Connections to avoid hardcoded credentials.

## Pipeline Steps

1. **Extract**: Fetches daily earthquake data from the USGS API.
2. **Upload**: Saves raw data as CSV to a GCS bucket.
3. **Load**: Ingests CSV into a raw BigQuery table.
4. **Transform**: Cleans and partitions data into a final staging table in BigQuery.

## Airflow Configuration

Set the following Airflow Variables via the UI:

| Key               | Value                                |
|------------------|--------------------------------------|
| `gcp_project_id` | `composite-keel-455007-b9`           |
| `gcs_bucket`     | `earthquake-etl-bucket-sgc`          |
| `bq_dataset`     | `earthquake_data`                    |

## DAG Details

- **DAG ID**: `earthquake_data_pipeline_sandy_v2`
- **Schedule**: Daily (`@daily`)
- **Start Date**: August 10, 2025
- **Owner**: Sandy
- **Tags**: `gcp`, `bigquery`, `api`, `etl`

## Operators Used

- `@task` decorator for Python-based extraction and GCS upload
- `GCSToBigQueryOperator` for raw data ingestion
- `BigQueryInsertJobOperator` for SQL-based transformation

## Notes on Transform Logic

- Uses `REGEXP_EXTRACT` to normalize location data
- Partitions final table by `dt` (date derived from timestamp)
- Ensures null timestamps are excluded

## Audit & Reproducibility

- Diagnostic print statements included for traceability
- File naming and table creation are date-indexed for version control
- SQL and configuration are separated for clarity and maintainability

---

For details, refer to the DAG code for inline annotations.
