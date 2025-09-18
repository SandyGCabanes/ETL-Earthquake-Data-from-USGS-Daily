# Earthquake Data Pipeline (Quick Summary)
## Airflow, Google Cloud Composer, BigQuery, Python, (Tableau, Power BI under construction)

##### [Click Here for Technical Version of README](README-tech.md)
## Objective:

This automated pipeline pulls daily earthquake data from a trusted public source (USGS) stores it securely in the cloud (GCS, BigQuery), and organizes it into clean, searchable tables for analysis (BigQuery). It replaces a manual, error-prone process with a reliable system that runs itself.

## This Will Benefit Analysts, Managers, Auditors:
- **Analysts**: Want clean, daily data without chasing files.
- **Managers**: Need confidence that the system won’t break or go stale.
- **Auditors**: Can trace every step, every file, every transformation.

## Advantages of This ETL Process

- **Daily Updates**: Fresh data every day, no human intervention needed. Could be re-applied to daily or weekly sales data.
- **Cloud-Based**: Everything lives in Google Cloud—no local files, no lost spreadsheets.
- **Clean & Searchable**: Final tables are easy to filter by date, location, or magnitude. Could be re-applied to sales dates, stores, locations.
- **Audit-Ready**: Every step is logged, versioned, and reproducible.
  
## Project Walkthrough
- Goes through each task from extract to upload to transform
- Shows the airflow UI after successful python loading to earthquake raw table in BigQuery
- Shows the transformation of earthquake raw table to earthquake stage table in BigQuery
<br>![Project Walkthrough](composer_usgs_2x.mp4_3.0x_720px_.gif)

## Workflow:  [Click here for the flowchart version](flowchart_composer.txt)
1. **Extracts the Data Using API call**  
   Pulls earthquake reports from the USGS website every morning.

2. **Stores the Raw File in GCS**  
   Saves the original data file to a secure cloud folder (Google Cloud Storage).

3. **Loads It into a Table in a Cloud Database**  
   Uploads the raw file into a BigQuery table for safekeeping.

4. **Transforms and Cleans It Up Using Python Code**  
   Filters out bad data, standardizes location names, and organizes it by date.

5. **Loads Final Output into a Table in a Cloud Database**  
   A clean, reliable table that analysts can use for dashboards, reports, or alerts.
   [Click here for a sample dashboard made from a snapshot of this dataset](work in progress)

## Behind the Scenes (For the Curious)

- Built with **Apache Airflow** on **Google Cloud Composer**
- Uses **Google Cloud Storage** for extracted files
- Uses **BigQuery** for storage and transformation
- Fully automated, runs daily without manual triggers

---

 Big Idea in under 60 seconds:

> “It’s a daily earthquake data pipeline that runs in the cloud, stores everything securely, and gives us clean tables for analysis—no manual work, no missing files, and everything’s audit-ready.”


