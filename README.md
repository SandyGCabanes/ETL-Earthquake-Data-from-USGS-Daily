# Earthquake Data Pipeline (Client Summary)

## Objective:

This automated pipeline pulls daily earthquake data from a trusted public source, stores it securely in the cloud, and organizes it into clean, searchable tables for analysis. It replaces a manual, error-prone process with a reliable system that runs itself.

## Why This Matters

- **Daily Updates**: Fresh data every day, no human intervention needed.
- **Cloud-Based**: Everything lives in Google Cloud—no local files, no lost spreadsheets.
- **Clean & Searchable**: Final tables are easy to filter by date, location, or magnitude.
- **Audit-Ready**: Every step is logged, versioned, and reproducible.

## What It Does

1. **Gets the Data**  
   Pulls earthquake reports from the USGS website every morning.

2. **Stores the Raw File**  
   Saves the original data file to a secure cloud folder (Google Cloud Storage).

3. **Loads It into a Table**  
   Uploads the raw file into a BigQuery table for safekeeping.

4. **Cleans It Up**  
   Filters out bad data, standardizes location names, and organizes it by date.

5. **Final Output**  
   A clean, reliable table that analysts can use for dashboards, reports, or alerts.

## Who It's For

- **Analysts**: Want clean, daily data without chasing files.
- **Managers**: Need confidence that the system won’t break or go stale.
- **Auditors**: Can trace every step, every file, every transformation.

## Behind the Scenes (For the Curious)

- Built with **Apache Airflow** on **Google Cloud Composer**
- Uses **BigQuery** for storage and transformation
- Fully automated, runs daily without manual triggers

---

 Big Idea to stakeholder in under 60 seconds:

> “It’s a daily earthquake data pipeline that runs in the cloud, stores everything securely, and gives us clean tables for analysis—no manual work, no missing files, and everything’s audit-ready.”


