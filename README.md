# airflow_dataproc_project

GCP Dataproc Spark Job Automation with Airflow
Project Overview

This project automates the processing of daily CSV files stored in a Google Cloud Storage (GCS) bucket using Apache Airflow. The workflow:
Detects new CSV files in GCS using a GCS Sensor.
Creates a Dataproc cluster dynamically.
Submits a PySpark job to process the CSV data.
Saves the transformed data back to GCS.
Deletes the Dataproc cluster after processing.

This project demonstrates Airflow DAGs integration with GCP services like Dataproc and GCS.
