from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from config import PROJECT_ID, REGION, CLUSTER_NAME, GCS_BUCKET, INPUT_FILE, PYSPARK_FILE

default_args = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
}

dag = DAG(
    'gcp_dataproc_spark_job',
    default_args=default_args,
    description='Run PySpark job on Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

# Cluster config
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30}
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30}
    },
    'software_config': {'image_version': '2.1-debian11'}
}

# GCS sensor
file_sensor_task = GCSObjectExistenceSensor(
    task_id='file_sensor_task',
    bucket=GCS_BUCKET,
    object=INPUT_FILE,
    poke_interval=300,
    timeout=43200,
    mode='poke',
    dag=dag,
)

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

# Submit PySpark job
pyspark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/{PYSPARK_FILE}"
    },
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=pyspark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Delete cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',
    dag=dag,
)

# Task dependencies
file_sensor_task >> create_cluster >> submit_pyspark_job >> delete_cluster
