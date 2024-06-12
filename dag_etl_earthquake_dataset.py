from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

tables = [
    "dim_location",
    "dim_time",
    "dim_demographic",
    "dim_infrastructure",
    "dim_geological",
    "dim_historical",
    "fact_earthquake",
]

default_args = {
    "owner": "daniel",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_earthquake_dataset",
    default_args=default_args,
    description="ETL earthquake dataset from csv >> pentaho >> spark >> gcs >> bigquery using airflow",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

start = DummyOperator(
    task_id="start",
    dag=dag,
)

finish = DummyOperator(
    task_id="finish",
    dag=dag,
)

run_pentaho_job = BashOperator(
    task_id="run_pentaho_job",
    bash_command="bash /home/cekson/run_pan.sh ",
    dag=dag,
)

run_spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/home/cekson/airflow/spark-jobs/spark_oltp_process.py",
    conn_id="spark_default",
    total_executor_cores="2",
    executor_cores="1",
    executor_memory="2g",
    num_executors="2",
    driver_memory="2g",
    verbose=False,
    dag=dag,
)

for table in tables:
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"load_gcs_to_bq_{table}",
        bucket="de_final_project",
        source_objects=[f"{table}.parquet/*"],
        destination_project_dataset_table=f"practice-413515.de_final_project.{table}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    start >> run_pentaho_job >> run_spark_job >> load_gcs_to_bq >> finish
