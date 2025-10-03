from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os

AIRFLOW_WEATHER_DATA_DIR = os.getenv("AIRFLOW_WEATHER_DATA_DIR", "/opt/airflow/data/weather_data")
AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER = os.getenv("AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER", "weather_data")
AIRFLOW_SPARK_APPLICATION = os.getenv("AIRFLOW_SPARK_APPLICATION", "/opt/airflow/spark_tasks/weather_task.py")
AIRFLOW_SPARK_CONN_ID = os.getenv("AIRFLOW_SPARK_CONN_ID", "spark_default")
AIRFLOW_SPARK_JARS = os.getenv("AIRFLOW_SPARK_JARS", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/bundle-2.29.52.jar,/opt/spark/jars/checker-qual-3.49.3.jar,/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar,/opt/spark/jars/postgresql-42.7.7.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar")

default_args = {
    "owner": "user",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_weather_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["batch"],
) as dag:
    
    def run_ingestion(**context):
        os.environ["CSV_PATH"] = AIRFLOW_WEATHER_DATA_DIR
        os.environ["BUCKET_FOLDER"] = AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER
        from ingestion import ingestion
        ingestion.main()

    ingestion_task = PythonOperator(
        task_id="run_ingestion_task",
        python_callable=run_ingestion,
    )
    
    spark_weather_task = SparkSubmitOperator(
        task_id="run_spark_weather_task",
        application=AIRFLOW_SPARK_APPLICATION,
        conn_id=AIRFLOW_SPARK_CONN_ID,
        verbose=True,
        jars=AIRFLOW_SPARK_JARS
    )

    ingestion_task >> spark_weather_task