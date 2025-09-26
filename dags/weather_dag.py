from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import os

default_args = {
    "owner": "user",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_weather_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["batch"],
) as dag:
    
    def run_ingestion(**context):
        os.environ["CSV_PATH"] = "/opt/airflow/data/weather_data"
        os.environ["BUCKET_FOLDER"] = "weather_data"
        import ingestion.ingestion as ingestion
        ingestion.main()

    ingestion_task = PythonOperator(
        task_id="ingestion_task",
        python_callable=run_ingestion,
    )
    
    spark_weather_task = SparkSubmitOperator(
        task_id="run_spark_weather_task",
        application="/opt/airflow/spark_tasks/weather_task.py",
        conn_id="spark_default",
        verbose=True,
        jars="/opt/spark/jars/hadoop-aws-3.4.2.jar,\
              /opt/spark/jars/bundle-2.33.1.jar,\
              /opt/spark/jars/checker-qual-3.49.3.jar,\
              /opt/spark/jars/wildfly-openssl-1.0.7.Final.jar,\
              /opt/spark/jars/postgresql-42.7.7.jar,\
              /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    ingestion_task >> spark_weather_task