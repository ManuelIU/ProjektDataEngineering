from datetime import datetime
from py4j.java_gateway import java_import
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.functions import expr
import logging
import os
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "dataengineeringbucket")
MINIO_WEATHER_SOURCE_FOLDER = os.getenv("MINIO_WEATHER_SOURCE_FOLDER")
MINIO_WEATHER_TARGET_FOLDER = os.getenv("MINIO_WEATHER_TARGET_FOLDER")

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER")

SPARK_CONF_MASTER = os.getenv("SPARK_CONF_MASTER")
SPARK_CONF_UI_PORT = os.getenv("SPARK_CONF_UI_PORT")

AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER = os.getenv("AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

today_str = datetime.today().strftime("%Y-%m-%d")
target_folder_with_date = MINIO_WEATHER_TARGET_FOLDER + today_str + "/"

def get_db_properties():
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": POSTGRES_DRIVER
    }

def get_spark() -> SparkSession:
    conf = (SparkConf()
        .setAppName("WeatherDataProcessor")
        .setMaster(SPARK_CONF_MASTER)
        .set("spark.ui.port", SPARK_CONF_UI_PORT))

    return (SparkSession.builder.appName("ReadFromMinIO")
        .config(conf=conf)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate())

def read_weather_data(spark: SparkSession):
    df = spark.read.option("header", True).csv(MINIO_WEATHER_SOURCE_FOLDER)

    return (
        df.withColumn("StartTimeUTC", col("StartTimeUTC").cast(TimestampType()))
          .withColumn("EndTimeUTC", col("EndTimeUTC").cast(TimestampType()))
          .withColumn("PrecipitationIn", col("PrecipitationIn").cast(DoubleType()))
          .withColumn("LocationLat", col("LocationLat").cast(DoubleType()))
          .withColumn("LocationLng", col("LocationLng").cast(DoubleType()))
    )


def filter_new_records(spark: SparkSession, df):
    try:
        last_ts_df = spark.read.jdbc(
            url=POSTGRES_URL,
            table="(SELECT MAX(starttimeutc) AS max_ts FROM weather_data) as t",
            properties=get_db_properties()
        )

        last_ts = last_ts_df.collect()[0]["max_ts"]

        if last_ts is not None:
            log.info(f"Last available StartTimeUTC in DB: {last_ts}")
            df = df.filter(col("StartTimeUTC") > last_ts)
        else:
            log.info("No existing data found, all records will be taken.")

    except Exception as e:
        log.error(f"Error loading last timestamp: {e}")
        
    return df
    

def write_to_postgres(df):
    count = df.count()
    if count > 0:
        df.write.mode("append").jdbc(
            url=POSTGRES_URL,
            table=AIRFLOW_MINIO_WEATHER_BUCKET_FOLDER,
            properties=get_db_properties(),
        )
        log.info(f"{count} new records written to PostgreSQL!")
    else:
        log.info("No new records found – nothing written.")


def move_processed_files(spark: SparkSession, file_list):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    java_import(sc._jvm, "org.apache.hadoop.fs.Path")
    java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")

    dst_dir = sc._jvm.Path(target_folder_with_date)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(dst_dir.toUri(), hadoop_conf)

    log.info(f"Ensuring target directory exists: {target_folder_with_date}")
    fs.mkdirs(dst_dir)

    for file in file_list:
        filename = os.path.basename(file)

        timestamp = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%H%M%S")
        unique_name = f"{timestamp}_{filename}"

        dst = sc._jvm.Path(os.path.join(target_folder_with_date, unique_name))
        log.info(f"Moving {file} -> {dst}")
        fs.rename(sc._jvm.Path(file), dst)


def main():
    log.info("Spark service started!")
    spark = get_spark()
    try:
        df = read_weather_data(spark)
        df_filtered = filter_new_records(spark, df)

        write_to_postgres(df_filtered)

        input_files = df.inputFiles()
        if input_files:
            move_processed_files(spark, input_files)

    except Exception as e:
        log.error(f"Error processing CSV files: {e}")
    
    finally:
        spark.stop()
        log.info("Spark service complete!")


if __name__ == "__main__":
    main()