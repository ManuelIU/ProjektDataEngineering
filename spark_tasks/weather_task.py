from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType, DoubleType
from py4j.java_gateway import java_import
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql.functions import expr

bucket_name = "dataengineeringbucket"
source_prefix = f"s3a://{bucket_name}/weather_data/"
target_prefix = f"s3a://{bucket_name}/processed/weather_data/"

today_str = datetime.today().strftime("%Y-%m-%d")
target_prefix_with_date = target_prefix + today_str + "/"

db_url = "jdbc:postgresql://postgres:5432/dataEngineering_db"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

conf = (SparkConf()
    .setAppName("App_Name")
    .setMaster("spark://spark-master:7077")
    .set("spark.ui.port", "8081"))

spark = (
    SparkSession.builder.appName("ReadFromMinIO")
    .config(conf=conf)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .getOrCreate()
)

def move_processed_files(file_list):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    java_import(sc._jvm, "org.apache.hadoop.fs.Path")
    java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")

    dst_dir = sc._jvm.Path(target_prefix_with_date)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(dst_dir.toUri(), hadoop_conf)

    print(f"Ensuring target directory exists: {target_prefix_with_date}")
    fs.mkdirs(dst_dir)

    for file in file_list:
        filename = file.split("/")[-1]
        dst = sc._jvm.Path(target_prefix_with_date + filename)
        print(f"Moving {file} -> {dst.toString()}")
        fs.rename(sc._jvm.Path(file), dst)

def filter_new_records(df, db_url, db_properties):
    try:
        last_ts_df = spark.read.jdbc(
            url=db_url,
            table="(SELECT MAX(starttimeutc) AS max_ts FROM weather_data) as t",
            properties=db_properties
        )

        last_ts = last_ts_df.collect()[0]["max_ts"]

        if last_ts is not None:
            print(f"Last available StartTimeUTC in DB: {last_ts}")
            df = df.filter(col("StartTimeUTC") > last_ts)
        else:
            print("No existing data found, all records will be taken.")

    except Exception as e:
        print(f"Error loading last timestamp: {e}")

    return df


def main():
    print("Spark service started!")

    try:
        df = spark.read.option("header", True).csv(source_prefix)

        df = (
            df.withColumn("StartTimeUTC", col("StartTimeUTC").cast(TimestampType()))
              .withColumn("EndTimeUTC", col("EndTimeUTC").cast(TimestampType()))
              .withColumn("PrecipitationIn", col("PrecipitationIn").cast(DoubleType()))
              .withColumn("LocationLat", col("LocationLat").cast(DoubleType()))
              .withColumn("LocationLng", col("LocationLng").cast(DoubleType()))
        )

        df_filtered = filter_new_records(df, db_url, db_properties)

        if df_filtered.count() > 0:
            df_filtered.write.mode("append").jdbc(
                url=db_url,
                table="weather_data",
                properties=db_properties
            )
            print(f"{df_filtered.count()} new records written to PostgreSQL!")
            df_filtered.show()
        else:
            print("No new records found – nothing written.")

    except Exception as e:
        print(f"Error processing CSV files: {e}")
        spark.stop()
        return
    
    input_files = df.inputFiles()
    move_processed_files(input_files)

    spark.stop()
    print("Spark service complete!")


if __name__ == "__main__":
    main()