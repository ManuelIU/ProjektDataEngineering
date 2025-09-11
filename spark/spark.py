from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from datetime import datetime

bucket_name = "dataengineeringbucket"
source_prefix = f"s3a://{bucket_name}/weather/"
target_prefix = f"s3a://{bucket_name}/processed/weather/"

today_str = datetime.today().strftime("%Y-%m-%d")
target_prefix_with_date = target_prefix + today_str + "/"

spark = (
    SparkSession.builder.appName("ReadFromMinIO")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.4.2.jar,/opt/bitnami/spark/jars/bundle-2.33.1.jar")
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

def main():
    print("Spark service started!")

    try:
        df = spark.read.option("header", True).csv(source_prefix)
        df.show()
    except Exception:
        print("No csv files found!")
        return

    input_files = df.inputFiles()
    move_processed_files(input_files)


    spark.stop()
    print("Spark service complete!")


if __name__ == "__main__":
    main()