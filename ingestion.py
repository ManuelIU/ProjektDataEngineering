import pathlib
from minio import Minio
from minio.error import S3Error

bucket_name = "dataengineeringbucket"
csv_path = pathlib.Path('data')

client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
)


def create_bucket_if_not_exist():
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' was successfully created.")
    except S3Error as e:
        print(f"Error creating bucket: {e}")


def upload_csv_files_to_minio():
    for csv in csv_path.glob("*.csv"):
        dest = f"weather/{csv.name}"
        try:
            # Check if the file already exists
            client.stat_object(bucket_name, dest)
            print(f"{csv.name} exists in MinIO, skipping upload.")
        except S3Error as e:
            # If the file does not exist, upload it
            if e.code == 'NoSuchKey':
                print(f"{csv.name} does not exist in MinIO, uploading...")
                client.fput_object(bucket_name, dest, str(csv))
            else:
                print(f"Error checking {csv.name}: {e}")
        

def main():
    print("Ingestion start!")

    create_bucket_if_not_exist()

    if not csv_path.exists() and not csv_path.is_dir():
        raise SystemExit(f"CSV-directory not found: {csv_path}") 

    upload_csv_files_to_minio()

    print("Ingestion complete!")

if __name__ == "__main__":
    main()