from minio import Minio
from minio.error import S3Error
from pathlib import Path
import logging
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "dataengineeringbucket")

csv_path = Path(os.environ["CSV_PATH"])
bucket_folder = Path(os.environ["BUCKET_FOLDER"])

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)


def create_bucket_if_not_exist() -> None:
    try:
        if not client.bucket_exists(MINIO_BUCKET_NAME):
            client.make_bucket(MINIO_BUCKET_NAME)
            log.info(f"Bucket '{MINIO_BUCKET_NAME}' was successfully created.")
    except S3Error as e:
        log.error(f"Error creating bucket: {e}")


def file_exists(bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def upload_csv_files_to_minio() -> None:
    files = list(csv_path.glob("*.csv"))
    if not files:
        log.info("No CSV files found to upload.")
        return
    
    for csv in files:
        dest = f"{bucket_folder}/{csv.name}"
        if file_exists(MINIO_BUCKET_NAME, dest):
            log.info(f"{csv.name} exists in MinIO, skipping upload.")
            csv.unlink()
        else:
            try:
                log.info(f"Uploading {csv.name} -> {dest}")
                client.fput_object(MINIO_BUCKET_NAME, dest, str(csv))
                csv.unlink()
            except Exception as upload_error:
                log.error(f"Error uploading {csv.name}: {upload_error}")
        

def main():
    log.info("Ingestion start!")

    if not csv_path.exists() or not csv_path.is_dir():
        raise SystemExit(f"CSV-directory not found: {csv_path}") 

    create_bucket_if_not_exist()

    upload_csv_files_to_minio()

    log.info("Ingestion complete!")

if __name__ == "__main__":
    main()