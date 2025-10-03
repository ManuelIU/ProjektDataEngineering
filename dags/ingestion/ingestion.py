from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from minio import Minio
from minio.error import S3Error
from pathlib import Path
import hashlib
import logging
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

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
        raise


def file_exists(bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_file(dest: str, csv: Path) -> None:
    try:
        log.info(f"Uploading {csv.name} -> {dest} (overwrite if exists)")
        client.fput_object(
            MINIO_BUCKET_NAME,
            dest,
            str(csv),
            metadata={
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "checksum": sha256sum(csv),
                "source": "local-ingestion"
            },
        )
        csv.unlink()
        log.info(f"Uploaded and deleted local {csv.name}")
    except Exception as e:
        log.error(f"Error uploading {csv.name}: {e}")
        raise


def upload_csv_files_to_minio() -> None:
    files = list(csv_path.glob("*.csv"))
    if not files:
        log.info("No CSV files found to upload.")
        return
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(upload_file, f"{bucket_folder}/{csv.name}", csv): csv
            for csv in files
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log.error(f"Failed to upload {futures[future]}: {e}")
        

def main():
    log.info("Ingestion start!")

    if not csv_path.exists() or not csv_path.is_dir():
        raise SystemExit(f"CSV-directory not found: {csv_path}") 

    create_bucket_if_not_exist()

    upload_csv_files_to_minio()

    log.info("Ingestion complete!")

if __name__ == "__main__":
    main()