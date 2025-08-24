import pathlib

def main():
    print("Ingestion start!")
    csvPath = pathlib.Path('data')
    if not csvPath.exists() and not csvPath.is_dir():
        raise SystemExit(f"CSV-directory not found: {csvPath}")

    # Upload all csv files to MinIO
    for p in csvPath.glob("*.csv"):
        print(f"Uploading {p} -> MinIO")

    print("Ingestion complete!")

if __name__ == "__main__":
    main()