"""
Load Green, Yellow (2019-2020) and FHV (2019) taxi data into BigQuery via GCS.
Data source: https://github.com/DataTalksClub/nyc-tlc-data/releases
"""
import os
import subprocess
import urllib.request
from concurrent.futures import ThreadPoolExecutor

PROJECT_ID = "aiagentsintensive"
DATASET_ID = "nytaxi"
BUCKET_NAME = "dezoomcamp-hw4-2026"
DOWNLOAD_DIR = "/tmp/taxi_data"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

BASE_URLS = {
    "green": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02d}.csv.gz",
    "yellow": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz",
    "fhv": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz",
}


def download_file(args):
    url, file_path = args
    if os.path.exists(file_path):
        print(f"  Already downloaded: {os.path.basename(file_path)}")
        return file_path
    try:
        print(f"  Downloading {os.path.basename(file_path)}...")
        urllib.request.urlretrieve(url, file_path)
        return file_path
    except Exception as e:
        print(f"  Failed: {os.path.basename(file_path)}: {e}")
        return None


def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
    return result.returncode == 0


def process_dataset(taxi_type, years):
    # Build download tasks
    tasks = []
    for year in years:
        for month in range(1, 13):
            url = BASE_URLS[taxi_type].format(year=year, month=month)
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            tasks.append((url, file_path))

    # Download in parallel
    print(f"\n{'='*50}")
    print(f"Downloading {taxi_type} data ({len(tasks)} files)...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, tasks))

    # Upload to GCS
    print(f"\nUploading {taxi_type} data to GCS...")
    gcs_prefix = f"gs://{BUCKET_NAME}/{taxi_type}/"
    for fp in file_paths:
        if fp:
            print(f"  Uploading {os.path.basename(fp)}...")
            run_cmd(f'gcloud storage cp "{fp}" "{gcs_prefix}"')

    # Load from GCS to BigQuery using wildcard
    table_id = f"{PROJECT_ID}:{DATASET_ID}.{taxi_type}_tripdata"
    gcs_uri = f"gs://{BUCKET_NAME}/{taxi_type}/*.csv.gz"
    print(f"\nLoading {taxi_type} data from GCS to BigQuery...")
    success = run_cmd(
        f'bq load --autodetect --source_format=CSV '
        f'--replace "{table_id}" "{gcs_uri}"'
    )
    if success:
        print(f"  Successfully loaded {taxi_type}_tripdata")
    else:
        print(f"  Failed to load {taxi_type}_tripdata")


if __name__ == "__main__":
    # Create GCS bucket
    print("Creating GCS bucket...")
    run_cmd(f"gcloud storage buckets create gs://{BUCKET_NAME} --project={PROJECT_ID} --location=US 2>/dev/null || true")

    process_dataset("green", [2019, 2020])
    process_dataset("yellow", [2019, 2020])
    process_dataset("fhv", [2019])

    # Print table row counts
    print("\n" + "="*50)
    print("Table row counts:")
    for table in ["green_tripdata", "yellow_tripdata", "fhv_tripdata"]:
        result = subprocess.run(
            f'bq query --use_legacy_sql=false --format=csv "SELECT COUNT(*) as cnt FROM {PROJECT_ID}.{DATASET_ID}.{table}"',
            shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            count = lines[-1] if len(lines) > 1 else "unknown"
            print(f"  {table}: {count} rows")

    print("\nDone!")
