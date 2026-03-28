"""
Daily ingestion script: upload today's RETRAN_NODs CSV to GCS,
append to BigQuery raw table, then run dbt.
Run manually or triggered by Kestra.

Usage:
    python3 daily_load.py [YYYY-MM-DD]
    If date is omitted, uses today.
"""
import sys
import os
import subprocess
from datetime import date
from google.cloud import storage, bigquery

PROJECT_ID  = "aiagentsintensive"
BUCKET_NAME = "aiagentsintensive-nod-lake"
DATASET_ID  = "nod_raw"
TABLE_ID    = "nods"
NOD_DIR     = "/home/highview/retran"
GCS_PREFIX  = "nods"
DBT_DIR     = "/home/highview/DataEngineeringZoomcamp2026/project1/dbt"


def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    local_path = f"{NOD_DIR}/RETRAN_NODs_{run_date}.csv"

    if not os.path.exists(local_path):
        print(f"ERROR: {local_path} not found")
        sys.exit(1)

    # Step 1: Upload to GCS
    print(f"Uploading {local_path} to GCS...")
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob_name = f"{GCS_PREFIX}/RETRAN_NODs_{run_date}.csv"
    bucket.blob(blob_name).upload_from_filename(local_path)
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    print(f"  → {gcs_uri}")

    # Step 2: Append to BigQuery
    print("Loading into BigQuery (append)...")
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()
    print(f"  → Appended {job.output_rows} rows to {table_ref}")

    # Step 3: Run dbt
    print("Running dbt...")
    result = subprocess.run(["dbt", "run"], cwd=DBT_DIR, capture_output=True, text=True)
    print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    if result.returncode != 0:
        print("ERROR: dbt run failed")
        print(result.stderr[-1000:])
        sys.exit(1)
    print("Done!")


if __name__ == "__main__":
    main()
