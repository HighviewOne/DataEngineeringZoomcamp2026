"""
Bootstrap script: upload all historical RETRAN_NODs CSVs to GCS,
then load them into BigQuery raw table (partitioned + clustered).
"""
import glob
import os
from google.cloud import storage, bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning, TimePartitioningType

PROJECT_ID  = "aiagentsintensive"
BUCKET_NAME = "aiagentsintensive-nod-lake"
DATASET_ID  = "nod_raw"
TABLE_ID    = "nods"
NOD_DIR     = "/home/highview/retran"
GCS_PREFIX  = "nods"

SCHEMA = [
    SchemaField("ltv",                  "FLOAT"),
    SchemaField("APN",                  "STRING"),
    SchemaField("assessed_value",       "FLOAT"),
    SchemaField("Tax_Rate",             "STRING"),
    SchemaField("Land_Year",            "STRING"),
    SchemaField("land_value",           "FLOAT"),
    SchemaField("Improvement_Value",    "FLOAT"),
    SchemaField("Thomas_Guide",         "STRING"),
    SchemaField("Improve_Year",         "STRING"),
    SchemaField("improve_value",        "FLOAT"),
    SchemaField("Situs_House",          "STRING"),
    SchemaField("Situs_Fract",          "STRING"),
    SchemaField("Situs_Compass",        "STRING"),
    SchemaField("Situs_Street",         "STRING"),
    SchemaField("Situs_Unit",           "STRING"),
    SchemaField("Situs_City",           "STRING"),
    SchemaField("Situs_Zip",            "STRING"),
    SchemaField("Situs_Carrier_Route",  "STRING"),
    SchemaField("Mail_House",           "STRING"),
    SchemaField("Mail_Fract",           "STRING"),
    SchemaField("Mail_Compass",         "STRING"),
    SchemaField("Mail_Street",          "STRING"),
    SchemaField("Mail_Unit",            "STRING"),
    SchemaField("Mail_City",            "STRING"),
    SchemaField("Mail_Zip",             "STRING"),
    SchemaField("Mail_Carrier_Route",   "STRING"),
    SchemaField("Owner_First_Name",     "STRING"),
    SchemaField("Owner_Phone_Number",   "STRING"),
    SchemaField("Special_Name_Alias",   "STRING"),
    SchemaField("Legal_Description",    "STRING"),
    SchemaField("Owner_Second_Name",    "STRING"),
    SchemaField("Last_Transfer_Date",   "STRING"),
    SchemaField("tax_status",           "STRING"),
    SchemaField("year_sold_to_state",   "STRING"),
    SchemaField("zoning",               "STRING"),
    SchemaField("use_code",             "STRING"),
    SchemaField("interest_trans",       "STRING"),
    SchemaField("document_reason",      "STRING"),
    SchemaField("ownership_code",       "STRING"),
    SchemaField("exemption",            "STRING"),
    SchemaField("last_sale_key",        "STRING"),
    SchemaField("last_sale_amt",        "FLOAT"),
    SchemaField("last_sale_date",       "STRING"),
    SchemaField("sub",                  "STRING"),
    SchemaField("design",               "STRING"),
    SchemaField("shape",                "STRING"),
    SchemaField("yr_built",             "STRING"),
    SchemaField("number_of_units",      "STRING"),
    SchemaField("bed",                  "STRING"),
    SchemaField("bath",                 "STRING"),
    SchemaField("sq_feet",              "STRING"),
    SchemaField("tract_number",         "STRING"),
    SchemaField("lot_size",             "STRING"),
    SchemaField("last_sale_doc_number", "STRING"),
    SchemaField("last_sale_doc_file",   "STRING"),
    SchemaField("spare",                "STRING"),
    SchemaField("situs_street2",        "STRING"),
    SchemaField("situs_city2",          "STRING"),
    SchemaField("situs_zip2",           "STRING"),
    SchemaField("trustor_full_name",    "STRING"),
    SchemaField("trustor_first_name",   "STRING"),
    SchemaField("tor_mailing_st",       "STRING"),
    SchemaField("tor_mailing_city",     "STRING"),
    SchemaField("tor_mailing_zip",      "STRING"),
    SchemaField("trustee_name",         "STRING"),
    SchemaField("tee_st",               "STRING"),
    SchemaField("tee_suite",            "STRING"),
    SchemaField("tee_city_state",       "STRING"),
    SchemaField("tee_zip",              "STRING"),
    SchemaField("tee_contact",          "STRING"),
    SchemaField("tee_phone",            "STRING"),
    SchemaField("beneficiary_name",     "STRING"),
    SchemaField("ben_st",               "STRING"),
    SchemaField("ben_ste",              "STRING"),
    SchemaField("ben_city_state",       "STRING"),
    SchemaField("ben_zip",              "STRING"),
    SchemaField("ben_contact",          "STRING"),
    SchemaField("ben_phone",            "STRING"),
    SchemaField("document_type",        "STRING"),
    SchemaField("recording_date",       "STRING"),
    SchemaField("document_number",      "STRING"),
    SchemaField("tee_number",           "STRING"),
    SchemaField("tee_loan_number",      "STRING"),
    SchemaField("default_date",         "STRING"),
    SchemaField("amount",               "FLOAT"),
    SchemaField("as_of_date",           "STRING"),
    SchemaField("date_default_recorded","STRING"),
    SchemaField("document_umber_of_def","STRING"),
    SchemaField("loan_date",            "STRING"),
    SchemaField("loan_amt",             "FLOAT"),
    SchemaField("loan_doc_number",      "STRING"),
    SchemaField("sale_date",            "STRING"),
    SchemaField("sale_time",            "STRING"),
    SchemaField("min_bid",              "FLOAT"),
    SchemaField("sale_location",        "STRING"),
    SchemaField("sale_location_city",   "STRING"),
    SchemaField("dpd_number",           "STRING"),
    SchemaField("county",               "STRING"),
    SchemaField("last_updated",         "STRING"),
    SchemaField("new",                  "STRING"),
    SchemaField("count",                "STRING"),
    SchemaField("rrec_date",            "STRING"),
    SchemaField("rdocument",            "STRING"),
    SchemaField("latitude",             "FLOAT"),
    SchemaField("longtitude",           "FLOAT"),
    SchemaField("dpd_number2",          "STRING"),
    SchemaField("flagged",              "STRING"),
    SchemaField("note",                 "STRING"),
    SchemaField("marks",                "STRING"),
]


def upload_to_gcs(local_path, gcs_blob_name):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_path)
    print(f"  Uploaded {os.path.basename(local_path)} → gs://{BUCKET_NAME}/{gcs_blob_name}")
    return f"gs://{BUCKET_NAME}/{gcs_blob_name}"


def load_gcs_to_bq(gcs_uris):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=TimePartitioning(
            type_=TimePartitioningType.DAY,
            field=None,  # uses _PARTITIONTIME (ingestion time)
        ),
        clustering_fields=["county"],
    )

    job = client.load_table_from_uri(gcs_uris, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {job.output_rows} rows into {table_ref}")


def main():
    csv_files = sorted(glob.glob(f"{NOD_DIR}/RETRAN_NODs_*.csv"))
    print(f"Found {len(csv_files)} CSV files\n")

    print("Step 1: Uploading to GCS...")
    gcs_uris = []
    for path in csv_files:
        fname = os.path.basename(path)
        blob_name = f"{GCS_PREFIX}/{fname}"
        uri = upload_to_gcs(path, blob_name)
        gcs_uris.append(uri)

    print(f"\nStep 2: Loading {len(gcs_uris)} files into BigQuery...")
    load_gcs_to_bq(gcs_uris)
    print("\nDone!")


if __name__ == "__main__":
    main()
