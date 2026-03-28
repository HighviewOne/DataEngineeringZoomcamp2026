# SoCal NOD Tracker

An end-to-end data pipeline tracking **Notice of Default (NOD)** filings across Southern California counties — a leading indicator of foreclosure activity.

## Problem Description

A Notice of Default is the first formal step in the foreclosure process. Lenders file them with the county recorder when a homeowner is delinquent on their mortgage. This project ingests daily NOD filings from 6 SoCal counties (Los Angeles, Orange, Riverside, San Bernardino, San Diego, Ventura), loads them into a data warehouse, transforms the data with dbt, and visualizes trends in a Looker Studio dashboard.

**Business questions answered:**
- Which cities have the highest foreclosure activity?
- How is NOD volume trending week-over-week by county?
- Which lenders are filing the most NODs?
- What is the average loan-to-value (LTV) ratio for defaulting properties?

## Architecture

```
Daily CSV files (retran/)
        │
        ▼
  Kestra DAG (daily @ 9am)
        │
        ├─► GCS Bucket (data lake)
        │      gs://aiagentsintensive-nod-lake/nods/
        │
        ├─► BigQuery Raw (nod_raw.nods)
        │      partitioned by ingestion date
        │      clustered by county
        │
        └─► dbt transformations
               └─► BigQuery Marts (nod_production)
                      ├─ stg_nods (view)
                      ├─ mart_nods_by_county_week (table)
                      ├─ mart_nods_by_city (table)
                      └─ mart_nods_by_lender (table)
                             │
                             ▼
                      Looker Studio Dashboard
```

## Technologies

| Layer | Tool |
|-------|------|
| Cloud | Google Cloud Platform (GCP) |
| Infrastructure as Code | Terraform |
| Workflow Orchestration | Kestra |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Transformations | dbt (dbt-bigquery) |
| Dashboard | Looker Studio |

## Dataset

Daily NOD filings from [retran.net](https://retran.net) covering 6 Southern California counties:
- Los Angeles, Orange, Riverside, San Bernardino, San Diego, Ventura

Each record includes: property address, owner name, APN, loan amount, LTV ratio, lender/trustee, recording date, scheduled sale date, and geo-coordinates.

## How to Reproduce

### Prerequisites
- GCP project with BigQuery and GCS APIs enabled
- `gcloud` authenticated (`gcloud auth application-default login`)
- Python 3.10+: `pip3 install google-cloud-storage google-cloud-bigquery dbt-bigquery`
- Terraform: [install](https://developer.hashicorp.com/terraform/install)

### 1. Provision Infrastructure (Terraform)

```bash
cd project1/terraform
terraform init
terraform apply
```

Creates:
- GCS bucket: `aiagentsintensive-nod-lake`
- BigQuery dataset: `nod_raw`
- BigQuery dataset: `nod_production`

### 2. Bootstrap Historical Data

```bash
python3 project1/scripts/bootstrap_load.py
```

Uploads all existing `RETRAN_NODs_*.csv` files to GCS and loads them into `nod_raw.nods`.

### 3. Run dbt Transformations

```bash
cd project1/dbt
dbt run
```

Creates staging view and 3 mart tables in `nod_production`.

### 4. Daily Ingestion (Manual)

```bash
python3 project1/scripts/daily_load.py 2026-03-27
```

Or trigger via Kestra using `project1/kestra/nod_daily_pipeline.yml`.

### 5. Dashboard

Open [Looker Studio](https://lookerstudio.google.com) and connect to:
- `aiagentsintensive.nod_production.mart_nods_by_county_week` — for the temporal tile
- `aiagentsintensive.nod_production.mart_nods_by_city` — for the categorical tile

## Dashboard

Two tiles:
1. **NODs per Week by County** — line/area chart showing foreclosure volume over time, broken out by SoCal county
2. **Top Cities by NOD Count** — bar chart of the cities with the most NOD filings

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_nods` | View | Cleaned, typed, county-labeled staging layer |
| `mart_nods_by_county_week` | Table (partitioned by month, clustered by county) | Weekly NOD counts + avg financials by county |
| `mart_nods_by_city` | Table (clustered by county) | Aggregate NOD stats by city |
| `mart_nods_by_lender` | Table (clustered by county) | Aggregate NOD stats by lender |
