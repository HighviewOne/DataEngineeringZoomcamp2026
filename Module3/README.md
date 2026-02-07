# Module 3 Homework: Data Warehousing & BigQuery

## Setup

### Data
Yellow Taxi Trip Records for January 2024 - June 2024 from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

### Loading Data to GCS
Used the provided `load_yellow_taxi_data.py` script to download 6 parquet files and upload them to GCS bucket `dezoomcamp-hw3-2026`.

```bash
python3 load_yellow_taxi_data.py
```

### BigQuery Table Setup

**Create External Table:**
```sql
CREATE OR REPLACE EXTERNAL TABLE `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_external`
OPTIONS (
  format = "PARQUET",
  uris = ["gs://dezoomcamp-hw3-2026/yellow_tripdata_2024-*.parquet"]
);
```

**Create Materialized Table (no partitioning or clustering):**
```sql
CREATE OR REPLACE TABLE `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`
AS
SELECT * FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_external`;
```

---

## Question 1: Counting Records

**What is the count of records for the 2024 Yellow Taxi Data?**

```sql
SELECT COUNT(*) as total_records
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
```

Result: `20,332,093`

**Answer: 20,332,093**

---

## Question 2: Data Read Estimation

**What is the estimated amount of data read when counting distinct PULocationIDs on the External Table vs the Materialized Table?**

```sql
-- External Table
SELECT COUNT(DISTINCT PULocationID)
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_external`;
-- Estimated: 0 MB

-- Materialized Table
SELECT COUNT(DISTINCT PULocationID)
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
-- Estimated: ~155.12 MB (162,656,744 bytes)
```

BigQuery cannot estimate scan size for external tables (shows 0 MB), but for native tables it knows the column size precisely.

**Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table**

---

## Question 3: Understanding Columnar Storage

**Why are the estimated bytes different when querying 1 column vs 2 columns from the materialized table?**

```sql
-- Query 1 column
SELECT PULocationID
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
-- Estimated: 162,656,744 bytes (~155 MB)

-- Query 2 columns
SELECT PULocationID, DOLocationID
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
-- Estimated: 325,313,488 bytes (~310 MB)
```

The 2-column query estimates exactly double the bytes of the 1-column query because each column is stored independently at ~155 MB.

**Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query.** Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

---

## Question 4: Counting Zero Fare Trips

**How many records have a fare_amount of 0?**

```sql
SELECT COUNT(*) as zero_fare_trips
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`
WHERE fare_amount = 0;
```

Result: `8,333`

**Answer: 8,333**

---

## Question 5: Partitioning and Clustering

**What is the best strategy to make an optimized table if your query will always filter on tpep_dropoff_datetime and order by VendorID?**

```sql
CREATE OR REPLACE TABLE `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
```

- **Partition** on the filter column (datetime) to prune entire data blocks
- **Cluster** on the sort/order column for efficient ordering within partitions

**Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**

---

## Question 6: Partition Benefits

**Compare estimated bytes for distinct VendorIDs between 2024-03-01 and 2024-03-15 on both tables.**

```sql
-- Non-partitioned table
SELECT DISTINCT VendorID
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN "2024-03-01" AND "2024-03-15";
-- Estimated: 325,313,488 bytes (~310.24 MB)

-- Partitioned table
SELECT DISTINCT VendorID
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN "2024-03-01" AND "2024-03-15";
-- Estimated: 28,141,776 bytes (~26.84 MB)
```

Partitioning reduced the scan by ~91% because BigQuery only reads the partitions covering March 1-15.

**Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

---

## Question 7: External Table Storage

**Where is the data stored in the External Table?**

External tables don't store data in BigQuery — they reference files stored in Google Cloud Storage.

**Answer: GCP Bucket**

---

## Question 8: Clustering Best Practices

**Is it best practice in BigQuery to always cluster your data?**

Clustering adds overhead and isn't beneficial for small tables or when queries don't filter/sort on the clustered columns. It's best used for large tables with frequent filtering on specific columns.

**Answer: False**

---

## Question 9: Understanding Table Scans

**How many bytes does a `SELECT COUNT(*)` query estimate on the materialized table? Why?**

```sql
SELECT COUNT(*)
FROM `aiagentsintensive.dezoomcamp_hw3.yellow_taxi_materialized`;
-- Estimated: 0 bytes
```

**Answer: 0 bytes.** BigQuery maintains internal metadata for each table, including the total row count. Since `SELECT COUNT(*)` with no `WHERE` clause doesn't need to read any actual column data, BigQuery resolves it directly from its cached metadata, resulting in 0 bytes processed.
