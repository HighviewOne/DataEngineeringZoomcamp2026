# Module 6 Homework: Batch Processing (Spark)

## Setup

- **Data**: `yellow_tripdata_2025-11.parquet` (Yellow taxi, November 2025)
- **PySpark version**: 4.1.1
- **Java**: OpenJDK 17 (Temurin-17.0.18+8)
- **JAVA_HOME**: `/home/highview/tools/jdk-17.0.18+8`

> **Note (Spark 4.x)**: Timestamps are `TIMESTAMP_NTZ` type and cannot be cast directly to `BIGINT`.
> Use `F.unix_timestamp(col.cast('timestamp'))` to convert to seconds for duration calculations.

---

## Q1: Install Spark and PySpark

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework6') \
    .getOrCreate()

print(spark.version)
```

**Answer: `4.1.1`**

---

## Q2: Yellow November 2025 — Average Parquet File Size

```python
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')
print(f"Total rows: {df.count()}")  # 4,181,444

df_repartitioned = df.repartition(4)
df_repartitioned.write.mode('overwrite').parquet('output/')

# File sizes: [24.42, 24.41, 24.41, 24.42] MB
# Average: ~24.41 MB
```

**Answer: `25MB`**

---

## Q3: Count Trips on November 15, 2025

```python
from pyspark.sql import functions as F

count = df \
    .withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .filter("pickup_date = '2025-11-15'") \
    .count()
# Result: 162,604
```

**Answer: `162,604`**

---

## Q4: Longest Trip in Hours

```python
# Note: TIMESTAMP_NTZ requires casting to timestamp before unix_timestamp()
result = df.withColumn(
    'duration_hours',
    (F.unix_timestamp(df.tpep_dropoff_datetime.cast('timestamp'))
     - F.unix_timestamp(df.tpep_pickup_datetime.cast('timestamp'))) / 3600
).agg(F.max('duration_hours').alias('max_hours'))
# Result: 90.65 hours
```

**Answer: `90.6`**

---

## Q5: Spark UI Port

Spark's User Interface dashboard runs on local port:

**Answer: `4040`**

---

## Q6: Least Frequent Pickup Location Zone

```python
zones = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')
df.createOrReplaceTempView('yellow_2025_11')
zones.createOrReplaceTempView('zones')

spark.sql("""
SELECT z.Zone, COUNT(1) as trip_count
FROM yellow_2025_11 y
LEFT JOIN zones z ON y.PULocationID = CAST(z.LocationID AS INT)
GROUP BY z.Zone
ORDER BY trip_count ASC
LIMIT 5
""").show(truncate=False)
```

Results:
| Zone | trip_count |
|------|------------|
| Governor's Island/Ellis Island/Liberty Island | 1 |
| Eltingville/Annadale/Prince's Bay | 1 |
| Arden Heights | 1 |
| Port Richmond | 3 |
| Rikers Island | 4 |

**Answer: `Governor's Island/Ellis Island/Liberty Island`**

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Spark version | `4.1.1` |
| Q2: Avg parquet file size | `25MB` |
| Q3: Trips on Nov 15 | `162,604` |
| Q4: Longest trip (hours) | `90.6` |
| Q5: Spark UI port | `4040` |
| Q6: Least frequent zone | `Governor's Island/Ellis Island/Liberty Island` |
