# Module 5 Homework: Batch Processing (Spark)

## Setup

- **Data**: `yellow_tripdata_2024-10.parquet` (Yellow taxi, October 2024)
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
    .appName('homework5') \
    .getOrCreate()

print(spark.version)
```

**Answer: `4.1.1`**

---

## Q2: Yellow October 2024 — Average Parquet File Size

```python
df = spark.read.parquet('yellow_tripdata_2024-10.parquet')
df_repartitioned = df.repartition(4)
df_repartitioned.write.mode('overwrite').parquet('output/')

# File sizes: [22.43, 22.37, 22.40, 22.38] MB
# Average: 22.39 MB
```

**Answer: `25MB`** (closest option to actual ~22 MB)

---

## Q3: Count Trips on October 15, 2024

```python
from pyspark.sql import functions as F

count_oct15 = df \
    .withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .filter("pickup_date = '2024-10-15'") \
    .count()
# Result: 128,893
```

**Answer: `125,567`** (closest option to actual 128,893)

---

## Q4: Longest Trip in Hours

```python
# Note: TIMESTAMP_NTZ requires casting to timestamp before unix_timestamp()
result = df.withColumn(
    'duration_hours',
    (F.unix_timestamp(df.tpep_dropoff_datetime.cast('timestamp'))
     - F.unix_timestamp(df.tpep_pickup_datetime.cast('timestamp'))) / 3600
).agg(F.max('duration_hours').alias('max_hours'))
# Result: 162.62 hours
```

**Answer: `162`**

---

## Q5: Spark UI Port

Spark's User Interface dashboard runs on local port:

**Answer: `4040`**

---

## Q6: Least Frequent Pickup Location Zone

```python
zones = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')
df.createOrReplaceTempView('yellow_2024_10')
zones.createOrReplaceTempView('zones')

spark.sql("""
SELECT
    z.Zone,
    COUNT(1) as trip_count
FROM yellow_2024_10 y
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
| Rikers Island | 2 |
| Arden Heights | 2 |
| Jamaica Bay | 3 |
| Green-Wood Cemetery | 3 |

**Answer: `Governor's Island/Ellis Island/Liberty Island`**

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Spark version | `4.1.1` |
| Q2: Avg parquet file size | `25MB` |
| Q3: Trips on Oct 15 | `125,567` |
| Q4: Longest trip (hours) | `162` |
| Q5: Spark UI port | `4040` |
| Q6: Least frequent zone | `Governor's Island/Ellis Island/Liberty Island` |
