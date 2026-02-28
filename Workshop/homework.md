# Workshop Homework: Build Your Own dlt Pipeline

## Setup

- Install dlt: `pip install "dlt[duckdb]"`
- API: `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`
- Format: Paginated JSON (1,000 records per page), stop when empty page returned

---

## Pipeline

Built a custom dlt pipeline using a generator function to handle pagination:

```python
import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.resource(name="rides", write_disposition="replace")
def taxi_rides():
    page = 1
    while True:
        response = requests.get(BASE_URL, params={"page": page})
        data = response.json() if response.content else []
        if not data:
            break
        yield data
        page += 1

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
)

load_info = pipeline.run(taxi_rides())
print(load_info)
```

The API returns 1,000 records on page 1 and an empty response from page 2 onwards (10,000 total records).

---

## Q1: What is the start date and end date of the dataset?

```sql
SELECT
    MIN(trip_pickup_date_time) AS start_date,
    MAX(trip_pickup_date_time) AS end_date
FROM taxi_data.rides;
```

Result:
| start_date | end_date |
|------------|----------|
| 2009-06-01 | 2009-06-30 |

**Answer: `2009-06-01 to 2009-07-01`**

---

## Q2: What proportion of trips are paid with credit card?

```sql
SELECT
    payment_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM taxi_data.rides
GROUP BY payment_type
ORDER BY count DESC;
```

Result:
| payment_type | count | pct |
|---|---|---|
| CASH | 7,235 | 72.35% |
| Credit | 2,666 | 26.66% |
| Cash | 97 | 0.97% |
| No Charge | 1 | 0.01% |
| Dispute | 1 | 0.01% |

**Answer: `26.66%`**

---

## Q3: What is the total amount of money generated in tips?

```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM taxi_data.rides;
```

Result: `6063.41`

**Answer: `$6,063.41`**

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Dataset date range | `2009-06-01 to 2009-07-01` |
| Q2: Credit card proportion | `26.66%` |
| Q3: Total tips | `$6,063.41` |
