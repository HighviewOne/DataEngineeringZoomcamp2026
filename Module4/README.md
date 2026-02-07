# Module 4 Homework: Analytics Engineering with dbt

## Setup

### Data
- Green and Yellow taxi trip records for 2019-2020
- For-Hire Vehicle (FHV) trip records for 2019
- Source: [DataTalksClub NYC TLC Data](https://github.com/DataTalksClub/nyc-tlc-data/releases)

### Loading Data to BigQuery
Used `load_taxi_data.py` to download CSV files from GitHub releases, upload to GCS bucket `dezoomcamp-hw4-2026`, and load into BigQuery dataset `nytaxi`.

```bash
python3 load_taxi_data.py
```

Final row counts:
| Table | Rows |
|-------|------|
| green_tripdata | 7,778,101 |
| yellow_tripdata | 109,047,518 |
| fhv_tripdata | 43,244,696 |

### dbt Project Setup

Copied the course dbt project from `04-analytics-engineering/taxi_rides_ny/` and configured for BigQuery.

**Profile** (`~/.dbt/profiles.yml`):
```yaml
taxi_rides_ny:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: aiagentsintensive
      dataset: dbt_production
      threads: 4
      location: US
```

**BigQuery-specific fixes applied:**
- `models/marts/schema.yml`: Changed `varchar` to `string`, `integer` to `int64`, `bigint` to `int64`, `store_and_fwd_flag` type to `boolean`
- `models/marts/reporting/fct_monthly_zone_revenue.sql`: Changed `date_trunc('month', pickup_datetime)` to `date_trunc(pickup_datetime, month)` for BigQuery syntax

**Build commands:**
```bash
export GCP_PROJECT_ID=aiagentsintensive
dbt deps
dbt seed
dbt build --target prod --full-refresh
```

---

## Question 1: dbt Lineage and Execution

**If you run `dbt run --select int_trips_unioned`, what models will be built?**

**Answer: `int_trips_unioned` only**

`dbt run --select <model>` builds only the specified model. To include upstream dependencies you would need the `+` prefix (`+int_trips_unioned`), and for downstream the `+` suffix.

---

## Question 2: dbt Tests

**A new value `6` appears in source data. What happens when you run `dbt test --select fct_trips`?**

**Answer: dbt will fail the test, returning a non-zero exit code**

The `accepted_values` test only allows `[1, 2, 3, 4, 5]`. Value `6` violates this constraint, causing the test to fail.

---

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**What is the count of records in the `fct_monthly_zone_revenue` model?**

```sql
SELECT COUNT(*) as cnt
FROM `aiagentsintensive.dbt_production.fct_monthly_zone_revenue`;
```

Result: `12,184`

**Answer: 12,184**

---

## Question 4: Best Performing Zone for Green Taxis (2020)

**Which pickup zone had the highest total revenue for Green taxi trips in 2020?**

```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
FROM `aiagentsintensive.dbt_production.fct_monthly_zone_revenue`
WHERE service_type = "Green" AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 5;
```

| pickup_zone | total_revenue |
|-------------|---------------|
| East Harlem North | 1,817,348.45 |
| East Harlem South | 1,652,945.71 |
| Central Harlem | 1,097,529.02 |
| Washington Heights South | 879,827.40 |
| Morningside Heights | 764,282.04 |

**Answer: East Harlem North**

---

## Question 5: Green Taxi Trip Counts (October 2019)

**What is the total number of trips for Green taxis in October 2019?**

```sql
SELECT SUM(total_monthly_trips) as total_trips
FROM `aiagentsintensive.dbt_production.fct_monthly_zone_revenue`
WHERE service_type = "Green" AND revenue_month = "2019-10-01";
```

Result: `384,624`

**Answer: 384,624**

---

## Question 6: Build a Staging Model for FHV Data

**Create a staging model `stg_fhv_tripdata` and count the records.**

Created `models/staging/stg_fhv_tripdata.sql`:
```sql
select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as pickup_location_id,
    cast(DOlocationID as integer) as dropoff_location_id,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from {{ source('raw', 'fhv_tripdata') }}
where dispatching_base_num is not null
```

```sql
SELECT COUNT(*) as cnt
FROM `aiagentsintensive.dbt_production.stg_fhv_tripdata`;
```

Result: `43,244,693`

**Answer: 43,244,693**
