# Module 2 Homework Solutions: Workflow Orchestration

## Question 1: Uncompressed file size

**Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size?**

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz
gunzip yellow_tripdata_2020-12.csv.gz
ls -lh yellow_tripdata_2020-12.csv
```

**Answer: `128.3 MiB`**

---

## Question 2: Rendered variable value

**What is the rendered value of the variable `file` when taxi=`green`, year=`2020`, month=`04`?**

The Kestra template `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` renders the input values directly.

**Answer: `green_tripdata_2020-04.csv`**

---

## Question 3: Yellow Taxi 2020 row count

**How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?**

```bash
total=0
for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
  count=$(zcat "yellow_tripdata_2020-${month}.csv.gz" | wc -l)
  count=$((count - 1))  # Subtract header
  total=$((total + count))
done
echo "Total: ${total}"
```

Monthly breakdown:
| Month | Rows |
|-------|------|
| 01 | 6,405,008 |
| 02 | 6,299,354 |
| 03 | 3,007,292 |
| 04 | 237,993 |
| 05 | 348,371 |
| 06 | 549,760 |
| 07 | 800,412 |
| 08 | 1,007,284 |
| 09 | 1,341,012 |
| 10 | 1,681,131 |
| 11 | 1,508,985 |
| 12 | 1,461,897 |

**Answer: `24,648,499`**

---

## Question 4: Green Taxi 2020 row count

**How many rows are there for the Green Taxi data for all CSV files in the year 2020?**

Monthly breakdown:
| Month | Rows |
|-------|------|
| 01 | 447,770 |
| 02 | 398,632 |
| 03 | 223,406 |
| 04 | 35,612 |
| 05 | 57,360 |
| 06 | 63,109 |
| 07 | 72,257 |
| 08 | 81,063 |
| 09 | 87,987 |
| 10 | 95,120 |
| 11 | 88,605 |
| 12 | 83,130 |

**Answer: `1,734,051`**

---

## Question 5: Yellow Taxi March 2021 row count

**How many rows are there for the Yellow Taxi data for the March 2021 CSV file?**

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz
zcat yellow_tripdata_2021-03.csv.gz | wc -l
# Subtract 1 for header
```

**Answer: `1,925,152`**

---

## Question 6: Timezone configuration in Kestra

**How would you configure the timezone to New York in a Schedule trigger?**

In Kestra, timezones must be specified using IANA timezone format (e.g., `America/New_York`), not abbreviations like `EST` or offsets like `UTC-5`.

Example configuration:
```yaml
triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 * * *"
    timezone: America/New_York
```

**Answer: Add a `timezone` property set to `America/New_York` in the Schedule trigger configuration**

---

## Summary

| Question | Answer |
|----------|--------|
| Q1 | 128.3 MiB |
| Q2 | green_tripdata_2020-04.csv |
| Q3 | 24,648,499 |
| Q4 | 1,734,051 |
| Q5 | 1,925,152 |
| Q6 | timezone property set to America/New_York |
