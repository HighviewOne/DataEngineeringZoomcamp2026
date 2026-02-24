# Module 6 Homework: Streaming with PyFlink and RedPanda

## Setup

- RedPanda (Kafka-compatible broker) + Flink Job/Task Manager + Postgres via docker-compose
- Data: Green taxi trips, October 2019

```bash
cd data-engineering-zoomcamp-main/06-streaming/pyflink/
docker compose up -d
```

---

## Q1: Redpanda Version

Run `rpk help` inside the container to find the version command, then execute it:

```bash
docker exec redpanda-1 rpk help
docker exec redpanda-1 rpk version
```

Output:
```
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```

**Answer: `v24.2.18`**

---

## Q2: Creating a Topic

```bash
docker exec redpanda-1 rpk topic create green-trips
```

Output:
```
TOPIC        STATUS
green-trips  OK
```

**Answer:**
```
TOPIC        STATUS
green-trips  OK
```

---

## Q3: Connecting to the Kafka Server

```python
import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

**Answer: `True`**

---

## Q4: Sending the Trip Data

Download the data and send all rows to `green-trips`, keeping only the required columns:

```python
import json
import gzip
import csv
from kafka import KafkaProducer
from time import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

topic_name = 'green-trips'
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

t0 = time()

with gzip.open('green_tripdata_2019-10.csv.gz', 'rt') as f:
    reader = csv.DictReader(f)
    for row in reader:
        message = {col: row[col] for col in columns}
        producer.send(topic_name, value=message)

producer.flush()

t1 = time()
print(f'Took {t1 - t0:.2f} seconds')
```

Result: Sent **476,386 messages**

**Answer: ~46 seconds**

---

## Q5: Build a Sessionization Window

Created `session_job.py` based on `aggregation_job.py`:

- Reads from `green-trips` topic
- Schema includes all 7 columns with `lpep_dropoff_datetime` parsed as TIMESTAMP
- Watermark on `dropoff_watermark` with 5-second tolerance
- SESSION window with 5-minute gap
- Groups by `PULocationID` and `DOLocationID`
- Configured `table.exec.source.idle-timeout=5000` to handle idle source subtasks (only 1 Kafka partition with parallelism 3)

Key notes:
- Used older GROUP BY SESSION syntax (compatible with Flink 1.16)
- Idle timeout required because only 1 Kafka partition assigned to 1 of 3 subtasks — without it, watermark never advances

```sql
SELECT
    PULocationID,
    DOLocationID,
    SESSION_START(dropoff_watermark, INTERVAL '5' MINUTES) AS window_start,
    SESSION_END(dropoff_watermark, INTERVAL '5' MINUTES) AS window_end,
    COUNT(*) AS num_trips
FROM green_trips
GROUP BY
    SESSION(dropoff_watermark, INTERVAL '5' MINUTES),
    PULocationID,
    DOLocationID
```

Top results:

| PULocationID | DOLocationID | Zone | num_trips | window_start | window_end |
|---|---|---|---|---|---|
| 95 | 95 | Forest Hills / Forest Hills | 44 | 2019-10-16 18:18:42 | 2019-10-16 19:26:16 |
| 7 | 7 | Astoria / Astoria | 43 | 2019-10-16 19:21:56 | 2019-10-16 20:32:06 |
| 82 | 138 | Elmhurst / LaGuardia Airport | 35 | 2019-10-11 17:18:12 | 2019-10-11 17:58:34 |

**Answer: `PULocationID=95, DOLocationID=95 — Forest Hills / Forest Hills` (44 trips)**

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Redpanda version | `v24.2.18` |
| Q2: Create topic output | `TOPIC: green-trips, STATUS: OK` |
| Q3: bootstrap_connected() | `True` |
| Q4: Time to send 476,386 messages | ~46 seconds |
| Q5: Longest session streak | `Forest Hills / Forest Hills` (PU=95, DO=95, 44 trips) |
