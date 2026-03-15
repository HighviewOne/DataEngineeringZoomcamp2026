# Module 7 Homework: Streaming with PyFlink and Redpanda

## Setup

- Data: Green taxi trips, October 2025 (`green_tripdata_2025-10.parquet`, 49,416 rows)
- Infrastructure: Redpanda + Flink Job/Task Manager + Postgres via docker-compose
- Directory: `07-streaming/workshop/` (container prefix: `workshop-`)

```bash
cd 07-streaming/workshop/
docker compose up -d
```

> **Note**: NaN values in `passenger_count` must be handled before sending to Kafka.
> Use a custom serializer that converts `float('nan')` → `null`:
> ```python
> import math
> def json_serializer(data):
>     cleaned = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in data.items()}
>     return json.dumps(cleaned).encode('utf-8')
> ```

---

## Q1: Redpanda Version

```bash
docker exec workshop-redpanda-1 rpk version
```

Output:
```
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1
```

**Answer: `v24.2.18`**

---

## Q2: Sending Data to Redpanda

Created topic and sent 49,416 rows from the parquet file to `green-trips`, keeping columns:
`lpep_pickup_datetime`, `lpep_dropoff_datetime`, `PULocationID`, `DOLocationID`,
`passenger_count`, `trip_distance`, `tip_amount`, `total_amount`

```python
t0 = time()
for _, row in df.iterrows():
    producer.send('green-trips', value=row.to_dict())
producer.flush()
t1 = time()
print(f'took {(t1 - t0):.2f} seconds')  # ~10.97 seconds
```

**Answer: `10 seconds`**

---

## Q3: Consumer — Trip Distance

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
)

count = sum(1 for msg in consumer if msg.value.get('trip_distance', 0) > 5.0)
print(f"Trips with distance > 5: {count}")  # 8506
```

**Answer: `8,506`**

---

## Q4: Tumbling Window — Pickup Location

5-minute tumbling window counting trips per `PULocationID`, using `lpep_pickup_datetime`
as event time with 5-second watermark tolerance. Parallelism set to 1.

```sql
INSERT INTO tumbling_pickup
SELECT window_start, PULocationID, COUNT(*) AS num_trips
FROM TABLE(
    TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
)
GROUP BY window_start, PULocationID
```

Results:
```sql
SELECT PULocationID, MAX(num_trips) FROM tumbling_pickup
GROUP BY PULocationID ORDER BY max DESC LIMIT 3;
```
| PULocationID | max_trips |
|---|---|
| 74 | 81 |
| 75 | 55 |
| 82 | 15 |

**Answer: `74`**

---

## Q5: Session Window — Longest Streak

Session window with 5-minute gap on `PULocationID`, using `lpep_pickup_datetime`
as event time with 5-second watermark tolerance.

```sql
INSERT INTO session_pickup
SELECT
    PULocationID,
    SESSION_START(event_timestamp, INTERVAL '5' MINUTES) AS window_start,
    SESSION_END(event_timestamp, INTERVAL '5' MINUTES) AS window_end,
    COUNT(*) AS num_trips
FROM green_trips
GROUP BY SESSION(event_timestamp, INTERVAL '5' MINUTES), PULocationID
```

Results:
| PULocationID | max_trips |
|---|---|
| 74 | 81 |
| 75 | 55 |
| 82 | 15 |

**Answer: `81`** (PULocationID 74 had 81 trips in its longest session)

---

## Q6: Tumbling Window — Largest Tip Hour

1-hour tumbling window computing total `tip_amount` across all locations.

```sql
INSERT INTO tumbling_tips
SELECT window_start, SUM(tip_amount) AS total_tip
FROM TABLE(
    TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
)
GROUP BY window_start
```

Results:
| window_start | total_tip |
|---|---|
| 2025-10-16 18:00:00 | 524.96 |
| 2025-10-30 16:00:00 | 507.10 |
| 2025-10-10 17:00:00 | 499.60 |

**Answer: `2025-10-16 18:00:00`**

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Redpanda version | `v24.2.18` |
| Q2: Time to send 49,416 rows | `10 seconds` |
| Q3: Trips with distance > 5 | `8,506` |
| Q4: PULocationID most trips (5-min window) | `74` |
| Q5: Trips in longest session | `81` |
| Q6: Hour with highest total tips | `2025-10-16 18:00:00` |
