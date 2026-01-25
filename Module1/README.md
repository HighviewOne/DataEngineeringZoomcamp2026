# Module 1 Homework Solutions: Docker & SQL

## Question 1: Understanding Docker images

**What's the version of `pip` in the `python:3.13` image?**

```bash
docker run -it --entrypoint bash python:3.13
pip --version
```

**Answer: `24.3.1`**

---

## Question 2: Understanding Docker networking and docker-compose

**What is the hostname and port that pgadmin should use to connect to the postgres database?**

Within Docker Compose's internal network, services communicate using:
- **Service name** (not container name) as the hostname
- **Internal container port** (not the mapped host port)

The postgres service is named `db` and exposes port `5432` internally.

**Answer: `db:5432`**

---

## Question 3: Counting short trips

**How many trips had a `trip_distance` of less than or equal to 1 mile in November 2025?**

```sql
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

**Answer: `8,007`**

---

## Question 4: Longest trip for each day

**Which was the pickup day with the longest trip distance? (Only trips < 100 miles)**

```sql
SELECT DATE(lpep_pickup_datetime) as pickup_day, MAX(trip_distance) as max_distance
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

**Answer: `2025-11-14`** (88.03 miles)

---

## Question 5: Biggest pickup zone

**Which pickup zone had the largest `total_amount` on November 18th, 2025?**

```sql
SELECT z.Zone, SUM(t.total_amount) as total
FROM green_taxi_trips t
JOIN taxi_zones z ON t.PULocationID = z.LocationID
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.Zone
ORDER BY total DESC
LIMIT 1;
```

**Answer: `East Harlem North`** ($9,281.92)

---

## Question 6: Largest tip

**For passengers picked up in "East Harlem North" in November 2025, which drop off zone had the largest tip?**

```sql
SELECT dz.Zone as dropoff_zone, t.tip_amount
FROM green_taxi_trips t
JOIN taxi_zones pz ON t.PULocationID = pz.LocationID
JOIN taxi_zones dz ON t.DOLocationID = dz.LocationID
WHERE pz.Zone = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
ORDER BY t.tip_amount DESC
LIMIT 1;
```

**Answer: `Yorkville West`** ($81.89 tip)

---

## Question 7: Terraform Workflow

**Which sequence describes the workflow for:**
1. Downloading provider plugins and setting up backend
2. Generating proposed changes and auto-executing the plan
3. Removing all resources managed by Terraform

**Answer: `terraform init, terraform apply -auto-approve, terraform destroy`**

- `terraform init` - Initializes the working directory, downloads providers
- `terraform apply -auto-approve` - Creates execution plan and applies it without prompting
- `terraform destroy` - Removes all resources defined in the Terraform configuration

---

## Summary

| Question | Answer |
|----------|--------|
| Q1 | 24.3.1 |
| Q2 | db:5432 |
| Q3 | 8,007 |
| Q4 | 2025-11-14 |
| Q5 | East Harlem North |
| Q6 | Yorkville West |
| Q7 | terraform init, terraform apply -auto-approve, terraform destroy |
