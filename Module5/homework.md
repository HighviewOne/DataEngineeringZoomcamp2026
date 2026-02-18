# Module 5 Homework: Data Platforms with Bruin

## Setup

- Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
- Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
- Configure `.bruin.yml` with a DuckDB connection

---

## Q1: Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

**Answer: `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`**

After `bruin init`, the project structure is:
```
my-pipeline/
├── .bruin.yml        ← project-level config (environments & connections)
└── pipeline/
    ├── pipeline.yml  ← pipeline config (name, schedule, etc.)
    └── assets/       ← asset definitions (SQL, Python, YAML)
```

---

## Q2: Materialization Strategies

Processing NYC taxi data organized by month based on `pickup_datetime`. Which strategy for the staging layer that deduplicates and cleans the data?

**Answer: `time_interval`**

`time_interval` incrementally loads data within specific time windows using an `incremental_key` (e.g. `pickup_datetime`). It deletes and reinserts records within the window, achieving deduplication without a full rebuild.

```yaml
materialization:
  type: time_interval
  incremental_key: pickup_datetime
  time_granularity: date
```

- `append` — just adds rows, no deduplication
- `replace` — full table rebuild every run, inefficient
- `view` — virtual table, not materialized

---

## Q3: Pipeline Variables

Overriding an array variable to only process yellow taxis:

```yaml
# pipeline.yml
variables:
  taxi_types:
    type: array
    default: ["yellow", "green"]
```

**Answer: `bruin run --var 'taxi_types=["yellow"]'`**

Complex types (arrays, objects) require JSON syntax with the `--var` flag.

---

## Q4: Running with Dependencies

Modified `ingestion/trips.py` and want to run it plus all downstream assets.

**Answer: `bruin run ingestion/trips.py --downstream`**

The `--downstream` flag runs the specified asset and all assets that depend on it. The asset is referenced by its file path.

---

## Q5: Quality Checks

Ensure the `pickup_datetime` column never has NULL values.

**Answer: `not_null: true`**

```yaml
columns:
  - name: pickup_datetime
    checks:
      - name: not_null
```

The `not_null` check verifies that none of the column's values are null.

---

## Q6: Lineage and Dependencies

Visualize the dependency graph between assets.

**Answer: `bruin lineage`**

```bash
bruin lineage path/to/asset.sql
bruin lineage path/to/asset.sql --full   # includes all indirect dependencies
```

`bruin graph`, `bruin dependencies`, and `bruin show` do not exist as Bruin commands.

---

## Q7: First-Time Run

Running on a new DuckDB database — ensure tables are created from scratch.

**Answer: `--full-refresh`**

```bash
bruin run --full-refresh
```

`--full-refresh` truncates the table before running. Also sets the `full_refresh` Jinja variable to `True` and `BRUIN_FULL_REFRESH=1`.

---

## Summary

| Q | Answer |
|---|--------|
| Q1: Required project structure | `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/` |
| Q2: Materialization strategy | `time_interval` |
| Q3: Override array variable | `bruin run --var 'taxi_types=["yellow"]'` |
| Q4: Run with downstream | `bruin run ingestion/trips.py --downstream` |
| Q5: Null quality check | `not_null: true` |
| Q6: Visualize dependency graph | `bruin lineage` |
| Q7: First-time run flag | `--full-refresh` |
