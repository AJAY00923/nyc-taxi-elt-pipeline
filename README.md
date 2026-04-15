# NYC Taxi ELT Pipeline

A production-grade batch ELT pipeline that ingests NYC TLC yellow taxi trip data monthly,
transforms it through a layered dbt model, and enforces data quality with Great Expectations.

---

## Architecture

```
NYC TLC API (public Parquet)
    │
    ▼
[Airflow DAG — monthly schedule]
    │
    ├── 1. check_source_availability   → HEAD request to verify file exists
    ├── 2. ingest_to_snowflake         → Stream download → write_pandas → RAW schema
    ├── 3. check_row_count             → Anomaly detection vs 6-month average
    │
    ▼
[dbt transformations]
    │
    ├── staging/stg_yellow_trips       → Clean, cast, filter bad rows (VIEW)
    ├── intermediate/int_trips_enriched → Business logic, derived metrics (INCREMENTAL TABLE)
    └── marts/
        ├── mart_daily_trip_summary    → Daily aggregates by time-of-day (TABLE)
        └── mart_zone_performance      → Monthly revenue by pickup zone (TABLE)
    │
    ▼
[Great Expectations]
    │
    └── nyc_taxi_checkpoint            → Validates staging + marts layers
    │
    ▼
[Audit log → NYC_TAXI.AUDIT.PIPELINE_RUNS]
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | Apache Airflow 2.9 | Industry standard, DAG-based, great observability |
| Storage | Snowflake (free trial) | Scales to petabytes, columnar, dbt native |
| Transformation | dbt Core 1.7 | Version-controlled SQL, lineage, built-in testing |
| Data Quality | Great Expectations 0.18 | Configurable expectations, HTML data docs |
| Containerization | Docker Compose | One-command local setup |
| CI | GitHub Actions | Runs dbt tests on every PR automatically |

---

## Project Structure

```
nyc_taxi_pipeline/
├── dags/
│   └── nyc_taxi_elt.py          # Main Airflow DAG
├── ingestion/
│   └── (ingest logic is in the DAG — keeps ingestion co-located)
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_yellow_trips.sql
│   │   │   └── stg_yellow_trips.yml
│   │   ├── intermediate/
│   │   │   └── int_trips_enriched.sql
│   │   └── marts/
│   │       ├── mart_daily_trip_summary.sql
│   │       ├── mart_zone_performance.sql
│   │       └── marts.yml
│   ├── tests/
│   │   └── custom_tests.sql     # Custom dbt generic tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── expectations/
│   ├── build_suite.py           # Builds GE expectation suites
│   └── nyc_taxi_checkpoint.yml  # GE checkpoint config
├── docker/
│   └── docker-compose.yml
├── .github/
│   └── workflows/
│       └── dbt_ci.yml           # CI: runs dbt tests on every PR
└── docs/
    └── snowflake_setup.sql      # One-time Snowflake setup
```

---

## Quickstart (local)

### Prerequisites
- Docker + Docker Compose
- A free Snowflake account (https://signup.snowflake.com — no credit card needed)

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 2. Set up Snowflake

Run `docs/snowflake_setup.sql` in your Snowflake worksheet. This creates:
- Database: `NYC_TAXI`
- Schemas: `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`, `AUDIT`
- Roles: `TRANSFORMER`, `ANALYST`
- Warehouse: `COMPUTE_WH` (XS, auto-suspend 60s)

### 3. Start Airflow

```bash
cd docker
docker-compose up -d

# Wait ~60 seconds, then open:
# http://localhost:8080  (admin / admin)
```

### 4. Configure the Snowflake connection in Airflow

Admin → Connections → Add:
- Conn ID: `snowflake_default`
- Conn Type: Snowflake
- Account: your Snowflake account identifier
- Login/Password: `PIPELINE_USER` credentials from setup script
- Schema: `RAW`
- Database: `NYC_TAXI`
- Warehouse: `COMPUTE_WH`
- Role: `TRANSFORMER`

### 5. Trigger a run

In Airflow UI, find `nyc_taxi_elt_pipeline` → trigger manually.

---

## Data Quality

The pipeline has three layers of data quality enforcement:

**Layer 1 — Airflow row count check**
After ingestion, the DAG compares the ingested row count against the 6-month rolling average.
If deviation exceeds 20%, an alert fires and the issue is logged — but the pipeline continues.

**Layer 2 — dbt tests**
Every model has schema tests defined in `.yml` files:
- `not_null` on all critical columns
- `accepted_values` for categorical columns (payment_type, time_of_day)
- `relationships` ensuring referential integrity
- Custom tests: `no_future_dates`, `positive_values`, `reasonable_trip_duration`

Run manually:
```bash
cd dbt_project
dbt test --select staging   # test just staging
dbt test                    # test everything
```

**Layer 3 — Great Expectations**
Validates row counts, column distributions, null rates, and value ranges.
Generates HTML data docs for each checkpoint run.

```bash
great_expectations checkpoint run nyc_taxi_checkpoint
# HTML report: great_expectations/uncommitted/data_docs/local_site/
```

---

## Design Decisions

**Why dbt incremental for the intermediate layer?**
The staging layer is a view — cheap to recompute. But intermediate enriches and widens each row
with computed fields. Running it as `incremental` means we only process new months, not the full
historical dataset every time. Cost and speed both improve significantly at scale.

**Why separate staging, intermediate, and marts?**
Following the dbt best practice of "one model, one responsibility":
- `staging`: only cleaning and casting — no business logic
- `intermediate`: business logic and enrichment — no aggregation
- `marts`: aggregation for specific use cases — no raw columns

This makes debugging easy. If revenue numbers are wrong, you check the mart. If a column is cast
incorrectly, you check staging.

**Why Great Expectations AND dbt tests?**
dbt tests are schema-level: they check shape, types, and simple value constraints.
Great Expectations adds statistical expectations: distribution checks, row count thresholds,
and cross-column expectations that dbt's built-in tests can't express. Both serve different purposes.

**Why not load to a local Postgres instead of Snowflake?**
Snowflake's free trial gives you $400 of credits and the same SQL dialect used in production
at most companies. Building against it means your skills transfer directly. Local Postgres
teaches you different things.

---

## What I'd Do Differently in Production

1. **Secrets management**: Replace env vars with AWS Secrets Manager or HashiCorp Vault.
   Never commit credentials even in `.env` files.

2. **Partitioning strategy**: Snowflake doesn't auto-partition like BigQuery. In production
   I'd add `CLUSTER BY (pickup_date)` to the intermediate table to reduce scan costs.

3. **dbt Cloud vs dbt Core**: dbt Core works here, but dbt Cloud adds a UI, scheduling,
   and lineage visualization. Worth the cost for a team.

4. **Alerting**: The current row count alert is basic. Production would use PagerDuty or
   Opsgenie with severity levels, and track MTTR (mean time to resolve) for the pipeline.

5. **Idempotency**: The current ingest appends rows. In production, I'd add a
   `_source_month` partition delete before re-inserting to make reruns safe.

---

## Data Source

NYC TLC Trip Record Data — public domain, updated monthly.
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## Author

Built as a portfolio project to demonstrate production-grade data engineering practices.
Stack: Airflow · dbt · Snowflake · Great Expectations · Docker · GitHub Actions
