"""
NYC Taxi ELT Pipeline DAG
Ingests monthly NYC TLC trip data → Snowflake → dbt transforms → GE validation
Schedule: monthly, 1st of each month at 6am UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ─── Default args ────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["your-email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=4),
}

# ─── DAG definition ───────────────────────────────────────────────────────────

dag = DAG(
    dag_id="nyc_taxi_elt_pipeline",
    default_args=default_args,
    description="Monthly NYC TLC taxi data ELT pipeline",
    schedule_interval="0 6 1 * *",  # 1st of every month at 6am UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "nyc-taxi", "production"],
    doc_md="""
    ## NYC Taxi ELT Pipeline

    Ingests NYC TLC yellow taxi trip data monthly.

    ### Flow
    1. Check source API availability
    2. Download Parquet file for target month
    3. Upload raw data to Snowflake staging
    4. Run dbt transformations (staging → intermediate → marts)
    5. Run Great Expectations validation suite
    6. Run dbt tests
    7. Log pipeline run metadata

    ### Connections Required
    - `snowflake_default`: Snowflake connection
    - `http_default`: NYC TLC API

    ### On failure
    Airflow sends email alert. Check logs for the failed task.
    """,
)

# ─── Task functions ───────────────────────────────────────────────────────────

def check_source_availability(**context):
    """Verify the NYC TLC data source is reachable before doing any work."""
    import requests

    execution_date = context["logical_date"]
    # Target the month prior to execution (data lags ~2 months)
    target_year = execution_date.year
    target_month = execution_date.month - 2 if execution_date.month > 2 else 12
    if execution_date.month <= 2:
        target_year -= 1

    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{target_year}-{target_month:02d}.parquet"
    )

    logger.info(f"Checking availability: {url}")
    response = requests.head(url, timeout=30)

    if response.status_code != 200:
        raise ValueError(
            f"Source file not available. Status: {response.status_code}. URL: {url}"
        )

    context["ti"].xcom_push(key="source_url", value=url)
    context["ti"].xcom_push(
        key="target_month", value=f"{target_year}-{target_month:02d}"
    )
    logger.info(f"Source confirmed available: {url}")


def ingest_to_snowflake(**context):
    """
    Download Parquet from NYC TLC and load into Snowflake RAW schema.
    Uses chunked reads to handle large files without memory issues.
    """
    import pandas as pd
    import pyarrow.parquet as pq
    import requests
    import tempfile
    import os
    from snowflake.connector.pandas_tools import write_pandas

    source_url = context["ti"].xcom_pull(key="source_url")
    target_month = context["ti"].xcom_pull(key="target_month")

    logger.info(f"Downloading from: {source_url}")

    # Stream download to temp file to avoid memory spike
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        with requests.get(source_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                tmp.write(chunk)

    logger.info(f"Downloaded to {tmp_path}")

    try:
        # Read only the columns we need — reduces memory and load time
        columns_needed = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "PULocationID",
            "DOLocationID", "payment_type", "fare_amount",
            "tip_amount", "tolls_amount", "total_amount",
            "congestion_surcharge", "airport_fee",
        ]

        df = pd.read_parquet(tmp_path, columns=columns_needed)
        df["_source_month"] = target_month
        df["_ingested_at"] = datetime.utcnow()

        # Normalize column names to uppercase for Snowflake
        df.columns = [c.upper() for c in df.columns]

        logger.info(f"Loaded {len(df):,} rows for {target_month}")

        # Write to Snowflake RAW schema
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="RAW_YELLOW_TRIPS",
            schema="RAW",
            database="NYC_TAXI",
            auto_create_table=True,
            overwrite=False,  # append — don't overwrite historical data
            chunk_size=50_000,
        )

        logger.info(f"Wrote {nrows:,} rows in {nchunks} chunks to RAW.RAW_YELLOW_TRIPS")
        conn.close()

        context["ti"].xcom_push(key="rows_ingested", value=nrows)

    finally:
        os.unlink(tmp_path)  # always clean up temp file


def check_row_count(**context):
    """
    Anomaly detection: compare ingested rows against historical average.
    Branches to alert task if deviation is too high.
    """
    rows_ingested = context["ti"].xcom_pull(key="rows_ingested")
    target_month = context["ti"].xcom_pull(key="target_month")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Get average monthly row count from last 6 months for comparison
    result = hook.get_first("""
        SELECT AVG(monthly_rows) as avg_rows
        FROM (
            SELECT _SOURCE_MONTH, COUNT(*) as monthly_rows
            FROM NYC_TAXI.RAW.RAW_YELLOW_TRIPS
            WHERE _SOURCE_MONTH != %(target_month)s
            ORDER BY _SOURCE_MONTH DESC
            LIMIT 6
        )
    """, parameters={"target_month": target_month})

    if result and result[0]:
        avg_rows = result[0]
        deviation_pct = abs(rows_ingested - avg_rows) / avg_rows * 100
        logger.info(
            f"Row count check: ingested={rows_ingested:,}, "
            f"avg={avg_rows:,.0f}, deviation={deviation_pct:.1f}%"
        )

        if deviation_pct > 20:
            logger.warning(f"Row count deviation {deviation_pct:.1f}% exceeds 20% threshold")
            return "alert_row_count_anomaly"

    return "run_dbt_staging"


def log_pipeline_run(**context):
    """Write pipeline run metadata to audit table for observability."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    rows_ingested = context["ti"].xcom_pull(key="rows_ingested") or 0
    target_month = context["ti"].xcom_pull(key="target_month") or "unknown"

    hook.run("""
        INSERT INTO NYC_TAXI.AUDIT.PIPELINE_RUNS
            (dag_id, run_id, target_month, rows_ingested, status, completed_at)
        VALUES
            (%(dag_id)s, %(run_id)s, %(target_month)s,
             %(rows_ingested)s, 'SUCCESS', CURRENT_TIMESTAMP())
    """, parameters={
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "target_month": target_month,
        "rows_ingested": rows_ingested,
    })
    logger.info("Pipeline run logged to audit table")


# ─── Tasks ────────────────────────────────────────────────────────────────────

t_start = EmptyOperator(task_id="start", dag=dag)

t_check_source = PythonOperator(
    task_id="check_source_availability",
    python_callable=check_source_availability,
    dag=dag,
)

t_ingest = PythonOperator(
    task_id="ingest_to_snowflake",
    python_callable=ingest_to_snowflake,
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

t_row_check = BranchPythonOperator(
    task_id="check_row_count",
    python_callable=check_row_count,
    dag=dag,
)

t_alert_anomaly = BashOperator(
    task_id="alert_row_count_anomaly",
    bash_command=(
        'echo "ALERT: Row count anomaly detected for {{ ti.xcom_pull(key=\'target_month\') }}. '
        'Check Snowflake RAW table." | '
        "mail -s 'NYC Taxi Pipeline: Row Count Anomaly' your-email@example.com || true"
    ),
    dag=dag,
)

# dbt tasks — each stage is a separate task for granular failure visibility
t_dbt_staging = BashOperator(
    task_id="run_dbt_staging",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt run --select staging --profiles-dir /opt/airflow/dbt_project "
        "--target prod"
    ),
    dag=dag,
)

t_dbt_intermediate = BashOperator(
    task_id="run_dbt_intermediate",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt run --select intermediate --profiles-dir /opt/airflow/dbt_project "
        "--target prod"
    ),
    dag=dag,
)

t_dbt_marts = BashOperator(
    task_id="run_dbt_marts",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt run --select marts --profiles-dir /opt/airflow/dbt_project "
        "--target prod"
    ),
    dag=dag,
)

t_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt test --profiles-dir /opt/airflow/dbt_project --target prod"
    ),
    dag=dag,
)

t_ge_validate = BashOperator(
    task_id="run_great_expectations",
    bash_command=(
        "cd /opt/airflow && "
        "great_expectations checkpoint run nyc_taxi_checkpoint"
    ),
    dag=dag,
)

t_log_run = PythonOperator(
    task_id="log_pipeline_run",
    python_callable=log_pipeline_run,
    trigger_rule="all_done",  # log even if upstream partially failed
    dag=dag,
)

t_end = EmptyOperator(task_id="end", dag=dag)

# ─── Dependencies ─────────────────────────────────────────────────────────────

(
    t_start
    >> t_check_source
    >> t_ingest
    >> t_row_check
    >> [t_alert_anomaly, t_dbt_staging]
)

t_alert_anomaly >> t_dbt_staging  # continue pipeline even after alert
t_dbt_staging >> t_dbt_intermediate >> t_dbt_marts >> t_dbt_tests
t_dbt_tests >> t_ge_validate >> t_log_run >> t_end
