"""
ingestion/ingest_nyc_taxi.py
----------------------------
Standalone ingestion script — mirrors the Airflow task logic so you can
test ingestion locally without spinning up the full Airflow stack.

Usage:
    python ingestion/ingest_nyc_taxi.py --year 2024 --month 1

Requirements:
    pip install pandas pyarrow snowflake-connector-python requests
"""

import argparse
import logging
import os
import tempfile
import sys
from datetime import datetime

import pandas as pd
import requests
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

COLUMNS_NEEDED = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

# ─── Functions ────────────────────────────────────────────────────────────────

def build_url(year: int, month: int) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def check_source(url: str) -> bool:
    """Return True if the file exists at the given URL."""
    logger.info(f"Checking: {url}")
    response = requests.head(url, timeout=30)
    if response.status_code == 200:
        size_mb = int(response.headers.get("Content-Length", 0)) / 1_048_576
        logger.info(f"File found — {size_mb:.1f} MB")
        return True
    logger.error(f"File not found — HTTP {response.status_code}")
    return False


def download_parquet(url: str) -> str:
    """Stream download to a temp file. Returns path to temp file."""
    logger.info("Downloading...")
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        downloaded = 0
        for chunk in r.iter_content(chunk_size=65_536):
            tmp.write(chunk)
            downloaded += len(chunk)
            if downloaded % (50 * 1_048_576) == 0:  # log every 50MB
                logger.info(f"  Downloaded {downloaded / 1_048_576:.0f} MB...")
    tmp.close()
    logger.info(f"Download complete: {tmp.name}")
    return tmp.name


def load_to_snowflake(parquet_path: str, target_month: str) -> int:
    """Load parquet file to Snowflake RAW schema. Returns row count."""

    logger.info("Reading parquet into DataFrame...")
    df = pd.read_parquet(parquet_path, columns=COLUMNS_NEEDED)
    df["_source_month"] = target_month
    df["_ingested_at"] = datetime.utcnow()

    # Snowflake wants uppercase column names
    df.columns = [c.upper() for c in df.columns]

    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Sample row:\n{df.iloc[0]}")

    # Connect to Snowflake using env vars
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="NYC_TAXI",
        schema="RAW",
        warehouse="COMPUTE_WH",
        role="TRANSFORMER",
    )

    logger.info("Writing to Snowflake RAW.RAW_YELLOW_TRIPS...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="RAW_YELLOW_TRIPS",
        schema="RAW",
        database="NYC_TAXI",
        auto_create_table=True,
        overwrite=False,
        chunk_size=50_000,
    )

    conn.close()

    if success:
        logger.info(f"Success — wrote {nrows:,} rows in {nchunks} chunks")
    else:
        raise RuntimeError("write_pandas returned success=False")

    return nrows


def validate_basic(parquet_path: str) -> None:
    """Quick local validation before uploading to Snowflake."""
    df = pd.read_parquet(parquet_path, columns=["fare_amount", "total_amount", "trip_distance"])

    null_fare_pct = df["fare_amount"].isna().mean() * 100
    negative_fare = (df["fare_amount"] < 0).sum()
    negative_distance = (df["trip_distance"] < 0).sum()

    logger.info(f"Validation — null fare: {null_fare_pct:.2f}%")
    logger.info(f"Validation — negative fares: {negative_fare:,}")
    logger.info(f"Validation — negative distances: {negative_distance:,}")

    if null_fare_pct > 10:
        raise ValueError(f"Too many null fares: {null_fare_pct:.1f}% (threshold: 10%)")
    if negative_fare > 1000:
        raise ValueError(f"Too many negative fares: {negative_fare:,} (threshold: 1,000)")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest NYC taxi data to Snowflake")
    parser.add_argument("--year",  type=int, required=True, help="e.g. 2024")
    parser.add_argument("--month", type=int, required=True, help="e.g. 1 for January")
    parser.add_argument("--dry-run", action="store_true", help="Download and validate only — skip Snowflake load")
    args = parser.parse_args()

    target_month = f"{args.year}-{args.month:02d}"
    url = build_url(args.year, args.month)

    logger.info(f"=== NYC Taxi Ingestion — {target_month} ===")

    # 1. Check source
    if not check_source(url):
        sys.exit(1)

    # 2. Download
    parquet_path = download_parquet(url)

    try:
        # 3. Basic local validation
        validate_basic(parquet_path)

        if args.dry_run:
            logger.info("Dry run — skipping Snowflake load. Validation passed.")
            return

        # 4. Load to Snowflake
        nrows = load_to_snowflake(parquet_path, target_month)
        logger.info(f"=== Done — {nrows:,} rows loaded for {target_month} ===")

    finally:
        # Always clean up temp file
        import os
        try:
            os.unlink(parquet_path)
            logger.info(f"Cleaned up temp file: {parquet_path}")
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
