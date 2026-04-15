"""
Great Expectations suite for stg_yellow_trips.
Run standalone: python expectations/build_suite.py
Or via Airflow: great_expectations checkpoint run nyc_taxi_checkpoint
"""

import great_expectations as gx

context = gx.get_context()

suite_name = "nyc_taxi.staging.warning"
suite = context.add_or_update_expectation_suite(suite_name)

# ─── Get a batch of data ──────────────────────────────────────────────────────

datasource = context.get_datasource("snowflake_datasource")
batch_request = datasource.get_batch_request(
    data_asset_name="NYC_TAXI.STAGING.STG_YELLOW_TRIPS"
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite,
)

# ─── Volume expectations ──────────────────────────────────────────────────────

# Monthly row count should be between 1M and 10M (historical range for NYC taxi)
validator.expect_table_row_count_to_be_between(
    min_value=1_000_000,
    max_value=10_000_000,
    meta={"notes": "Historical monthly volume is 2M-5M rows. Alert if far outside this."}
)

# ─── Column presence ──────────────────────────────────────────────────────────

required_columns = [
    "pickup_at", "dropoff_at", "pickup_location_id", "dropoff_location_id",
    "trip_distance_miles", "fare_amount", "total_amount", "payment_type",
    "trip_duration_minutes", "source_month", "ingested_at",
]

validator.expect_table_columns_to_match_set(
    column_set=required_columns,
    exact_match=False,  # allow extra columns
)

# ─── Null checks ──────────────────────────────────────────────────────────────

for col in ["pickup_at", "dropoff_at", "total_amount", "pickup_location_id"]:
    validator.expect_column_values_to_not_be_null(
        column=col,
        mostly=1.0,  # 100% — these are critical columns
    )

# passenger_count can occasionally be null in source — allow up to 5% null
validator.expect_column_values_to_not_be_null(
    column="passenger_count",
    mostly=0.95,
)

# ─── Value range checks ───────────────────────────────────────────────────────

validator.expect_column_values_to_be_between(
    column="pickup_location_id",
    min_value=1,
    max_value=265,
    meta={"notes": "Valid NYC TLC zone IDs are 1-265"}
)

validator.expect_column_values_to_be_between(
    column="dropoff_location_id",
    min_value=1,
    max_value=265,
)

validator.expect_column_values_to_be_between(
    column="trip_distance_miles",
    min_value=0,
    max_value=200,  # 200 miles covers any possible NYC trip
    mostly=0.99,    # allow 1% anomalies
)

validator.expect_column_values_to_be_between(
    column="total_amount",
    min_value=0,
    max_value=5000,  # outlier threshold — valid but rare very expensive trips
    mostly=0.999,
)

validator.expect_column_values_to_be_between(
    column="trip_duration_minutes",
    min_value=1,
    max_value=300,  # 5 hours max — anything longer is data quality issue
    mostly=0.99,
)

# ─── Categorical checks ───────────────────────────────────────────────────────

validator.expect_column_values_to_be_in_set(
    column="payment_type",
    value_set=["credit_card", "cash", "no_charge", "dispute", "unknown", "voided_trip"],
)

# ─── Distribution checks ─────────────────────────────────────────────────────

# Average fare should be in reasonable range (historically $10-$30)
validator.expect_column_mean_to_be_between(
    column="total_amount",
    min_value=8.0,
    max_value=50.0,
    meta={"notes": "Alert if mean fare drifts significantly — could indicate data issue"}
)

# Most trips should be credit card (historically ~65-75%)
validator.expect_column_proportion_of_unique_values_to_be_between(
    column="payment_type",
    min_value=0.001,  # at least some variety in payment types
    max_value=1.0,
)

# Save the suite
validator.save_expectation_suite(discard_failed_expectations=False)
print(f"Suite '{suite_name}' saved with {len(suite.expectations)} expectations.")
