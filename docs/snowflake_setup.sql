-- ============================================================
-- NYC Taxi Pipeline — Snowflake Setup Script
-- Run this ONCE in your Snowflake account before starting
-- Free trial: https://signup.snowflake.com/
-- ============================================================

-- Step 1: create database and schemas
CREATE DATABASE IF NOT EXISTS NYC_TAXI;
USE DATABASE NYC_TAXI;

CREATE SCHEMA IF NOT EXISTS RAW;           -- raw ingested data
CREATE SCHEMA IF NOT EXISTS STAGING;       -- dbt staging views
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;  -- dbt intermediate tables
CREATE SCHEMA IF NOT EXISTS MARTS;         -- dbt mart tables (analyst-facing)
CREATE SCHEMA IF NOT EXISTS AUDIT;         -- pipeline run logs

-- Step 2: create warehouse (XS is free-tier friendly)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60          -- suspend after 60s idle — saves credits
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Step 3: create roles
CREATE ROLE IF NOT EXISTS TRANSFORMER;   -- used by Airflow + dbt
CREATE ROLE IF NOT EXISTS ANALYST;       -- read-only for dashboards

-- Step 4: grant permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT ALL ON DATABASE NYC_TAXI TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE NYC_TAXI TO ROLE ANALYST;
GRANT ALL ON ALL SCHEMAS IN DATABASE NYC_TAXI TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA NYC_TAXI.MARTS TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA NYC_TAXI.MARTS TO ROLE ANALYST;

-- Step 5: create service user for pipeline
CREATE USER IF NOT EXISTS PIPELINE_USER
    PASSWORD = 'change-this-password-123!'
    DEFAULT_ROLE = TRANSFORMER
    DEFAULT_WAREHOUSE = COMPUTE_WH
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE TRANSFORMER TO USER PIPELINE_USER;

-- Step 6: create audit table
CREATE TABLE IF NOT EXISTS NYC_TAXI.AUDIT.PIPELINE_RUNS (
    id              INTEGER AUTOINCREMENT PRIMARY KEY,
    dag_id          VARCHAR(200),
    run_id          VARCHAR(500),
    target_month    VARCHAR(7),     -- YYYY-MM
    rows_ingested   INTEGER,
    status          VARCHAR(50),    -- SUCCESS, FAILED, PARTIAL
    completed_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    notes           VARCHAR(2000)
);

-- Verify setup
SHOW SCHEMAS IN DATABASE NYC_TAXI;
