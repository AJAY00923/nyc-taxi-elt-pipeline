{{
  config(
    materialized = 'view',
    tags = ['staging']
  )
}}

/*
  stg_yellow_trips.sql
  --------------------
  Cleans and standardizes raw yellow taxi records.

  Key decisions:
  - Filter out obvious bad rows (negative fares, zero-distance trips logged as paid)
  - Cast all timestamps to proper types
  - Rename columns to snake_case business-friendly names
  - No business logic here — that belongs in intermediate/marts
*/

with source as (

    select * from {{ source('raw', 'raw_yellow_trips') }}

),

cleaned as (

    select
        -- timestamps
        TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000)     as pickup_at,
        TO_TIMESTAMP(TPEP_DROPOFF_DATETIME / 1000000)    as dropoff_at,

        -- derived time fields (useful in downstream models)
        date(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000))              as pickup_date,
        hour(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000))              as pickup_hour,
        dayofweek(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000))         as pickup_dow,  -- 0=Sun, 6=Sat
        month(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000))             as pickup_month,
        year(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000))              as pickup_year,

        -- trip details
        PULOCATIONID::integer                   as pickup_location_id,
        DOLOCATIONID::integer                   as dropoff_location_id,
        coalesce(PASSENGER_COUNT, 1)::integer   as passenger_count,
        TRIP_DISTANCE::float                    as trip_distance_miles,

        -- calculate trip duration in minutes
        datediff(
            'minute',
            TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000),
            TO_TIMESTAMP(TPEP_DROPOFF_DATETIME / 1000000)
        )                                       as trip_duration_minutes,

        -- fares
        FARE_AMOUNT::float                      as fare_amount,
        TIP_AMOUNT::float                       as tip_amount,
        TOLLS_AMOUNT::float                     as tolls_amount,
        coalesce(CONGESTION_SURCHARGE, 0)::float as congestion_surcharge,
        TOTAL_AMOUNT::float                     as total_amount,

        -- payment type decoded
        case PAYMENT_TYPE::integer
            when 1 then 'credit_card'
            when 2 then 'cash'
            when 3 then 'no_charge'
            when 4 then 'dispute'
            when 5 then 'unknown'
            when 6 then 'voided_trip'
            else 'unknown'
        end                                     as payment_type,

        -- tip rate (only meaningful for credit card payments)
        case
            when PAYMENT_TYPE = 1 and FARE_AMOUNT > 0
            then round(TIP_AMOUNT / FARE_AMOUNT * 100, 2)
            else null
        end                                     as tip_rate_pct,

        -- metadata
        _SOURCE_MONTH                           as source_month,
        _INGESTED_AT                            as ingested_at

    from source

    where
        -- remove clearly bad records
        TPEP_PICKUP_DATETIME is not null
        and TPEP_DROPOFF_DATETIME is not null
        and TPEP_DROPOFF_DATETIME > TPEP_PICKUP_DATETIME   -- dropoff must be after pickup
        and TOTAL_AMOUNT > 0                               -- must have been charged something
        and TRIP_DISTANCE >= 0                             -- distance can't be negative
        and FARE_AMOUNT >= 0                               -- fare can't be negative
        -- filter out test/dummy records (very common in raw data)
        and year(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000)) between 2019 and 2026
        and PULOCATIONID between 1 and 265                 -- valid NYC TLC zone IDs
        and DOLOCATIONID between 1 and 265

)

select * from cleaned
