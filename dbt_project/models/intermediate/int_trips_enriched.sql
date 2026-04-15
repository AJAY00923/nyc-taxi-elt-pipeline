{{
  config(
    materialized = 'incremental',
    unique_key = ['pickup_at', 'pickup_location_id', 'dropoff_location_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['intermediate']
  )
}}

/*
  int_trips_enriched.sql
  ----------------------
  Adds business logic and derived metrics on top of cleaned staging data.

  Key decisions:
  - Incremental model: only processes new months not already in the table
  - Adds time-of-day buckets, fare efficiency metrics, and trip categorization
  - This is the "wide" model downstream marts roll up from
*/

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

    {% if is_incremental() %}
        -- only process months not already loaded — avoids full reprocessing
        where source_month not in (
            select distinct source_month from {{ this }}
        )
    {% endif %}

),

enriched as (

    select
        -- all staging columns
        pickup_at,
        dropoff_at,
        pickup_date,
        pickup_hour,
        pickup_dow,
        pickup_month,
        pickup_year,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance_miles,
        trip_duration_minutes,
        fare_amount,
        tip_amount,
        tolls_amount,
        congestion_surcharge,
        airport_fee,
        total_amount,
        payment_type,
        tip_rate_pct,
        source_month,
        ingested_at,

        -- time-of-day bucket (useful for hourly analysis)
        case
            when pickup_hour between 6  and 9  then 'morning_rush'
            when pickup_hour between 10 and 15 then 'midday'
            when pickup_hour between 16 and 19 then 'evening_rush'
            when pickup_hour between 20 and 23 then 'night'
            else 'overnight'
        end                                             as time_of_day,

        -- weekend flag
        iff(pickup_dow in (0, 6), true, false)          as is_weekend,

        -- fare per mile (efficiency metric)
        case
            when trip_distance_miles > 0
            then round(fare_amount / trip_distance_miles, 2)
            else null
        end                                             as fare_per_mile,

        -- fare per minute
        case
            when trip_duration_minutes > 0
            then round(fare_amount / trip_duration_minutes, 2)
            else null
        end                                             as fare_per_minute,

        -- trip distance category
        case
            when trip_distance_miles < 1    then 'short'       -- under 1 mile
            when trip_distance_miles < 5    then 'medium'      -- 1–5 miles
            when trip_distance_miles < 15   then 'long'        -- 5–15 miles
            else 'very_long'                                   -- 15+ miles (likely airport)
        end                                             as trip_distance_category,

        -- likely airport trip flag (JFK=132, LGA=138, EWR=1)
        iff(
            dropoff_location_id in (132, 138, 1)
            or pickup_location_id in (132, 138, 1),
            true, false
        )                                               as is_airport_trip,

        -- rush hour flag
        iff(
            (pickup_hour between 7 and 9 or pickup_hour between 17 and 19)
            and pickup_dow between 1 and 5,
            true, false
        )                                               as is_rush_hour,

        -- high tip flag (tip > 20% of fare)
        iff(tip_rate_pct > 20, true, false)             as is_high_tip

    from trips

)

select * from enriched
