{{
  config(
    materialized = 'table',
    tags = ['marts'],
    post_hook = [
      "GRANT SELECT ON {{ this }} TO ROLE ANALYST",
      "ALTER TABLE {{ this }} CLUSTER BY (pickup_date)"
    ]
  )
}}

/*
  mart_daily_trip_summary.sql
  ---------------------------
  Daily aggregated trip metrics for dashboards and analyst queries.
  This is the primary mart — built for fast, cheap queries by analysts.

  Grain: one row per (pickup_date, time_of_day)
*/

with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

daily_summary as (

    select
        pickup_date,
        pickup_year,
        pickup_month,
        time_of_day,
        is_weekend,

        -- volume
        count(*)                                            as total_trips,
        sum(passenger_count)                                as total_passengers,

        -- distance
        round(sum(trip_distance_miles), 2)                  as total_miles,
        round(avg(trip_distance_miles), 2)                  as avg_trip_miles,
        round(median(trip_distance_miles), 2)               as median_trip_miles,

        -- duration
        round(avg(trip_duration_minutes), 1)                as avg_duration_mins,
        round(median(trip_duration_minutes), 1)             as median_duration_mins,

        -- revenue
        round(sum(total_amount), 2)                         as total_revenue,
        round(avg(total_amount), 2)                         as avg_fare,
        round(sum(tip_amount), 2)                           as total_tips,
        round(avg(tip_rate_pct), 2)                         as avg_tip_rate_pct,

        -- efficiency
        round(avg(fare_per_mile), 2)                        as avg_fare_per_mile,

        -- segment breakdowns
        count_if(is_airport_trip)                           as airport_trips,
        count_if(is_rush_hour)                              as rush_hour_trips,
        count_if(is_high_tip)                               as high_tip_trips,
        count_if(payment_type = 'credit_card')              as credit_card_trips,
        count_if(payment_type = 'cash')                     as cash_trips,

        -- rates
        round(
            count_if(is_airport_trip) / count(*) * 100, 2
        )                                                   as airport_trip_rate_pct,
        round(
            count_if(payment_type = 'credit_card') / count(*) * 100, 2
        )                                                   as credit_card_rate_pct

    from trips
    group by 1, 2, 3, 4, 5

)

select * from daily_summary
order by pickup_date, time_of_day
