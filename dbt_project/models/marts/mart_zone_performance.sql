{{
  config(
    materialized = 'table',
    tags = ['marts'],
    post_hook = "GRANT SELECT ON {{ this }} TO ROLE ANALYST"
  )
}}

/*
  mart_zone_performance.sql
  -------------------------
  Monthly revenue and volume metrics per pickup location zone.
  Useful for: "which zones generate the most revenue?" type questions.

  Grain: one row per (source_month, pickup_location_id)
*/

with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

zone_stats as (

    select
        source_month,
        pickup_location_id,

        count(*)                                        as total_trips,
        round(sum(total_amount), 2)                     as total_revenue,
        round(avg(total_amount), 2)                     as avg_fare,
        round(sum(tip_amount), 2)                       as total_tips,
        round(avg(trip_distance_miles), 2)              as avg_trip_miles,
        round(avg(trip_duration_minutes), 1)            as avg_duration_mins,
        count_if(is_airport_trip)                       as airport_trips,
        count_if(is_rush_hour)                          as rush_hour_trips,
        count_if(payment_type = 'credit_card')          as credit_card_trips,

        -- % of total revenue this zone represents in the month
        round(
            sum(total_amount) / sum(sum(total_amount)) over (partition by source_month) * 100,
            3
        )                                               as revenue_share_pct,

        -- rank within month by revenue
        dense_rank() over (
            partition by source_month
            order by sum(total_amount) desc
        )                                               as revenue_rank

    from trips
    group by 1, 2

)

select * from zone_stats
